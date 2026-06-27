//! Generic Arrow schema validation helper.

use arrow::datatypes::{DataType, Field, Fields, Schema};
use tracing::debug;

use crate::diagnostic::{Artifact, Category, Diagnostic, Location};

/// Describes a single expected column in an Arrow schema.
pub struct ExpectedColumn {
    pub name: &'static str,
    pub dtype: DataType,
    pub nullable: bool,
}

impl ExpectedColumn {
    /// Create a new expected column descriptor.
    pub fn new(name: &'static str, dtype: DataType, nullable: bool) -> Self {
        Self {
            name,
            dtype,
            nullable,
        }
    }
}

/// Check whether `actual` is a "large" variant of `expected` (LargeBinary for Binary,
/// LargeList for List).  Returns `true` only when the pair is a known large-variant
/// relationship — exact matches are NOT considered here.
fn is_large_variant(actual: &DataType, expected: &DataType) -> bool {
    match (actual, expected) {
        // LargeBinary where Binary is expected.
        (DataType::LargeBinary, DataType::Binary) => true,
        // Binary where LargeBinary is expected (reversed — unexpected but still warn).
        (DataType::Binary, DataType::LargeBinary) => true,
        // LargeList<T> where List<T> is expected (item types must match exactly).
        (DataType::LargeList(a_field), DataType::List(e_field)) => {
            a_field.data_type() == e_field.data_type()
        }
        (DataType::List(a_field), DataType::LargeList(e_field)) => {
            a_field.data_type() == e_field.data_type()
        }
        _ => false,
    }
}

fn data_type_matches(actual: &DataType, expected: &DataType) -> bool {
    if actual == expected {
        return true;
    }
    match (actual, expected) {
        (DataType::List(actual_field), DataType::List(expected_field)) => {
            actual_field.data_type() == expected_field.data_type()
                && actual_field.is_nullable() == expected_field.is_nullable()
        }
        (DataType::Struct(actual_fields), DataType::Struct(expected_fields)) => {
            actual_fields.len() == expected_fields.len()
                && actual_fields
                    .iter()
                    .zip(expected_fields.iter())
                    .all(|(a, e)| a.name() == e.name() && a.data_type() == e.data_type())
        }
        _ => false,
    }
}

/// Validate an Arrow schema against expected columns.
///
/// For each expected column, checks that:
/// 1. The column exists in `actual`.
/// 2. The data type matches exactly.  When the type is a "large" Arrow variant
///    (e.g. [`DataType::LargeBinary`] instead of [`DataType::Binary`], or
///    [`DataType::LargeList`] instead of [`DataType::List`]) an **error** is
///    emitted, because the spec mandates specific on-disk types.
/// 3. Non-nullable columns that are marked nullable in the actual schema emit an
///    **error** (not a warning), because a producer that omits the non-null
///    constraint may silently produce null values that downstream readers cannot
///    handle safely.  The reverse (non-nullable where nullable is expected) is
///    only a warning.
///
/// Returns a [`Diagnostic`] for each mismatch.  An empty vec means the schema is valid.
pub fn validate_schema(
    actual: &Schema,
    expected: &[ExpectedColumn],
    artifact: Artifact,
) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    for col in expected {
        match actual.field_with_name(col.name) {
            Err(_) => {
                debug!(column = col.name, "missing required column");
                diags.push(
                    Diagnostic::error(
                        "schema.missing_column",
                        Category::Schema,
                        artifact,
                        format!("missing required column '{}'", col.name),
                    )
                    .at(Location::Column {
                        name: col.name.to_string(),
                    }),
                );
            }
            Ok(field) => {
                if data_type_matches(field.data_type(), &col.dtype) {
                    // Exact match — no type diagnostic.
                } else if is_large_variant(field.data_type(), &col.dtype) {
                    // Compatible "large" variant — warn rather than error.
                    debug!(
                        column = col.name,
                        actual = ?field.data_type(),
                        expected = ?col.dtype,
                        "column uses large Arrow variant; spec requires the standard type"
                    );
                    diags.push(
                        Diagnostic::error(
                            "schema.large_variant",
                            Category::Schema,
                            artifact,
                            format!(
                                "column '{}' has type {:?} but the spec requires {:?}; \
                                 large variants are non-conformant",
                                col.name,
                                field.data_type(),
                                col.dtype
                            ),
                        )
                        .at(Location::Column {
                            name: col.name.to_string(),
                        }),
                    );
                } else {
                    debug!(
                        column = col.name,
                        actual = ?field.data_type(),
                        expected = ?col.dtype,
                        "column type mismatch"
                    );
                    diags.push(
                        Diagnostic::error(
                            "schema.wrong_type",
                            Category::Schema,
                            artifact,
                            format!(
                                "column '{}' has type {:?}, expected {:?}",
                                col.name,
                                field.data_type(),
                                col.dtype
                            ),
                        )
                        .at(Location::Column {
                            name: col.name.to_string(),
                        }),
                    );
                }

                // Nullability check.
                if field.is_nullable() != col.nullable {
                    debug!(
                        column = col.name,
                        actual_nullable = field.is_nullable(),
                        expected_nullable = col.nullable,
                        "column nullability mismatch"
                    );
                    // A non-nullable column declared as nullable is an error: the producer
                    // may write nulls that readers cannot safely handle.
                    // The reverse (nullable expected, non-nullable actual) is only a warning.
                    let diag = if !col.nullable && field.is_nullable() {
                        Diagnostic::error(
                            "schema.wrong_nullability",
                            Category::Schema,
                            artifact,
                            format!(
                                "column '{}' is declared nullable but the spec requires it to be \
                                 non-nullable; null values in this column will cause read errors",
                                col.name,
                            ),
                        )
                    } else {
                        Diagnostic::warning(
                            "schema.wrong_nullability",
                            Category::Schema,
                            artifact,
                            format!(
                                "column '{}' nullable={}, expected nullable={}",
                                col.name,
                                field.is_nullable(),
                                col.nullable
                            ),
                        )
                    };
                    diags.push(diag.at(Location::Column {
                        name: col.name.to_string(),
                    }));
                }
            }
        }
    }

    diags
}

/// Build the list field type used for upstream_ids (List<Int64>).
pub fn list_int64_field() -> DataType {
    DataType::List(std::sync::Arc::new(Field::new(
        "item",
        DataType::Int64,
        true,
    )))
}

/// Check whether a row group has statistics for all four bbox columns.
pub(crate) fn row_group_has_bbox_stats(meta: &parquet::file::metadata::RowGroupMetaData) -> bool {
    let bbox_cols = ["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"];
    let schema_desc = meta.schema_descr();
    for col_name in &bbox_cols {
        let col_idx =
            (0..schema_desc.num_columns()).find(|&i| schema_desc.column(i).name() == *col_name);
        let Some(idx) = col_idx else {
            return false;
        };
        let col_meta = meta.column(idx);
        if col_meta.statistics().is_none() {
            return false;
        }
    }
    true
}

/// The catchments/snap `bbox` struct column type: four non-nullable `float32`
/// leaves `xmin`, `ymin`, `xmax`, `ymax`, in GeoParquet 1.1 covering order.
pub(crate) fn bbox_struct_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("xmin", DataType::Float32, false),
        Field::new("ymin", DataType::Float32, false),
        Field::new("xmax", DataType::Float32, false),
        Field::new("ymax", DataType::Float32, false),
    ]))
}

/// Check whether a row group has statistics on all four `bbox` struct leaves.
///
/// Struct leaves are matched on the FULL dotted column path
/// (`column(i).path().string() == "bbox.xmin"`), because the parquet
/// `ColumnDescriptor::name()` collapses to the bare leaf name (`"xmin"`).
pub(crate) fn row_group_has_struct_bbox_stats(
    meta: &parquet::file::metadata::RowGroupMetaData,
) -> bool {
    let leaf_paths = ["bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax"];
    let schema_desc = meta.schema_descr();
    for leaf_path in &leaf_paths {
        let col_idx = (0..schema_desc.num_columns())
            .find(|&i| schema_desc.column(i).path().string() == *leaf_path);
        let Some(idx) = col_idx else {
            return false;
        };
        if meta.column(idx).statistics().is_none() {
            return false;
        }
    }
    true
}

/// Validate the GeoParquet 1.1 `bbox` covering metadata for a struct-bbox file.
///
/// The geometry column is literally named `geometry`; the covering MUST be
/// declared at `geo.columns.geometry.covering.bbox.{xmin,ymin,xmax,ymax}`, each
/// entry referencing the matching leaf of the `bbox` struct as `["bbox","<leaf>"]`.
pub(crate) fn check_bbox_covering(
    kv: Option<&Vec<parquet::format::KeyValue>>,
    artifact: Artifact,
) -> Vec<Diagnostic> {
    let check_id = match artifact {
        Artifact::Catchments => "schema.catchments.covering_missing",
        Artifact::Snap => "schema.snap.covering_missing",
        _ => "schema.bbox_covering",
    };
    let err = |msg: String| vec![Diagnostic::error(check_id, Category::Schema, artifact, msg)];

    let Some(geo_raw) = kv
        .into_iter()
        .flatten()
        .find(|entry| entry.key == "geo")
        .and_then(|entry| entry.value.as_deref())
    else {
        return err(
            "missing GeoParquet 'geo' file metadata; the bbox covering must be declared at \
             geo.columns.geometry.covering.bbox"
                .to_string(),
        );
    };

    let geo: serde_json::Value = match serde_json::from_str(geo_raw) {
        Ok(value) => value,
        Err(parse_err) => {
            return err(format!(
                "GeoParquet 'geo' metadata is not valid JSON: {parse_err}"
            ));
        }
    };

    let bbox = &geo["columns"]["geometry"]["covering"]["bbox"];
    if !bbox.is_object() {
        return err(
            "GeoParquet covering is missing at geo.columns.geometry.covering.bbox".to_string(),
        );
    }

    let mut diags = Vec::new();
    for leaf in ["xmin", "ymin", "xmax", "ymax"] {
        let expected = serde_json::json!(["bbox", leaf]);
        if bbox[leaf] != expected {
            diags.push(Diagnostic::error(
                check_id,
                Category::Schema,
                artifact,
                format!("GeoParquet covering bbox.{leaf} must reference [\"bbox\", \"{leaf}\"]"),
            ));
        }
    }
    diags
}
