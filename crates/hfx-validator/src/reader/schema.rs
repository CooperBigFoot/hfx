//! Generic Arrow schema validation helper.

use arrow::datatypes::{DataType, Field, Schema};
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
                if field.data_type() == &col.dtype {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::diagnostic::{Artifact, Severity};

    fn make_schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    #[test]
    fn valid_schema_no_diagnostics() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("area_km2", DataType::Float32, false),
        ]);
        let expected = vec![
            ExpectedColumn::new("id", DataType::Int64, false),
            ExpectedColumn::new("area_km2", DataType::Float32, false),
        ];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert!(diags.is_empty(), "expected no diagnostics: {diags:#?}");
    }

    #[test]
    fn missing_column_produces_error() {
        let schema = make_schema(vec![Field::new("id", DataType::Int64, false)]);
        let expected = vec![
            ExpectedColumn::new("id", DataType::Int64, false),
            ExpectedColumn::new("missing_col", DataType::Float32, false),
        ];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.missing_column");
        assert_eq!(diags[0].severity, Severity::Error);
    }

    #[test]
    fn wrong_type_produces_error() {
        let schema = make_schema(vec![Field::new("id", DataType::Int32, false)]);
        let expected = vec![ExpectedColumn::new("id", DataType::Int64, false)];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.wrong_type");
    }

    #[test]
    fn large_binary_instead_of_binary_produces_error() {
        let schema = make_schema(vec![Field::new("geometry", DataType::LargeBinary, false)]);
        let expected = vec![ExpectedColumn::new("geometry", DataType::Binary, false)];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(
            diags.len(),
            1,
            "expected exactly one diagnostic: {diags:#?}"
        );
        assert_eq!(diags[0].check_id, "schema.large_variant");
        assert_eq!(diags[0].severity, Severity::Error);
    }

    #[test]
    fn binary_instead_of_large_binary_produces_error() {
        let schema = make_schema(vec![Field::new("geometry", DataType::Binary, false)]);
        let expected = vec![ExpectedColumn::new(
            "geometry",
            DataType::LargeBinary,
            false,
        )];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(
            diags.len(),
            1,
            "expected exactly one diagnostic: {diags:#?}"
        );
        assert_eq!(diags[0].check_id, "schema.large_variant");
        assert_eq!(diags[0].severity, Severity::Error);
    }

    #[test]
    fn large_list_instead_of_list_produces_error() {
        let large_list_field =
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true)));
        let schema = make_schema(vec![Field::new("upstream_ids", large_list_field, false)]);
        let expected = vec![ExpectedColumn::new(
            "upstream_ids",
            list_int64_field(),
            false,
        )];
        let diags = validate_schema(&schema, &expected, Artifact::Graph);
        assert_eq!(
            diags.len(),
            1,
            "expected exactly one diagnostic: {diags:#?}"
        );
        assert_eq!(diags[0].check_id, "schema.large_variant");
        assert_eq!(diags[0].severity, Severity::Error);
    }

    #[test]
    fn non_nullable_column_declared_nullable_produces_error() {
        // expected: nullable=false, actual: nullable=true → error (producer may write nulls)
        let schema = make_schema(vec![Field::new("id", DataType::Int64, true)]);
        let expected = vec![ExpectedColumn::new("id", DataType::Int64, false)];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.wrong_nullability");
        assert_eq!(diags[0].severity, Severity::Error);
    }

    #[test]
    fn nullable_column_declared_non_nullable_produces_warning() {
        // expected: nullable=true, actual: nullable=false → warning (stricter than required)
        let schema = make_schema(vec![Field::new("up_area_km2", DataType::Float32, false)]);
        let expected = vec![ExpectedColumn::new("up_area_km2", DataType::Float32, true)];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.wrong_nullability");
        assert_eq!(diags[0].severity, Severity::Warning);
    }
}
