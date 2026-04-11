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
        Self { name, dtype, nullable }
    }
}

/// Returns `true` if `actual` is compatible with `expected`.
///
/// Geometry columns accept both [`DataType::Binary`] and [`DataType::LargeBinary`].
/// List columns accept both [`DataType::List`] and [`DataType::LargeList`] with
/// compatible item types.
fn types_compatible(actual: &DataType, expected: &DataType) -> bool {
    // Exact match.
    if actual == expected {
        return true;
    }

    // Binary / LargeBinary interchangeable for geometry.
    match (actual, expected) {
        (DataType::Binary, DataType::LargeBinary)
        | (DataType::LargeBinary, DataType::Binary) => return true,
        _ => {}
    }

    // List / LargeList with compatible item types.
    let actual_item = match actual {
        DataType::List(f) => Some(f.data_type()),
        DataType::LargeList(f) => Some(f.data_type()),
        _ => None,
    };
    let expected_item = match expected {
        DataType::List(f) => Some(f.data_type()),
        DataType::LargeList(f) => Some(f.data_type()),
        _ => None,
    };
    if let (Some(ai), Some(ei)) = (actual_item, expected_item) {
        return types_compatible(ai, ei);
    }

    false
}

/// Validate an Arrow schema against expected columns.
///
/// For each expected column, checks that:
/// 1. The column exists in `actual`.
/// 2. The data type is compatible (with relaxed rules for Binary/LargeBinary and List/LargeList).
/// 3. The nullability matches.
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
                    .at(Location::Column { name: col.name.to_string() }),
                );
            }
            Ok(field) => {
                if !types_compatible(field.data_type(), &col.dtype) {
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
                        .at(Location::Column { name: col.name.to_string() }),
                    );
                }

                if field.is_nullable() != col.nullable {
                    debug!(
                        column = col.name,
                        actual_nullable = field.is_nullable(),
                        expected_nullable = col.nullable,
                        "column nullability mismatch"
                    );
                    diags.push(
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
                        .at(Location::Column { name: col.name.to_string() }),
                    );
                }
            }
        }
    }

    diags
}

/// Build the list field type used for upstream_ids (List<Int64>).
pub fn list_int64_field() -> DataType {
    DataType::List(std::sync::Arc::new(Field::new("item", DataType::Int64, true)))
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
    fn binary_and_large_binary_are_compatible() {
        // schema has LargeBinary, expected is Binary → compatible
        let schema = make_schema(vec![Field::new("geometry", DataType::LargeBinary, false)]);
        let expected = vec![ExpectedColumn::new("geometry", DataType::Binary, false)];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert!(diags.is_empty());

        // schema has Binary, expected is LargeBinary → compatible
        let schema2 = make_schema(vec![Field::new("geometry", DataType::Binary, false)]);
        let expected2 = vec![ExpectedColumn::new("geometry", DataType::LargeBinary, false)];
        let diags2 = validate_schema(&schema2, &expected2, Artifact::Catchments);
        assert!(diags2.is_empty());
    }

    #[test]
    fn list_and_large_list_int64_are_compatible() {
        let large_list_field = DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true)));
        let schema = make_schema(vec![Field::new("upstream_ids", large_list_field, false)]);
        let expected = vec![ExpectedColumn::new("upstream_ids", list_int64_field(), false)];
        let diags = validate_schema(&schema, &expected, Artifact::Graph);
        assert!(diags.is_empty());
    }

    #[test]
    fn nullability_mismatch_produces_warning() {
        // expected: nullable=false, actual: nullable=true
        let schema = make_schema(vec![Field::new("id", DataType::Int64, true)]);
        let expected = vec![ExpectedColumn::new("id", DataType::Int64, false)];
        let diags = validate_schema(&schema, &expected, Artifact::Catchments);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.wrong_nullability");
        assert_eq!(diags[0].severity, Severity::Warning);
    }
}
