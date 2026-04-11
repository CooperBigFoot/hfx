//! Graph Arrow IPC reader.

use std::path::Path;

use arrow::array::{Array, Int64Array, ListArray};
use arrow::datatypes::DataType;
use arrow::ipc::reader::FileReader;
use tracing::{debug, warn};

use crate::dataset::GraphData;
use crate::diagnostic::{Artifact, Category, Diagnostic};
use crate::reader::schema::{list_int64_field, validate_schema, ExpectedColumn};

/// Expected schema for graph.arrow.
fn expected_columns() -> Vec<ExpectedColumn> {
    vec![
        ExpectedColumn::new("id", DataType::Int64, false),
        ExpectedColumn::new("upstream_ids", list_int64_field(), false),
    ]
}

/// Read `graph.arrow` (Arrow IPC file format) and return the extracted data plus diagnostics.
///
/// Returns `(None, diagnostics)` on I/O or schema errors that prevent reading.
pub fn read_graph(path: &Path) -> (Option<GraphData>, Vec<Diagnostic>) {
    debug!(path = %path.display(), "reading graph.arrow");

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot open graph.arrow");
            return (
                None,
                vec![Diagnostic::error(
                    "graph.read",
                    Category::Schema,
                    Artifact::Graph,
                    format!("cannot open graph.arrow: {err}"),
                )],
            );
        }
    };

    let reader = match FileReader::try_new(file, None) {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, "cannot open graph.arrow as Arrow IPC");
            return (
                None,
                vec![Diagnostic::error(
                    "graph.ipc_open",
                    Category::Schema,
                    Artifact::Graph,
                    format!("cannot open graph.arrow as Arrow IPC: {err}"),
                )],
            );
        }
    };

    // --- Schema validation ---
    let arrow_schema = reader.schema();
    let mut diags = validate_schema(&arrow_schema, &expected_columns(), Artifact::Graph);
    if diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error) {
        warn!("graph.arrow schema has errors; skipping data extraction");
        return (None, diags);
    }

    // --- Stream record batches ---
    let mut ids: Vec<i64> = Vec::new();
    let mut upstream_ids: Vec<Vec<i64>> = Vec::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(err) => {
                warn!(error = %err, "error reading graph record batch");
                diags.push(Diagnostic::error(
                    "graph.batch_read",
                    Category::Schema,
                    Artifact::Graph,
                    format!("error reading record batch: {err}"),
                ));
                continue;
            }
        };

        let num_rows = batch.num_rows();

        // id column
        if let Some(col) = batch.column_by_name("id") {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                ids.extend(arr.values().iter().copied());
            }
        }

        // upstream_ids column — List<Int64> or LargeList<Int64>
        if let Some(col) = batch.column_by_name("upstream_ids") {
            if let Some(list_arr) = col.as_any().downcast_ref::<ListArray>() {
                for i in 0..num_rows {
                    let values = list_arr.value(i);
                    let int_arr = values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.values().iter().copied().collect::<Vec<_>>())
                        .unwrap_or_default();
                    upstream_ids.push(int_arr);
                }
            } else if let Some(large_arr) = col.as_any().downcast_ref::<arrow::array::LargeListArray>() {
                for i in 0..num_rows {
                    let values = large_arr.value(i);
                    let int_arr = values
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .map(|a| a.values().iter().copied().collect::<Vec<_>>())
                        .unwrap_or_default();
                    upstream_ids.push(int_arr);
                }
            } else {
                // Unexpected type — push empty vecs to keep lengths aligned
                for _ in 0..num_rows {
                    upstream_ids.push(Vec::new());
                }
            }
        }
    }

    let row_count = ids.len();
    debug!(row_count, "graph.arrow read complete");

    (Some(GraphData { ids, upstream_ids }), diags)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, ListArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::FileWriter;
    use arrow::record_batch::RecordBatch;

    use super::*;

    /// Build a ListArray<Int64> from a vec of vecs.
    fn make_list_int64(rows: &[Vec<i64>]) -> ListArray {
        let values: Vec<i64> = rows.iter().flat_map(|r| r.iter().copied()).collect();
        let values_array = Arc::new(Int64Array::from(values));

        // Build offsets: 0, len(row0), len(row0)+len(row1), ...
        let mut offsets: Vec<i32> = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for row in rows {
            offsets.push(*offsets.last().unwrap() + row.len() as i32);
        }

        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        ListArray::new(field, offsets, values_array, None)
    }

    fn write_arrow_ipc(schema: Arc<Schema>, batch: RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = FileWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    #[test]
    fn valid_graph_arrow_reads_correctly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "upstream_ids",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
        ]));

        let upstream = make_list_int64(&[vec![2, 3], vec![], vec![4]]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(upstream),
            ],
        )
        .unwrap();

        let buf = write_arrow_ipc(schema, batch);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_graph(&path);
        let data = data.expect("should read successfully");
        assert_eq!(data.ids, vec![1, 2, 3]);
        assert_eq!(data.upstream_ids[0], vec![2, 3]);
        assert_eq!(data.upstream_ids[1], vec![] as Vec<i64>);
        assert_eq!(data.upstream_ids[2], vec![4]);
        assert!(!diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error));
    }

    #[test]
    fn missing_file_returns_none_and_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");

        let (data, diags) = read_graph(&path);
        assert!(data.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "graph.read");
    }

    #[test]
    fn wrong_schema_returns_none_with_error() {
        // upstream_ids is Float32 — wrong type
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("upstream_ids", DataType::Float32, false),
        ]));
        use arrow::array::Float32Array;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Float32Array::from(vec![1.0_f32])),
            ],
        )
        .unwrap();

        let buf = write_arrow_ipc(schema, batch);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_graph(&path);
        assert!(data.is_none());
        assert!(diags.iter().any(|d| d.check_id == "schema.wrong_type"));
    }

    #[test]
    fn missing_column_returns_none_with_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();

        let buf = write_arrow_ipc(schema, batch);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.arrow");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_graph(&path);
        assert!(data.is_none());
        assert!(diags.iter().any(|d| d.check_id == "schema.missing_column"));
    }
}
