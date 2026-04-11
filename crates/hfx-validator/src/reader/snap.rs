//! Snap Parquet reader.

use std::path::Path;

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};

use crate::dataset::SnapData;
use crate::diagnostic::{Artifact, Category, Diagnostic};
use crate::reader::schema::{validate_schema, ExpectedColumn};

/// Expected schema for snap.parquet.
fn expected_columns() -> Vec<ExpectedColumn> {
    vec![
        ExpectedColumn::new("id", DataType::Int64, false),
        ExpectedColumn::new("catchment_id", DataType::Int64, false),
        ExpectedColumn::new("weight", DataType::Float32, false),
        ExpectedColumn::new("is_mainstem", DataType::Boolean, false),
        ExpectedColumn::new("bbox_minx", DataType::Float32, false),
        ExpectedColumn::new("bbox_miny", DataType::Float32, false),
        ExpectedColumn::new("bbox_maxx", DataType::Float32, false),
        ExpectedColumn::new("bbox_maxy", DataType::Float32, false),
        ExpectedColumn::new("geometry", DataType::Binary, false),
    ]
}

/// Check whether a row-group has statistics for all four bbox columns.
fn row_group_has_bbox_stats(meta: &parquet::file::metadata::RowGroupMetaData) -> bool {
    let bbox_cols = ["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"];
    let schema_desc = meta.schema_descr();
    for col_name in &bbox_cols {
        let col_idx = (0..schema_desc.num_columns()).find(|&i| {
            schema_desc.column(i).name() == *col_name
        });
        let Some(idx) = col_idx else { return false; };
        let col_meta = meta.column(idx);
        if col_meta.statistics().is_none() {
            return false;
        }
    }
    true
}

/// Read `snap.parquet` and return the extracted data plus any diagnostics.
///
/// Returns `(None, diagnostics)` on I/O or schema errors that prevent reading.
pub fn read_snap(path: &Path) -> (Option<SnapData>, Vec<Diagnostic>) {
    debug!(path = %path.display(), "reading snap.parquet");

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot open snap.parquet");
            return (
                None,
                vec![Diagnostic::error(
                    "snap.read",
                    Category::Schema,
                    Artifact::Snap,
                    format!("cannot open snap.parquet: {err}"),
                )],
            );
        }
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(err) => {
            warn!(error = %err, "cannot read snap.parquet as Parquet");
            return (
                None,
                vec![Diagnostic::error(
                    "snap.parquet_open",
                    Category::Schema,
                    Artifact::Snap,
                    format!("cannot read snap.parquet as Parquet: {err}"),
                )],
            );
        }
    };

    // --- Schema validation ---
    let arrow_schema = builder.schema();
    let mut diags = validate_schema(arrow_schema, &expected_columns(), Artifact::Snap);
    if diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error) {
        warn!("snap.parquet schema has errors; skipping data extraction");
        return (None, diags);
    }

    // --- Row group metadata ---
    let parquet_meta = builder.metadata().clone();
    let num_row_groups = parquet_meta.num_row_groups();
    let mut row_group_sizes: Vec<usize> = Vec::with_capacity(num_row_groups);
    let mut row_group_has_bbox_stats_vec: Vec<bool> = Vec::with_capacity(num_row_groups);

    for rg_idx in 0..num_row_groups {
        let rg = parquet_meta.row_group(rg_idx);
        row_group_sizes.push(rg.num_rows() as usize);
        row_group_has_bbox_stats_vec.push(row_group_has_bbox_stats(rg));
    }

    // --- Stream record batches ---
    let reader = match builder.with_batch_size(8192).build() {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, "cannot build snap record batch reader");
            return (
                None,
                vec![Diagnostic::error(
                    "snap.reader_build",
                    Category::Schema,
                    Artifact::Snap,
                    format!("cannot build snap record batch reader: {err}"),
                )],
            );
        }
    };

    let mut ids: Vec<i64> = Vec::new();
    let mut catchment_ids: Vec<i64> = Vec::new();
    let mut weights: Vec<f32> = Vec::new();
    let mut bboxes: Vec<[f32; 4]> = Vec::new();
    let mut geometry_wkb: Vec<Vec<u8>> = Vec::new();

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(err) => {
                warn!(error = %err, "error reading snap record batch");
                diags.push(Diagnostic::error(
                    "snap.batch_read",
                    Category::Schema,
                    Artifact::Snap,
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

        // catchment_id column
        if let Some(col) = batch.column_by_name("catchment_id") {
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                catchment_ids.extend(arr.values().iter().copied());
            }
        }

        // weight column
        if let Some(col) = batch.column_by_name("weight") {
            if let Some(arr) = col.as_any().downcast_ref::<Float32Array>() {
                weights.extend(arr.values().iter().copied());
            }
        }

        // bbox columns
        let minx = batch
            .column_by_name("bbox_minx")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let miny = batch
            .column_by_name("bbox_miny")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let maxx = batch
            .column_by_name("bbox_maxx")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        let maxy = batch
            .column_by_name("bbox_maxy")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());

        if let (Some(minx), Some(miny), Some(maxx), Some(maxy)) = (minx, miny, maxx, maxy) {
            for i in 0..num_rows {
                bboxes.push([minx.value(i), miny.value(i), maxx.value(i), maxy.value(i)]);
            }
        }

        // geometry column — accept both Binary and LargeBinary
        if let Some(col) = batch.column_by_name("geometry") {
            if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
                for i in 0..num_rows {
                    geometry_wkb.push(arr.value(i).to_vec());
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
                for i in 0..num_rows {
                    geometry_wkb.push(arr.value(i).to_vec());
                }
            }
        }
    }

    let row_count = ids.len();
    debug!(row_count, "snap.parquet read complete");

    (
        Some(SnapData {
            row_count,
            ids,
            catchment_ids,
            weights,
            bboxes,
            geometry_wkb,
            row_group_sizes,
            row_group_has_bbox_stats: row_group_has_bbox_stats_vec,
        }),
        diags,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryArray, BooleanArray, Float32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn make_valid_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("catchment_id", DataType::Int64, false),
            Field::new("weight", DataType::Float32, false),
            Field::new("is_mainstem", DataType::Boolean, false),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Float32Array::from(vec![0.5_f32, 1.0])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Float32Array::from(vec![0.0_f32, 1.0])),
                Arc::new(Float32Array::from(vec![0.0_f32, 1.0])),
                Arc::new(Float32Array::from(vec![1.0_f32, 2.0])),
                Arc::new(Float32Array::from(vec![1.0_f32, 2.0])),
                Arc::new(BinaryArray::from_vec(vec![b"wkb1".as_ref(), b"wkb2"])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn write_parquet(schema: Arc<Schema>, batch: RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    #[test]
    fn valid_snap_parquet_reads_correctly() {
        let (schema, batch) = make_valid_batch();
        let buf = write_parquet(schema, batch);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_snap(&path);
        let data = data.expect("should read successfully");
        assert_eq!(data.row_count, 2);
        assert_eq!(data.ids, vec![10, 20]);
        assert_eq!(data.catchment_ids, vec![1, 2]);
        assert_eq!(data.weights, vec![0.5, 1.0]);
        assert_eq!(data.bboxes[0], [0.0, 0.0, 1.0, 1.0]);
        assert_eq!(data.geometry_wkb[0], b"wkb1");
        assert!(!diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error));
    }

    #[test]
    fn missing_file_returns_none_and_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.parquet");

        let (data, diags) = read_snap(&path);
        assert!(data.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "snap.read");
    }

    #[test]
    fn missing_column_returns_none_with_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let buf = write_parquet(schema, batch);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_snap(&path);
        assert!(data.is_none());
        assert!(diags.iter().any(|d| d.check_id == "schema.missing_column"));
    }
}
