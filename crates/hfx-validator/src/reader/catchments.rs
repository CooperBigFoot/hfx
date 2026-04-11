//! Catchments Parquet reader.

use std::path::Path;

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};

use crate::dataset::CatchmentsData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::schema::{validate_schema, ExpectedColumn};

/// Expected schema for catchments.parquet.
fn expected_columns() -> Vec<ExpectedColumn> {
    vec![
        ExpectedColumn::new("id", DataType::Int64, false),
        ExpectedColumn::new("area_km2", DataType::Float32, false),
        ExpectedColumn::new("up_area_km2", DataType::Float32, true),
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
        // Find the column index by name.
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

/// Read `catchments.parquet` and return the extracted data plus any diagnostics.
///
/// Returns `(None, diagnostics)` on I/O or schema errors that prevent reading.
pub fn read_catchments(path: &Path) -> (Option<CatchmentsData>, Vec<Diagnostic>) {
    debug!(path = %path.display(), "reading catchments.parquet");

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot open catchments.parquet");
            return (
                None,
                vec![Diagnostic::error(
                    "catchments.read",
                    Category::Schema,
                    Artifact::Catchments,
                    format!("cannot open catchments.parquet: {err}"),
                )],
            );
        }
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(err) => {
            warn!(error = %err, "cannot read catchments.parquet as Parquet");
            return (
                None,
                vec![Diagnostic::error(
                    "catchments.parquet_open",
                    Category::Schema,
                    Artifact::Catchments,
                    format!("cannot read catchments.parquet as Parquet: {err}"),
                )],
            );
        }
    };

    // --- Schema validation ---
    let arrow_schema = builder.schema();
    let mut diags = validate_schema(arrow_schema, &expected_columns(), Artifact::Catchments);
    if diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error) {
        warn!("catchments.parquet schema has errors; skipping data extraction");
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
            warn!(error = %err, "cannot build catchments record batch reader");
            return (
                None,
                vec![Diagnostic::error(
                    "catchments.reader_build",
                    Category::Schema,
                    Artifact::Catchments,
                    format!("cannot build catchments record batch reader: {err}"),
                )],
            );
        }
    };

    let mut ids: Vec<i64> = Vec::new();
    let mut areas_km2: Vec<f32> = Vec::new();
    let mut bboxes: Vec<[f32; 4]> = Vec::new();
    // TODO: For large datasets, geometry should be read lazily or sampled during reading.
    // Currently all WKB bytes are loaded into memory even though the geometry checker only
    // samples ~1% of rows.  A future improvement would be to accept row indices from the
    // checker and re-read the parquet file for just those rows, avoiding the full load.
    let mut geometry_wkb: Vec<Vec<u8>> = Vec::new();
    let mut up_area_null_count: usize = 0;
    let mut up_area_total: usize = 0;
    let mut total_rows: usize = 0;

    for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(err) => {
                warn!(error = %err, "error reading catchments record batch");
                diags.push(Diagnostic::error(
                    "catchments.batch_read",
                    Category::Schema,
                    Artifact::Catchments,
                    format!("error reading record batch: {err}"),
                ));
                continue;
            }
        };

        let num_rows = batch.num_rows();

        // id column (non-nullable — check each row)
        let id_col = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        if let Some(arr) = id_col {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    diags.push(
                        Diagnostic::error(
                            "catchments.null_id",
                            Category::Schema,
                            Artifact::Catchments,
                            format!("row {}: id is null in a non-nullable column", total_rows + i),
                        )
                        .at(Location::Row { index: total_rows + i }),
                    );
                    ids.push(0); // sentinel to keep indices aligned
                } else {
                    ids.push(arr.value(i));
                }
            }
        }

        // area_km2 column (non-nullable)
        let area_col = batch
            .column_by_name("area_km2")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        if let Some(arr) = area_col {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    diags.push(
                        Diagnostic::error(
                            "catchments.null_area_km2",
                            Category::Schema,
                            Artifact::Catchments,
                            format!("row {}: area_km2 is null in a non-nullable column", total_rows + i),
                        )
                        .at(Location::Row { index: total_rows + i }),
                    );
                    areas_km2.push(0.0); // sentinel
                } else {
                    areas_km2.push(arr.value(i));
                }
            }
        }

        // up_area_km2 (nullable — existing null-counting logic is correct)
        up_area_total += num_rows;
        if let Some(up_col) = batch.column_by_name("up_area_km2") {
            up_area_null_count += up_col.null_count();
        } else {
            up_area_null_count += num_rows;
        }

        // bbox columns (all non-nullable)
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
                let bbox_null = minx.is_null(i) || miny.is_null(i) || maxx.is_null(i) || maxy.is_null(i);
                if bbox_null {
                    diags.push(
                        Diagnostic::error(
                            "catchments.null_bbox",
                            Category::Schema,
                            Artifact::Catchments,
                            format!("row {}: one or more bbox columns are null in a non-nullable column", total_rows + i),
                        )
                        .at(Location::Row { index: total_rows + i }),
                    );
                    bboxes.push([0.0, 0.0, 0.0, 0.0]); // sentinel
                } else {
                    bboxes.push([minx.value(i), miny.value(i), maxx.value(i), maxy.value(i)]);
                }
            }
        }

        // geometry column (non-nullable) — accept both Binary and LargeBinary
        let geom_col = batch.column_by_name("geometry");
        if let Some(col) = geom_col {
            if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_geometry",
                                Category::Schema,
                                Artifact::Catchments,
                                format!("row {}: geometry is null in a non-nullable column", total_rows + i),
                            )
                            .at(Location::Row { index: total_rows + i }),
                        );
                        geometry_wkb.push(Vec::new()); // sentinel
                    } else {
                        geometry_wkb.push(arr.value(i).to_vec());
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_geometry",
                                Category::Schema,
                                Artifact::Catchments,
                                format!("row {}: geometry is null in a non-nullable column", total_rows + i),
                            )
                            .at(Location::Row { index: total_rows + i }),
                        );
                        geometry_wkb.push(Vec::new()); // sentinel
                    } else {
                        geometry_wkb.push(arr.value(i).to_vec());
                    }
                }
            }
        }

        total_rows += num_rows;
    }

    let row_count = ids.len();
    debug!(row_count, "catchments.parquet read complete");

    (
        Some(CatchmentsData {
            row_count,
            ids,
            areas_km2,
            bboxes,
            up_area_null_count,
            up_area_total,
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

    use arrow::array::{BinaryArray, Float32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn make_valid_batch() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])),
                Arc::new(Float32Array::from(vec![1.0_f32, 2.0, 3.0])),
                Arc::new(Float32Array::from(vec![Some(10.0_f32), None, Some(30.0)])),
                Arc::new(Float32Array::from(vec![0.0_f32, 1.0, 2.0])),
                Arc::new(Float32Array::from(vec![0.0_f32, 1.0, 2.0])),
                Arc::new(Float32Array::from(vec![1.0_f32, 2.0, 3.0])),
                Arc::new(Float32Array::from(vec![1.0_f32, 2.0, 3.0])),
                Arc::new(BinaryArray::from_vec(vec![b"wkb1".as_ref(), b"wkb2", b"wkb3"])),
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
    fn valid_catchments_parquet_reads_correctly() {
        let (schema, batch) = make_valid_batch();
        let buf = write_parquet(schema, batch);

        // Write to temp file
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);
        let data = data.expect("should read successfully");
        assert_eq!(data.row_count, 3);
        assert_eq!(data.ids, vec![1, 2, 3]);
        assert_eq!(data.areas_km2, vec![1.0, 2.0, 3.0]);
        assert_eq!(data.bboxes.len(), 3);
        assert_eq!(data.bboxes[0], [0.0, 0.0, 1.0, 1.0]);
        assert_eq!(data.up_area_null_count, 1);
        assert_eq!(data.up_area_total, 3);
        assert_eq!(data.geometry_wkb.len(), 3);
        assert_eq!(data.geometry_wkb[0], b"wkb1");
        // No schema errors
        assert!(!diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error));
    }

    #[test]
    fn missing_file_returns_none_and_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");

        let (data, diags) = read_catchments(&path);
        assert!(data.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "catchments.read");
    }

    #[test]
    fn wrong_schema_returns_none_with_schema_error() {
        // id is Int32 instead of Int64 — schema error
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));
        use arrow::array::Int32Array;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1_i32])),
                Arc::new(Float32Array::from(vec![1.0_f32])),
                Arc::new(Float32Array::from(vec![Some(1.0_f32)])),
                Arc::new(Float32Array::from(vec![0.0_f32])),
                Arc::new(Float32Array::from(vec![0.0_f32])),
                Arc::new(Float32Array::from(vec![1.0_f32])),
                Arc::new(Float32Array::from(vec![1.0_f32])),
                Arc::new(BinaryArray::from_vec(vec![b"wkb".as_ref()])),
            ],
        )
        .unwrap();
        let buf = write_parquet(schema, batch);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);
        assert!(data.is_none());
        assert!(
            diags.iter().any(|d| d.check_id == "schema.wrong_type"),
            "expected schema.wrong_type diagnostic"
        );
    }

    #[test]
    fn missing_column_returns_none_with_error() {
        // Only id column — all others missing
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let buf = write_parquet(schema, batch);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);
        assert!(data.is_none());
        assert!(
            diags.iter().any(|d| d.check_id == "schema.missing_column"),
            "expected schema.missing_column diagnostic"
        );
    }
}
