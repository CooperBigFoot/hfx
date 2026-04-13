//! Catchments Parquet reader.

use std::path::Path;

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};

use crate::dataset::CatchmentsData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::schema::{validate_schema, ExpectedColumn};
use super::{MAX_NULL_DIAGNOSTICS_PER_COLUMN, MAX_CONSECUTIVE_BATCH_FAILURES, MAX_TOTAL_BATCH_FAILURES};

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

    // Per-column null counters (used to cap per-row diagnostics).
    let mut null_id_count: usize = 0;
    let mut null_area_count: usize = 0;
    let mut null_bbox_count: usize = 0;
    let mut null_geom_count: usize = 0;

    let mut consecutive_batch_failures: usize = 0;
    let mut total_batch_failures: usize = 0;
    let mut batch_read_aborted = false;

    for batch_result in reader {
        if consecutive_batch_failures >= MAX_CONSECUTIVE_BATCH_FAILURES
            || total_batch_failures >= MAX_TOTAL_BATCH_FAILURES
        {
            batch_read_aborted = true;
            break;
        }

        let batch = match batch_result {
            Ok(b) => {
                consecutive_batch_failures = 0;
                b
            }
            Err(err) => {
                warn!(error = %err, "error reading catchments record batch");
                consecutive_batch_failures += 1;
                total_batch_failures += 1;
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
                    null_id_count += 1;
                    if null_id_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_id",
                                Category::Schema,
                                Artifact::Catchments,
                                format!("row {}: id is null in a non-nullable column", total_rows + i),
                            )
                            .at(Location::Row { index: total_rows + i }),
                        );
                    }
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
                    null_area_count += 1;
                    if null_area_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_area_km2",
                                Category::Schema,
                                Artifact::Catchments,
                                format!("row {}: area_km2 is null in a non-nullable column", total_rows + i),
                            )
                            .at(Location::Row { index: total_rows + i }),
                        );
                    }
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
                    null_bbox_count += 1;
                    if null_bbox_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_bbox",
                                Category::Schema,
                                Artifact::Catchments,
                                format!("row {}: one or more bbox columns are null in a non-nullable column", total_rows + i),
                            )
                            .at(Location::Row { index: total_rows + i }),
                        );
                    }
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
                        null_geom_count += 1;
                        if null_geom_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                            diags.push(
                                Diagnostic::error(
                                    "catchments.null_geometry",
                                    Category::Schema,
                                    Artifact::Catchments,
                                    format!("row {}: geometry is null in a non-nullable column", total_rows + i),
                                )
                                .at(Location::Row { index: total_rows + i }),
                            );
                        }
                        geometry_wkb.push(Vec::new()); // sentinel
                    } else {
                        geometry_wkb.push(arr.value(i).to_vec());
                    }
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        null_geom_count += 1;
                        if null_geom_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                            diags.push(
                                Diagnostic::error(
                                    "catchments.null_geometry",
                                    Category::Schema,
                                    Artifact::Catchments,
                                    format!("row {}: geometry is null in a non-nullable column", total_rows + i),
                                )
                                .at(Location::Row { index: total_rows + i }),
                            );
                        }
                        geometry_wkb.push(Vec::new()); // sentinel
                    } else {
                        geometry_wkb.push(arr.value(i).to_vec());
                    }
                }
            }
        }

        total_rows += num_rows;
    }

    // Emit abort summary if we broke out early OR if the iterator exhausted
    // right after hitting the cap (so the break never fired).
    if batch_read_aborted
        || consecutive_batch_failures >= MAX_CONSECUTIVE_BATCH_FAILURES
        || total_batch_failures >= MAX_TOTAL_BATCH_FAILURES
    {
        batch_read_aborted = true;
        diags.push(Diagnostic::error(
            "catchments.batch_read_aborted",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "aborting read after batch failures ({} consecutive, {} total); \
                 file may be unreadable (unsupported codec or corruption)",
                consecutive_batch_failures, total_batch_failures
            ),
        ));
    }

    if batch_read_aborted {
        return (None, diags);
    }

    // Emit summary diagnostics for columns that exceeded the per-row cap.
    if null_id_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_id_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "catchments.null_id",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "{suppressed} additional null violation(s) in 'id' column suppressed ({null_id_count} total)"
            ),
        ));
    }
    if null_area_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_area_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "catchments.null_area_km2",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "{suppressed} additional null violation(s) in 'area_km2' column suppressed ({null_area_count} total)"
            ),
        ));
    }
    if null_bbox_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_bbox_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "catchments.null_bbox",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "{suppressed} additional null violation(s) in 'bbox' column suppressed ({null_bbox_count} total)"
            ),
        ));
    }
    if null_geom_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_geom_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "catchments.null_geometry",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "{suppressed} additional null violation(s) in 'geometry' column suppressed ({null_geom_count} total)"
            ),
        ));
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
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

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

    #[test]
    fn zstd_compressed_parquet_reads_correctly() {
        let (schema, batch) = make_valid_batch();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);
        let data = data.expect("zstd-compressed parquet should read successfully");
        assert_eq!(data.row_count, 3);
        assert!(!diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error));
    }

    #[test]
    fn snappy_compressed_parquet_reads_correctly() {
        let (schema, batch) = make_valid_batch();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);
        let data = data.expect("snappy-compressed parquet should read successfully");
        assert_eq!(data.row_count, 3);
        assert!(!diags.iter().any(|d| d.severity == crate::diagnostic::Severity::Error));
    }

    #[test]
    fn nullable_id_column_triggers_schema_error_not_null_capping() {
        // When a catchments.parquet file declares the `id` column as nullable
        // (violating the spec which requires non-nullable), the reader must:
        //   1. Detect the nullability mismatch via schema validation.
        //   2. Return (None, diags) — the schema error causes an early exit before
        //      any row-level null scanning occurs.
        //   3. Emit exactly one schema.wrong_nullability error diagnostic.
        //
        // Implementation note: Parquet's type system maps nullable=true to OPTIONAL
        // column repetition, which means definition levels are written and real null
        // values CAN appear in the data.  However, the reader's schema validation
        // catches the nullability violation before reaching the per-row null scan,
        // so the null-capping counter (MAX_NULL_DIAGNOSTICS_PER_COLUMN) is never
        // exercised in this path.  The null-capping code guards against a theoretical
        // scenario where Arrow arrays with null bits are produced despite the file
        // schema claiming non-nullable.
        let num_rows = 30_usize;

        // Schema with nullable=true for id — this makes Parquet write OPTIONAL columns
        // so the data pages contain real null definition levels.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),   // nullable: violates spec
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let id_arr = Int64Array::from_iter((0..num_rows).map(|_| None::<i64>));
        let area_arr = Float32Array::from(vec![1.0_f32; num_rows]);
        let up_area_arr: Float32Array =
            Float32Array::from_iter((0..num_rows).map(|_| None::<f32>));
        let minx_arr = Float32Array::from(vec![-1.0_f32; num_rows]);
        let miny_arr = Float32Array::from(vec![-1.0_f32; num_rows]);
        let maxx_arr = Float32Array::from(vec![1.0_f32; num_rows]);
        let maxy_arr = Float32Array::from(vec![1.0_f32; num_rows]);
        let wkb: Vec<u8> = b"wkb".to_vec();
        let geom_data: Vec<&[u8]> = vec![wkb.as_slice(); num_rows];
        let geom_arr = BinaryArray::from_iter_values(geom_data);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_arr),
                Arc::new(area_arr),
                Arc::new(up_area_arr),
                Arc::new(minx_arr),
                Arc::new(miny_arr),
                Arc::new(maxx_arr),
                Arc::new(maxy_arr),
                Arc::new(geom_arr),
            ],
        )
        .unwrap();

        let buf = write_parquet(schema, batch);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);

        // Schema validation must catch the nullable=true id column and return None.
        assert!(
            data.is_none(),
            "reader should reject a file where a non-nullable column is declared nullable"
        );

        // The wrong_nullability error must be present.
        assert!(
            diags.iter().any(|d| d.check_id == "schema.wrong_nullability"),
            "expected schema.wrong_nullability diagnostic for nullable id column"
        );

        // No per-row null diagnostics should be emitted — the reader exits before
        // the row-level null scan.
        assert!(
            !diags.iter().any(|d| d.check_id == "catchments.null_id"),
            "no per-row null diagnostics should appear before schema validation completes"
        );
    }

    #[test]
    fn null_id_capping_is_unreachable_after_schema_validation() {
        // This test verifies the null-diagnostic capping behaviour described by the
        // MAX_NULL_DIAGNOSTICS_PER_COLUMN constant.
        //
        // Background: in Parquet, REQUIRED column repetition means no definition levels
        // are stored, so null information cannot survive the Parquet write → read
        // round-trip.  A file with a REQUIRED id column therefore always presents
        // non-null values to the reader.  The capping code guards against a scenario
        // where a malformed producer writes OPTIONAL columns while claiming non-nullable
        // in the Arrow schema metadata — but the Arrow Parquet reader always derives
        // nullability from the column repetition, not from the embedded schema hint,
        // so that scenario also cannot produce null arrays through normal reading.
        //
        // Because of this, we test the capping indirectly: we write a file with
        // nullable=true for id (OPTIONAL Parquet), which DOES produce real null arrays,
        // but the schema validator detects the nullability violation and exits before
        // the per-row scan.  The test confirms that:
        //   - data is None (schema error causes early exit)
        //   - no "catchments.null_id" diagnostics are emitted (capping never fires)
        //   - the schema.wrong_nullability error is present
        //
        // If the architecture is ever changed so that null arrays reach the row scanner
        // even for non-nullable columns (e.g., by relaxing the schema validation for
        // this specific error), the assertions below should be updated to match the
        // expected cap of MAX_NULL_DIAGNOSTICS_PER_COLUMN + 1.
        let num_rows = 30_usize;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),   // nullable=true → OPTIONAL Parquet
            Field::new("area_km2", DataType::Float32, false),
            Field::new("up_area_km2", DataType::Float32, true),
            Field::new("bbox_minx", DataType::Float32, false),
            Field::new("bbox_miny", DataType::Float32, false),
            Field::new("bbox_maxx", DataType::Float32, false),
            Field::new("bbox_maxy", DataType::Float32, false),
            Field::new("geometry", DataType::Binary, false),
        ]));

        let id_arr = Int64Array::from_iter((0..num_rows).map(|_| None::<i64>));
        let area_arr = Float32Array::from(vec![1.0_f32; num_rows]);
        let up_area_arr: Float32Array =
            Float32Array::from_iter((0..num_rows).map(|_| None::<f32>));
        let minx_arr = Float32Array::from(vec![-1.0_f32; num_rows]);
        let miny_arr = Float32Array::from(vec![-1.0_f32; num_rows]);
        let maxx_arr = Float32Array::from(vec![1.0_f32; num_rows]);
        let maxy_arr = Float32Array::from(vec![1.0_f32; num_rows]);
        let wkb: Vec<u8> = b"wkb".to_vec();
        let geom_data: Vec<&[u8]> = vec![wkb.as_slice(); num_rows];
        let geom_arr = BinaryArray::from_iter_values(geom_data);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_arr),
                Arc::new(area_arr),
                Arc::new(up_area_arr),
                Arc::new(minx_arr),
                Arc::new(miny_arr),
                Arc::new(maxx_arr),
                Arc::new(maxy_arr),
                Arc::new(geom_arr),
            ],
        )
        .unwrap();

        let buf = write_parquet(schema, batch);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catchments.parquet");
        std::fs::write(&path, &buf).unwrap();

        let (data, diags) = read_catchments(&path);

        // The schema validator catches the nullable id column and exits early.
        // data is None; the per-row null scan (and therefore null-capping) never runs.
        assert!(
            data.is_none(),
            "schema.wrong_nullability error causes early exit → data should be None"
        );

        let null_id_diags: Vec<_> = diags
            .iter()
            .filter(|d| d.check_id == "catchments.null_id")
            .collect();

        // With the current architecture (schema error → early exit), the null-capping
        // path cannot fire.  Zero per-row null diagnostics are expected.
        assert_eq!(
            null_id_diags.len(),
            0,
            "no catchments.null_id diagnostics expected when schema validation exits early"
        );

        // The schema validation error must be present.
        assert!(
            diags.iter().any(|d| d.check_id == "schema.wrong_nullability"),
            "schema.wrong_nullability error must be present"
        );
    }
}

