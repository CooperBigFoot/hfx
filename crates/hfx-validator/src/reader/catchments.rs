//! Catchments Parquet reader.

use std::path::Path;

use arrow::array::{
    Array, BinaryArray, Float32Array, Float64Array, Int16Array, Int64Array, LargeBinaryArray,
};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};

use super::{
    MAX_CONSECUTIVE_BATCH_FAILURES, MAX_NULL_DIAGNOSTICS_PER_COLUMN, MAX_TOTAL_BATCH_FAILURES,
};
use crate::dataset::CatchmentsData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::schema::{ExpectedColumn, row_group_has_bbox_stats, validate_schema};

/// Expected schema for catchments.parquet.
fn expected_columns() -> Vec<ExpectedColumn> {
    vec![
        ExpectedColumn::new("id", DataType::Int64, false),
        ExpectedColumn::new("level", DataType::Int16, false),
        ExpectedColumn::new("parent_id", DataType::Int64, true),
        ExpectedColumn::new("area_km2", DataType::Float32, false),
        ExpectedColumn::new("up_area_km2", DataType::Float32, true),
        ExpectedColumn::new("outlet_lon", DataType::Float64, false),
        ExpectedColumn::new("outlet_lat", DataType::Float64, false),
        ExpectedColumn::new("bbox_minx", DataType::Float32, false),
        ExpectedColumn::new("bbox_miny", DataType::Float32, false),
        ExpectedColumn::new("bbox_maxx", DataType::Float32, false),
        ExpectedColumn::new("bbox_maxy", DataType::Float32, false),
        ExpectedColumn::new("geometry", DataType::Binary, false),
    ]
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
    if diags
        .iter()
        .any(|d| d.severity == crate::diagnostic::Severity::Error)
    {
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
    let mut levels: Vec<i16> = Vec::new();
    let mut parent_ids: Vec<Option<i64>> = Vec::new();
    let mut areas_km2: Vec<f32> = Vec::new();
    let mut outlet_lons: Vec<f64> = Vec::new();
    let mut outlet_lats: Vec<f64> = Vec::new();
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
    let mut null_level_count: usize = 0;
    let mut null_area_count: usize = 0;
    let mut null_outlet_count: usize = 0;
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
                                format!(
                                    "row {}: id is null in a non-nullable column",
                                    total_rows + i
                                ),
                            )
                            .at(Location::Row {
                                index: total_rows + i,
                            }),
                        );
                    }
                    ids.push(0); // sentinel to keep indices aligned
                } else {
                    ids.push(arr.value(i));
                }
            }
        }

        // level column (non-nullable)
        let level_col = batch
            .column_by_name("level")
            .and_then(|c| c.as_any().downcast_ref::<Int16Array>());
        if let Some(arr) = level_col {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    null_level_count += 1;
                    if null_level_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_level",
                                Category::Schema,
                                Artifact::Catchments,
                                format!(
                                    "row {}: level is null in a non-nullable column",
                                    total_rows + i
                                ),
                            )
                            .at(Location::Row {
                                index: total_rows + i,
                            }),
                        );
                    }
                    levels.push(0);
                } else {
                    levels.push(arr.value(i));
                }
            }
        }

        // parent_id column (nullable)
        let parent_col = batch
            .column_by_name("parent_id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
        if let Some(arr) = parent_col {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    parent_ids.push(None);
                } else {
                    parent_ids.push(Some(arr.value(i)));
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
                                format!(
                                    "row {}: area_km2 is null in a non-nullable column",
                                    total_rows + i
                                ),
                            )
                            .at(Location::Row {
                                index: total_rows + i,
                            }),
                        );
                    }
                    areas_km2.push(0.0); // sentinel
                } else {
                    areas_km2.push(arr.value(i));
                }
            }
        }

        // outlet_lon/outlet_lat columns (non-nullable)
        let outlet_lon = batch
            .column_by_name("outlet_lon")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let outlet_lat = batch
            .column_by_name("outlet_lat")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        if let (Some(lons), Some(lats)) = (outlet_lon, outlet_lat) {
            for i in 0..num_rows {
                let outlet_null = lons.is_null(i) || lats.is_null(i);
                if outlet_null {
                    null_outlet_count += 1;
                    if null_outlet_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "catchments.null_outlet",
                                Category::Schema,
                                Artifact::Catchments,
                                format!("row {}: outlet_lon or outlet_lat is null in a non-nullable column", total_rows + i),
                            )
                            .at(Location::Row { index: total_rows + i }),
                        );
                    }
                    outlet_lons.push(0.0);
                    outlet_lats.push(0.0);
                } else {
                    outlet_lons.push(lons.value(i));
                    outlet_lats.push(lats.value(i));
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
                let bbox_null =
                    minx.is_null(i) || miny.is_null(i) || maxx.is_null(i) || maxy.is_null(i);
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
                                    format!(
                                        "row {}: geometry is null in a non-nullable column",
                                        total_rows + i
                                    ),
                                )
                                .at(Location::Row {
                                    index: total_rows + i,
                                }),
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
                                    format!(
                                        "row {}: geometry is null in a non-nullable column",
                                        total_rows + i
                                    ),
                                )
                                .at(Location::Row {
                                    index: total_rows + i,
                                }),
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
    if null_level_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_level_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "catchments.null_level",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "{suppressed} additional null violation(s) in 'level' column suppressed ({null_level_count} total)"
            ),
        ));
    }
    if null_outlet_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_outlet_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "catchments.null_outlet",
            Category::Schema,
            Artifact::Catchments,
            format!(
                "{suppressed} additional null violation(s) in outlet columns suppressed ({null_outlet_count} total)"
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
            levels,
            parent_ids,
            areas_km2,
            outlet_lons,
            outlet_lats,
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
