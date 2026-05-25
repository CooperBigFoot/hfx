//! Snap Parquet reader.

use std::path::Path;

use arrow::array::{Array, BinaryArray, Float32Array, Int64Array, LargeBinaryArray, StringArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};

use super::{
    MAX_CONSECUTIVE_BATCH_FAILURES, MAX_NULL_DIAGNOSTICS_PER_COLUMN, MAX_TOTAL_BATCH_FAILURES,
};
use crate::dataset::SnapData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::schema::{ExpectedColumn, row_group_has_bbox_stats, validate_schema};

/// Expected schema for snap.parquet.
fn expected_columns() -> Vec<ExpectedColumn> {
    vec![
        ExpectedColumn::new("id", DataType::Int64, false),
        ExpectedColumn::new("unit_id", DataType::Int64, false),
        ExpectedColumn::new("weight", DataType::Float32, false),
        ExpectedColumn::new("stem_role", DataType::Utf8, true),
        ExpectedColumn::new("bbox_minx", DataType::Float32, true),
        ExpectedColumn::new("bbox_miny", DataType::Float32, true),
        ExpectedColumn::new("bbox_maxx", DataType::Float32, true),
        ExpectedColumn::new("bbox_maxy", DataType::Float32, true),
        ExpectedColumn::new("geometry", DataType::Binary, false),
    ]
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
    if diags
        .iter()
        .any(|d| d.severity == crate::diagnostic::Severity::Error)
    {
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
    let mut unit_ids: Vec<i64> = Vec::new();
    let mut stem_roles: Vec<Option<String>> = Vec::new();
    let mut weights: Vec<f32> = Vec::new();
    let mut bboxes: Vec<Option<[f32; 4]>> = Vec::new();
    let mut geometry_wkb: Vec<Vec<u8>> = Vec::new();
    let mut total_rows: usize = 0;

    // Per-column null counters (used to cap per-row diagnostics).
    let mut null_id_count: usize = 0;
    let mut null_unit_id_count: usize = 0;
    let mut null_weight_count: usize = 0;
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
                warn!(error = %err, "error reading snap record batch");
                consecutive_batch_failures += 1;
                total_batch_failures += 1;
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

        // id column (non-nullable)
        if let Some(col) = batch.column_by_name("id")
            && let Some(arr) = col.as_any().downcast_ref::<Int64Array>()
        {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    null_id_count += 1;
                    if null_id_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "snap.null_id",
                                Category::Schema,
                                Artifact::Snap,
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

        // unit_id column (non-nullable)
        if let Some(col) = batch.column_by_name("unit_id")
            && let Some(arr) = col.as_any().downcast_ref::<Int64Array>()
        {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    null_unit_id_count += 1;
                    if null_unit_id_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "snap.null_unit_id",
                                Category::Schema,
                                Artifact::Snap,
                                format!(
                                    "row {}: unit_id is null in a non-nullable column",
                                    total_rows + i
                                ),
                            )
                            .at(Location::Row {
                                index: total_rows + i,
                            }),
                        );
                    }
                    unit_ids.push(0); // sentinel
                } else {
                    unit_ids.push(arr.value(i));
                }
            }
        }

        // weight column (non-nullable)
        if let Some(col) = batch.column_by_name("weight")
            && let Some(arr) = col.as_any().downcast_ref::<Float32Array>()
        {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    null_weight_count += 1;
                    if null_weight_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "snap.null_weight",
                                Category::Schema,
                                Artifact::Snap,
                                format!(
                                    "row {}: weight is null in a non-nullable column",
                                    total_rows + i
                                ),
                            )
                            .at(Location::Row {
                                index: total_rows + i,
                            }),
                        );
                    }
                    weights.push(0.0); // sentinel
                } else {
                    weights.push(arr.value(i));
                }
            }
        }

        // stem_role column (nullable)
        if let Some(col) = batch.column_by_name("stem_role")
            && let Some(arr) = col.as_any().downcast_ref::<StringArray>()
        {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    stem_roles.push(None);
                } else {
                    stem_roles.push(Some(arr.value(i).to_string()));
                }
            }
        }

        // bbox columns (nullable as a group)
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
                    bboxes.push(None);
                } else {
                    bboxes.push(Some([
                        minx.value(i),
                        miny.value(i),
                        maxx.value(i),
                        maxy.value(i),
                    ]));
                }
            }
        }

        // geometry column (non-nullable) — accept both Binary and LargeBinary
        if let Some(col) = batch.column_by_name("geometry") {
            if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
                for i in 0..num_rows {
                    if arr.is_null(i) {
                        null_geom_count += 1;
                        if null_geom_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                            diags.push(
                                Diagnostic::error(
                                    "snap.null_geometry",
                                    Category::Schema,
                                    Artifact::Snap,
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
                                    "snap.null_geometry",
                                    Category::Schema,
                                    Artifact::Snap,
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
            "snap.batch_read_aborted",
            Category::Schema,
            Artifact::Snap,
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
            "snap.null_id",
            Category::Schema,
            Artifact::Snap,
            format!(
                "{suppressed} additional null violation(s) in 'id' column suppressed ({null_id_count} total)"
            ),
        ));
    }
    if null_unit_id_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_unit_id_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "snap.null_unit_id",
            Category::Schema,
            Artifact::Snap,
            format!(
                "{suppressed} additional null violation(s) in 'unit_id' column suppressed ({null_unit_id_count} total)"
            ),
        ));
    }
    if null_weight_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_weight_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "snap.null_weight",
            Category::Schema,
            Artifact::Snap,
            format!(
                "{suppressed} additional null violation(s) in 'weight' column suppressed ({null_weight_count} total)"
            ),
        ));
    }
    if null_geom_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_geom_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "snap.null_geometry",
            Category::Schema,
            Artifact::Snap,
            format!(
                "{suppressed} additional null violation(s) in 'geometry' column suppressed ({null_geom_count} total)"
            ),
        ));
    }

    let row_count = ids.len();
    debug!(row_count, "snap.parquet read complete");

    (
        Some(SnapData {
            row_count,
            ids,
            unit_ids,
            weights,
            stem_roles,
            bboxes,
            geometry_wkb,
            row_group_sizes,
            row_group_has_bbox_stats: row_group_has_bbox_stats_vec,
        }),
        diags,
    )
}
