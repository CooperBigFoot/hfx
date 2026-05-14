//! Graph Parquet reader.

use std::path::Path;

use arrow::array::{Array, Int16Array, Int64Array, LargeListArray, ListArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tracing::{debug, warn};

use super::{
    MAX_CONSECUTIVE_BATCH_FAILURES, MAX_NULL_DIAGNOSTICS_PER_COLUMN, MAX_TOTAL_BATCH_FAILURES,
};
use crate::dataset::GraphData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::schema::{ExpectedColumn, list_int64_field, validate_schema};

/// Expected schema for graph.parquet.
fn expected_columns() -> Vec<ExpectedColumn> {
    vec![
        ExpectedColumn::new("id", DataType::Int64, false),
        ExpectedColumn::new("level", DataType::Int16, false),
        ExpectedColumn::new("upstream_ids", list_int64_field(), false),
    ]
}

/// Read `graph.parquet` and return the extracted data plus diagnostics.
pub fn read_graph(path: &Path) -> (Option<GraphData>, Vec<Diagnostic>) {
    debug!(path = %path.display(), "reading graph.parquet");

    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot open graph.parquet");
            return (
                None,
                vec![Diagnostic::error(
                    "graph.read",
                    Category::Schema,
                    Artifact::Graph,
                    format!("cannot open graph.parquet: {err}"),
                )],
            );
        }
    };

    let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
        Ok(b) => b,
        Err(err) => {
            warn!(error = %err, "cannot read graph.parquet as Parquet");
            return (
                None,
                vec![Diagnostic::error(
                    "graph.parquet_open",
                    Category::Schema,
                    Artifact::Graph,
                    format!("cannot read graph.parquet as Parquet: {err}"),
                )],
            );
        }
    };

    let arrow_schema = builder.schema();
    let mut diags = validate_schema(arrow_schema, &expected_columns(), Artifact::Graph);
    if diags
        .iter()
        .any(|d| d.severity == crate::diagnostic::Severity::Error)
    {
        warn!("graph.parquet schema has errors; skipping data extraction");
        return (None, diags);
    }

    let reader = match builder.with_batch_size(8192).build() {
        Ok(r) => r,
        Err(err) => {
            warn!(error = %err, "cannot build graph record batch reader");
            return (
                None,
                vec![Diagnostic::error(
                    "graph.reader_build",
                    Category::Schema,
                    Artifact::Graph,
                    format!("cannot build graph record batch reader: {err}"),
                )],
            );
        }
    };

    let mut ids = Vec::new();
    let mut levels = Vec::new();
    let mut upstream_ids = Vec::new();
    let mut total_rows = 0usize;
    let mut null_id_count = 0usize;
    let mut null_level_count = 0usize;
    let mut null_upstream_ids_count = 0usize;
    let mut consecutive_batch_failures = 0usize;
    let mut total_batch_failures = 0usize;
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
                warn!(error = %err, "error reading graph record batch");
                consecutive_batch_failures += 1;
                total_batch_failures += 1;
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

        if let Some(col) = batch.column_by_name("id")
            && let Some(arr) = col.as_any().downcast_ref::<Int64Array>()
        {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    null_id_count += 1;
                    if null_id_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "graph.null_id",
                                Category::Schema,
                                Artifact::Graph,
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
                    ids.push(0);
                } else {
                    ids.push(arr.value(i));
                }
            }
        }

        if let Some(col) = batch.column_by_name("level")
            && let Some(arr) = col.as_any().downcast_ref::<Int16Array>()
        {
            for i in 0..num_rows {
                if arr.is_null(i) {
                    null_level_count += 1;
                    if null_level_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                        diags.push(
                            Diagnostic::error(
                                "graph.null_level",
                                Category::Schema,
                                Artifact::Graph,
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

        if let Some(col) = batch.column_by_name("upstream_ids") {
            if let Some(list_arr) = col.as_any().downcast_ref::<ListArray>() {
                read_list_column(
                    list_arr,
                    num_rows,
                    total_rows,
                    &mut null_upstream_ids_count,
                    &mut upstream_ids,
                    &mut diags,
                );
            } else if let Some(list_arr) = col.as_any().downcast_ref::<LargeListArray>() {
                for i in 0..num_rows {
                    if list_arr.is_null(i) {
                        null_upstream_ids_count += 1;
                        if null_upstream_ids_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                            diags.push(null_upstream_ids_diag(total_rows + i));
                        }
                        upstream_ids.push(Vec::new());
                    } else {
                        let values = list_arr.value(i);
                        upstream_ids.push(
                            values
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .map(|a| a.values().iter().copied().collect())
                                .unwrap_or_default(),
                        );
                    }
                }
            }
        }

        total_rows += num_rows;
    }

    if batch_read_aborted
        || consecutive_batch_failures >= MAX_CONSECUTIVE_BATCH_FAILURES
        || total_batch_failures >= MAX_TOTAL_BATCH_FAILURES
    {
        batch_read_aborted = true;
        diags.push(Diagnostic::error(
            "graph.batch_read_aborted",
            Category::Schema,
            Artifact::Graph,
            format!(
                "aborting read after batch failures ({} consecutive, {} total); file may be unreadable",
                consecutive_batch_failures, total_batch_failures
            ),
        ));
    }

    if batch_read_aborted {
        return (None, diags);
    }

    if null_id_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_id_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "graph.null_id",
            Category::Schema,
            Artifact::Graph,
            format!(
                "{suppressed} additional null violation(s) in 'id' column suppressed ({null_id_count} total)"
            ),
        ));
    }
    if null_level_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_level_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "graph.null_level",
            Category::Schema,
            Artifact::Graph,
            format!(
                "{suppressed} additional null violation(s) in 'level' column suppressed ({null_level_count} total)"
            ),
        ));
    }
    if null_upstream_ids_count > MAX_NULL_DIAGNOSTICS_PER_COLUMN {
        let suppressed = null_upstream_ids_count - MAX_NULL_DIAGNOSTICS_PER_COLUMN;
        diags.push(Diagnostic::error(
            "graph.null_upstream_ids",
            Category::Schema,
            Artifact::Graph,
            format!(
                "{suppressed} additional null violation(s) in 'upstream_ids' column suppressed ({null_upstream_ids_count} total)"
            ),
        ));
    }

    debug!(row_count = ids.len(), "graph.parquet read complete");

    (
        Some(GraphData {
            ids,
            levels,
            upstream_ids,
        }),
        diags,
    )
}

fn read_list_column(
    list_arr: &ListArray,
    num_rows: usize,
    total_rows: usize,
    null_count: &mut usize,
    upstream_ids: &mut Vec<Vec<i64>>,
    diags: &mut Vec<Diagnostic>,
) {
    for i in 0..num_rows {
        if list_arr.is_null(i) {
            *null_count += 1;
            if *null_count <= MAX_NULL_DIAGNOSTICS_PER_COLUMN {
                diags.push(null_upstream_ids_diag(total_rows + i));
            }
            upstream_ids.push(Vec::new());
        } else {
            let values = list_arr.value(i);
            upstream_ids.push(
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| a.values().iter().copied().collect())
                    .unwrap_or_default(),
            );
        }
    }
}

fn null_upstream_ids_diag(index: usize) -> Diagnostic {
    Diagnostic::error(
        "graph.null_upstream_ids",
        Category::Schema,
        Artifact::Graph,
        format!("row {index}: upstream_ids is null in a non-nullable column"),
    )
    .at(Location::Row { index })
}
