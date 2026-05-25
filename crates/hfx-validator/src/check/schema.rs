//! Parquet/Arrow schema checks (B1-B6).

use tracing::debug;

use crate::dataset::ParsedDataset;
use crate::diagnostic::{Artifact, Category, Diagnostic};

// Row-group size bounds from the spec.
const RG_SIZE_MIN: usize = 4096;
const RG_SIZE_MAX: usize = 8192;

#[derive(Debug, PartialEq, Eq)]
enum RgLayoutVerdict {
    /// A small file is represented by one row group.
    SmallFileSingleRg,
    /// A small file is split across multiple row groups.
    SmallFileMultipleRgs { rg_count: usize },
    /// A large file has all row groups within the recommended size range.
    LargeFileInRange,
    /// A large file has at least one row group outside the recommended size range.
    LargeFileOutOfRange { rg_idx: usize, size: usize },
}

/// Run schema-level checks B1–B6 on a parsed dataset.
///
/// # Checks
/// - B1: catchments schema (diagnostics already collected by reader)
/// - B2: graph schema (diagnostics already collected by reader)
/// - B3: snap schema (diagnostics already collected by reader, if present)
/// - B4: all bbox columns in every row group have statistics
/// - B5: row-group layout follows small-file and large-file size rules
/// - B6: `unit_count` in manifest matches catchments row count
pub fn check_schemas(dataset: &ParsedDataset) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    // B1/B2/B3: Schema diagnostics are emitted by the readers and stored in
    // `read_diagnostics`.  We do not re-emit them here to avoid duplicates.
    // The caller already includes `read_diagnostics` via `run_checks`.

    // B4: Bbox column statistics presence in catchments row groups.
    if let Some(catchments) = &dataset.catchments {
        for (rg_idx, has_stats) in catchments.row_group_has_bbox_stats.iter().enumerate() {
            if !has_stats {
                diags.push(Diagnostic::error(
                    "schema.catchments.bbox_stats_missing",
                    Category::Schema,
                    Artifact::Catchments,
                    format!(
                        "catchments.parquet row group {rg_idx} is missing statistics for bbox columns; \
                         spec requires row group statistics on bbox columns"
                    ),
                ));
            }
        }

        // B5: Row group sizes.
        emit_row_group_diag(
            classify_row_groups(catchments.row_count, &catchments.row_group_sizes),
            catchments.row_count,
            Artifact::Catchments,
            "catchments.parquet",
            "schema.catchments.rg_size",
            "schema.catchments.rg_count",
            &mut diags,
        );
    }

    if let Some(graph) = &dataset.graph {
        for (rg_idx, has_stats) in graph.row_group_has_bbox_stats.iter().enumerate() {
            if !has_stats {
                diags.push(Diagnostic::error(
                    "schema.graph.bbox_stats_missing",
                    Category::Schema,
                    Artifact::Graph,
                    format!(
                        "graph.parquet row group {rg_idx} is missing statistics for bbox columns; \
                         spec requires row group statistics on bbox columns"
                    ),
                ));
            }
        }

        emit_row_group_diag(
            classify_row_groups(graph.ids.len(), &graph.row_group_sizes),
            graph.ids.len(),
            Artifact::Graph,
            "graph.parquet",
            "schema.graph.rg_size",
            "schema.graph.rg_count",
            &mut diags,
        );
    }

    // B4/B5: Same checks for snap.parquet when present.
    if let Some(snap) = &dataset.snap {
        for (rg_idx, has_stats) in snap.row_group_has_bbox_stats.iter().enumerate() {
            if !has_stats {
                diags.push(Diagnostic::error(
                    "schema.snap.bbox_stats_missing",
                    Category::Schema,
                    Artifact::Snap,
                    format!(
                        "snap.parquet row group {rg_idx} is missing statistics for bbox columns; \
                         spec requires row group statistics on bbox columns"
                    ),
                ));
            }
        }

        emit_row_group_diag(
            classify_row_groups(snap.row_count, &snap.row_group_sizes),
            snap.row_count,
            Artifact::Snap,
            "snap.parquet",
            "schema.snap.rg_size",
            "schema.snap.rg_count",
            &mut diags,
        );
    }

    // B6: unit_count in manifest matches catchments row count.
    // Use raw_manifest so a bad fabric_name (or any other unparseable field)
    // does not suppress this check.
    if let (Some(raw), Some(catchments)) = (&dataset.raw_manifest, &dataset.catchments)
        && let Some(declared) = raw.unit_count
    {
        let actual = catchments.row_count as u64;
        if declared != actual {
            diags.push(Diagnostic::error(
                "schema.unit_count_mismatch",
                Category::Schema,
                Artifact::CrossFile,
                format!(
                    "manifest unit_count ({declared}) does not match \
                     catchments.parquet row count ({actual})"
                ),
            ));
        }
    }

    debug!(count = diags.len(), "schema checks complete");
    diags
}

fn classify_row_groups(row_count: usize, sizes: &[usize]) -> RgLayoutVerdict {
    if row_count < RG_SIZE_MIN {
        return if sizes.len() > 1 {
            RgLayoutVerdict::SmallFileMultipleRgs {
                rg_count: sizes.len(),
            }
        } else {
            RgLayoutVerdict::SmallFileSingleRg
        };
    }

    sizes
        .iter()
        .enumerate()
        .find_map(|(rg_idx, &size)| {
            (!((RG_SIZE_MIN..=RG_SIZE_MAX).contains(&size)))
                .then_some(RgLayoutVerdict::LargeFileOutOfRange { rg_idx, size })
        })
        .unwrap_or(RgLayoutVerdict::LargeFileInRange)
}

fn emit_row_group_diag(
    verdict: RgLayoutVerdict,
    row_count: usize,
    artifact: Artifact,
    file_label: &str,
    rg_size_check_id: &'static str,
    rg_count_check_id: &'static str,
    diags: &mut Vec<Diagnostic>,
) {
    match verdict {
        RgLayoutVerdict::SmallFileSingleRg | RgLayoutVerdict::LargeFileInRange => {}
        RgLayoutVerdict::SmallFileMultipleRgs { rg_count } => {
            diags.push(Diagnostic::warning(
                rg_count_check_id,
                Category::Schema,
                artifact,
                format!(
                    "{file_label} has {row_count} rows split across {rg_count} row groups; \
                     files with fewer than {RG_SIZE_MIN} rows must be written as a single row group"
                ),
            ));
        }
        RgLayoutVerdict::LargeFileOutOfRange { rg_idx, size } => {
            diags.push(Diagnostic::warning(
                rg_size_check_id,
                Category::Schema,
                artifact,
                format!(
                    "{file_label} row group {rg_idx} has {size} rows; \
                     recommended range is [{RG_SIZE_MIN}, {RG_SIZE_MAX}]"
                ),
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dataset::{
        CatchmentsData, FilePresenceMap, GraphData, ParsedDataset, RasterMeta, SnapData,
    };
    use crate::diagnostic::{Artifact, Severity};
    use crate::reader::manifest::RawManifest;

    use super::check_schemas;

    fn dataset_with_graph(graph: GraphData) -> ParsedDataset {
        ParsedDataset {
            files: FilePresenceMap {
                manifest_path: None,
                catchments_path: None,
                graph_path: None,
                legacy_graph_arrow_path: None,
                snap_path: None,
                flow_dir_path: None,
                flow_acc_path: None,
            },
            manifest_json: None,
            raw_manifest: None::<RawManifest>,
            manifest: None,
            catchments: None::<CatchmentsData>,
            graph: Some(graph),
            snap: None::<SnapData>,
            flow_dir: None::<RasterMeta>,
            flow_acc: None::<RasterMeta>,
            read_diagnostics: Vec::new(),
        }
    }

    fn graph_with_row_groups(
        row_count: usize,
        sizes: Vec<usize>,
        has_stats: Vec<bool>,
    ) -> GraphData {
        GraphData {
            ids: (1..=row_count as i64).collect(),
            levels: vec![0; row_count],
            upstream_ids: vec![Vec::new(); row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            row_group_sizes: sizes,
            row_group_has_bbox_stats: has_stats,
        }
    }

    #[test]
    fn graph_bbox_stats_missing_emits_error() {
        let dataset = dataset_with_graph(graph_with_row_groups(1, vec![1], vec![false]));
        let diagnostics = check_schemas(&dataset);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "schema.graph.bbox_stats_missing"
                && diag.severity == Severity::Error
                && diag.artifact == Artifact::Graph
        }));
    }

    #[test]
    fn graph_small_file_multiple_row_groups_emits_warning() {
        let dataset = dataset_with_graph(graph_with_row_groups(2, vec![1, 1], vec![true, true]));
        let diagnostics = check_schemas(&dataset);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "schema.graph.rg_count"
                && diag.severity == Severity::Warning
                && diag.artifact == Artifact::Graph
        }));
    }

    #[test]
    fn graph_large_file_bad_row_group_size_emits_warning() {
        let dataset =
            dataset_with_graph(graph_with_row_groups(4096, vec![4095, 1], vec![true, true]));
        let diagnostics = check_schemas(&dataset);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "schema.graph.rg_size"
                && diag.severity == Severity::Warning
                && diag.artifact == Artifact::Graph
        }));
    }
}
