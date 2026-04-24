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
/// - B6: `atom_count` in manifest matches catchments row count
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

    // B6: atom_count in manifest matches catchments row count.
    // Use raw_manifest so a bad fabric_name (or any other unparseable field)
    // does not suppress this check.
    if let (Some(raw), Some(catchments)) = (&dataset.raw_manifest, &dataset.catchments)
        && let Some(declared) = raw.atom_count
    {
        let actual = catchments.row_count as u64;
        if declared != actual {
            diags.push(Diagnostic::error(
                "schema.atom_count_mismatch",
                Category::Schema,
                Artifact::CrossFile,
                format!(
                    "manifest atom_count ({declared}) does not match \
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
    use super::*;
    use crate::dataset::{CatchmentsData, FilePresenceMap, ParsedDataset, SnapData};
    use crate::diagnostic::Severity;
    use crate::reader::manifest::RawManifest;

    fn empty_dataset() -> ParsedDataset {
        ParsedDataset {
            files: FilePresenceMap {
                manifest_path: None,
                catchments_path: None,
                graph_path: None,
                snap_path: None,
                flow_dir_path: None,
                flow_acc_path: None,
            },
            manifest_json: None,
            raw_manifest: None,
            manifest: None,
            catchments: None,
            graph: None,
            snap: None,
            flow_dir: None,
            flow_acc: None,
            read_diagnostics: Vec::new(),
        }
    }

    fn catchments_with_rg(sizes: Vec<usize>, has_bbox_stats: Vec<bool>) -> CatchmentsData {
        let row_count: usize = sizes.iter().sum();
        CatchmentsData {
            row_count,
            ids: (0..row_count as i64).collect(),
            areas_km2: vec![1.0; row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            up_area_null_count: 0,
            up_area_total: row_count,
            geometry_wkb: vec![vec![]; row_count],
            row_group_sizes: sizes,
            row_group_has_bbox_stats: has_bbox_stats,
        }
    }

    fn snap_with_rg(sizes: Vec<usize>, has_bbox_stats: Vec<bool>) -> SnapData {
        let row_count: usize = sizes.iter().sum();
        SnapData {
            row_count,
            ids: (0..row_count as i64).collect(),
            catchment_ids: (0..row_count as i64).collect(),
            weights: vec![1.0; row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            geometry_wkb: vec![vec![]; row_count],
            row_group_sizes: sizes,
            row_group_has_bbox_stats: has_bbox_stats,
        }
    }

    fn raw_manifest_with_atom_count(count: u64) -> RawManifest {
        RawManifest {
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            has_rasters: Some(false),
            has_snap: Some(false),
            flow_dir_encoding: None,
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(count),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        }
    }

    #[test]
    fn no_catchments_or_manifest_no_diags() {
        let dataset = empty_dataset();
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn b4_bbox_stats_missing_produces_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![false]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 1);
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "schema.catchments.bbox_stats_missing")
        );
    }

    #[test]
    fn b4_bbox_stats_present_no_warning() {
        let mut dataset = empty_dataset();
        // 5000 rows (in range), stats present
        dataset.catchments = Some(catchments_with_rg(vec![5000], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn b5_large_file_trailing_small_rg_produces_rg_size_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![4096, 500], vec![true, true]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.catchments.rg_size");
        assert_eq!(diags[0].severity, Severity::Warning);
    }

    #[test]
    fn b5_small_file_single_rg_no_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![12], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn b5_small_file_711_rows_no_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![711], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn b5_small_file_multiple_rgs_produces_rg_count_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![6, 6], vec![true, true]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.catchments.rg_count");
        assert_eq!(diags[0].severity, Severity::Warning);
        assert!(diags[0].message.contains("12 rows"));
        assert!(diags[0].message.contains("2 row groups"));
    }

    #[test]
    fn b5_large_file_in_range_multiple_rgs_no_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![4251; 16], vec![true; 16]));
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn b5_rg_size_in_range_no_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn b5_rg_size_exceeds_max_produces_warning() {
        let mut dataset = empty_dataset();
        dataset.catchments = Some(catchments_with_rg(vec![9000], vec![true]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.catchments.rg_size");
    }

    #[test]
    fn b6_atom_count_mismatch_produces_error() {
        let mut dataset = empty_dataset();
        // raw_manifest says 10, catchments has 3 rows
        dataset.raw_manifest = Some(raw_manifest_with_atom_count(10));
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![true]));
        // Override row_count to force a mismatch
        let mut c = dataset.catchments.take().unwrap();
        c.row_count = 3;
        dataset.catchments = Some(c);
        let diags = check_schemas(&dataset);
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "schema.atom_count_mismatch")
        );
        assert_eq!(
            diags
                .iter()
                .find(|d| d.check_id == "schema.atom_count_mismatch")
                .unwrap()
                .severity,
            Severity::Error
        );
    }

    #[test]
    fn b6_atom_count_matches_no_error() {
        let mut dataset = empty_dataset();
        dataset.raw_manifest = Some(raw_manifest_with_atom_count(4096));
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(
            !diags
                .iter()
                .any(|d| d.check_id == "schema.atom_count_mismatch")
        );
    }

    #[test]
    fn b6_bad_fabric_name_does_not_suppress_atom_count_check() {
        // A raw_manifest with an invalid fabric_name would fail try_build_manifest,
        // so dataset.manifest would be None. But the B6 check should still fire
        // because it reads from raw_manifest directly.
        let mut dataset = empty_dataset();
        let mut raw = raw_manifest_with_atom_count(10);
        raw.fabric_name = Some("INVALID NAME".into()); // would cause try_build_manifest to return None
        dataset.raw_manifest = Some(raw);
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![true]));
        let mut c = dataset.catchments.take().unwrap();
        c.row_count = 3;
        dataset.catchments = Some(c);
        // manifest is intentionally None (simulating failed build)
        let diags = check_schemas(&dataset);
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "schema.atom_count_mismatch"),
            "atom_count check should fire even when manifest build fails; got: {diags:#?}"
        );
    }

    #[test]
    fn snap_b4_b5_checks_work() {
        let mut dataset = empty_dataset();
        dataset.snap = Some(snap_with_rg(vec![4096], vec![false]));
        let diags = check_schemas(&dataset);
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "schema.snap.bbox_stats_missing")
        );
        assert!(!diags.iter().any(|d| d.check_id == "schema.snap.rg_size"));
    }

    #[test]
    fn snap_small_single_rg_no_warning() {
        let mut dataset = empty_dataset();
        dataset.snap = Some(snap_with_rg(vec![12], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(diags.is_empty());
    }

    #[test]
    fn snap_small_multi_rg_produces_rg_count_warning() {
        let mut dataset = empty_dataset();
        dataset.snap = Some(snap_with_rg(vec![6, 6], vec![true, true]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.snap.rg_count");
        assert_eq!(diags[0].severity, Severity::Warning);
        assert!(diags[0].message.contains("12 rows"));
        assert!(diags[0].message.contains("2 row groups"));
    }

    #[test]
    fn classify_row_groups_truth_table() {
        assert_eq!(
            classify_row_groups(100, &[100]),
            RgLayoutVerdict::SmallFileSingleRg
        );
        assert_eq!(
            classify_row_groups(711, &[711]),
            RgLayoutVerdict::SmallFileSingleRg
        );
        assert_eq!(
            classify_row_groups(12, &[6, 6]),
            RgLayoutVerdict::SmallFileMultipleRgs { rg_count: 2 }
        );
        assert_eq!(
            classify_row_groups(4096, &[4096]),
            RgLayoutVerdict::LargeFileInRange
        );
        assert_eq!(
            classify_row_groups(8192, &[4096, 4096]),
            RgLayoutVerdict::LargeFileInRange
        );
        assert_eq!(
            classify_row_groups(4096, &[2048, 2048]),
            RgLayoutVerdict::LargeFileOutOfRange {
                rg_idx: 0,
                size: 2048
            }
        );
        assert_eq!(
            classify_row_groups(4596, &[4096, 500]),
            RgLayoutVerdict::LargeFileOutOfRange {
                rg_idx: 1,
                size: 500
            }
        );
        assert_eq!(
            classify_row_groups(9000, &[9000]),
            RgLayoutVerdict::LargeFileOutOfRange {
                rg_idx: 0,
                size: 9000
            }
        );
    }
}
