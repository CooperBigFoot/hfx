//! Parquet/Arrow schema checks (B1-B6).

use tracing::debug;

use crate::dataset::ParsedDataset;
use crate::diagnostic::{Artifact, Category, Diagnostic};

// Row-group size bounds from the spec.
const RG_SIZE_MIN: usize = 4096;
const RG_SIZE_MAX: usize = 8192;

/// Run schema-level checks B1–B6 on a parsed dataset.
///
/// # Checks
/// - B1: catchments schema (diagnostics already collected by reader)
/// - B2: graph schema (diagnostics already collected by reader)
/// - B3: snap schema (diagnostics already collected by reader, if present)
/// - B4: all bbox columns in every row group have statistics
/// - B5: row group sizes are in `[4096, 8192]` (warning, not error)
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
                diags.push(Diagnostic::warning(
                    "schema.catchments.bbox_stats_missing",
                    Category::Schema,
                    Artifact::Catchments,
                    format!(
                        "catchments.parquet row group {rg_idx} is missing statistics for bbox columns; spatial filtering will be slower"
                    ),
                ));
            }
        }

        // B5: Row group sizes.
        for (rg_idx, &size) in catchments.row_group_sizes.iter().enumerate() {
            if size < RG_SIZE_MIN || size > RG_SIZE_MAX {
                diags.push(Diagnostic::warning(
                    "schema.catchments.rg_size",
                    Category::Schema,
                    Artifact::Catchments,
                    format!(
                        "catchments.parquet row group {rg_idx} has {size} rows; \
                         recommended range is [{RG_SIZE_MIN}, {RG_SIZE_MAX}]"
                    ),
                ));
            }
        }
    }

    // B4/B5: Same checks for snap.parquet when present.
    if let Some(snap) = &dataset.snap {
        for (rg_idx, has_stats) in snap.row_group_has_bbox_stats.iter().enumerate() {
            if !has_stats {
                diags.push(Diagnostic::warning(
                    "schema.snap.bbox_stats_missing",
                    Category::Schema,
                    Artifact::Snap,
                    format!(
                        "snap.parquet row group {rg_idx} is missing statistics for bbox columns; spatial filtering will be slower"
                    ),
                ));
            }
        }

        for (rg_idx, &size) in snap.row_group_sizes.iter().enumerate() {
            if size < RG_SIZE_MIN || size > RG_SIZE_MAX {
                diags.push(Diagnostic::warning(
                    "schema.snap.rg_size",
                    Category::Schema,
                    Artifact::Snap,
                    format!(
                        "snap.parquet row group {rg_idx} has {size} rows; \
                         recommended range is [{RG_SIZE_MIN}, {RG_SIZE_MAX}]"
                    ),
                ));
            }
        }
    }

    // B6: atom_count in manifest matches catchments row count.
    if let (Some(manifest), Some(catchments)) = (&dataset.manifest, &dataset.catchments) {
        let declared = manifest.atom_count().get() as usize;
        let actual = catchments.row_count;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{CatchmentsData, FilePresenceMap, ParsedDataset, SnapData};
    use crate::diagnostic::Severity;

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

    fn build_manifest_with_atom_count(count: u64) -> hfx_core::Manifest {
        use hfx_core::{AtomCount, BoundingBox, Crs, FormatVersion, ManifestBuilder, Topology};
        use std::str::FromStr;

        let builder = ManifestBuilder::new(
            FormatVersion::from_str("0.1").unwrap(),
            "hydrobasins",
            Crs::from_str("EPSG:4326").unwrap(),
            Topology::from_str("tree").unwrap(),
            0,
            BoundingBox::new(-180.0, -90.0, 180.0, 90.0).unwrap(),
            AtomCount::new(count).unwrap(),
            "2026-01-01T00:00:00Z",
            "v1",
        )
        .unwrap();
        builder.build()
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
        dataset.catchments = Some(catchments_with_rg(vec![100], vec![false]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 2); // bbox_stats_missing + rg_size (100 < 4096)
        assert!(diags.iter().any(|d| d.check_id == "schema.catchments.bbox_stats_missing"));
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
    fn b5_rg_size_out_of_range_produces_warning() {
        let mut dataset = empty_dataset();
        // 100 < 4096 → out of range
        dataset.catchments = Some(catchments_with_rg(vec![100], vec![true]));
        let diags = check_schemas(&dataset);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "schema.catchments.rg_size");
        assert_eq!(diags[0].severity, Severity::Warning);
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
        // Manifest says 10, catchments has 3 rows
        dataset.manifest = Some(build_manifest_with_atom_count(10));
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![true]));
        // Override row_count
        let mut c = dataset.catchments.take().unwrap();
        c.row_count = 3;
        dataset.catchments = Some(c);
        let diags = check_schemas(&dataset);
        assert!(diags.iter().any(|d| d.check_id == "schema.atom_count_mismatch"));
        assert_eq!(diags.iter().find(|d| d.check_id == "schema.atom_count_mismatch").unwrap().severity, Severity::Error);
    }

    #[test]
    fn b6_atom_count_matches_no_error() {
        let mut dataset = empty_dataset();
        dataset.manifest = Some(build_manifest_with_atom_count(4096));
        dataset.catchments = Some(catchments_with_rg(vec![4096], vec![true]));
        let diags = check_schemas(&dataset);
        assert!(!diags.iter().any(|d| d.check_id == "schema.atom_count_mismatch"));
    }

    #[test]
    fn snap_b4_b5_checks_work() {
        let mut dataset = empty_dataset();
        dataset.snap = Some(SnapData {
            row_count: 100,
            ids: vec![],
            catchment_ids: vec![],
            weights: vec![],
            bboxes: vec![],
            geometry_wkb: vec![],
            row_group_sizes: vec![100],       // out of range
            row_group_has_bbox_stats: vec![false], // missing stats
        });
        let diags = check_schemas(&dataset);
        assert!(diags.iter().any(|d| d.check_id == "schema.snap.bbox_stats_missing"));
        assert!(diags.iter().any(|d| d.check_id == "schema.snap.rg_size"));
    }
}
