//! File presence checks: verifies that required HFX artifact files exist on disk.

use tracing::debug;

use crate::dataset::FilePresenceMap;
use crate::diagnostic::{Artifact, Category, Diagnostic};
use crate::reader::manifest::RawManifest;

/// Check that all required artifact files are present given the discovered
/// `files` map and the (optionally parsed) manifest.
///
/// Rules:
/// - `catchments.parquet` is always required.
/// - `graph.arrow` is always required.
/// - `snap.parquet` is required when `manifest.has_snap == true`.
/// - `flow_dir.tif` and `flow_acc.tif` are both required when
///   `manifest.has_rasters == true`.
pub fn check_file_presence(
    files: &FilePresenceMap,
    raw_manifest: Option<&RawManifest>,
) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    // manifest.json — always required.
    if files.manifest_path.is_none() {
        diags.push(Diagnostic::error(
            "file_presence.manifest",
            Category::FilePresence,
            Artifact::Manifest,
            "manifest.json is missing from the dataset directory",
        ));
    }

    // catchments.parquet — always required.
    if files.catchments_path.is_none() {
        diags.push(Diagnostic::error(
            "file_presence.catchments",
            Category::FilePresence,
            Artifact::Catchments,
            "catchments.parquet is missing from the dataset directory",
        ));
    }

    // graph.arrow — always required.
    if files.graph_path.is_none() {
        diags.push(Diagnostic::error(
            "file_presence.graph",
            Category::FilePresence,
            Artifact::Graph,
            "graph.arrow is missing from the dataset directory",
        ));
    }

    // snap.parquet — required when manifest declares has_snap = true.
    if raw_manifest.and_then(|m| m.has_snap) == Some(true) && files.snap_path.is_none() {
        diags.push(Diagnostic::error(
            "file_presence.snap",
            Category::FilePresence,
            Artifact::Snap,
            "snap.parquet is missing but manifest declares has_snap = true",
        ));
    }

    // flow_dir.tif and flow_acc.tif — required when manifest declares has_rasters = true.
    if raw_manifest.and_then(|m| m.has_rasters) == Some(true) {
        if files.flow_dir_path.is_none() {
            diags.push(Diagnostic::error(
                "file_presence.flow_dir",
                Category::FilePresence,
                Artifact::FlowDir,
                "flow_dir.tif is missing but manifest declares has_rasters = true",
            ));
        }
        if files.flow_acc_path.is_none() {
            diags.push(Diagnostic::error(
                "file_presence.flow_acc",
                Category::FilePresence,
                Artifact::FlowAcc,
                "flow_acc.tif is missing but manifest declares has_rasters = true",
            ));
        }
    }

    debug!(count = diags.len(), "file presence checks complete");
    diags
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    /// Create a `FilePresenceMap` where every path is absent.
    fn empty_files() -> FilePresenceMap {
        FilePresenceMap {
            manifest_path: None,
            catchments_path: None,
            graph_path: None,
            snap_path: None,
            flow_dir_path: None,
            flow_acc_path: None,
        }
    }

    fn touch(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::File::create(&p).unwrap().write_all(b"").unwrap();
        p
    }

    fn errors_with_id(diags: &[Diagnostic], id: &str) -> usize {
        diags.iter().filter(|d| d.check_id == id).count()
    }

    // --- all required files present, no manifest ---

    #[test]
    fn all_required_files_present_no_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));

        // manifest_path is None → manifest check fires
        let diags = check_file_presence(&files, None);
        assert_eq!(errors_with_id(&diags, "file_presence.manifest"), 1);
        // no other errors beyond manifest
        assert_eq!(
            diags.len(),
            1,
            "expected only manifest error, got: {diags:#?}"
        );
    }

    #[test]
    fn all_required_files_present_with_manifest_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));

        let diags = check_file_presence(&files, None);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    // --- missing required files ---

    #[test]
    fn missing_manifest_produces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));
        // manifest_path intentionally absent

        let diags = check_file_presence(&files, None);
        assert_eq!(errors_with_id(&diags, "file_presence.manifest"), 1);
    }

    #[test]
    fn missing_catchments_produces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));

        let diags = check_file_presence(&files, None);
        assert_eq!(errors_with_id(&diags, "file_presence.catchments"), 1);
    }

    #[test]
    fn missing_graph_produces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));

        let diags = check_file_presence(&files, None);
        assert_eq!(errors_with_id(&diags, "file_presence.graph"), 1);
    }

    #[test]
    fn missing_both_required_produces_two_errors() {
        let diags = check_file_presence(&empty_files(), None);
        // manifest + catchments + graph are all missing
        assert_eq!(errors_with_id(&diags, "file_presence.manifest"), 1);
        assert_eq!(errors_with_id(&diags, "file_presence.catchments"), 1);
        assert_eq!(errors_with_id(&diags, "file_presence.graph"), 1);
    }

    // --- snap presence ---

    #[test]
    fn has_snap_true_but_file_missing_produces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));

        let raw = RawManifest {
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            has_rasters: Some(false),
            has_snap: Some(true),
            flow_dir_encoding: None,
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(1),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        };

        let diags = check_file_presence(&files, Some(&raw));
        assert_eq!(errors_with_id(&diags, "file_presence.snap"), 1);
    }

    #[test]
    fn has_snap_true_and_file_present_no_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));
        files.snap_path = Some(touch(dir.path(), "snap.parquet"));

        let raw = RawManifest {
            has_snap: Some(true),
            has_rasters: Some(false),
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            flow_dir_encoding: None,
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(1),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        };

        let diags = check_file_presence(&files, Some(&raw));
        let snap_errors: Vec<_> = diags
            .iter()
            .filter(|d| d.check_id == "file_presence.snap")
            .collect();
        assert!(snap_errors.is_empty());
    }

    #[test]
    fn has_snap_false_snap_file_not_required() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));
        // snap_path is None and that is fine

        let raw = RawManifest {
            has_snap: Some(false),
            has_rasters: Some(false),
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            flow_dir_encoding: None,
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(1),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        };

        let diags = check_file_presence(&files, Some(&raw));
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    // --- raster presence ---

    #[test]
    fn has_rasters_true_but_flow_dir_missing_produces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));
        files.flow_acc_path = Some(touch(dir.path(), "flow_acc.tif"));
        // flow_dir intentionally absent

        let raw = RawManifest {
            has_rasters: Some(true),
            has_snap: Some(false),
            flow_dir_encoding: Some("esri".into()),
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(1),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        };

        let diags = check_file_presence(&files, Some(&raw));
        assert_eq!(errors_with_id(&diags, "file_presence.flow_dir"), 1);
        assert_eq!(errors_with_id(&diags, "file_presence.flow_acc"), 0);
    }

    #[test]
    fn has_rasters_true_but_flow_acc_missing_produces_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));
        files.flow_dir_path = Some(touch(dir.path(), "flow_dir.tif"));
        // flow_acc intentionally absent

        let raw = RawManifest {
            has_rasters: Some(true),
            has_snap: Some(false),
            flow_dir_encoding: Some("esri".into()),
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(1),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        };

        let diags = check_file_presence(&files, Some(&raw));
        assert_eq!(errors_with_id(&diags, "file_presence.flow_acc"), 1);
        assert_eq!(errors_with_id(&diags, "file_presence.flow_dir"), 0);
    }

    #[test]
    fn has_rasters_false_raster_files_not_required() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));

        let raw = RawManifest {
            has_rasters: Some(false),
            has_snap: Some(false),
            flow_dir_encoding: None,
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area: Some(false),
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox: Some(vec![-180.0, -90.0, 180.0, 90.0]),
            atom_count: Some(1),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        };

        let diags = check_file_presence(&files, Some(&raw));
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn no_raw_manifest_raster_files_not_required() {
        let dir = tempfile::tempdir().unwrap();
        let mut files = empty_files();
        files.manifest_path = Some(touch(dir.path(), "manifest.json"));
        files.catchments_path = Some(touch(dir.path(), "catchments.parquet"));
        files.graph_path = Some(touch(dir.path(), "graph.arrow"));

        // raw_manifest is None but manifest_path is present — raster files are optional
        let diags = check_file_presence(&files, None);
        assert!(
            diags.is_empty(),
            "expected no diagnostics when raw manifest is None"
        );
    }
}
