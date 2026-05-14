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
/// - `graph.parquet` is always required.
/// - `snap.parquet` is optional.
/// - Auxiliary artifact files are required when referenced by `manifest.json`.
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

    // graph.parquet — always required.
    if files.graph_path.is_none() {
        diags.push(Diagnostic::error(
            "file_presence.graph",
            Category::FilePresence,
            Artifact::Graph,
            "graph.parquet is missing from the dataset directory",
        ));
    }

    if files.graph_path.is_none() && files.legacy_graph_arrow_path.is_some() {
        diags.push(Diagnostic::error(
            "graph.legacy_arrow_format",
            Category::FilePresence,
            Artifact::Graph,
            "dataset contains graph.arrow but HFX v0.2 requires graph.parquet",
        ));
    }

    if let Some(aux_entries) = raw_manifest.and_then(|m| m.auxiliary.as_ref()) {
        for entry in aux_entries {
            if entry.schema.as_deref() == Some("hfx.aux.d8_raster.v1") {
                if entry
                    .artifacts
                    .as_ref()
                    .and_then(|a| a.get("flow_dir"))
                    .is_some()
                    && files.flow_dir_path.is_none()
                {
                    diags.push(Diagnostic::error(
                        "file_presence.flow_dir",
                        Category::FilePresence,
                        Artifact::FlowDir,
                        "flow_dir auxiliary artifact is declared but missing",
                    ));
                }
                if entry
                    .artifacts
                    .as_ref()
                    .and_then(|a| a.get("flow_acc"))
                    .is_some()
                    && files.flow_acc_path.is_none()
                {
                    diags.push(Diagnostic::error(
                        "file_presence.flow_acc",
                        Category::FilePresence,
                        Artifact::FlowAcc,
                        "flow_acc auxiliary artifact is declared but missing",
                    ));
                }
            }
        }
    }

    debug!(count = diags.len(), "file presence checks complete");
    diags
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
