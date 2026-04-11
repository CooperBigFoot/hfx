//! I/O layer: reads HFX dataset files into the ParsedDataset intermediate representation.

pub mod manifest;
pub mod catchments;
pub mod graph;
pub mod snap;
pub mod raster;
pub mod schema;

use std::path::Path;

use crate::check::manifest::try_build_manifest;
use crate::dataset::{FilePresenceMap, ParsedDataset};

/// Read all files from a dataset directory and produce a ParsedDataset.
///
/// This function never panics. All I/O errors become diagnostics.
pub fn read_dataset(dir: &Path) -> ParsedDataset {
    let files = discover_files(dir);
    let mut read_diagnostics: Vec<crate::diagnostic::Diagnostic> = Vec::new();

    // --- Manifest ---
    let (manifest_json, raw_manifest, manifest_diags) = match &files.manifest_path {
        Some(path) => manifest::read_manifest(path),
        None => (None, None, vec![]),
    };
    read_diagnostics.extend(manifest_diags);

    let manifest = raw_manifest.as_ref().and_then(try_build_manifest);

    // TODO: Step 2 will implement catchments, graph, snap, raster reading

    ParsedDataset {
        files,
        manifest_json,
        raw_manifest,
        manifest,
        catchments: None,
        graph: None,
        snap: None,
        flow_dir: None,
        flow_acc: None,
        read_diagnostics,
    }
}

fn discover_files(dir: &Path) -> FilePresenceMap {
    let check = |name: &str| {
        let p = dir.join(name);
        if p.exists() { Some(p) } else { None }
    };

    FilePresenceMap {
        manifest_path: check("manifest.json"),
        catchments_path: check("catchments.parquet"),
        graph_path: check("graph.arrow"),
        snap_path: check("snap.parquet"),
        flow_dir_path: check("flow_dir.tif"),
        flow_acc_path: check("flow_acc.tif"),
    }
}
