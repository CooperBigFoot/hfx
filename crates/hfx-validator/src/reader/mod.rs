//! I/O layer: reads HFX dataset files into the ParsedDataset intermediate representation.

pub mod catchments;
pub mod graph;
pub mod manifest;
pub mod raster;
pub mod schema;
pub mod snap;

use std::path::Path;

/// Maximum number of per-row null diagnostics emitted per column before
/// remaining violations are suppressed with a summary count.
pub(crate) const MAX_NULL_DIAGNOSTICS_PER_COLUMN: usize = 10;

/// Maximum number of consecutive batch-read failures before the reader
/// aborts with a summary diagnostic.
pub(crate) const MAX_CONSECUTIVE_BATCH_FAILURES: usize = 3;

/// Maximum total batch-read failures (across the entire file) before the
/// reader aborts. This catches intermittent corruption that the consecutive
/// counter misses.
pub(crate) const MAX_TOTAL_BATCH_FAILURES: usize = 10;

use crate::check::manifest::try_build_manifest;
use crate::dataset::{FilePresenceMap, ParsedDataset};

/// Read all files from a dataset directory and produce a ParsedDataset.
///
/// This function never panics. All I/O errors become diagnostics.
#[tracing::instrument(skip_all, fields(dir = %dir.display()))]
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

    // --- Catchments ---
    let catchments = if let Some(path) = &files.catchments_path {
        let (data, diags) = catchments::read_catchments(path);
        read_diagnostics.extend(diags);
        data
    } else {
        None
    };

    // --- Graph ---
    let graph = if let Some(path) = &files.graph_path {
        let (data, diags) = graph::read_graph(path);
        read_diagnostics.extend(diags);
        data
    } else {
        None
    };

    // --- Snap (optional — only read if has_snap is true or file present) ---
    let has_snap = raw_manifest
        .as_ref()
        .and_then(|r| r.has_snap)
        .unwrap_or(false);
    let snap = if has_snap {
        if let Some(path) = &files.snap_path {
            let (data, diags) = snap::read_snap(path);
            read_diagnostics.extend(diags);
            data
        } else {
            None
        }
    } else {
        None
    };

    // --- Rasters (only read if manifest declares has_rasters = true) ---
    let has_rasters = raw_manifest
        .as_ref()
        .and_then(|m| m.has_rasters)
        .unwrap_or(false);

    let mut flow_dir = None;
    let mut flow_acc = None;

    if has_rasters {
        if let Some(ref path) = files.flow_dir_path {
            let (meta, diags) = raster::read_raster_meta(path, "flow_dir.tif");
            read_diagnostics.extend(diags);
            flow_dir = meta;
        }
        if let Some(ref path) = files.flow_acc_path {
            let (meta, diags) = raster::read_raster_meta(path, "flow_acc.tif");
            read_diagnostics.extend(diags);
            flow_acc = meta;
        }
    }

    ParsedDataset {
        files,
        manifest_json,
        raw_manifest,
        manifest,
        catchments,
        graph,
        snap,
        flow_dir,
        flow_acc,
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
