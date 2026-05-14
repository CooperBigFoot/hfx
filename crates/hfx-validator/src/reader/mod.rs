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
    read_dataset_with_options(dir, false)
}

/// Read all files from a dataset directory with validator read options.
///
/// This function never panics. All I/O errors become diagnostics.
#[tracing::instrument(skip_all, fields(dir = %dir.display(), skip_rasters))]
pub fn read_dataset_with_options(dir: &Path, skip_rasters: bool) -> ParsedDataset {
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

    // --- Snap (optional; read when present) ---
    let snap = if let Some(path) = &files.snap_path {
        let (data, diags) = snap::read_snap(path);
        read_diagnostics.extend(diags);
        data
    } else {
        None
    };

    // --- Blessed D8 raster auxiliary (read when declared or legacy files present) ---
    let has_d8_aux = raw_manifest
        .as_ref()
        .and_then(|m| m.auxiliary.as_ref())
        .is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.schema.as_deref() == Some("hfx.aux.d8_raster.v1"))
        });
    let mut flow_dir = None;
    let mut flow_acc = None;

    if has_d8_aux && !skip_rasters {
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
        graph_path: check("graph.parquet"),
        legacy_graph_arrow_path: check("graph.arrow"),
        snap_path: check("snap.parquet"),
        flow_dir_path: check("flow_dir.tif"),
        flow_acc_path: check("flow_acc.tif"),
    }
}
