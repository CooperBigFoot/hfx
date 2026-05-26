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
use crate::dataset::{D8RasterEntry, FilePresenceMap, ParsedDataset};
use crate::reader::manifest::RawAuxEntry;

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

    let mut d8_rasters = raw_manifest
        .as_ref()
        .map(|manifest| discover_d8_rasters(dir, manifest))
        .unwrap_or_default();

    if !skip_rasters {
        for entry in &mut d8_rasters {
            if let Some(ref path) = entry.flow_dir_path {
                let (meta, diags) = raster::read_raster_meta(path, "flow_dir.tif");
                read_diagnostics.extend(label_diagnostics(&entry.name, diags));
                entry.flow_dir = meta;
            }
            if let Some(ref path) = entry.flow_acc_path {
                let (meta, diags) = raster::read_raster_meta(path, "flow_acc.tif");
                read_diagnostics.extend(label_diagnostics(&entry.name, diags));
                entry.flow_acc = meta;
            }
        }
    }

    let mut files = files;
    files.d8_rasters = d8_rasters.clone();

    ParsedDataset {
        files,
        manifest_json,
        raw_manifest,
        manifest,
        catchments,
        graph,
        d8_rasters,
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
        d8_rasters: Vec::new(),
    }
}

fn discover_d8_rasters(dir: &Path, manifest: &manifest::RawManifest) -> Vec<D8RasterEntry> {
    manifest
        .auxiliary
        .as_deref()
        .into_iter()
        .flatten()
        .enumerate()
        .filter(|(_, entry)| entry.schema.as_deref() == Some("hfx.aux.d8_raster.v1"))
        .map(|(idx, entry)| {
            let flow_dir_artifact = entry
                .artifacts
                .as_ref()
                .and_then(|artifacts| artifacts.get("flow_dir"))
                .cloned();
            let flow_acc_artifact = entry
                .artifacts
                .as_ref()
                .and_then(|artifacts| artifacts.get("flow_acc"))
                .cloned();
            let name = d8_raster_label(idx, entry, flow_dir_artifact.as_deref());

            D8RasterEntry {
                name,
                flow_dir_path: existing_safe_artifact_path(dir, flow_dir_artifact.as_deref()),
                flow_acc_path: existing_safe_artifact_path(dir, flow_acc_artifact.as_deref()),
                flow_dir_artifact,
                flow_acc_artifact,
                flow_dir: None,
                flow_acc: None,
            }
        })
        .collect()
}

fn existing_safe_artifact_path(dir: &Path, rel_path: Option<&str>) -> Option<std::path::PathBuf> {
    let rel_path = rel_path?;
    if rel_path.starts_with('/') || rel_path.contains("..") {
        return None;
    }

    let path = dir.join(rel_path);
    path.exists().then_some(path)
}

fn d8_raster_label(idx: usize, entry: &RawAuxEntry, flow_dir_artifact: Option<&str>) -> String {
    if let Some(name) = entry
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("name"))
        .and_then(serde_json::Value::as_str)
    {
        return name.to_owned();
    }

    if let Some(parent_name) = flow_dir_artifact
        .and_then(|path| Path::new(path).parent())
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        && !parent_name.is_empty()
    {
        return parent_name.to_owned();
    }

    format!("auxiliary[{idx}]")
}

fn label_diagnostics(
    label: &str,
    mut diagnostics: Vec<crate::diagnostic::Diagnostic>,
) -> Vec<crate::diagnostic::Diagnostic> {
    for diagnostic in &mut diagnostics {
        if !diagnostic.message.starts_with(label) {
            diagnostic.message = format!("{label}: {}", std::mem::take(&mut diagnostic.message));
        }
    }
    diagnostics
}
