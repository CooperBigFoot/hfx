//! I/O layer: reads HFX dataset files into the ParsedDataset intermediate representation.

pub mod manifest;
pub mod catchments;
pub mod graph;
pub mod snap;
pub mod raster;
pub mod schema;

use std::path::Path;

use crate::dataset::{FilePresenceMap, ParsedDataset};

/// Read all files from a dataset directory and produce a ParsedDataset.
///
/// This function never panics. All I/O errors become diagnostics.
pub fn read_dataset(dir: &Path) -> ParsedDataset {
    let files = discover_files(dir);

    // TODO: Step 1 will implement manifest reading
    // TODO: Step 2 will implement catchments, graph, snap, raster reading

    ParsedDataset {
        files,
        manifest_json: None,
        manifest: None,
        catchments: None,
        graph: None,
        snap: None,
        flow_dir: None,
        flow_acc: None,
        read_diagnostics: Vec::new(),
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
