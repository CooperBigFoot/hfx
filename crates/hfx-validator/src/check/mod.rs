//! Validation check modules.

pub mod file_presence;
pub mod manifest;
pub mod schema;
pub mod ids;
pub mod referential;
pub mod graph;
pub mod values;
pub mod geometry;
pub mod raster;

use crate::dataset::ParsedDataset;
use crate::diagnostic::Diagnostic;

/// Run all validation checks on a parsed dataset.
///
/// Checks are executed in phase order. Later phases may be skipped
/// if earlier phases indicate that required data is missing.
#[tracing::instrument(skip_all)]
pub fn run_checks(dataset: &ParsedDataset, _strict: bool, skip_rasters: bool, sample_pct: f64) -> Vec<Diagnostic> {
    let mut all = Vec::new();

    // Include any diagnostics from the read phase (B1/B2/B3 schema errors come from here).
    all.extend(dataset.read_diagnostics.iter().cloned());

    // Phase 1a: file presence
    let raw_manifest_ref = dataset.raw_manifest.as_ref();
    all.extend(file_presence::check_file_presence(&dataset.files, raw_manifest_ref));

    // Phase 1b: manifest field validation (only when successfully deserialized)
    if let Some(raw) = raw_manifest_ref {
        all.extend(manifest::check_manifest(raw));
    }

    // Phase 2: schema checks (B4-B6)
    all.extend(schema::check_schemas(dataset));

    // Phase 3: ID + value checks
    if let Some(ref catchments) = dataset.catchments {
        all.extend(ids::check_catchment_ids(catchments));
        all.extend(ids::check_catchment_bboxes(catchments));
        all.extend(ids::check_catchment_areas(catchments));

        if let Some(raw) = raw_manifest_ref {
            all.extend(values::check_up_area_consistency(raw, catchments));
        }
    }

    if let Some(ref graph) = dataset.graph {
        all.extend(ids::check_graph_ids(graph));
        all.extend(ids::check_upstream_ids(graph));
    }

    if let Some(ref snap) = dataset.snap {
        all.extend(ids::check_snap_data(snap));
    }

    // Phase 4: referential integrity
    if let (Some(catchments), Some(graph)) = (&dataset.catchments, &dataset.graph) {
        all.extend(referential::check_id_coverage(catchments, graph));
        all.extend(referential::check_upstream_refs(catchments, graph));
    }

    // D3 snap refs — only needs catchments + snap, not graph
    if let (Some(catchments), Some(snap)) = (&dataset.catchments, &dataset.snap) {
        all.extend(referential::check_snap_refs(catchments, snap));
    }

    // D4 bbox enclosure — only needs catchments + manifest, not graph
    if let Some(catchments) = &dataset.catchments {
        if let Some(raw) = raw_manifest_ref {
            all.extend(values::check_bbox_enclosure(raw, catchments));
        }
    }

    // Phase 5: graph structure
    if let Some(ref graph) = dataset.graph {
        all.extend(graph::check_acyclicity(graph));
    }

    // Phase 6: geometry
    if let Some(ref catchments) = dataset.catchments {
        all.extend(geometry::check_catchment_geometries(catchments, sample_pct));
    }

    if let Some(ref snap) = dataset.snap {
        all.extend(geometry::check_snap_geometries(snap));
    }

    // Phase 7: raster (skipped if skip_rasters is true)
    if !skip_rasters {
        let has_rasters = raw_manifest_ref
            .and_then(|m| m.has_rasters)
            .unwrap_or(false);

        if let Some(ref flow_dir_meta) = dataset.flow_dir {
            all.extend(raster::check_flow_dir(flow_dir_meta));
        }

        if let Some(ref flow_acc_meta) = dataset.flow_acc {
            all.extend(raster::check_flow_acc(flow_acc_meta));
        }

        if has_rasters {
            all.extend(raster::crs_extent_not_implemented());
        }
    }

    all
}
