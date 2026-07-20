//! Validation check modules.

pub mod auxiliary;
pub mod file_presence;
pub mod geometry;
pub mod graph;
pub mod ids;
pub mod levels;
pub mod manifest;
pub mod ordering;
pub mod parent;
pub mod raster;
pub mod referential;
pub mod schema;
pub mod values;

use crate::dataset::ParsedDataset;
use crate::diagnostic::Diagnostic;

/// Run all validation checks on a parsed dataset.
///
/// Checks are executed in phase order. Later phases may be skipped
/// if earlier phases indicate that required data is missing.
#[tracing::instrument(skip_all)]
pub fn run_checks(
    dataset: &ParsedDataset,
    _strict: bool,
    skip_rasters: bool,
    sample_pct: f64,
) -> Vec<Diagnostic> {
    let mut all = Vec::new();

    // Include any diagnostics from the read phase (B1/B2/B3 schema errors come from here).
    all.extend(dataset.read_diagnostics.iter().cloned());

    // Phase 1a: file presence
    let raw_manifest_ref = dataset.raw_manifest.as_ref();
    all.extend(file_presence::check_file_presence(
        &dataset.files,
        raw_manifest_ref,
    ));

    // Phase 1b: manifest field validation (only when successfully deserialized)
    if let Some(raw) = raw_manifest_ref {
        all.extend(manifest::check_manifest(raw));
        if let Some(manifest_path) = &dataset.files.manifest_path
            && let Some(root) = manifest_path.parent()
        {
            all.extend(auxiliary::check_auxiliary(
                raw,
                root,
                dataset.catchments.as_ref(),
            ));
        }
    }

    // Phase 2: schema checks (B4-B6)
    all.extend(schema::check_schemas(dataset));
    let _deferred_hilbert_diagnostics = ordering::deferred_hilbert_diagnostics();

    if let Some(ref catchments) = dataset.catchments {
        all.extend(ordering::check_catchments_ordering(catchments));
    }

    if let Some(ref graph) = dataset.graph {
        all.extend(ordering::check_graph_ordering(graph));
    }

    // Phase 3: ID + value checks
    if let Some(ref catchments) = dataset.catchments {
        all.extend(ids::check_unit_ids(catchments));
        all.extend(ids::check_catchment_bboxes(catchments));
        all.extend(ids::check_catchment_areas(catchments));
        all.extend(values::check_outlet_coords(catchments));

        if let Some(raw) = raw_manifest_ref {
            all.extend(values::check_up_area_consistency(raw, catchments));
        }
    }

    if let Some(ref graph) = dataset.graph {
        all.extend(ids::check_graph_ids(graph));
        all.extend(ids::check_graph_bboxes(graph));
        all.extend(ids::check_upstream_ids(graph));
    }

    // Phase 4: referential integrity
    if let (Some(catchments), Some(graph)) = (&dataset.catchments, &dataset.graph) {
        all.extend(referential::check_id_coverage(catchments, graph));
        all.extend(referential::check_upstream_refs(catchments, graph));
        all.extend(levels::check_level_consistency(catchments, graph));
    }

    if let Some(catchments) = &dataset.catchments {
        all.extend(parent::check_parent_forest(catchments));
    }

    // D4 bbox enclosure — only needs catchments + manifest, not graph
    if let Some(catchments) = &dataset.catchments
        && let Some(raw) = raw_manifest_ref
    {
        all.extend(values::check_bbox_enclosure(raw, catchments));
    }

    // Phase 5: graph structure
    if let Some(ref graph) = dataset.graph {
        all.extend(graph::check_acyclicity(graph));
    }

    // Phase 6: geometry
    if let Some(ref catchments) = dataset.catchments {
        all.extend(geometry::check_catchment_geometries(catchments, sample_pct));
    }

    // Phase 7: raster (skipped if skip_rasters is true)
    if !skip_rasters && !dataset.d8_rasters.is_empty() {
        let manifest_ref = dataset.manifest.as_ref();

        for entry in &dataset.d8_rasters {
            all.extend(label_diagnostics(
                &entry.name,
                raster::check_crs_consistency(entry),
            ));

            if let Some(ref flow_dir_meta) = entry.flow_dir {
                all.extend(label_diagnostics(
                    &entry.name,
                    raster::check_flow_dir(flow_dir_meta),
                ));
                if let Some(manifest) = manifest_ref {
                    all.extend(label_diagnostics(
                        &entry.name,
                        raster::check_spatial_consistency(
                            flow_dir_meta,
                            manifest,
                            crate::diagnostic::Artifact::FlowDir,
                        ),
                    ));
                }
            }

            if let Some(ref flow_acc_meta) = entry.flow_acc {
                all.extend(label_diagnostics(
                    &entry.name,
                    raster::check_flow_acc(flow_acc_meta, entry.metadata.flow_acc_units()),
                ));
                if let Some(manifest) = manifest_ref {
                    all.extend(label_diagnostics(
                        &entry.name,
                        raster::check_spatial_consistency(
                            flow_acc_meta,
                            manifest,
                            crate::diagnostic::Artifact::FlowAcc,
                        ),
                    ));
                }
            }
        }
    }

    all
}

fn label_diagnostics(label: &str, mut diagnostics: Vec<Diagnostic>) -> Vec<Diagnostic> {
    for diagnostic in &mut diagnostics {
        if !diagnostic.message.starts_with(label) {
            diagnostic.message = format!("{label}: {}", std::mem::take(&mut diagnostic.message));
        }
    }
    diagnostics
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::dataset::{
        D8RasterEntry, FilePresenceMap, ParsedDataset, RasterMeta, RasterSampleFormat,
    };
    use crate::diagnostic::{Artifact, Severity};

    use super::run_checks;

    fn base_dataset(d8_rasters: Vec<D8RasterEntry>) -> ParsedDataset {
        ParsedDataset {
            files: FilePresenceMap {
                manifest_path: Some(PathBuf::from("manifest.json")),
                catchments_path: Some(PathBuf::from("catchments.parquet")),
                graph_path: Some(PathBuf::from("graph.parquet")),
                legacy_graph_arrow_path: None,
                snap_path: None,
                d8_rasters: d8_rasters.clone(),
            },
            manifest_json: None,
            raw_manifest: None,
            manifest: None,
            catchments: None,
            graph: None,
            d8_rasters,
            read_diagnostics: Vec::new(),
        }
    }

    fn flow_dir_meta(bits_per_sample: u16, sample_format: RasterSampleFormat) -> RasterMeta {
        RasterMeta {
            path: PathBuf::from("flow_dir.tif"),
            width: 1,
            height: 1,
            bits_per_sample,
            sample_format,
            is_tiled: true,
            tile_width: Some(256),
            tile_height: Some(256),
            nodata: Some(255.0),
            spatial_ref: None,
            bbox_wgs84: None,
            pixel_width: None,
            pixel_height: None,
        }
    }

    fn d8_entry(name: &str, flow_dir: RasterMeta) -> D8RasterEntry {
        D8RasterEntry {
            name: name.to_owned(),
            metadata: hfx::D8RasterMetadataV2::parse(
                Some("EPSG:4326"),
                Some("esri"),
                Some("cells"),
            )
            .unwrap(),
            flow_dir_artifact: Some(format!("aux/d8/{name}/flow_dir.tif")),
            flow_acc_artifact: None,
            flow_dir_path: Some(PathBuf::from(format!("aux/d8/{name}/flow_dir.tif"))),
            flow_acc_path: None,
            flow_dir: Some(flow_dir),
            flow_acc: None,
        }
    }

    #[test]
    fn malformed_second_d8_entry_reports_that_entry_name() {
        let dataset = base_dataset(vec![
            d8_entry("first", flow_dir_meta(8, RasterSampleFormat::UnsignedInt)),
            d8_entry("bad_second", flow_dir_meta(32, RasterSampleFormat::Float)),
        ]);

        let diagnostics = run_checks(&dataset, false, false, 100.0);

        assert!(diagnostics.iter().any(|diag| {
            diag.check_id == "raster.flow_dir_dtype"
                && diag.severity == Severity::Error
                && diag.artifact == Artifact::FlowDir
                && diag.message.contains("bad_second:")
        }));
        assert!(!diagnostics.iter().any(|diag| {
            diag.check_id == "raster.flow_dir_dtype" && diag.message.contains("first:")
        }));
    }
}
