//! Raster metadata checks (G1, G2, G3).
//!
//! These checks operate on [`RasterMeta`] values that have already been read
//! from TIFF headers and GDAL spatial metadata by [`crate::reader::raster`].

use std::path::PathBuf;

use hfx::{FlowAccumulationUnits, Manifest};

use crate::dataset::{D8RasterEntry, RasterBoundingBox, RasterMeta, RasterSampleFormat};
use crate::diagnostic::{Artifact, Category, Diagnostic};

/// Raster spatial validation failures required by the HFX spec.
#[derive(Debug, thiserror::Error)]
pub enum RasterSpatialCheckError {
    /// Returned when a raster CRS does not match the D8 auxiliary declaration.
    #[error("raster CRS mismatch for {path}: expected {expected}, got {got}")]
    RasterCrsMismatch {
        /// Path to the raster being validated.
        path: PathBuf,
        /// CRS declared by the D8 auxiliary entry.
        expected: String,
        /// CRS extracted from the raster.
        got: String,
    },

    /// Returned when the two rasters in one D8 entry resolve to different CRSs.
    #[error(
        "D8 raster header CRS mismatch: flow_dir {flow_dir_path} is {flow_dir_crs}, flow_acc {flow_acc_path} is {flow_acc_crs}"
    )]
    RasterPairCrsMismatch {
        /// Path to the flow-direction raster.
        flow_dir_path: PathBuf,
        /// Canonical flow-direction header CRS.
        flow_dir_crs: String,
        /// Path to the flow-accumulation raster.
        flow_acc_path: PathBuf,
        /// Canonical flow-accumulation header CRS.
        flow_acc_crs: String,
    },

    /// Returned when a raster footprint does not overlap the manifest bbox.
    #[error(
        "raster extent does not overlap manifest bbox for {path}: raster_bbox={raster_bbox}, manifest_bbox={manifest_bbox}"
    )]
    RasterExtentNoOverlap {
        /// Path to the raster being validated.
        path: PathBuf,
        /// Bounding box enclosing the densified raster footprint in EPSG:4326.
        raster_bbox: RasterBoundingBox,
        /// Bounding box declared by the manifest.
        manifest_bbox: RasterBoundingBox,
    },
}

/// G1: Validate `flow_dir.tif` structural properties.
///
/// Required: an eight-bit integer sample format, a nodata tag,
/// and the file must be COG-tiled (`is_tiled == true`).
#[tracing::instrument(skip_all, fields(path = %meta.path.display()))]
pub fn check_flow_dir(meta: &RasterMeta) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    if !is_flow_dir_dtype(meta) {
        diags.push(Diagnostic::error(
            "raster.flow_dir_dtype",
            Category::Raster,
            Artifact::FlowDir,
            format!(
                "flow_dir.tif must be uint8 or int8 but found {}-bit {}",
                meta.bits_per_sample,
                sample_format_label(meta.sample_format),
            ),
        ));
    }

    if !meta.is_tiled {
        diags.push(Diagnostic::error(
            "raster.flow_dir_not_tiled",
            Category::Raster,
            Artifact::FlowDir,
            "flow_dir.tif must be COG-tiled (TileWidth / TileLength tags present)",
        ));
    }

    if meta.nodata.is_none() {
        diags.push(Diagnostic::error(
            "raster.flow_dir_nodata",
            Category::Raster,
            Artifact::FlowDir,
            "flow_dir.tif is missing a nodata tag",
        ));
    }

    diags
}

/// G2: Validate `flow_acc.tif` structural properties.
///
/// Required: an allowed 32-bit sample format, a nodata tag,
/// and the file must be COG-tiled (`is_tiled == true`).
#[tracing::instrument(skip_all, fields(path = %meta.path.display()))]
pub fn check_flow_acc(meta: &RasterMeta, units: FlowAccumulationUnits) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    if !is_flow_acc_dtype(meta) {
        diags.push(Diagnostic::error(
            "raster.flow_acc_dtype",
            Category::Raster,
            Artifact::FlowAcc,
            format!(
                "flow_acc.tif must be float32 or int32 but found {}-bit {}",
                meta.bits_per_sample,
                sample_format_label(meta.sample_format),
            ),
        ));
    }

    if !meta.is_tiled {
        diags.push(Diagnostic::error(
            "raster.flow_acc_not_tiled",
            Category::Raster,
            Artifact::FlowAcc,
            "flow_acc.tif must be COG-tiled (TileWidth / TileLength tags present)",
        ));
    }

    if meta.nodata.is_none() {
        diags.push(Diagnostic::error(
            "raster.flow_acc_nodata",
            Category::Raster,
            Artifact::FlowAcc,
            "flow_acc.tif is missing a nodata tag",
        ));
    }

    if units == FlowAccumulationUnits::Cells && is_int32(meta) {
        diags.push(Diagnostic::error(
            "raster.flow_acc_units_dtype",
            Category::Raster,
            Artifact::FlowAcc,
            "flow_acc_units cells requires float32 but flow_acc.tif is int32",
        ));
    }

    diags
}

/// Validate raster header authorities against the D8 auxiliary declaration and each other.
#[tracing::instrument(skip_all, fields(entry = %entry.name))]
pub fn check_crs_consistency(entry: &D8RasterEntry) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let expected = entry.metadata.crs().as_str();

    for (meta, artifact) in [
        (entry.flow_dir.as_ref(), Artifact::FlowDir),
        (entry.flow_acc.as_ref(), Artifact::FlowAcc),
    ] {
        if let Some(meta) = meta
            && let Some(got) = meta.spatial_ref.as_deref()
            && got != expected
        {
            let error = RasterSpatialCheckError::RasterCrsMismatch {
                path: meta.path.clone(),
                expected: expected.to_owned(),
                got: got.to_owned(),
            };
            diags.push(Diagnostic::error(
                "raster.crs_mismatch",
                Category::Raster,
                artifact,
                error.to_string(),
            ));
        }
    }

    if let (Some(flow_dir), Some(flow_acc)) = (&entry.flow_dir, &entry.flow_acc)
        && let (Some(flow_dir_crs), Some(flow_acc_crs)) =
            (&flow_dir.spatial_ref, &flow_acc.spatial_ref)
        && flow_dir_crs != flow_acc_crs
    {
        let error = RasterSpatialCheckError::RasterPairCrsMismatch {
            flow_dir_path: flow_dir.path.clone(),
            flow_dir_crs: flow_dir_crs.clone(),
            flow_acc_path: flow_acc.path.clone(),
            flow_acc_crs: flow_acc_crs.clone(),
        };
        diags.push(Diagnostic::error(
            "raster.crs_pair_mismatch",
            Category::Raster,
            Artifact::CrossFile,
            error.to_string(),
        ));
    }

    diags
}

/// G3: Validate the geographic raster footprint against the manifest bbox.
#[tracing::instrument(skip_all, fields(path = %meta.path.display(), artifact = %artifact))]
pub fn check_spatial_consistency(
    meta: &RasterMeta,
    manifest: &Manifest,
    artifact: Artifact,
) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    if let Some(raster_bbox) = meta.bbox_wgs84.as_ref() {
        let manifest_bbox = RasterBoundingBox::from_manifest_bbox(manifest.bbox());
        if !raster_bbox.overlaps_with_epsilon(&manifest_bbox, 0.0) {
            let error = RasterSpatialCheckError::RasterExtentNoOverlap {
                path: meta.path.clone(),
                raster_bbox: raster_bbox.clone(),
                manifest_bbox,
            };
            diags.push(Diagnostic::error(
                "raster.extent_no_overlap",
                Category::Raster,
                artifact,
                error.to_string(),
            ));
        }
    }

    diags
}

fn is_flow_dir_dtype(meta: &RasterMeta) -> bool {
    meta.bits_per_sample == 8
        && matches!(
            meta.sample_format,
            RasterSampleFormat::UnsignedInt | RasterSampleFormat::SignedInt
        )
}

fn is_flow_acc_dtype(meta: &RasterMeta) -> bool {
    meta.bits_per_sample == 32
        && matches!(
            meta.sample_format,
            RasterSampleFormat::Float | RasterSampleFormat::SignedInt
        )
}

fn is_int32(meta: &RasterMeta) -> bool {
    meta.bits_per_sample == 32 && meta.sample_format == RasterSampleFormat::SignedInt
}

fn sample_format_label(fmt: RasterSampleFormat) -> &'static str {
    match fmt {
        RasterSampleFormat::UnsignedInt => "unsigned-int",
        RasterSampleFormat::SignedInt => "signed-int",
        RasterSampleFormat::Float => "float",
        RasterSampleFormat::Unknown(_) => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use hfx::{D8RasterMetadataV2, FlowAccumulationUnits};

    use crate::check::manifest::try_build_manifest;
    use crate::dataset::{D8RasterEntry, RasterBoundingBox, RasterMeta, RasterSampleFormat};
    use crate::diagnostic::Artifact;
    use crate::reader::manifest::RawManifest;

    use super::{check_crs_consistency, check_flow_acc, check_flow_dir, check_spatial_consistency};

    fn meta(bits_per_sample: u16, sample_format: RasterSampleFormat) -> RasterMeta {
        RasterMeta {
            path: PathBuf::from("raster.tif"),
            width: 256,
            height: 256,
            bits_per_sample,
            sample_format,
            is_tiled: true,
            tile_width: Some(256),
            tile_height: Some(256),
            nodata: Some(7.0),
            spatial_ref: Some("EPSG:4326".to_owned()),
            bbox_wgs84: Some(RasterBoundingBox::new(0.0, 0.0, 2.0, 2.0)),
            pixel_width: Some(1.0),
            pixel_height: Some(1.0),
        }
    }

    fn entry(declared_crs: &str, flow_dir_crs: &str, flow_acc_crs: &str) -> D8RasterEntry {
        let mut flow_dir = meta(8, RasterSampleFormat::UnsignedInt);
        flow_dir.path = PathBuf::from("flow_dir.tif");
        flow_dir.spatial_ref = Some(flow_dir_crs.to_owned());
        let mut flow_acc = meta(32, RasterSampleFormat::Float);
        flow_acc.path = PathBuf::from("flow_acc.tif");
        flow_acc.spatial_ref = Some(flow_acc_crs.to_owned());
        D8RasterEntry {
            name: "test".to_owned(),
            metadata: D8RasterMetadataV2::parse(Some(declared_crs), Some("esri"), Some("cells"))
                .unwrap(),
            flow_dir_artifact: Some("flow_dir.tif".to_owned()),
            flow_acc_artifact: Some("flow_acc.tif".to_owned()),
            flow_dir_path: Some(PathBuf::from("flow_dir.tif")),
            flow_acc_path: Some(PathBuf::from("flow_acc.tif")),
            flow_dir: Some(flow_dir),
            flow_acc: Some(flow_acc),
        }
    }

    fn manifest() -> hfx::Manifest {
        let raw: RawManifest = serde_json::from_value(serde_json::json!({
            "format_version": "0.3.0",
            "fabric_name": "test",
            "crs": "EPSG:4326",
            "topology": "tree",
            "has_up_area": false,
            "bbox": [0.0, 0.0, 2.0, 2.0],
            "unit_count": 1,
            "created_at": "2026-01-01T00:00:00Z",
            "adapter_version": "test"
        }))
        .unwrap();
        try_build_manifest(&raw).unwrap()
    }

    fn has(diags: &[crate::diagnostic::Diagnostic], check_id: &str) -> bool {
        diags.iter().any(|diag| diag.check_id == check_id)
    }

    #[test]
    fn flow_dir_enforces_dtype_table_and_nodata_presence() {
        for format in [
            RasterSampleFormat::UnsignedInt,
            RasterSampleFormat::SignedInt,
        ] {
            assert!(!has(
                &check_flow_dir(&meta(8, format)),
                "raster.flow_dir_dtype"
            ));
        }
        assert!(has(
            &check_flow_dir(&meta(32, RasterSampleFormat::Float)),
            "raster.flow_dir_dtype"
        ));
        assert!(!has(
            &check_flow_dir(&meta(8, RasterSampleFormat::UnsignedInt)),
            "raster.flow_dir_nodata"
        ));
        let mut missing = meta(8, RasterSampleFormat::UnsignedInt);
        missing.nodata = None;
        assert!(has(&check_flow_dir(&missing), "raster.flow_dir_nodata"));
    }

    #[test]
    fn flow_acc_enforces_general_dtype_table_and_nodata_presence() {
        for format in [RasterSampleFormat::Float, RasterSampleFormat::SignedInt] {
            assert!(!has(
                &check_flow_acc(&meta(32, format), FlowAccumulationUnits::Km2),
                "raster.flow_acc_dtype"
            ));
        }
        assert!(has(
            &check_flow_acc(
                &meta(16, RasterSampleFormat::UnsignedInt),
                FlowAccumulationUnits::Km2,
            ),
            "raster.flow_acc_dtype"
        ));
        assert!(!has(
            &check_flow_acc(
                &meta(32, RasterSampleFormat::Float),
                FlowAccumulationUnits::Cells,
            ),
            "raster.flow_acc_nodata"
        ));
        let mut missing = meta(32, RasterSampleFormat::Float);
        missing.nodata = None;
        assert!(has(
            &check_flow_acc(&missing, FlowAccumulationUnits::Cells),
            "raster.flow_acc_nodata"
        ));
    }

    #[test]
    fn flow_acc_units_narrow_the_general_dtype_table() {
        assert!(
            check_flow_acc(
                &meta(32, RasterSampleFormat::Float),
                FlowAccumulationUnits::Cells,
            )
            .is_empty()
        );
        let cells_int = check_flow_acc(
            &meta(32, RasterSampleFormat::SignedInt),
            FlowAccumulationUnits::Cells,
        );
        assert_eq!(cells_int.len(), 1);
        assert_eq!(cells_int[0].check_id, "raster.flow_acc_units_dtype");
        for format in [RasterSampleFormat::SignedInt, RasterSampleFormat::Float] {
            assert!(check_flow_acc(&meta(32, format), FlowAccumulationUnits::Km2).is_empty());
        }
    }

    #[test]
    fn entry_crs_checks_declaration_and_pair_authorities() {
        assert!(check_crs_consistency(&entry("EPSG:4326", "EPSG:4326", "EPSG:4326")).is_empty());

        let declaration = check_crs_consistency(&entry("EPSG:8857", "EPSG:4326", "EPSG:8857"));
        assert!(declaration.iter().any(|diag| {
            diag.check_id == "raster.crs_mismatch" && diag.artifact == Artifact::FlowDir
        }));

        let pair = check_crs_consistency(&entry("EPSG:4326", "EPSG:4326", "EPSG:8857"));
        assert!(pair.iter().any(|diag| {
            diag.check_id == "raster.crs_pair_mismatch" && diag.artifact == Artifact::CrossFile
        }));
    }

    #[test]
    fn geographic_extent_preserves_overlap_diagnostic() {
        let manifest = manifest();
        assert!(
            check_spatial_consistency(
                &meta(32, RasterSampleFormat::Float),
                &manifest,
                Artifact::FlowAcc,
            )
            .is_empty()
        );
        let mut disjoint = meta(32, RasterSampleFormat::Float);
        disjoint.bbox_wgs84 = Some(RasterBoundingBox::new(10.0, 10.0, 12.0, 12.0));
        assert!(has(
            &check_spatial_consistency(&disjoint, &manifest, Artifact::FlowAcc),
            "raster.extent_no_overlap"
        ));
    }
}
