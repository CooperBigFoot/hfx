//! Raster metadata checks (G1, G2, G3).
//!
//! These checks operate on [`RasterMeta`] values that have already been read
//! from TIFF headers and GDAL spatial metadata by [`crate::reader::raster`].

use std::path::PathBuf;

use hfx_core::Manifest;

use crate::dataset::{RasterBoundingBox, RasterMeta, RasterSampleFormat};
use crate::diagnostic::{Artifact, Category, Diagnostic};

/// Raster spatial validation failures required by the HFX spec.
#[derive(Debug, thiserror::Error)]
pub enum RasterSpatialCheckError {
    /// Returned when a raster CRS does not match the manifest CRS.
    #[error("raster CRS mismatch for {path}: expected {expected}, got {got}")]
    RasterCrsMismatch {
        /// Path to the raster being validated.
        path: PathBuf,
        /// CRS declared by the manifest.
        expected: String,
        /// CRS extracted from the raster.
        got: String,
    },

    /// Returned when a raster footprint does not fully contain the manifest bbox.
    #[error(
        "raster extent does not contain manifest bbox for {path}: raster_bbox={raster_bbox}, manifest_bbox={manifest_bbox}"
    )]
    RasterExtentNotContained {
        /// Path to the raster being validated.
        path: PathBuf,
        /// Bounding box derived from the raster geotransform.
        raster_bbox: RasterBoundingBox,
        /// Bounding box declared by the manifest.
        manifest_bbox: RasterBoundingBox,
    },
}

/// G1: Validate `flow_dir.tif` structural properties.
///
/// Required: `bits_per_sample == 8`, `sample_format == UnsignedInt`,
/// and the file must be COG-tiled (`is_tiled == true`).
#[tracing::instrument(skip_all, fields(path = %meta.path.display()))]
pub fn check_flow_dir(meta: &RasterMeta) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    if meta.bits_per_sample != 8 || meta.sample_format != RasterSampleFormat::UnsignedInt {
        diags.push(Diagnostic::error(
            "raster.flow_dir_dtype",
            Category::Raster,
            Artifact::FlowDir,
            format!(
                "flow_dir.tif must be uint8 but found {}‑bit {}",
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

    match meta.nodata {
        Some(nodata) if (nodata - 255.0).abs() > f64::EPSILON => {
            diags.push(Diagnostic::error(
                "raster.flow_dir_nodata",
                Category::Raster,
                Artifact::FlowDir,
                format!("flow_dir.tif nodata must be 255, got {nodata}"),
            ));
        }
        None => {
            diags.push(Diagnostic::error(
                "raster.flow_dir_nodata",
                Category::Raster,
                Artifact::FlowDir,
                "flow_dir.tif is missing a nodata value; spec requires 255",
            ));
        }
        _ => {}
    }

    diags
}

/// G2: Validate `flow_acc.tif` structural properties.
///
/// Required: `bits_per_sample == 32`, `sample_format == Float`,
/// and the file must be COG-tiled (`is_tiled == true`).
#[tracing::instrument(skip_all, fields(path = %meta.path.display()))]
pub fn check_flow_acc(meta: &RasterMeta) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    if meta.bits_per_sample != 32 || meta.sample_format != RasterSampleFormat::Float {
        diags.push(Diagnostic::error(
            "raster.flow_acc_dtype",
            Category::Raster,
            Artifact::FlowAcc,
            format!(
                "flow_acc.tif must be float32 but found {}‑bit {}",
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

    match meta.nodata {
        Some(nodata) if (nodata - (-1.0)).abs() > f64::EPSILON => {
            diags.push(Diagnostic::error(
                "raster.flow_acc_nodata",
                Category::Raster,
                Artifact::FlowAcc,
                format!("flow_acc.tif nodata must be -1.0, got {nodata}"),
            ));
        }
        None => {
            diags.push(Diagnostic::error(
                "raster.flow_acc_nodata",
                Category::Raster,
                Artifact::FlowAcc,
                "flow_acc.tif is missing a nodata value; spec requires -1.0",
            ));
        }
        _ => {}
    }

    diags
}

/// G3: Validate raster CRS and spatial extent against the manifest.
#[tracing::instrument(skip_all, fields(path = %meta.path.display(), artifact = %artifact))]
pub fn check_spatial_consistency(
    meta: &RasterMeta,
    manifest: &Manifest,
    artifact: Artifact,
) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let expected = manifest.crs().to_string();

    if let Some(got) = meta.spatial_ref.as_deref()
        && got != expected
    {
        let error = RasterSpatialCheckError::RasterCrsMismatch {
            path: meta.path.clone(),
            expected: expected.clone(),
            got: got.to_string(),
        };
        diags.push(Diagnostic::error(
            "raster.crs_mismatch",
            Category::Raster,
            artifact,
            error.to_string(),
        ));
    }

    if let Some(raster_bbox) = meta.bbox.as_ref() {
        let manifest_bbox = RasterBoundingBox::from_manifest_bbox(manifest.bbox());
        if !raster_bbox.contains_with_epsilon(&manifest_bbox, containment_epsilon(meta)) {
            let error = RasterSpatialCheckError::RasterExtentNotContained {
                path: meta.path.clone(),
                raster_bbox: raster_bbox.clone(),
                manifest_bbox,
            };
            diags.push(Diagnostic::error(
                "raster.extent_not_contained",
                Category::Raster,
                artifact,
                error.to_string(),
            ));
        }
    }

    diags
}

fn containment_epsilon(meta: &RasterMeta) -> f64 {
    let pixel_width = meta.pixel_width.unwrap_or(0.0).abs();
    let pixel_height = meta.pixel_height.unwrap_or(0.0).abs();
    pixel_width.max(pixel_height) / 100.0
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

    use hfx_core::{AtomCount, BoundingBox, Crs, FormatVersion, ManifestBuilder, Topology};

    use super::*;
    use crate::diagnostic::Severity;

    fn valid_manifest() -> Manifest {
        ManifestBuilder::new(
            FormatVersion::V0_1,
            "test-fabric",
            Crs::Epsg4326,
            Topology::Tree,
            0,
            BoundingBox::new(-10.0, -5.0, 10.0, 5.0).unwrap(),
            AtomCount::new(3).unwrap(),
            "2026-01-01T00:00:00Z",
            "v1.0.0",
        )
        .unwrap()
        .build()
    }

    fn valid_flow_dir_meta() -> RasterMeta {
        RasterMeta {
            path: PathBuf::from("flow_dir.tif"),
            width: 1024,
            height: 1024,
            bits_per_sample: 8,
            sample_format: RasterSampleFormat::UnsignedInt,
            is_tiled: true,
            tile_width: Some(256),
            tile_height: Some(256),
            nodata: Some(255.0),
            spatial_ref: Some("EPSG:4326".to_string()),
            bbox: Some(RasterBoundingBox::new(-10.1, -5.1, 10.1, 5.1)),
            pixel_width: Some(0.01),
            pixel_height: Some(0.01),
        }
    }

    fn valid_flow_acc_meta() -> RasterMeta {
        RasterMeta {
            path: PathBuf::from("flow_acc.tif"),
            width: 1024,
            height: 1024,
            bits_per_sample: 32,
            sample_format: RasterSampleFormat::Float,
            is_tiled: true,
            tile_width: Some(256),
            tile_height: Some(256),
            nodata: Some(-1.0),
            spatial_ref: Some("EPSG:4326".to_string()),
            bbox: Some(RasterBoundingBox::new(-10.1, -5.1, 10.1, 5.1)),
            pixel_width: Some(0.01),
            pixel_height: Some(0.01),
        }
    }

    #[test]
    fn valid_flow_dir_produces_no_errors() {
        let diags = check_flow_dir(&valid_flow_dir_meta());
        assert!(
            diags.iter().all(|d| d.severity != Severity::Error),
            "expected no errors, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_dir_wrong_dtype_float32_produces_error() {
        let meta = RasterMeta {
            bits_per_sample: 32,
            sample_format: RasterSampleFormat::Float,
            ..valid_flow_dir_meta()
        };
        let diags = check_flow_dir(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_dir_dtype"),
            "expected raster.flow_dir_dtype, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_dir_wrong_dtype_uint16_produces_error() {
        let meta = RasterMeta {
            bits_per_sample: 16,
            sample_format: RasterSampleFormat::UnsignedInt,
            ..valid_flow_dir_meta()
        };
        let diags = check_flow_dir(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_dir_dtype"),
            "expected raster.flow_dir_dtype, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_dir_not_tiled_produces_error() {
        let meta = RasterMeta {
            is_tiled: false,
            tile_width: None,
            tile_height: None,
            ..valid_flow_dir_meta()
        };
        let diags = check_flow_dir(&meta);
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "raster.flow_dir_not_tiled"),
            "expected raster.flow_dir_not_tiled, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_dir_wrong_nodata_produces_error() {
        let meta = RasterMeta {
            nodata: Some(0.0),
            ..valid_flow_dir_meta()
        };
        let diags = check_flow_dir(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_dir_nodata"),
            "expected raster.flow_dir_nodata, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_dir_absent_nodata_produces_error() {
        let meta = RasterMeta {
            nodata: None,
            ..valid_flow_dir_meta()
        };
        let diags = check_flow_dir(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_dir_nodata"),
            "expected raster.flow_dir_nodata, got: {diags:#?}"
        );
    }

    #[test]
    fn valid_flow_acc_produces_no_errors() {
        let diags = check_flow_acc(&valid_flow_acc_meta());
        assert!(
            diags.iter().all(|d| d.severity != Severity::Error),
            "expected no errors, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_acc_wrong_dtype_uint32_produces_error() {
        let meta = RasterMeta {
            sample_format: RasterSampleFormat::UnsignedInt,
            ..valid_flow_acc_meta()
        };
        let diags = check_flow_acc(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_acc_dtype"),
            "expected raster.flow_acc_dtype, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_acc_wrong_dtype_uint8_produces_error() {
        let meta = RasterMeta {
            bits_per_sample: 8,
            sample_format: RasterSampleFormat::UnsignedInt,
            ..valid_flow_acc_meta()
        };
        let diags = check_flow_acc(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_acc_dtype"),
            "expected raster.flow_acc_dtype, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_acc_not_tiled_produces_error() {
        let meta = RasterMeta {
            is_tiled: false,
            tile_width: None,
            tile_height: None,
            ..valid_flow_acc_meta()
        };
        let diags = check_flow_acc(&meta);
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "raster.flow_acc_not_tiled"),
            "expected raster.flow_acc_not_tiled, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_acc_wrong_nodata_produces_error() {
        let meta = RasterMeta {
            nodata: Some(0.0),
            ..valid_flow_acc_meta()
        };
        let diags = check_flow_acc(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_acc_nodata"),
            "expected raster.flow_acc_nodata, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_acc_absent_nodata_produces_error() {
        let meta = RasterMeta {
            nodata: None,
            ..valid_flow_acc_meta()
        };
        let diags = check_flow_acc(&meta);
        assert!(
            diags.iter().any(|d| d.check_id == "raster.flow_acc_nodata"),
            "expected raster.flow_acc_nodata, got: {diags:#?}"
        );
    }

    #[test]
    fn spatial_checks_accept_valid_raster() {
        let diags =
            check_spatial_consistency(&valid_flow_dir_meta(), &valid_manifest(), Artifact::FlowDir);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn spatial_checks_report_crs_mismatch() {
        let meta = RasterMeta {
            spatial_ref: Some("EPSG:3857".to_string()),
            ..valid_flow_dir_meta()
        };

        let diags = check_spatial_consistency(&meta, &valid_manifest(), Artifact::FlowDir);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "raster.crs_mismatch");
    }

    #[test]
    fn spatial_checks_report_extent_not_contained() {
        let meta = RasterMeta {
            bbox: Some(RasterBoundingBox::new(-9.9, -4.9, 9.9, 4.9)),
            ..valid_flow_dir_meta()
        };

        let diags = check_spatial_consistency(&meta, &valid_manifest(), Artifact::FlowDir);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "raster.extent_not_contained");
    }

    #[test]
    fn spatial_checks_skip_missing_spatial_metadata() {
        let meta = RasterMeta {
            spatial_ref: None,
            bbox: None,
            pixel_width: None,
            pixel_height: None,
            ..valid_flow_dir_meta()
        };

        let diags = check_spatial_consistency(&meta, &valid_manifest(), Artifact::FlowDir);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }
}
