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
