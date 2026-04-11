//! Raster metadata checks (G1, G2, G3).
//!
//! These checks operate on [`RasterMeta`] values that have already been
//! decoded from TIFF headers by [`crate::reader::raster`].

use crate::dataset::{RasterMeta, RasterSampleFormat};
use crate::diagnostic::{Artifact, Category, Diagnostic};

// ---------------------------------------------------------------------------
// Public check functions
// ---------------------------------------------------------------------------

/// G1: Validate `flow_dir.tif` structural properties.
///
/// Required: `bits_per_sample == 8`, `sample_format == UnsignedInt`,
/// and the file must be COG-tiled (`is_tiled == true`).
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

    diags
}

/// G2: Validate `flow_acc.tif` structural properties.
///
/// Required: `bits_per_sample == 32`, `sample_format == Float`,
/// and the file must be COG-tiled (`is_tiled == true`).
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

    diags
}

/// G3: Emit an informational note that CRS and extent checks are deferred
/// because they require GDAL, which is not available in this validator.
pub fn crs_extent_deferred_note() -> Vec<Diagnostic> {
    vec![Diagnostic::info(
        "raster.crs_extent_deferred",
        Category::Raster,
        Artifact::FlowDir,
        "CRS and spatial extent checks are deferred: they require GDAL, which is not linked",
    )]
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sample_format_label(fmt: RasterSampleFormat) -> &'static str {
    match fmt {
        RasterSampleFormat::UnsignedInt => "unsigned-int",
        RasterSampleFormat::SignedInt => "signed-int",
        RasterSampleFormat::Float => "float",
        RasterSampleFormat::Unknown(_) => "unknown",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{RasterMeta, RasterSampleFormat};
    use crate::diagnostic::Severity;

    fn valid_flow_dir_meta() -> RasterMeta {
        RasterMeta {
            width: 1024,
            height: 1024,
            bits_per_sample: 8,
            sample_format: RasterSampleFormat::UnsignedInt,
            is_tiled: true,
            tile_width: Some(256),
            tile_height: Some(256),
            nodata: Some(255.0),
        }
    }

    fn valid_flow_acc_meta() -> RasterMeta {
        RasterMeta {
            width: 1024,
            height: 1024,
            bits_per_sample: 32,
            sample_format: RasterSampleFormat::Float,
            is_tiled: true,
            tile_width: Some(256),
            tile_height: Some(256),
            nodata: Some(-1.0),
        }
    }

    // -----------------------------------------------------------------------
    // flow_dir checks
    // -----------------------------------------------------------------------

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
            diags.iter().any(|d| d.check_id == "raster.flow_dir_not_tiled"),
            "expected raster.flow_dir_not_tiled, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_dir_wrong_dtype_and_not_tiled_produces_two_errors() {
        let meta = RasterMeta {
            bits_per_sample: 32,
            sample_format: RasterSampleFormat::Float,
            is_tiled: false,
            tile_width: None,
            tile_height: None,
            ..valid_flow_dir_meta()
        };
        let diags = check_flow_dir(&meta);
        assert!(diags.iter().any(|d| d.check_id == "raster.flow_dir_dtype"));
        assert!(diags.iter().any(|d| d.check_id == "raster.flow_dir_not_tiled"));
    }

    // -----------------------------------------------------------------------
    // flow_acc checks
    // -----------------------------------------------------------------------

    #[test]
    fn valid_flow_acc_produces_no_errors() {
        let diags = check_flow_acc(&valid_flow_acc_meta());
        assert!(
            diags.iter().all(|d| d.severity != Severity::Error),
            "expected no errors, got: {diags:#?}"
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
    fn flow_acc_wrong_dtype_uint32_produces_error() {
        let meta = RasterMeta {
            bits_per_sample: 32,
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
            diags.iter().any(|d| d.check_id == "raster.flow_acc_not_tiled"),
            "expected raster.flow_acc_not_tiled, got: {diags:#?}"
        );
    }

    #[test]
    fn flow_acc_wrong_dtype_and_not_tiled_produces_two_errors() {
        let meta = RasterMeta {
            bits_per_sample: 8,
            sample_format: RasterSampleFormat::UnsignedInt,
            is_tiled: false,
            tile_width: None,
            tile_height: None,
            ..valid_flow_acc_meta()
        };
        let diags = check_flow_acc(&meta);
        assert!(diags.iter().any(|d| d.check_id == "raster.flow_acc_dtype"));
        assert!(diags.iter().any(|d| d.check_id == "raster.flow_acc_not_tiled"));
    }

    // -----------------------------------------------------------------------
    // CRS deferred note
    // -----------------------------------------------------------------------

    #[test]
    fn crs_deferred_note_produces_info_diagnostic() {
        let diags = crs_extent_deferred_note();
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].severity, Severity::Info);
        assert_eq!(diags[0].check_id, "raster.crs_extent_deferred");
    }
}
