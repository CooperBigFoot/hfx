//! Raster GeoTIFF reader.
//!
//! Reads structural metadata from GeoTIFF headers without loading pixel data.
//! The [read_raster_meta] function is the primary entry point.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use tracing::{debug, warn};

use tiff::decoder::{ChunkType, Decoder};
use tiff::tags::{SampleFormat, Tag};

use crate::dataset::{RasterMeta, RasterSampleFormat};
use crate::diagnostic::{Artifact, Category, Diagnostic};

/// Read basic structural metadata from a GeoTIFF file without loading pixel data.
///
/// Returns `(Some(meta), diagnostics)` on success, or `(None, diagnostics)`
/// when the file cannot be opened or is not a valid TIFF.
///
/// # Errors
///
/// | Condition | check_id |
/// |---|---|
/// | File cannot be opened | `"raster.open"` |
/// | File is not a valid TIFF | `"raster.parse"` |
pub fn read_raster_meta(path: &Path, file_label: &str) -> (Option<RasterMeta>, Vec<Diagnostic>) {
    debug!(path = %path.display(), file_label, "reading raster metadata");

    let artifact = artifact_for_label(file_label);

    let file = match File::open(path) {
        Ok(f) => f,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot open raster file");
            return (
                None,
                vec![Diagnostic::error(
                    "raster.open",
                    Category::Raster,
                    artifact,
                    format!("cannot open {file_label}: {err}"),
                )],
            );
        }
    };

    let mut decoder = match Decoder::new(BufReader::new(file)) {
        Ok(d) => d,
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot parse raster file as TIFF");
            return (
                None,
                vec![Diagnostic::error(
                    "raster.parse",
                    Category::Raster,
                    artifact,
                    format!("cannot parse {file_label} as TIFF: {err}"),
                )],
            );
        }
    };

    let (width, height) = match decoder.dimensions() {
        Ok(dims) => dims,
        Err(err) => {
            warn!(error = %err, "cannot read TIFF dimensions");
            return (
                None,
                vec![Diagnostic::error(
                    "raster.parse",
                    Category::Raster,
                    artifact,
                    format!("cannot read dimensions from {file_label}: {err}"),
                )],
            );
        }
    };

    // Determine bits_per_sample from the ColorType.
    let bits_per_sample = match decoder.colortype() {
        Ok(ct) => bits_from_colortype(ct),
        Err(err) => {
            warn!(error = %err, "cannot read TIFF color type");
            return (
                None,
                vec![Diagnostic::error(
                    "raster.parse",
                    Category::Raster,
                    artifact,
                    format!("cannot read color type from {file_label}: {err}"),
                )],
            );
        }
    };

    // Determine sample format via the SampleFormat tag.
    let sample_format = read_sample_format(&mut decoder);

    // Determine tiling.
    let chunk_type = decoder.get_chunk_type();
    let is_tiled = chunk_type == ChunkType::Tile;

    let (tile_width, tile_height) = if is_tiled {
        let (tw, th) = decoder.chunk_dimensions();
        (Some(tw), Some(th))
    } else {
        (None, None)
    };

    // Read the GDAL nodata value (ASCII tag 42113).
    let nodata = read_nodata(&mut decoder);

    let meta = RasterMeta {
        width,
        height,
        bits_per_sample,
        sample_format,
        is_tiled,
        tile_width,
        tile_height,
        nodata,
    };

    debug!(
        width,
        height,
        bits_per_sample,
        is_tiled,
        "raster metadata read complete"
    );

    (Some(meta), vec![])
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map a file label to the matching [Artifact] variant.
fn artifact_for_label(label: &str) -> Artifact {
    match label {
        "flow_dir.tif" => Artifact::FlowDir,
        "flow_acc.tif" => Artifact::FlowAcc,
        _ => Artifact::FlowDir,
    }
}

/// Extract the bits-per-sample count from a [tiff::ColorType].
fn bits_from_colortype(ct: tiff::ColorType) -> u16 {
    match ct {
        tiff::ColorType::Gray(n)
        | tiff::ColorType::RGB(n)
        | tiff::ColorType::Palette(n)
        | tiff::ColorType::GrayA(n)
        | tiff::ColorType::RGBA(n)
        | tiff::ColorType::CMYK(n)
        | tiff::ColorType::YCbCr(n) => u16::from(n),
    }
}

/// Read the TIFF `SampleFormat` tag and translate it to [RasterSampleFormat].
///
/// If the tag is absent the TIFF default is unsigned integer.
fn read_sample_format<R: std::io::Read + std::io::Seek>(
    decoder: &mut Decoder<R>,
) -> RasterSampleFormat {
    // find_tag returns the first value of the tag; SampleFormat is a per-sample
    // list but we only support homogenous single-band rasters here.
    match decoder.find_tag(Tag::SampleFormat) {
        Ok(Some(val)) => match val.into_u16() {
            Ok(raw) => match SampleFormat::from_u16(raw) {
                Some(SampleFormat::Uint) => RasterSampleFormat::UnsignedInt,
                Some(SampleFormat::Int) => RasterSampleFormat::SignedInt,
                Some(SampleFormat::IEEEFP) => RasterSampleFormat::Float,
                _ => RasterSampleFormat::Unknown(raw),
            },
            Err(_) => RasterSampleFormat::UnsignedInt,
        },
        // Tag absent: TIFF default is unsigned integer.
        Ok(None) => RasterSampleFormat::UnsignedInt,
        Err(_) => RasterSampleFormat::UnsignedInt,
    }
}

/// Read the GDAL nodata tag (42113) from a decoded TIFF.
///
/// The tag stores an ASCII string representation of the nodata value.
/// Returns `None` if the tag is absent or cannot be parsed.
fn read_nodata<R: std::io::Read + std::io::Seek>(decoder: &mut Decoder<R>) -> Option<f64> {
    match decoder.find_tag(Tag::GdalNodata) {
        Ok(Some(val)) => {
            if let Ok(s) = val.into_string() {
                s.trim_end_matches('\0').parse::<f64>().ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tiff::encoder::{colortype, TiffEncoder};

    use super::*;

    /// Write a minimal strip-based Gray8 TIFF into a Vec<u8>.
    fn make_gray8_strip_tiff(width: u32, height: u32) -> Vec<u8> {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut enc = TiffEncoder::new(&mut buf).unwrap();
            let data: Vec<u8> = vec![0u8; (width * height) as usize];
            enc.write_image::<colortype::Gray8>(width, height, &data)
                .unwrap();
        }
        buf.into_inner()
    }

    /// Write a minimal strip-based Gray32Float TIFF into a Vec<u8>.
    fn make_gray32float_strip_tiff(width: u32, height: u32) -> Vec<u8> {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut enc = TiffEncoder::new(&mut buf).unwrap();
            let data: Vec<f32> = vec![0.0f32; (width * height) as usize];
            enc.write_image::<colortype::Gray32Float>(width, height, &data)
                .unwrap();
        }
        buf.into_inner()
    }

    #[test]
    fn missing_file_returns_none_with_open_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow_dir.tif");

        let (meta, diags) = read_raster_meta(&path, "flow_dir.tif");
        assert!(meta.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "raster.open");
    }

    #[test]
    fn invalid_file_content_returns_none_with_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow_dir.tif");
        std::fs::write(&path, b"not a tiff file").unwrap();

        let (meta, diags) = read_raster_meta(&path, "flow_dir.tif");
        assert!(meta.is_none());
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].check_id, "raster.parse");
    }

    #[test]
    fn gray8_strip_tiff_reads_correctly() {
        let tiff_bytes = make_gray8_strip_tiff(64, 64);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow_dir.tif");
        std::fs::write(&path, &tiff_bytes).unwrap();

        let (meta, diags) = read_raster_meta(&path, "flow_dir.tif");
        let meta = meta.expect("should read Gray8 strip TIFF");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:#?}");
        assert_eq!(meta.width, 64);
        assert_eq!(meta.height, 64);
        assert_eq!(meta.bits_per_sample, 8);
        assert_eq!(meta.sample_format, RasterSampleFormat::UnsignedInt);
        assert!(!meta.is_tiled, "strip TIFF must not be flagged as tiled");
        assert!(meta.tile_width.is_none());
        assert!(meta.tile_height.is_none());
    }

    #[test]
    fn gray32float_strip_tiff_reads_correctly() {
        let tiff_bytes = make_gray32float_strip_tiff(32, 32);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow_acc.tif");
        std::fs::write(&path, &tiff_bytes).unwrap();

        let (meta, diags) = read_raster_meta(&path, "flow_acc.tif");
        let meta = meta.expect("should read Gray32Float strip TIFF");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:#?}");
        assert_eq!(meta.bits_per_sample, 32);
        assert_eq!(meta.sample_format, RasterSampleFormat::Float);
        assert!(!meta.is_tiled);
    }
}
