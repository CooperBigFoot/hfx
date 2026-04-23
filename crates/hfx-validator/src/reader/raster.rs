//! Raster GeoTIFF reader.
//!
//! Reads structural TIFF metadata plus GDAL spatial metadata without loading
//! full raster payloads into memory.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use gdal::spatial_ref::SpatialRef;
use gdal::{Dataset, GeoTransformEx};
use tracing::{debug, warn};

use tiff::decoder::{ChunkType, Decoder};
use tiff::tags::{SampleFormat, Tag};

use crate::dataset::{RasterBoundingBox, RasterMeta, RasterSampleFormat};
use crate::diagnostic::{Artifact, Category, Diagnostic};

#[derive(Debug)]
struct RasterSpatialMeta {
    spatial_ref: String,
    bbox: RasterBoundingBox,
    pixel_width: f64,
    pixel_height: f64,
}

/// Errors returned when GDAL spatial metadata cannot be extracted.
#[derive(Debug, thiserror::Error)]
enum RasterSpatialReadError {
    /// Returned when GDAL cannot open a TIFF file that passed the header parser.
    #[error("cannot open {path} with GDAL: {source}")]
    Open {
        /// Raster path being opened.
        path: String,
        /// GDAL error returned by the open call.
        #[source]
        source: gdal::errors::GdalError,
    },

    /// Returned when the raster has no usable spatial reference.
    #[error("cannot read spatial reference from {path}: {source}")]
    SpatialRef {
        /// Raster path missing a spatial reference.
        path: String,
        /// GDAL error returned by the spatial reference lookup.
        #[source]
        source: gdal::errors::GdalError,
    },

    /// Returned when the spatial reference cannot be normalized for comparison.
    #[error("cannot normalize spatial reference from {path}: {source}")]
    SpatialRefNormalize {
        /// Raster path whose spatial reference could not be normalized.
        path: String,
        /// GDAL error returned by normalization.
        #[source]
        source: gdal::errors::GdalError,
    },

    /// Returned when the raster has no geotransform.
    #[error("cannot read geotransform from {path}: {source}")]
    GeoTransform {
        /// Raster path missing geotransform metadata.
        path: String,
        /// GDAL error returned by the geotransform lookup.
        #[source]
        source: gdal::errors::GdalError,
    },
}

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
#[tracing::instrument(skip_all, fields(path = %path.display(), file_label))]
pub fn read_raster_meta(path: &Path, file_label: &str) -> (Option<RasterMeta>, Vec<Diagnostic>) {
    debug!("reading raster metadata");

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

    let sample_format = read_sample_format(&mut decoder);

    let chunk_type = decoder.get_chunk_type();
    let is_tiled = chunk_type == ChunkType::Tile;

    let (tile_width, tile_height) = if is_tiled {
        let (tw, th) = decoder.chunk_dimensions();
        (Some(tw), Some(th))
    } else {
        (None, None)
    };

    let nodata = read_nodata(&mut decoder);

    let (spatial_ref, bbox, pixel_width, pixel_height, diagnostics) = match read_spatial_meta(path)
    {
        Ok(spatial) => (
            Some(spatial.spatial_ref),
            Some(spatial.bbox),
            Some(spatial.pixel_width),
            Some(spatial.pixel_height),
            vec![],
        ),
        Err(err) => {
            warn!(path = %path.display(), error = %err, "cannot read GDAL spatial metadata");
            (
                None,
                None,
                None,
                None,
                vec![Diagnostic::error(
                    "raster.parse",
                    Category::Raster,
                    artifact,
                    format!("cannot read GDAL spatial metadata from {file_label}: {err}"),
                )],
            )
        }
    };

    let meta = RasterMeta {
        path: path.to_path_buf(),
        width,
        height,
        bits_per_sample,
        sample_format,
        is_tiled,
        tile_width,
        tile_height,
        nodata,
        spatial_ref,
        bbox,
        pixel_width,
        pixel_height,
    };

    debug!(
        width,
        height,
        bits_per_sample,
        is_tiled,
        has_spatial_ref = meta.spatial_ref.is_some(),
        has_bbox = meta.bbox.is_some(),
        "raster metadata read complete"
    );

    (Some(meta), diagnostics)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_spatial_meta(path: &Path) -> Result<RasterSpatialMeta, RasterSpatialReadError> {
    let path_display = path.display().to_string();
    let dataset = Dataset::open(path).map_err(|source| RasterSpatialReadError::Open {
        path: path_display.clone(),
        source,
    })?;

    let spatial_ref =
        dataset
            .spatial_ref()
            .map_err(|source| RasterSpatialReadError::SpatialRef {
                path: path_display.clone(),
                source,
            })?;
    let spatial_ref = normalize_spatial_ref(&spatial_ref).map_err(|source| {
        RasterSpatialReadError::SpatialRefNormalize {
            path: path_display.clone(),
            source,
        }
    })?;

    let geo_transform =
        dataset
            .geo_transform()
            .map_err(|source| RasterSpatialReadError::GeoTransform {
                path: path_display,
                source,
            })?;
    let bbox = bbox_from_geo_transform(&geo_transform, dataset.raster_size());
    let pixel_width = geo_transform[1].hypot(geo_transform[4]);
    let pixel_height = geo_transform[2].hypot(geo_transform[5]);

    Ok(RasterSpatialMeta {
        spatial_ref,
        bbox,
        pixel_width,
        pixel_height,
    })
}

fn normalize_spatial_ref(spatial_ref: &SpatialRef) -> Result<String, gdal::errors::GdalError> {
    if let Ok(authority) = spatial_ref.authority() {
        return Ok(authority);
    }

    if let Ok(expected) = SpatialRef::from_epsg(4326)
        && spatial_ref == &expected
    {
        return Ok("EPSG:4326".to_string());
    }

    spatial_ref.to_wkt()
}

fn bbox_from_geo_transform(
    geo_transform: &[f64; 6],
    raster_size: (usize, usize),
) -> RasterBoundingBox {
    let (width, height) = (raster_size.0 as f64, raster_size.1 as f64);
    let corners = [
        geo_transform.apply(0.0, 0.0),
        geo_transform.apply(width, 0.0),
        geo_transform.apply(0.0, height),
        geo_transform.apply(width, height),
    ];

    let min_x = corners
        .iter()
        .map(|(x, _)| *x)
        .fold(f64::INFINITY, f64::min);
    let min_y = corners
        .iter()
        .map(|(_, y)| *y)
        .fold(f64::INFINITY, f64::min);
    let max_x = corners
        .iter()
        .map(|(x, _)| *x)
        .fold(f64::NEG_INFINITY, f64::max);
    let max_y = corners
        .iter()
        .map(|(_, y)| *y)
        .fold(f64::NEG_INFINITY, f64::max);

    RasterBoundingBox::new(min_x, min_y, max_x, max_y)
}

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
    use gdal::DriverManager;
    use gdal::raster::{Buffer, RasterCreationOptions};
    use gdal::spatial_ref::SpatialRef;

    use super::*;

    fn write_test_raster_u8(path: &Path, size: (usize, usize), geo_transform: [f64; 6], epsg: u32) {
        let driver = DriverManager::get_driver_by_name("GTiff").unwrap();
        let options = RasterCreationOptions::from_iter([
            "TILED=YES",
            "BLOCKXSIZE=32",
            "BLOCKYSIZE=32",
            "COMPRESS=DEFLATE",
            "INTERLEAVE=BAND",
        ]);
        let mut dataset = driver
            .create_with_band_type_with_options::<u8, _>(path, size.0, size.1, 1, &options)
            .unwrap();
        dataset
            .set_spatial_ref(&SpatialRef::from_epsg(epsg).unwrap())
            .unwrap();
        dataset.set_geo_transform(&geo_transform).unwrap();

        let mut band = dataset.rasterband(1).unwrap();
        band.set_no_data_value(Some(255.0)).unwrap();
        let mut buffer = Buffer::new(size, vec![1_u8; size.0 * size.1]);
        band.write((0, 0), size, &mut buffer).unwrap();
    }

    fn write_test_raster_f32(
        path: &Path,
        size: (usize, usize),
        geo_transform: [f64; 6],
        epsg: u32,
    ) {
        let driver = DriverManager::get_driver_by_name("GTiff").unwrap();
        let options = RasterCreationOptions::from_iter([
            "TILED=YES",
            "BLOCKXSIZE=32",
            "BLOCKYSIZE=32",
            "COMPRESS=DEFLATE",
            "INTERLEAVE=BAND",
        ]);
        let mut dataset = driver
            .create_with_band_type_with_options::<f32, _>(path, size.0, size.1, 1, &options)
            .unwrap();
        dataset
            .set_spatial_ref(&SpatialRef::from_epsg(epsg).unwrap())
            .unwrap();
        dataset.set_geo_transform(&geo_transform).unwrap();

        let mut band = dataset.rasterband(1).unwrap();
        band.set_no_data_value(Some(-1.0)).unwrap();
        let mut buffer = Buffer::new(size, vec![42.0_f32; size.0 * size.1]);
        band.write((0, 0), size, &mut buffer).unwrap();
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
    fn gray8_geotiff_reads_structural_and_spatial_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow_dir.tif");
        write_test_raster_u8(&path, (64, 64), [10.0, 0.5, 0.0, 20.0, 0.0, -0.5], 4326);

        let (meta, diags) = read_raster_meta(&path, "flow_dir.tif");
        let meta = meta.expect("should read Gray8 GeoTIFF");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:#?}");
        assert_eq!(meta.width, 64);
        assert_eq!(meta.height, 64);
        assert_eq!(meta.bits_per_sample, 8);
        assert_eq!(meta.sample_format, RasterSampleFormat::UnsignedInt);
        assert!(meta.is_tiled);
        assert_eq!(meta.tile_width, Some(32));
        assert_eq!(meta.tile_height, Some(32));
        assert_eq!(meta.nodata, Some(255.0));
        assert_eq!(meta.spatial_ref.as_deref(), Some("EPSG:4326"));
        assert_eq!(
            meta.bbox,
            Some(RasterBoundingBox::new(10.0, -12.0, 42.0, 20.0))
        );
        assert_eq!(meta.pixel_width, Some(0.5));
        assert_eq!(meta.pixel_height, Some(0.5));
    }

    #[test]
    fn gray32float_geotiff_reads_structural_and_spatial_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flow_acc.tif");
        write_test_raster_f32(&path, (32, 32), [100.0, 0.25, 0.0, 50.0, 0.0, -0.25], 4326);

        let (meta, diags) = read_raster_meta(&path, "flow_acc.tif");
        let meta = meta.expect("should read Gray32Float GeoTIFF");
        assert!(diags.is_empty(), "unexpected diagnostics: {diags:#?}");
        assert_eq!(meta.bits_per_sample, 32);
        assert_eq!(meta.sample_format, RasterSampleFormat::Float);
        assert!(meta.is_tiled);
        assert_eq!(meta.nodata, Some(-1.0));
        assert_eq!(meta.spatial_ref.as_deref(), Some("EPSG:4326"));
        assert_eq!(
            meta.bbox,
            Some(RasterBoundingBox::new(100.0, 42.0, 108.0, 50.0))
        );
    }
}
