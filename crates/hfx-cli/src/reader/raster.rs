//! Raster GeoTIFF reader.
//!
//! Reads structural TIFF metadata plus GDAL spatial metadata without loading
//! full raster payloads into memory.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use gdal::spatial_ref::{AxisMappingStrategy, CoordTransform, SpatialRef};
use gdal::{Dataset, GeoTransformEx};
use tracing::{debug, warn};

use tiff::decoder::{ChunkType, Decoder};
use tiff::tags::{SampleFormat, Tag};

use crate::dataset::{RasterBoundingBox, RasterMeta, RasterSampleFormat};
use crate::diagnostic::{Artifact, Category, Diagnostic};

#[derive(Debug)]
struct RasterSpatialMeta {
    spatial_ref: String,
    bbox_wgs84: RasterBoundingBox,
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

    /// Returned when the spatial reference authority cannot be resolved.
    #[error("cannot resolve spatial reference authority from {path}: {source}")]
    SpatialRefNormalize {
        /// Raster path whose spatial reference authority could not be resolved.
        path: String,
        /// GDAL error returned while resolving the authority.
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

    /// Returned when the raster perimeter cannot be transformed to EPSG:4326.
    #[error("cannot transform raster footprint from {path} to EPSG:4326: {source}")]
    FootprintTransform {
        /// Raster path whose footprint could not be transformed.
        path: String,
        /// GDAL error returned while constructing or applying the transform.
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

    let (spatial_ref, bbox_wgs84, pixel_width, pixel_height, diagnostics) =
        match read_spatial_meta(path) {
            Ok(spatial) => (
                Some(spatial.spatial_ref),
                Some(spatial.bbox_wgs84),
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
        bbox_wgs84,
        pixel_width,
        pixel_height,
    };

    debug!(
        width,
        height,
        bits_per_sample,
        is_tiled,
        has_spatial_ref = meta.spatial_ref.is_some(),
        has_bbox_wgs84 = meta.bbox_wgs84.is_some(),
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

    let mut spatial_ref =
        dataset
            .spatial_ref()
            .map_err(|source| RasterSpatialReadError::SpatialRef {
                path: path_display.clone(),
                source,
            })?;
    let canonical_spatial_ref = normalize_spatial_ref(&mut spatial_ref).map_err(|source| {
        RasterSpatialReadError::SpatialRefNormalize {
            path: path_display.clone(),
            source,
        }
    })?;

    let geo_transform =
        dataset
            .geo_transform()
            .map_err(|source| RasterSpatialReadError::GeoTransform {
                path: path_display.clone(),
                source,
            })?;
    let bbox_wgs84 = footprint_bbox_wgs84(&spatial_ref, &geo_transform, dataset.raster_size())
        .map_err(|source| RasterSpatialReadError::FootprintTransform {
            path: path_display,
            source,
        })?;
    let pixel_width = geo_transform[1].hypot(geo_transform[4]);
    let pixel_height = geo_transform[2].hypot(geo_transform[5]);

    Ok(RasterSpatialMeta {
        spatial_ref: canonical_spatial_ref,
        bbox_wgs84,
        pixel_width,
        pixel_height,
    })
}

fn normalize_spatial_ref(spatial_ref: &mut SpatialRef) -> Result<String, gdal::errors::GdalError> {
    if let Ok(authority) = spatial_ref.authority() {
        return Ok(authority);
    }

    spatial_ref.auto_identify_epsg()?;
    spatial_ref.authority()
}

const EDGE_SAMPLES_PER_SIDE: usize = 21;

fn densified_edge_points(raster_size: (usize, usize)) -> Vec<(f64, f64)> {
    let width = raster_size.0 as f64;
    let height = raster_size.1 as f64;
    let samples = (0..EDGE_SAMPLES_PER_SIDE).map(|i| {
        let t = i as f64 / (EDGE_SAMPLES_PER_SIDE - 1) as f64;
        (t, 1.0 - t)
    });

    samples
        .clone()
        .map(|(t, _)| (t * width, 0.0))
        .chain(samples.clone().map(|(t, _)| (width, t * height)))
        .chain(
            samples
                .clone()
                .map(|(_, inverse_t)| (inverse_t * width, height)),
        )
        .chain(samples.map(|(_, inverse_t)| (0.0, inverse_t * height)))
        .collect()
}

fn footprint_bbox_wgs84(
    source: &SpatialRef,
    geo_transform: &[f64; 6],
    raster_size: (usize, usize),
) -> Result<RasterBoundingBox, gdal::errors::GdalError> {
    let mut source = source.clone();
    source.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
    let mut target = SpatialRef::from_epsg(4326)?;
    target.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
    let transform = CoordTransform::new(&source, &target)?;
    let (mut xs, mut ys): (Vec<_>, Vec<_>) = densified_edge_points(raster_size)
        .into_iter()
        .map(|(pixel_x, pixel_y)| geo_transform.apply(pixel_x, pixel_y))
        .unzip();
    transform.transform_coords(&mut xs, &mut ys, &mut [])?;

    let finite_coords: Vec<_> = xs
        .into_iter()
        .zip(ys)
        .filter(|(x, y)| x.is_finite() && y.is_finite())
        .collect();
    if finite_coords.is_empty() {
        return Err(gdal::errors::GdalError::BadArgument(
            "raster footprint transform returned no finite coordinates".to_owned(),
        ));
    }
    let min_x = finite_coords
        .iter()
        .map(|(x, _)| *x)
        .fold(f64::INFINITY, f64::min);
    let min_y = finite_coords
        .iter()
        .map(|(_, y)| *y)
        .fold(f64::INFINITY, f64::min);
    let max_x = finite_coords
        .iter()
        .map(|(x, _)| *x)
        .fold(f64::NEG_INFINITY, f64::max);
    let max_y = finite_coords
        .iter()
        .map(|(_, y)| *y)
        .fold(f64::NEG_INFINITY, f64::max);

    Ok(RasterBoundingBox::new(min_x, min_y, max_x, max_y))
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
    use gdal::GeoTransformEx;
    use gdal::spatial_ref::{AxisMappingStrategy, SpatialRef};

    use super::{EDGE_SAMPLES_PER_SIDE, densified_edge_points, footprint_bbox_wgs84};

    #[test]
    fn densified_edges_use_full_outer_pixel_boundaries() {
        let points = densified_edge_points((200, 100));

        assert_eq!(points.len(), 4 * EDGE_SAMPLES_PER_SIDE);
        assert_eq!(points[0], (0.0, 0.0));
        assert_eq!(points[10], (100.0, 0.0));
        assert_eq!(points[20], (200.0, 0.0));
        assert_eq!(points[21], (200.0, 0.0));
        assert_eq!(points[31], (200.0, 50.0));
        assert_eq!(points[41], (200.0, 100.0));
        assert_eq!(points[42], (200.0, 100.0));
        assert_eq!(points[62], (0.0, 100.0));
        assert_eq!(points[63], (0.0, 100.0));
        assert_eq!(points[83], (0.0, 0.0));
    }

    #[test]
    fn rotated_sheared_wgs84_footprint_encloses_every_edge_sample() {
        let geo_transform = [10.0, 0.01, 0.003, 20.0, -0.002, -0.01];
        let mut source = SpatialRef::from_epsg(4326).unwrap();
        source.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);

        let bbox = footprint_bbox_wgs84(&source, &geo_transform, (200, 100)).unwrap();

        for (pixel_x, pixel_y) in densified_edge_points((200, 100)) {
            let (x, y) = geo_transform.apply(pixel_x, pixel_y);
            assert!(x >= bbox.min_x && x <= bbox.max_x);
            assert!(y >= bbox.min_y && y <= bbox.max_y);
        }
    }

    #[test]
    fn projected_equal_earth_footprint_transforms_to_finite_wgs84_bbox() {
        let geo_transform = [-100_000.0, 1_000.0, 0.0, 100_000.0, 0.0, -1_000.0];
        let mut source = SpatialRef::from_epsg(8857).unwrap();
        source.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);

        let bbox = footprint_bbox_wgs84(&source, &geo_transform, (200, 200)).unwrap();

        assert!(bbox.min_x.is_finite() && bbox.min_y.is_finite());
        assert!(bbox.max_x.is_finite() && bbox.max_y.is_finite());
        assert!(bbox.min_x <= bbox.max_x && bbox.min_y <= bbox.max_y);
        assert!(bbox.min_x <= 0.0 && bbox.max_x >= 0.0);
        assert!(bbox.min_y <= 0.0 && bbox.max_y >= 0.0);
    }
}
