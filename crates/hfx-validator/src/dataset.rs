//! Parsed dataset intermediate representation.

use std::path::PathBuf;

use hfx_core::{BoundingBox, Manifest};

use crate::diagnostic::Diagnostic;
use crate::reader::manifest::RawManifest;

/// File existence status for all artifacts.
#[derive(Debug, Clone)]
pub struct FilePresenceMap {
    pub manifest_path: Option<PathBuf>,
    pub catchments_path: Option<PathBuf>,
    pub graph_path: Option<PathBuf>,
    pub snap_path: Option<PathBuf>,
    pub flow_dir_path: Option<PathBuf>,
    pub flow_acc_path: Option<PathBuf>,
}

/// Column-level data extracted from catchments.parquet.
#[derive(Debug)]
pub struct CatchmentsData {
    pub row_count: usize,
    pub ids: Vec<i64>,
    pub areas_km2: Vec<f32>,
    pub bboxes: Vec<[f32; 4]>,
    pub up_area_null_count: usize,
    pub up_area_total: usize,
    pub geometry_wkb: Vec<Vec<u8>>,
    pub row_group_sizes: Vec<usize>,
    pub row_group_has_bbox_stats: Vec<bool>,
}

/// Column-level data extracted from graph.arrow.
#[derive(Debug)]
pub struct GraphData {
    pub ids: Vec<i64>,
    pub upstream_ids: Vec<Vec<i64>>,
}

/// Column-level data extracted from snap.parquet.
#[derive(Debug)]
pub struct SnapData {
    pub row_count: usize,
    pub ids: Vec<i64>,
    pub catchment_ids: Vec<i64>,
    pub weights: Vec<f32>,
    pub bboxes: Vec<[f32; 4]>,
    pub geometry_wkb: Vec<Vec<u8>>,
    pub row_group_sizes: Vec<usize>,
    pub row_group_has_bbox_stats: Vec<bool>,
}

/// Raster metadata extracted from GeoTIFF headers.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterMeta {
    pub path: PathBuf,
    pub width: u32,
    pub height: u32,
    pub bits_per_sample: u16,
    pub sample_format: RasterSampleFormat,
    pub is_tiled: bool,
    pub tile_width: Option<u32>,
    pub tile_height: Option<u32>,
    pub nodata: Option<f64>,
    pub spatial_ref: Option<String>,
    pub bbox: Option<RasterBoundingBox>,
    pub pixel_width: Option<f64>,
    pub pixel_height: Option<f64>,
}

/// Raster sample format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RasterSampleFormat {
    UnsignedInt,
    SignedInt,
    Float,
    Unknown(u16),
}

/// Raster bounding box derived from GDAL geotransform metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct RasterBoundingBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl RasterBoundingBox {
    /// Build a raster bbox from raw coordinate values.
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }

    /// Convert an HFX manifest bbox into the raster bbox representation.
    pub fn from_manifest_bbox(bbox: &BoundingBox) -> Self {
        Self {
            min_x: f64::from(bbox.min_x().get()),
            min_y: f64::from(bbox.min_y().get()),
            max_x: f64::from(bbox.max_x().get()),
            max_y: f64::from(bbox.max_y().get()),
        }
    }

    /// Return true when `self` fully contains `other` within `epsilon`.
    pub fn contains_with_epsilon(&self, other: &Self, epsilon: f64) -> bool {
        self.min_x <= other.min_x + epsilon
            && self.min_y <= other.min_y + epsilon
            && self.max_x >= other.max_x - epsilon
            && self.max_y >= other.max_y - epsilon
    }
}

impl std::fmt::Display for RasterBoundingBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{:.6}, {:.6}, {:.6}, {:.6}]",
            self.min_x, self.min_y, self.max_x, self.max_y
        )
    }
}

/// The complete parsed dataset, ready for validation checks.
#[derive(Debug)]
pub struct ParsedDataset {
    pub files: FilePresenceMap,
    pub manifest_json: Option<serde_json::Value>,
    pub raw_manifest: Option<RawManifest>,
    pub manifest: Option<Manifest>,
    pub catchments: Option<CatchmentsData>,
    pub graph: Option<GraphData>,
    pub snap: Option<SnapData>,
    pub flow_dir: Option<RasterMeta>,
    pub flow_acc: Option<RasterMeta>,
    pub read_diagnostics: Vec<Diagnostic>,
}
