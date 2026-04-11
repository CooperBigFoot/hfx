//! Parsed dataset intermediate representation.

use std::path::PathBuf;

use hfx_core::Manifest;

use crate::diagnostic::Diagnostic;

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
#[derive(Debug, Clone)]
pub struct RasterMeta {
    pub width: u32,
    pub height: u32,
    pub bits_per_sample: u16,
    pub sample_format: RasterSampleFormat,
    pub is_tiled: bool,
    pub tile_width: Option<u32>,
    pub tile_height: Option<u32>,
    pub nodata: Option<f64>,
}

/// Raster sample format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RasterSampleFormat {
    UnsignedInt,
    SignedInt,
    Float,
    Unknown(u16),
}

/// The complete parsed dataset, ready for validation checks.
#[derive(Debug)]
pub struct ParsedDataset {
    pub files: FilePresenceMap,
    pub manifest_json: Option<serde_json::Value>,
    pub manifest: Option<Manifest>,
    pub catchments: Option<CatchmentsData>,
    pub graph: Option<GraphData>,
    pub snap: Option<SnapData>,
    pub flow_dir: Option<RasterMeta>,
    pub flow_acc: Option<RasterMeta>,
    pub read_diagnostics: Vec<Diagnostic>,
}
