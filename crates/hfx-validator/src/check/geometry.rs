//! Geometry spot-check: validates WKB geometry bytes for catchments and snap rows.

use rand::seq::index::sample;
use rand::thread_rng;
use tracing::debug;

use geozero::GeomProcessor;
use geozero::wkb::process_wkb_geom;

use crate::dataset::{CatchmentsData, SnapData};
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate a random sample of catchment geometries as WKB Polygon/MultiPolygon.
///
/// Samples `sample_pct`% of rows (minimum 1 row if any rows exist). For each
/// sampled row, the geometry bytes are checked in three layers:
/// 1. WKB is at least 5 bytes (endianness byte + 4-byte type code).
/// 2. Geometry type is Polygon (3) or MultiPolygon (6), including Z/M/ZM variants.
/// 3. WKB is parseable by geozero without error.
///
/// An empty geometry list produces no diagnostics.
pub fn check_catchment_geometries(data: &CatchmentsData, sample_pct: f64) -> Vec<Diagnostic> {
    let n = data.geometry_wkb.len();
    if n == 0 {
        return Vec::new();
    }

    let sample_count = ((n as f64) * sample_pct / 100.0).ceil().max(1.0) as usize;
    let sample_count = sample_count.min(n);
    let indices = sample(&mut thread_rng(), n, sample_count);

    let mut diags = Vec::new();
    for idx in indices {
        check_single_catchment_geometry(&data.geometry_wkb[idx], idx, &mut diags);
    }

    debug!(
        sampled = sample_count,
        total = n,
        errors = diags.len(),
        "catchment geometry checks complete"
    );
    diags
}

/// Validate ALL snap geometries as WKB Point/LineString.
///
/// For each snap row, the geometry bytes are checked in three layers:
/// 1. WKB is at least 5 bytes.
/// 2. Geometry type is Point (1) or LineString (2), including Z/M/ZM variants.
/// 3. WKB is parseable by geozero without error.
///
/// An empty geometry list produces no diagnostics.
pub fn check_snap_geometries(data: &SnapData) -> Vec<Diagnostic> {
    let mut diags = Vec::new();

    for (idx, wkb) in data.geometry_wkb.iter().enumerate() {
        check_single_snap_geometry(wkb, idx, &mut diags);
    }

    debug!(
        total = data.geometry_wkb.len(),
        errors = diags.len(),
        "snap geometry checks complete"
    );
    diags
}

// ---------------------------------------------------------------------------
// Per-row helpers
// ---------------------------------------------------------------------------

fn check_single_catchment_geometry(wkb: &[u8], row: usize, diags: &mut Vec<Diagnostic>) {
    let location = Location::Row { index: row };

    if wkb.len() < 5 {
        diags.push(
            Diagnostic::error(
                "geometry.catchment_too_short",
                Category::Geometry,
                Artifact::Catchments,
                format!(
                    "row {row}: WKB geometry is only {} byte(s); minimum is 5",
                    wkb.len()
                ),
            )
            .at(location),
        );
        return;
    }

    match wkb_geometry_type(wkb) {
        None => {
            // Already handled by the length guard above; unreachable here.
        }
        Some(type_code) if !is_polygon_type(type_code) => {
            diags.push(
                Diagnostic::error(
                    "geometry.catchment_wrong_type",
                    Category::Geometry,
                    Artifact::Catchments,
                    format!(
                        "row {row}: expected Polygon or MultiPolygon WKB type, got type code {type_code}"
                    ),
                )
                .at(location),
            );
            return;
        }
        _ => {}
    }

    if !is_valid_wkb(wkb) {
        diags.push(
            Diagnostic::error(
                "geometry.catchment_invalid_wkb",
                Category::Geometry,
                Artifact::Catchments,
                format!("row {row}: WKB geometry failed to parse"),
            )
            .at(location),
        );
    }
}

fn check_single_snap_geometry(wkb: &[u8], row: usize, diags: &mut Vec<Diagnostic>) {
    let location = Location::Row { index: row };

    if wkb.len() < 5 {
        diags.push(
            Diagnostic::error(
                "geometry.snap_too_short",
                Category::Geometry,
                Artifact::Snap,
                format!(
                    "row {row}: WKB geometry is only {} byte(s); minimum is 5",
                    wkb.len()
                ),
            )
            .at(location),
        );
        return;
    }

    match wkb_geometry_type(wkb) {
        None => {}
        Some(type_code) if !is_point_or_linestring_type(type_code) => {
            diags.push(
                Diagnostic::error(
                    "geometry.snap_wrong_type",
                    Category::Geometry,
                    Artifact::Snap,
                    format!(
                        "row {row}: expected Point or LineString WKB type, got type code {type_code}"
                    ),
                )
                .at(location),
            );
            return;
        }
        _ => {}
    }

    if !is_valid_wkb(wkb) {
        diags.push(
            Diagnostic::error(
                "geometry.snap_invalid_wkb",
                Category::Geometry,
                Artifact::Snap,
                format!("row {row}: WKB geometry failed to parse"),
            )
            .at(location),
        );
    }
}

// ---------------------------------------------------------------------------
// WKB helpers
// ---------------------------------------------------------------------------

/// Extract the WKB geometry type code from raw bytes.
///
/// Returns `None` when the slice is fewer than 5 bytes. Byte 0 is the
/// endianness flag (`0` = big-endian, `1` = little-endian); bytes 1–4 hold
/// the `u32` type code in the indicated byte order.
fn wkb_geometry_type(wkb: &[u8]) -> Option<u32> {
    if wkb.len() < 5 {
        return None;
    }
    let is_le = wkb[0] == 1;
    let type_bytes: [u8; 4] = wkb[1..5].try_into().ok()?;
    Some(if is_le {
        u32::from_le_bytes(type_bytes)
    } else {
        u32::from_be_bytes(type_bytes)
    })
}

/// Return `true` when `type_code` represents a Polygon or MultiPolygon.
///
/// Covers base types (3, 6) and their Z (1003, 1006), M (2003, 2006), and ZM
/// (3003, 3006) variants, all encoded with the ISO WKB `base + N*1000`
/// convention.
fn is_polygon_type(type_code: u32) -> bool {
    let base = type_code % 1000;
    base == 3 || base == 6
}

/// Return `true` when `type_code` represents a Point or LineString.
///
/// Covers base types (1, 2) and their Z (1001, 1002), M (2001, 2002), and ZM
/// (3001, 3002) variants.
fn is_point_or_linestring_type(type_code: u32) -> bool {
    let base = type_code % 1000;
    base == 1 || base == 2
}

/// Return `true` when `wkb` can be parsed by geozero without error.
fn is_valid_wkb(mut wkb: &[u8]) -> bool {
    struct NullProcessor;
    impl GeomProcessor for NullProcessor {}

    process_wkb_geom(&mut wkb, &mut NullProcessor).is_ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- WKB factory helpers ----

    fn make_wkb_polygon() -> Vec<u8> {
        // Minimal valid little-endian WKB Polygon: 1 ring, 4 points (closed triangle).
        let mut wkb = Vec::new();
        wkb.push(1u8); // little-endian
        wkb.extend_from_slice(&3u32.to_le_bytes()); // type = Polygon
        wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
        wkb.extend_from_slice(&4u32.to_le_bytes()); // 4 points
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes()); // (0, 0)
        wkb.extend_from_slice(&1.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes()); // (1, 0)
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&1.0f64.to_le_bytes()); // (0, 1)
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes()); // (0, 0) close
        wkb
    }

    fn make_wkb_point() -> Vec<u8> {
        let mut wkb = Vec::new();
        wkb.push(1u8); // little-endian
        wkb.extend_from_slice(&1u32.to_le_bytes()); // type = Point
        wkb.extend_from_slice(&1.0f64.to_le_bytes()); // x
        wkb.extend_from_slice(&2.0f64.to_le_bytes()); // y
        wkb
    }

    fn make_wkb_linestring() -> Vec<u8> {
        let mut wkb = Vec::new();
        wkb.push(1u8); // little-endian
        wkb.extend_from_slice(&2u32.to_le_bytes()); // type = LineString
        wkb.extend_from_slice(&2u32.to_le_bytes()); // 2 points
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes()); // (0, 0)
        wkb.extend_from_slice(&1.0f64.to_le_bytes());
        wkb.extend_from_slice(&1.0f64.to_le_bytes()); // (1, 1)
        wkb
    }

    fn make_catchments_data(geometries: Vec<Vec<u8>>) -> CatchmentsData {
        let n = geometries.len();
        CatchmentsData {
            row_count: n,
            ids: vec![0i64; n],
            areas_km2: vec![1.0f32; n],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; n],
            up_area_null_count: 0,
            up_area_total: n,
            geometry_wkb: geometries,
            row_group_sizes: vec![n],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn make_snap_data(geometries: Vec<Vec<u8>>) -> SnapData {
        let n = geometries.len();
        SnapData {
            row_count: n,
            ids: vec![0i64; n],
            catchment_ids: vec![0i64; n],
            weights: vec![1.0f32; n],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; n],
            geometry_wkb: geometries,
            row_group_sizes: vec![n],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn ids_in(diags: &[Diagnostic]) -> Vec<&'static str> {
        diags.iter().map(|d| d.check_id).collect()
    }

    // ---- wkb_geometry_type ----

    #[test]
    fn wkb_geometry_type_too_short_returns_none() {
        assert!(wkb_geometry_type(&[1u8, 3, 0]).is_none());
    }

    #[test]
    fn wkb_geometry_type_le_polygon() {
        let wkb = make_wkb_polygon();
        assert_eq!(wkb_geometry_type(&wkb), Some(3));
    }

    #[test]
    fn wkb_geometry_type_be_polygon() {
        let mut wkb = vec![0u8]; // big-endian
        wkb.extend_from_slice(&3u32.to_be_bytes()); // type = Polygon BE
        assert_eq!(wkb_geometry_type(&wkb), Some(3));
    }

    // ---- is_polygon_type ----

    #[test]
    fn polygon_base_types_accepted() {
        assert!(is_polygon_type(3)); // Polygon
        assert!(is_polygon_type(6)); // MultiPolygon
    }

    #[test]
    fn polygon_z_variants_accepted() {
        assert!(is_polygon_type(1003)); // PolygonZ
        assert!(is_polygon_type(1006)); // MultiPolygonZ
    }

    #[test]
    fn polygon_m_variants_accepted() {
        assert!(is_polygon_type(2003));
        assert!(is_polygon_type(2006));
    }

    #[test]
    fn polygon_zm_variants_accepted() {
        assert!(is_polygon_type(3003));
        assert!(is_polygon_type(3006));
    }

    #[test]
    fn non_polygon_types_rejected() {
        assert!(!is_polygon_type(1)); // Point
        assert!(!is_polygon_type(2)); // LineString
        assert!(!is_polygon_type(4)); // MultiPoint
    }

    // ---- is_point_or_linestring_type ----

    #[test]
    fn point_and_linestring_base_types_accepted() {
        assert!(is_point_or_linestring_type(1));
        assert!(is_point_or_linestring_type(2));
    }

    #[test]
    fn point_z_variants_accepted() {
        assert!(is_point_or_linestring_type(1001));
        assert!(is_point_or_linestring_type(1002));
    }

    #[test]
    fn polygon_rejected_for_snap() {
        assert!(!is_point_or_linestring_type(3));
        assert!(!is_point_or_linestring_type(6));
    }

    // ---- is_valid_wkb ----

    #[test]
    fn valid_polygon_wkb_passes() {
        assert!(is_valid_wkb(&make_wkb_polygon()));
    }

    #[test]
    fn valid_point_wkb_passes() {
        assert!(is_valid_wkb(&make_wkb_point()));
    }

    #[test]
    fn garbage_bytes_fail_parse() {
        // Random garbage that won't parse as valid WKB geometry.
        let garbage = vec![0xFFu8; 20];
        assert!(!is_valid_wkb(&garbage));
    }

    // ---- check_catchment_geometries ----

    #[test]
    fn valid_polygon_produces_no_errors() {
        let data = make_catchments_data(vec![make_wkb_polygon()]);
        let diags = check_catchment_geometries(&data, 100.0);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn empty_catchment_list_produces_no_errors() {
        let data = make_catchments_data(vec![]);
        let diags = check_catchment_geometries(&data, 100.0);
        assert!(diags.is_empty());
    }

    #[test]
    fn too_short_wkb_in_catchments_produces_too_short_error() {
        let data = make_catchments_data(vec![vec![1u8, 2, 3]]);
        let diags = check_catchment_geometries(&data, 100.0);
        assert!(
            ids_in(&diags).contains(&"geometry.catchment_too_short"),
            "expected catchment_too_short, got: {diags:#?}"
        );
    }

    #[test]
    fn point_in_catchments_produces_wrong_type_error() {
        let data = make_catchments_data(vec![make_wkb_point()]);
        let diags = check_catchment_geometries(&data, 100.0);
        assert!(
            ids_in(&diags).contains(&"geometry.catchment_wrong_type"),
            "expected catchment_wrong_type, got: {diags:#?}"
        );
    }

    #[test]
    fn garbage_in_catchments_produces_wrong_type_or_invalid_wkb_error() {
        // 0xFF endianness → wkb_geometry_type will read but the type code will
        // very likely not be a polygon type, triggering wrong_type first.
        // Use a bytes sequence that looks like a valid header but has a correct
        // polygon type code while being otherwise unparseable.
        let mut bad = Vec::new();
        bad.push(1u8); // LE
        bad.extend_from_slice(&3u32.to_le_bytes()); // type = Polygon
        // Claim 999 rings (won't have the data for them → parse fails)
        bad.extend_from_slice(&999u32.to_le_bytes());
        // only 4 extra bytes — not enough to fulfill 999 rings
        bad.extend_from_slice(&[0u8; 4]);

        let data = make_catchments_data(vec![bad]);
        let diags = check_catchment_geometries(&data, 100.0);
        assert!(
            ids_in(&diags).contains(&"geometry.catchment_invalid_wkb"),
            "expected catchment_invalid_wkb, got: {diags:#?}"
        );
    }

    #[test]
    fn sampling_with_one_percent_samples_at_least_one_row() {
        // 100 rows, 1% → ceil(1.0) = 1 row sampled, so at most 1 diagnostic.
        let data = make_catchments_data(vec![make_wkb_polygon(); 100]);
        let diags = check_catchment_geometries(&data, 1.0);
        // All polygons are valid, so no errors regardless of how many are sampled.
        assert!(diags.is_empty());
    }

    #[test]
    fn catchments_with_multipolygon_produces_no_errors() {
        // type code 6 = MultiPolygon
        let mut wkb = Vec::new();
        wkb.push(1u8);
        wkb.extend_from_slice(&6u32.to_le_bytes()); // MultiPolygon
        wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 polygon
        // embed a polygon sub-geometry
        wkb.push(1u8);
        wkb.extend_from_slice(&3u32.to_le_bytes()); // Polygon
        wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
        wkb.extend_from_slice(&4u32.to_le_bytes()); // 4 points
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&1.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&1.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes());
        wkb.extend_from_slice(&0.0f64.to_le_bytes());

        let data = make_catchments_data(vec![wkb]);
        let diags = check_catchment_geometries(&data, 100.0);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    // ---- check_snap_geometries ----

    #[test]
    fn valid_point_in_snap_produces_no_errors() {
        let data = make_snap_data(vec![make_wkb_point()]);
        let diags = check_snap_geometries(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn valid_linestring_in_snap_produces_no_errors() {
        let data = make_snap_data(vec![make_wkb_linestring()]);
        let diags = check_snap_geometries(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn empty_snap_list_produces_no_errors() {
        let data = make_snap_data(vec![]);
        let diags = check_snap_geometries(&data);
        assert!(diags.is_empty());
    }

    #[test]
    fn too_short_wkb_in_snap_produces_too_short_error() {
        let data = make_snap_data(vec![vec![0u8, 0]]);
        let diags = check_snap_geometries(&data);
        assert!(
            ids_in(&diags).contains(&"geometry.snap_too_short"),
            "expected snap_too_short, got: {diags:#?}"
        );
    }

    #[test]
    fn polygon_in_snap_produces_wrong_type_error() {
        let data = make_snap_data(vec![make_wkb_polygon()]);
        let diags = check_snap_geometries(&data);
        assert!(
            ids_in(&diags).contains(&"geometry.snap_wrong_type"),
            "expected snap_wrong_type, got: {diags:#?}"
        );
    }

    #[test]
    fn garbage_in_snap_produces_invalid_wkb_error() {
        // Type code = Point (1) but content is not a valid point.
        let mut bad = Vec::new();
        bad.push(1u8); // LE
        bad.extend_from_slice(&1u32.to_le_bytes()); // type = Point
        // Only 4 bytes of x coordinate — missing y → parse failure
        bad.extend_from_slice(&[0u8; 4]);

        let data = make_snap_data(vec![bad]);
        let diags = check_snap_geometries(&data);
        assert!(
            ids_in(&diags).contains(&"geometry.snap_invalid_wkb"),
            "expected snap_invalid_wkb, got: {diags:#?}"
        );
    }

    #[test]
    fn snap_checks_all_rows() {
        // Two valid points and one polygon (wrong type) — all three rows checked.
        let data = make_snap_data(vec![make_wkb_point(), make_wkb_polygon(), make_wkb_point()]);
        let diags = check_snap_geometries(&data);
        let wrong_type_count = diags
            .iter()
            .filter(|d| d.check_id == "geometry.snap_wrong_type")
            .count();
        assert_eq!(wrong_type_count, 1);
    }
}
