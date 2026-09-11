//! Geometry spot-check: validates WKB geometry bytes for catchments and snap rows.

use rand::seq::index::sample;
use rand::thread_rng;
use tracing::debug;

use geozero::GeomProcessor;
use geozero::wkb::process_wkb_geom;

use crate::dataset::{CatchmentsData, GeometryRetention, SnapData};
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Validate catchment geometries as WKB Polygon/MultiPolygon.
///
/// Exactly 100% coverage visits rows sequentially, or returns checks completed
/// during reading. Partial coverage retains the existing random sample.
///
/// Samples `sample_pct`% of rows (minimum 1 row if any rows exist). For each
/// sampled row, the geometry bytes are checked in three layers:
/// 1. WKB is at least 5 bytes (endianness byte + 4-byte type code).
/// 2. Geometry type is Polygon (3) or MultiPolygon (6), including Z/M/ZM variants.
/// 3. WKB is parseable by geozero without error.
///
/// An empty geometry list produces no diagnostics.
pub fn check_catchment_geometries(data: &CatchmentsData, sample_pct: f64) -> Vec<Diagnostic> {
    let payloads = match &data.geometry {
        GeometryRetention::Checked(diags) => return diags.clone(),
        GeometryRetention::Buffered(payloads) => payloads,
    };
    let n = payloads.len();
    if n == 0 {
        return Vec::new();
    }

    if sample_pct == 100.0 {
        let mut diags = Vec::new();
        for (idx, wkb) in payloads.iter().enumerate() {
            check_single_catchment_geometry(wkb, idx, &mut diags);
        }
        debug!(
            sampled = n,
            total = n,
            errors = diags.len(),
            "catchment geometry checks complete"
        );
        return diags;
    }

    let sample_count = ((n as f64) * sample_pct / 100.0).ceil().max(1.0) as usize;
    let sample_count = sample_count.min(n);
    #[cfg(test)]
    RANDOM_SELECTIONS.with(|count| count.set(count.get() + 1));
    let indices = sample(&mut thread_rng(), n, sample_count);

    let mut diags = Vec::new();
    for idx in indices {
        check_single_catchment_geometry(&payloads[idx], idx, &mut diags);
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
    let payloads = match &data.geometry {
        GeometryRetention::Checked(diags) => return diags.clone(),
        GeometryRetention::Buffered(payloads) => payloads,
    };
    let mut diags = Vec::new();

    for (idx, wkb) in payloads.iter().enumerate() {
        check_single_snap_geometry(wkb, idx, &mut diags);
    }

    debug!(
        total = payloads.len(),
        errors = diags.len(),
        "snap geometry checks complete"
    );
    diags
}

// ---------------------------------------------------------------------------
// Per-row helpers
// ---------------------------------------------------------------------------

pub(crate) fn check_single_catchment_geometry(wkb: &[u8], row: usize, diags: &mut Vec<Diagnostic>) {
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

pub(crate) fn check_single_snap_geometry(wkb: &[u8], row: usize, diags: &mut Vec<Diagnostic>) {
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
thread_local! {
    static RANDOM_SELECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::{RANDOM_SELECTIONS, check_catchment_geometries};

    #[test]
    fn full_parquet_geometry_check_avoids_random_selection() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../conformance/valid/tiny/catchments.parquet");
        let (data, _) = crate::reader::catchments::read_catchments(&path);
        let data = data.expect("real Parquet fixture must read");
        assert!(data.row_count > 0);
        RANDOM_SELECTIONS.with(|count| count.set(0));
        check_catchment_geometries(&data, 100.0);
        RANDOM_SELECTIONS
            .with(|count| assert_eq!(count.get(), 0, "full coverage entered random sampling"));
        check_catchment_geometries(&data, 1.0);
        RANDOM_SELECTIONS
            .with(|count| assert_eq!(count.get(), 1, "partial coverage must retain sampling"));
    }
}
