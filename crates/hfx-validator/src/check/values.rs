//! Value consistency checks.
//!
//! These checks verify that tabular data values are internally consistent and
//! agree with what the manifest declares.

use tracing::debug;

use crate::dataset::CatchmentsData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};
use crate::reader::manifest::RawManifest;

// ---------------------------------------------------------------------------
// C4: up_area consistency
// ---------------------------------------------------------------------------

/// C4 — Check `up_area_km2` consistency.
///
/// If `manifest.has_up_area == true` then every row must carry a non-null
/// `up_area_km2` value, i.e. `up_area_null_count` must be 0.
pub fn check_up_area_consistency(
    raw_manifest: &RawManifest,
    data: &CatchmentsData,
) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    let has_up_area = match raw_manifest.has_up_area {
        Some(v) => v,
        // Field absent — manifest checks will have already reported this;
        // we cannot make an inference here, so skip.
        None => return diags,
    };

    if has_up_area && data.up_area_null_count > 0 {
        diags.push(
            Diagnostic::error(
                "values.up_area_consistency",
                Category::ValueConsistency,
                Artifact::Catchments,
                format!(
                    "manifest declares has_up_area = true but {} of {} up_area_km2 values are null",
                    data.up_area_null_count, data.up_area_total,
                ),
            )
            .at(Location::Column {
                name: "up_area_km2".into(),
            }),
        );
    }

    debug!(
        count = diags.len(),
        "C4 up_area consistency checks complete"
    );
    diags
}

// ---------------------------------------------------------------------------
// C5: outlet coordinate domain
// ---------------------------------------------------------------------------

/// C5 — Check outlet coordinates are finite WGS84 lon/lat values.
pub fn check_outlet_coords(data: &CatchmentsData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    for (idx, (&lon, &lat)) in data
        .outlet_lons
        .iter()
        .zip(data.outlet_lats.iter())
        .enumerate()
    {
        if !lon.is_finite() {
            diags.push(
                Diagnostic::error(
                    "values.outlet_lon",
                    Category::ValueConsistency,
                    Artifact::Catchments,
                    format!(
                        "outlet_lon at row {idx} is not finite; outlet longitude must be finite"
                    ),
                )
                .at(Location::Row { index: idx }),
            );
        } else if !(-180.0..=180.0).contains(&lon) {
            diags.push(
                Diagnostic::error(
                    "values.outlet_lon",
                    Category::ValueConsistency,
                    Artifact::Catchments,
                    format!(
                        "outlet_lon at row {idx} is {lon}; outlet longitude must be in [-180, 180]"
                    ),
                )
                .at(Location::Row { index: idx }),
            );
        }

        if !lat.is_finite() {
            diags.push(
                Diagnostic::error(
                    "values.outlet_lat",
                    Category::ValueConsistency,
                    Artifact::Catchments,
                    format!(
                        "outlet_lat at row {idx} is not finite; outlet latitude must be finite"
                    ),
                )
                .at(Location::Row { index: idx }),
            );
        } else if !(-90.0..=90.0).contains(&lat) {
            diags.push(
                Diagnostic::error(
                    "values.outlet_lat",
                    Category::ValueConsistency,
                    Artifact::Catchments,
                    format!(
                        "outlet_lat at row {idx} is {lat}; outlet latitude must be in [-90, 90]"
                    ),
                )
                .at(Location::Row { index: idx }),
            );
        }
    }

    debug!(count = diags.len(), "C5 outlet coordinate checks complete");
    diags
}

// ---------------------------------------------------------------------------
// D4: Manifest bbox enclosure
// ---------------------------------------------------------------------------

/// D4 — Check the manifest bbox encloses all catchment bboxes.
///
/// Computes the union of all catchment bboxes (min of minx/miny, max of
/// maxx/maxy).  Then verifies that the manifest bbox contains this union, i.e.:
///
/// ```text
/// manifest.minx <= union.minx  &&  manifest.miny <= union.miny
/// manifest.maxx >= union.maxx  &&  manifest.maxy >= union.maxy
/// ```
///
/// Catchment bbox components that are non-finite are skipped (they are caught
/// by C2).  If there are no finite catchment bboxes, this check is skipped.
pub fn check_bbox_enclosure(raw_manifest: &RawManifest, data: &CatchmentsData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    // Need the manifest bbox to proceed.
    let manifest_bbox = match raw_manifest.bbox.as_deref() {
        Some(coords) if coords.len() == 4 => coords,
        // Missing or malformed manifest bbox is already caught by manifest
        // checks; nothing we can do here.
        _ => return diags,
    };

    let (m_minx, m_miny, m_maxx, m_maxy) = (
        manifest_bbox[0],
        manifest_bbox[1],
        manifest_bbox[2],
        manifest_bbox[3],
    );

    // Compute catchment bbox union, skipping non-finite values.
    let mut union_minx = f64::INFINITY;
    let mut union_miny = f64::INFINITY;
    let mut union_maxx = f64::NEG_INFINITY;
    let mut union_maxy = f64::NEG_INFINITY;
    let mut any_finite = false;

    for bbox in &data.bboxes {
        let [minx, miny, maxx, maxy] = *bbox;
        // Skip entire bbox if any component is non-finite (C2 already reports it).
        if !minx.is_finite() || !miny.is_finite() || !maxx.is_finite() || !maxy.is_finite() {
            continue;
        }
        let (minx, miny, maxx, maxy) = (
            f64::from(minx),
            f64::from(miny),
            f64::from(maxx),
            f64::from(maxy),
        );
        union_minx = union_minx.min(minx);
        union_miny = union_miny.min(miny);
        union_maxx = union_maxx.max(maxx);
        union_maxy = union_maxy.max(maxy);
        any_finite = true;
    }

    if !any_finite {
        // No valid catchment bboxes to check against.
        return diags;
    }

    // Check enclosure in each dimension.
    if m_minx > union_minx {
        diags.push(
            Diagnostic::error(
                "values.bbox_enclosure",
                Category::ValueConsistency,
                Artifact::CrossFile,
                format!(
                    "manifest bbox minx ({m_minx}) is greater than catchment union minx ({union_minx:.6}); manifest bbox must enclose all catchments"
                ),
            )
            .at(Location::Field { name: "bbox".into() }),
        );
    }
    if m_miny > union_miny {
        diags.push(
            Diagnostic::error(
                "values.bbox_enclosure",
                Category::ValueConsistency,
                Artifact::CrossFile,
                format!(
                    "manifest bbox miny ({m_miny}) is greater than catchment union miny ({union_miny:.6}); manifest bbox must enclose all catchments"
                ),
            )
            .at(Location::Field { name: "bbox".into() }),
        );
    }
    if m_maxx < union_maxx {
        diags.push(
            Diagnostic::error(
                "values.bbox_enclosure",
                Category::ValueConsistency,
                Artifact::CrossFile,
                format!(
                    "manifest bbox maxx ({m_maxx}) is less than catchment union maxx ({union_maxx:.6}); manifest bbox must enclose all catchments"
                ),
            )
            .at(Location::Field { name: "bbox".into() }),
        );
    }
    if m_maxy < union_maxy {
        diags.push(
            Diagnostic::error(
                "values.bbox_enclosure",
                Category::ValueConsistency,
                Artifact::CrossFile,
                format!(
                    "manifest bbox maxy ({m_maxy}) is less than catchment union maxy ({union_maxy:.6}); manifest bbox must enclose all catchments"
                ),
            )
            .at(Location::Field { name: "bbox".into() }),
        );
    }

    debug!(count = diags.len(), "D4 bbox enclosure checks complete");
    diags
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
