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
/// If `manifest.has_up_area == true` then at least one row must carry a
/// non-null `up_area_km2` value. If `manifest.has_up_area == false`, no row may
/// carry a non-null `up_area_km2` value.
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

    let up_area_non_null_count = data.up_area_total.saturating_sub(data.up_area_null_count);

    if has_up_area && up_area_non_null_count == 0 {
        diags.push(
            Diagnostic::error(
                "values.up_area_empty",
                Category::ValueConsistency,
                Artifact::Catchments,
                format!(
                    "manifest declares has_up_area = true but all {} up_area_km2 values are null",
                    data.up_area_total,
                ),
            )
            .at(Location::Column {
                name: "up_area_km2".into(),
            }),
        );
    } else if !has_up_area && up_area_non_null_count > 0 {
        let first_row = data.first_up_area_non_null_row.unwrap_or(0);
        diags.push(
            Diagnostic::error(
                "values.up_area_unexpected",
                Category::ValueConsistency,
                Artifact::Catchments,
                format!(
                    "manifest declares has_up_area = false but {up_area_non_null_count} of {} up_area_km2 values are non-null; first non-null row is {first_row}",
                    data.up_area_total,
                ),
            )
            .at(Location::Row { index: first_row }),
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

#[cfg(test)]
mod tests {
    use crate::dataset::CatchmentsData;
    use crate::reader::manifest::RawManifest;

    use super::{check_bbox_enclosure, check_up_area_consistency};

    fn manifest(has_up_area: bool) -> RawManifest {
        RawManifest {
            format_version: None,
            fabric_name: None,
            fabric_version: None,
            crs: None,
            has_up_area: Some(has_up_area),
            topology: None,
            region: None,
            bbox: None,
            unit_count: None,
            created_at: None,
            adapter_version: None,
            auxiliary: None,
        }
    }

    fn catchments(
        row_count: usize,
        up_area_null_count: usize,
        first_up_area_non_null_row: Option<usize>,
    ) -> CatchmentsData {
        CatchmentsData {
            row_count,
            ids: (1..=row_count as i64).collect(),
            levels: vec![0; row_count],
            parent_ids: vec![None; row_count],
            areas_km2: vec![1.0; row_count],
            outlet_lons: vec![0.0; row_count],
            outlet_lats: vec![0.0; row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            up_area_null_count,
            first_up_area_non_null_row,
            up_area_total: row_count,
            geometry_wkb: vec![Vec::new(); row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    #[test]
    fn has_up_area_true_allows_partial_nulls() {
        let diagnostics = check_up_area_consistency(&manifest(true), &catchments(3, 1, Some(0)));

        assert!(diagnostics.is_empty());
    }

    #[test]
    fn has_up_area_true_rejects_entirely_null_column() {
        let diagnostics = check_up_area_consistency(&manifest(true), &catchments(3, 3, None));

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].check_id, "values.up_area_empty");
    }

    #[test]
    fn has_up_area_false_rejects_populated_values() {
        let diagnostics = check_up_area_consistency(&manifest(false), &catchments(3, 2, Some(1)));

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].check_id, "values.up_area_unexpected");
    }

    #[test]
    fn bbox_enclosure_allows_exact_widened_float32_boundary() {
        let maxx = -11.442605_f32;
        let mut raw_manifest = manifest(false);
        raw_manifest.bbox = Some(vec![-20.0, -1.0, f64::from(maxx), 1.0]);
        let mut data = catchments(1, 1, None);
        data.bboxes = vec![[-20.0, -1.0, maxx, 1.0]];

        assert_eq!(f64::from(maxx).to_bits(), 0xc026e29d20000000);
        let diagnostics = check_bbox_enclosure(&raw_manifest, &data);

        assert!(diagnostics.is_empty(), "{diagnostics:#?}");
    }
}
