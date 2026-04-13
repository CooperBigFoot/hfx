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
pub fn check_up_area_consistency(raw_manifest: &RawManifest, data: &CatchmentsData) -> Vec<Diagnostic> {
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
            .at(Location::Column { name: "up_area_km2".into() }),
        );
    }

    debug!(count = diags.len(), "C4 up_area consistency checks complete");
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
    use super::*;
    use crate::dataset::CatchmentsData;
    use crate::reader::manifest::RawManifest;

    // --- helpers ---

    fn make_catchments(
        bboxes: Vec<[f32; 4]>,
        up_area_null_count: usize,
    ) -> CatchmentsData {
        let row_count = bboxes.len().max(1);
        CatchmentsData {
            row_count,
            ids: (1..=(row_count as i64)).collect(),
            areas_km2: vec![1.0; row_count],
            bboxes,
            up_area_null_count,
            up_area_total: row_count,
            geometry_wkb: vec![vec![0u8; 5]; row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn raw_with_up_area(has_up_area: Option<bool>, bbox: Option<Vec<f64>>) -> RawManifest {
        RawManifest {
            format_version: Some("0.1".into()),
            fabric_name: Some("testfabric".into()),
            fabric_version: None,
            fabric_level: None,
            crs: Some("EPSG:4326".into()),
            has_up_area,
            has_rasters: Some(false),
            has_snap: Some(false),
            flow_dir_encoding: None,
            terminal_sink_id: Some(0),
            topology: Some("tree".into()),
            region: None,
            bbox,
            atom_count: Some(10),
            created_at: Some("2026-01-01T00:00:00Z".into()),
            adapter_version: Some("v1".into()),
        }
    }

    fn count_id(diags: &[Diagnostic], id: &str) -> usize {
        diags.iter().filter(|d| d.check_id == id).count()
    }

    // ========================
    // C4: check_up_area_consistency
    // ========================

    #[test]
    fn c4_has_up_area_true_all_present_no_error() {
        let raw = raw_with_up_area(Some(true), None);
        // null_count = 0 → all present
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_up_area_consistency(&raw, &data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c4_has_up_area_true_with_nulls_produces_error() {
        let raw = raw_with_up_area(Some(true), None);
        let mut data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]; 5], 2);
        data.up_area_total = 5;
        data.up_area_null_count = 2;
        let diags = check_up_area_consistency(&raw, &data);
        assert_eq!(count_id(&diags, "values.up_area_consistency"), 1);
    }

    #[test]
    fn c4_has_up_area_false_nulls_ok() {
        let raw = raw_with_up_area(Some(false), None);
        let mut data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 1);
        data.up_area_null_count = 1; // nulls present but has_up_area is false
        let diags = check_up_area_consistency(&raw, &data);
        assert!(diags.is_empty(), "has_up_area=false; nulls are not an error");
    }

    #[test]
    fn c4_has_up_area_absent_check_skipped() {
        let raw = raw_with_up_area(None, None);
        let mut data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 1);
        data.up_area_null_count = 99;
        let diags = check_up_area_consistency(&raw, &data);
        assert!(diags.is_empty(), "when has_up_area is absent, check should be skipped");
    }

    // ========================
    // D4: check_bbox_enclosure
    // ========================

    #[test]
    fn d4_manifest_bbox_encloses_catchments_no_error() {
        let raw = raw_with_up_area(Some(false), Some(vec![-20.0, -10.0, 20.0, 10.0]));
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn d4_manifest_bbox_too_small_in_x_produces_error() {
        // manifest minx (0) > catchment minx (-10)
        let raw = raw_with_up_area(Some(false), Some(vec![0.0, -10.0, 20.0, 10.0]));
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert_eq!(count_id(&diags, "values.bbox_enclosure"), 1);
    }

    #[test]
    fn d4_manifest_bbox_too_small_in_y_produces_error() {
        // manifest miny (-1) > catchment union miny (-5)
        let raw = raw_with_up_area(Some(false), Some(vec![-20.0, -1.0, 20.0, 10.0]));
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert_eq!(count_id(&diags, "values.bbox_enclosure"), 1);
    }

    #[test]
    fn d4_manifest_maxx_too_small_produces_error() {
        // manifest maxx (5) < catchment union maxx (10)
        let raw = raw_with_up_area(Some(false), Some(vec![-20.0, -10.0, 5.0, 10.0]));
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert_eq!(count_id(&diags, "values.bbox_enclosure"), 1);
    }

    #[test]
    fn d4_manifest_maxy_too_small_produces_error() {
        // manifest maxy (1) < catchment union maxy (5)
        let raw = raw_with_up_area(Some(false), Some(vec![-20.0, -10.0, 20.0, 1.0]));
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert_eq!(count_id(&diags, "values.bbox_enclosure"), 1);
    }

    #[test]
    fn d4_no_manifest_bbox_check_skipped() {
        let raw = raw_with_up_area(Some(false), None);
        let data = make_catchments(vec![[-10.0, -5.0, 10.0, 5.0]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert!(diags.is_empty(), "no manifest bbox → check skipped");
    }

    #[test]
    fn d4_all_catchment_bboxes_nonfinite_check_skipped() {
        let raw = raw_with_up_area(Some(false), Some(vec![-20.0, -10.0, 20.0, 10.0]));
        let data = make_catchments(vec![[f32::NAN, f32::NAN, f32::NAN, f32::NAN]], 0);
        let diags = check_bbox_enclosure(&raw, &data);
        assert!(diags.is_empty(), "no finite catchment bboxes → check skipped");
    }

    #[test]
    fn d4_multiple_catchments_union_checked() {
        // Two catchments whose union is [-15, -8, 15, 8].
        // Manifest covers exactly that → no error.
        let raw = raw_with_up_area(Some(false), Some(vec![-15.0, -8.0, 15.0, 8.0]));
        let data = make_catchments(
            vec![
                [-15.0, -8.0, 0.0, 0.0],
                [0.0, 0.0, 15.0, 8.0],
            ],
            0,
        );
        let diags = check_bbox_enclosure(&raw, &data);
        assert!(diags.is_empty(), "manifest exactly matches union — no error expected");
    }

    #[test]
    fn d4_multiple_catchments_union_exceeds_manifest_produces_errors() {
        // Catchment union is [-15, -8, 15, 8] but manifest only covers [-10, -5, 10, 5].
        let raw = raw_with_up_area(Some(false), Some(vec![-10.0, -5.0, 10.0, 5.0]));
        let data = make_catchments(
            vec![
                [-15.0, -8.0, 0.0, 0.0],
                [0.0, 0.0, 15.0, 8.0],
            ],
            0,
        );
        let diags = check_bbox_enclosure(&raw, &data);
        // All four dimensions violated
        assert_eq!(count_id(&diags, "values.bbox_enclosure"), 4);
    }
}
