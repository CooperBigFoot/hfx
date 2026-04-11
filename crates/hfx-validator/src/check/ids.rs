//! ID domain constraint checks.
//!
//! All functions accept pre-loaded column data and return a flat list of
//! [`Diagnostic`]s. They never panic and never perform I/O.
//!
//! Violation reporting is capped at 100 per check; a summary diagnostic is
//! appended when more violations exist.

use std::collections::HashSet;

use tracing::debug;

use crate::dataset::{CatchmentsData, GraphData, SnapData};
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};

// Maximum individual row-level violations reported before we emit a summary.
const MAX_VIOLATIONS: usize = 100;

// ---------------------------------------------------------------------------
// C1: Catchment IDs
// ---------------------------------------------------------------------------

/// C1 — Check all catchment IDs are positive and non-zero.
///
/// Every `id` in `CatchmentsData.ids` must be > 0.  Zero and negative values
/// each produce a separate check_id so they can be filtered independently.
pub fn check_catchment_ids(data: &CatchmentsData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut zero_count = 0usize;
    let mut neg_count = 0usize;

    for (i, &id) in data.ids.iter().enumerate() {
        if id == 0 {
            zero_count += 1;
            if zero_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.catchment_zero",
                        Category::IdConstraint,
                        Artifact::Catchments,
                        format!("catchment id at row {i} is 0; all IDs must be > 0"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        } else if id < 0 {
            neg_count += 1;
            if neg_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.catchment_negative",
                        Category::IdConstraint,
                        Artifact::Catchments,
                        format!("catchment id {id} at row {i} is negative; all IDs must be > 0"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    if zero_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.catchment_zero",
            Category::IdConstraint,
            Artifact::Catchments,
            format!("... and {} more zero catchment ID violations (only first {MAX_VIOLATIONS} shown)", zero_count - MAX_VIOLATIONS),
        ));
    }
    if neg_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.catchment_negative",
            Category::IdConstraint,
            Artifact::Catchments,
            format!("... and {} more negative catchment ID violations (only first {MAX_VIOLATIONS} shown)", neg_count - MAX_VIOLATIONS),
        ));
    }

    debug!(count = diags.len(), "C1 catchment ID checks complete");
    diags
}

// ---------------------------------------------------------------------------
// C2: Catchment bboxes
// ---------------------------------------------------------------------------

/// C2 — Check all catchment bbox values are valid.
///
/// Each bbox is `[minx, miny, maxx, maxy]` (f32). Checks:
/// - All four components are finite (not NaN, not ±Inf).
/// - `minx` and `maxx` are in `[-180, 180]`.
/// - `miny` and `maxy` are in `[-90, 90]`.
/// - `minx < maxx` (non-degenerate x extent).
/// - `miny < maxy` (non-degenerate y extent).
pub fn check_catchment_bboxes(data: &CatchmentsData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut violation_count = 0usize;

    for (i, bbox) in data.bboxes.iter().enumerate() {
        let [minx, miny, maxx, maxy] = *bbox;
        let row_errors = validate_bbox_f32(minx, miny, maxx, maxy, i, Artifact::Catchments);
        for d in row_errors {
            violation_count += 1;
            if violation_count <= MAX_VIOLATIONS {
                diags.push(d);
            }
        }
    }

    if violation_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.catchment_bbox",
            Category::IdConstraint,
            Artifact::Catchments,
            format!("... and {} more bbox violations (only first {MAX_VIOLATIONS} shown)", violation_count - MAX_VIOLATIONS),
        ));
    }

    debug!(count = diags.len(), "C2 catchment bbox checks complete");
    diags
}

// ---------------------------------------------------------------------------
// C3: Catchment areas
// ---------------------------------------------------------------------------

/// C3 — Check all `area_km2` values are finite and non-negative.
pub fn check_catchment_areas(data: &CatchmentsData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut violation_count = 0usize;

    for (i, &area) in data.areas_km2.iter().enumerate() {
        let ok = if !area.is_finite() {
            Some(format!("area_km2 at row {i} is not finite ({area}); must be a finite non-negative number"))
        } else if area < 0.0 {
            Some(format!("area_km2 {area} at row {i} is negative; must be >= 0"))
        } else {
            None
        };

        if let Some(msg) = ok {
            violation_count += 1;
            if violation_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.catchment_area",
                        Category::IdConstraint,
                        Artifact::Catchments,
                        msg,
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    if violation_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.catchment_area",
            Category::IdConstraint,
            Artifact::Catchments,
            format!("... and {} more area_km2 violations (only first {MAX_VIOLATIONS} shown)", violation_count - MAX_VIOLATIONS),
        ));
    }

    debug!(count = diags.len(), "C3 catchment area checks complete");
    diags
}

// ---------------------------------------------------------------------------
// C5: Graph IDs
// ---------------------------------------------------------------------------

/// C5 — Check all graph IDs are positive, non-zero, and unique.
pub fn check_graph_ids(data: &GraphData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut seen: HashSet<i64> = HashSet::with_capacity(data.ids.len());
    let mut zero_count = 0usize;
    let mut neg_count = 0usize;
    let mut dup_count = 0usize;

    for (i, &id) in data.ids.iter().enumerate() {
        if id == 0 {
            zero_count += 1;
            if zero_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.graph_zero",
                        Category::IdConstraint,
                        Artifact::Graph,
                        format!("graph id at row {i} is 0; all IDs must be > 0"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        } else if id < 0 {
            neg_count += 1;
            if neg_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.graph_negative",
                        Category::IdConstraint,
                        Artifact::Graph,
                        format!("graph id {id} at row {i} is negative; all IDs must be > 0"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        } else if !seen.insert(id) {
            // id > 0 but already in the set — duplicate
            dup_count += 1;
            if dup_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.graph_duplicate",
                        Category::IdConstraint,
                        Artifact::Graph,
                        format!("graph id {id} at row {i} is duplicated; all IDs must be unique"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    if zero_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.graph_zero",
            Category::IdConstraint,
            Artifact::Graph,
            format!("... and {} more zero graph ID violations (only first {MAX_VIOLATIONS} shown)", zero_count - MAX_VIOLATIONS),
        ));
    }
    if neg_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.graph_negative",
            Category::IdConstraint,
            Artifact::Graph,
            format!("... and {} more negative graph ID violations (only first {MAX_VIOLATIONS} shown)", neg_count - MAX_VIOLATIONS),
        ));
    }
    if dup_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.graph_duplicate",
            Category::IdConstraint,
            Artifact::Graph,
            format!("... and {} more duplicate graph ID violations (only first {MAX_VIOLATIONS} shown)", dup_count - MAX_VIOLATIONS),
        ));
    }

    debug!(count = diags.len(), "C5 graph ID checks complete");
    diags
}

// ---------------------------------------------------------------------------
// C6: Upstream IDs
// ---------------------------------------------------------------------------

/// C6 — Check all `upstream_ids` entries are positive and non-zero.
pub fn check_upstream_ids(data: &GraphData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut zero_count = 0usize;
    let mut neg_count = 0usize;

    for (row, upstream_list) in data.upstream_ids.iter().enumerate() {
        for (entry_idx, &uid) in upstream_list.iter().enumerate() {
            if uid == 0 {
                zero_count += 1;
                if zero_count <= MAX_VIOLATIONS {
                    diags.push(
                        Diagnostic::error(
                            "ids.upstream_zero",
                            Category::IdConstraint,
                            Artifact::Graph,
                            format!("upstream_id at row {row}, entry {entry_idx} is 0; all upstream IDs must be > 0"),
                        )
                        .at(Location::Row { index: row }),
                    );
                }
            } else if uid < 0 {
                neg_count += 1;
                if neg_count <= MAX_VIOLATIONS {
                    diags.push(
                        Diagnostic::error(
                            "ids.upstream_negative",
                            Category::IdConstraint,
                            Artifact::Graph,
                            format!("upstream_id {uid} at row {row}, entry {entry_idx} is negative; all upstream IDs must be > 0"),
                        )
                        .at(Location::Row { index: row }),
                    );
                }
            }
        }
    }

    if zero_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.upstream_zero",
            Category::IdConstraint,
            Artifact::Graph,
            format!("... and {} more zero upstream ID violations (only first {MAX_VIOLATIONS} shown)", zero_count - MAX_VIOLATIONS),
        ));
    }
    if neg_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.upstream_negative",
            Category::IdConstraint,
            Artifact::Graph,
            format!("... and {} more negative upstream ID violations (only first {MAX_VIOLATIONS} shown)", neg_count - MAX_VIOLATIONS),
        ));
    }

    debug!(count = diags.len(), "C6 upstream ID checks complete");
    diags
}

// ---------------------------------------------------------------------------
// C7: Snap data
// ---------------------------------------------------------------------------

/// C7 — Check snap IDs, catchment_ids, weights, and bboxes.
///
/// - `ids`: must be > 0.
/// - `catchment_ids`: must be > 0.
/// - `weights`: must be finite and >= 0.
/// - `bboxes`: must be valid (see [`validate_bbox_f32`]).
pub fn check_snap_data(data: &SnapData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut id_violation = 0usize;
    let mut cid_violation = 0usize;
    let mut weight_violation = 0usize;
    let mut bbox_violation = 0usize;

    for (i, &id) in data.ids.iter().enumerate() {
        if id <= 0 {
            id_violation += 1;
            if id_violation <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.snap_id",
                        Category::IdConstraint,
                        Artifact::Snap,
                        format!("snap id {id} at row {i} must be > 0"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    for (i, &cid) in data.catchment_ids.iter().enumerate() {
        if cid <= 0 {
            cid_violation += 1;
            if cid_violation <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.snap_catchment_id",
                        Category::IdConstraint,
                        Artifact::Snap,
                        format!("snap catchment_id {cid} at row {i} must be > 0"),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    for (i, &w) in data.weights.iter().enumerate() {
        let bad = if !w.is_finite() {
            Some(format!("snap weight at row {i} is not finite ({w})"))
        } else if w < 0.0 {
            Some(format!("snap weight {w} at row {i} is negative; must be >= 0"))
        } else {
            None
        };
        if let Some(msg) = bad {
            weight_violation += 1;
            if weight_violation <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.snap_weight",
                        Category::IdConstraint,
                        Artifact::Snap,
                        msg,
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    for (i, bbox) in data.bboxes.iter().enumerate() {
        let [minx, miny, maxx, maxy] = *bbox;
        let row_errors = validate_bbox_f32(minx, miny, maxx, maxy, i, Artifact::Snap);
        for d in row_errors {
            bbox_violation += 1;
            if bbox_violation <= MAX_VIOLATIONS {
                diags.push(d);
            }
        }
    }

    // Summary overflows
    if id_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_id",
            Category::IdConstraint,
            Artifact::Snap,
            format!("... and {} more snap ID violations (only first {MAX_VIOLATIONS} shown)", id_violation - MAX_VIOLATIONS),
        ));
    }
    if cid_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_catchment_id",
            Category::IdConstraint,
            Artifact::Snap,
            format!("... and {} more snap catchment_id violations (only first {MAX_VIOLATIONS} shown)", cid_violation - MAX_VIOLATIONS),
        ));
    }
    if weight_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_weight",
            Category::IdConstraint,
            Artifact::Snap,
            format!("... and {} more snap weight violations (only first {MAX_VIOLATIONS} shown)", weight_violation - MAX_VIOLATIONS),
        ));
    }
    if bbox_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_bbox",
            Category::IdConstraint,
            Artifact::Snap,
            format!("... and {} more snap bbox violations (only first {MAX_VIOLATIONS} shown)", bbox_violation - MAX_VIOLATIONS),
        ));
    }

    debug!(count = diags.len(), "C7 snap data checks complete");
    diags
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Validate a single `[minx, miny, maxx, maxy]` bbox (f32 values).
///
/// Returns zero or more [`Diagnostic`]s describing every constraint that is
/// violated. Uses `"ids.catchment_bbox"` or an equivalent check_id — the
/// artifact is supplied by the caller so this helper is reusable.
fn validate_bbox_f32(
    minx: f32,
    miny: f32,
    maxx: f32,
    maxy: f32,
    row: usize,
    artifact: Artifact,
) -> Vec<Diagnostic> {
    let mut out: Vec<Diagnostic> = Vec::new();

    let check_id: &'static str = match artifact {
        Artifact::Catchments => "ids.catchment_bbox",
        Artifact::Snap => "ids.snap_bbox",
        _ => "ids.bbox",
    };

    // Finite check
    for (name, val) in [("minx", minx), ("miny", miny), ("maxx", maxx), ("maxy", maxy)] {
        if !val.is_finite() {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox component '{name}' at row {row} is not finite ({val})"),
                )
                .at(Location::Row { index: row }),
            );
        }
    }

    // Only continue with range / ordering checks if all components are finite.
    if out.is_empty() {
        if !(-180.0..=180.0_f32).contains(&minx) {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox minx {minx} at row {row} is out of range [-180, 180]"),
                )
                .at(Location::Row { index: row }),
            );
        }
        if !(-180.0..=180.0_f32).contains(&maxx) {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox maxx {maxx} at row {row} is out of range [-180, 180]"),
                )
                .at(Location::Row { index: row }),
            );
        }
        if !(-90.0..=90.0_f32).contains(&miny) {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox miny {miny} at row {row} is out of range [-90, 90]"),
                )
                .at(Location::Row { index: row }),
            );
        }
        if !(-90.0..=90.0_f32).contains(&maxy) {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox maxy {maxy} at row {row} is out of range [-90, 90]"),
                )
                .at(Location::Row { index: row }),
            );
        }
        if minx >= maxx {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox at row {row} is degenerate in x: minx ({minx}) >= maxx ({maxx})"),
                )
                .at(Location::Row { index: row }),
            );
        }
        if miny >= maxy {
            out.push(
                Diagnostic::error(
                    check_id,
                    Category::IdConstraint,
                    artifact,
                    format!("bbox at row {row} is degenerate in y: miny ({miny}) >= maxy ({maxy})"),
                )
                .at(Location::Row { index: row }),
            );
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{CatchmentsData, GraphData, SnapData};

    // --- helpers ---

    fn make_catchments(ids: Vec<i64>, bboxes: Vec<[f32; 4]>, areas: Vec<f32>) -> CatchmentsData {
        let row_count = ids.len();
        CatchmentsData {
            row_count,
            ids,
            areas_km2: areas,
            bboxes,
            up_area_null_count: 0,
            up_area_total: row_count,
            geometry_wkb: vec![vec![0u8; 5]; row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn valid_bbox() -> [f32; 4] {
        [-10.0, -5.0, 10.0, 5.0]
    }

    fn make_graph(ids: Vec<i64>, upstream_ids: Vec<Vec<i64>>) -> GraphData {
        GraphData { ids, upstream_ids }
    }

    fn make_snap(
        ids: Vec<i64>,
        catchment_ids: Vec<i64>,
        weights: Vec<f32>,
        bboxes: Vec<[f32; 4]>,
    ) -> SnapData {
        let row_count = ids.len();
        SnapData {
            row_count,
            ids,
            catchment_ids,
            weights,
            bboxes,
            geometry_wkb: vec![vec![0u8; 5]; row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn count_id(diags: &[Diagnostic], id: &str) -> usize {
        diags.iter().filter(|d| d.check_id == id).count()
    }

    // ========================
    // C1: check_catchment_ids
    // ========================

    #[test]
    fn c1_valid_ids_produce_no_errors() {
        let data = make_catchments(vec![1, 2, 3], vec![valid_bbox(); 3], vec![1.0; 3]);
        let diags = check_catchment_ids(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c1_zero_id_produces_error() {
        let data = make_catchments(vec![1, 0, 3], vec![valid_bbox(); 3], vec![1.0; 3]);
        let diags = check_catchment_ids(&data);
        assert_eq!(count_id(&diags, "ids.catchment_zero"), 1);
        assert!(diags[0].location == Location::Row { index: 1 });
    }

    #[test]
    fn c1_negative_id_produces_error() {
        let data = make_catchments(vec![1, -5, 3], vec![valid_bbox(); 3], vec![1.0; 3]);
        let diags = check_catchment_ids(&data);
        assert_eq!(count_id(&diags, "ids.catchment_negative"), 1);
        assert!(diags[0].location == Location::Row { index: 1 });
    }

    #[test]
    fn c1_multiple_violations_reported() {
        let data = make_catchments(vec![0, -1, 0, -2], vec![valid_bbox(); 4], vec![1.0; 4]);
        let diags = check_catchment_ids(&data);
        assert_eq!(count_id(&diags, "ids.catchment_zero"), 2);
        assert_eq!(count_id(&diags, "ids.catchment_negative"), 2);
    }

    // ========================
    // C2: check_catchment_bboxes
    // ========================

    #[test]
    fn c2_valid_bboxes_produce_no_errors() {
        let data = make_catchments(vec![1], vec![valid_bbox()], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c2_nan_component_produces_error() {
        let data = make_catchments(vec![1], vec![[f32::NAN, -5.0, 10.0, 5.0]], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(!diags.is_empty(), "NaN bbox should produce an error");
        assert!(diags.iter().any(|d| d.check_id == "ids.catchment_bbox"));
    }

    #[test]
    fn c2_inf_component_produces_error() {
        let data = make_catchments(vec![1], vec![[f32::INFINITY, -5.0, 10.0, 5.0]], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(diags.iter().any(|d| d.check_id == "ids.catchment_bbox"));
    }

    #[test]
    fn c2_degenerate_bbox_minx_ge_maxx_produces_error() {
        // minx == maxx
        let data = make_catchments(vec![1], vec![[5.0, -5.0, 5.0, 5.0]], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(diags.iter().any(|d| d.check_id == "ids.catchment_bbox"));
    }

    #[test]
    fn c2_degenerate_bbox_miny_ge_maxy_produces_error() {
        let data = make_catchments(vec![1], vec![[-10.0, 5.0, 10.0, 5.0]], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(diags.iter().any(|d| d.check_id == "ids.catchment_bbox"));
    }

    #[test]
    fn c2_out_of_range_longitude_produces_error() {
        let data = make_catchments(vec![1], vec![[200.0, -5.0, 201.0, 5.0]], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(diags.iter().any(|d| d.check_id == "ids.catchment_bbox"));
    }

    #[test]
    fn c2_out_of_range_latitude_produces_error() {
        let data = make_catchments(vec![1], vec![[-10.0, -100.0, 10.0, -95.0]], vec![1.0]);
        let diags = check_catchment_bboxes(&data);
        assert!(diags.iter().any(|d| d.check_id == "ids.catchment_bbox"));
    }

    // ========================
    // C3: check_catchment_areas
    // ========================

    #[test]
    fn c3_valid_areas_produce_no_errors() {
        let data = make_catchments(vec![1, 2], vec![valid_bbox(); 2], vec![0.0, 100.5]);
        let diags = check_catchment_areas(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c3_negative_area_produces_error() {
        let data = make_catchments(vec![1], vec![valid_bbox()], vec![-0.1]);
        let diags = check_catchment_areas(&data);
        assert_eq!(count_id(&diags, "ids.catchment_area"), 1);
    }

    #[test]
    fn c3_nan_area_produces_error() {
        let data = make_catchments(vec![1], vec![valid_bbox()], vec![f32::NAN]);
        let diags = check_catchment_areas(&data);
        assert_eq!(count_id(&diags, "ids.catchment_area"), 1);
    }

    #[test]
    fn c3_inf_area_produces_error() {
        let data = make_catchments(vec![1], vec![valid_bbox()], vec![f32::INFINITY]);
        let diags = check_catchment_areas(&data);
        assert_eq!(count_id(&diags, "ids.catchment_area"), 1);
    }

    #[test]
    fn c3_zero_area_is_valid() {
        let data = make_catchments(vec![1], vec![valid_bbox()], vec![0.0]);
        let diags = check_catchment_areas(&data);
        assert!(diags.is_empty(), "zero area should be valid");
    }

    // ========================
    // C5: check_graph_ids
    // ========================

    #[test]
    fn c5_valid_graph_ids_produce_no_errors() {
        let data = make_graph(vec![1, 2, 3], vec![vec![]; 3]);
        let diags = check_graph_ids(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c5_zero_id_produces_error() {
        let data = make_graph(vec![1, 0, 3], vec![vec![]; 3]);
        let diags = check_graph_ids(&data);
        assert_eq!(count_id(&diags, "ids.graph_zero"), 1);
    }

    #[test]
    fn c5_negative_id_produces_error() {
        let data = make_graph(vec![1, -7, 3], vec![vec![]; 3]);
        let diags = check_graph_ids(&data);
        assert_eq!(count_id(&diags, "ids.graph_negative"), 1);
    }

    #[test]
    fn c5_duplicate_id_produces_error() {
        let data = make_graph(vec![1, 2, 1], vec![vec![]; 3]);
        let diags = check_graph_ids(&data);
        assert_eq!(count_id(&diags, "ids.graph_duplicate"), 1);
    }

    #[test]
    fn c5_multiple_duplicates_each_reported() {
        let data = make_graph(vec![1, 2, 1, 2, 3, 3], vec![vec![]; 6]);
        let diags = check_graph_ids(&data);
        assert_eq!(count_id(&diags, "ids.graph_duplicate"), 3);
    }

    // ========================
    // C6: check_upstream_ids
    // ========================

    #[test]
    fn c6_valid_upstream_ids_produce_no_errors() {
        let data = make_graph(vec![3], vec![vec![1, 2]]);
        let diags = check_upstream_ids(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c6_zero_upstream_id_produces_error() {
        let data = make_graph(vec![3], vec![vec![1, 0, 2]]);
        let diags = check_upstream_ids(&data);
        assert_eq!(count_id(&diags, "ids.upstream_zero"), 1);
        assert!(diags[0].location == Location::Row { index: 0 });
    }

    #[test]
    fn c6_negative_upstream_id_produces_error() {
        let data = make_graph(vec![3], vec![vec![1, -1]]);
        let diags = check_upstream_ids(&data);
        assert_eq!(count_id(&diags, "ids.upstream_negative"), 1);
    }

    #[test]
    fn c6_empty_upstream_list_is_valid() {
        let data = make_graph(vec![1, 2], vec![vec![], vec![1]]);
        let diags = check_upstream_ids(&data);
        assert!(diags.is_empty());
    }

    // ========================
    // C7: check_snap_data
    // ========================

    #[test]
    fn c7_valid_snap_data_produces_no_errors() {
        let data = make_snap(vec![1, 2], vec![10, 20], vec![0.5, 0.5], vec![valid_bbox(); 2]);
        let diags = check_snap_data(&data);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn c7_zero_snap_id_produces_error() {
        let data = make_snap(vec![0], vec![10], vec![0.5], vec![valid_bbox()]);
        let diags = check_snap_data(&data);
        assert_eq!(count_id(&diags, "ids.snap_id"), 1);
    }

    #[test]
    fn c7_negative_snap_id_produces_error() {
        let data = make_snap(vec![-1], vec![10], vec![0.5], vec![valid_bbox()]);
        let diags = check_snap_data(&data);
        assert_eq!(count_id(&diags, "ids.snap_id"), 1);
    }

    #[test]
    fn c7_zero_catchment_id_produces_error() {
        let data = make_snap(vec![1], vec![0], vec![0.5], vec![valid_bbox()]);
        let diags = check_snap_data(&data);
        assert_eq!(count_id(&diags, "ids.snap_catchment_id"), 1);
    }

    #[test]
    fn c7_negative_weight_produces_error() {
        let data = make_snap(vec![1], vec![10], vec![-0.1], vec![valid_bbox()]);
        let diags = check_snap_data(&data);
        assert_eq!(count_id(&diags, "ids.snap_weight"), 1);
    }

    #[test]
    fn c7_nan_weight_produces_error() {
        let data = make_snap(vec![1], vec![10], vec![f32::NAN], vec![valid_bbox()]);
        let diags = check_snap_data(&data);
        assert_eq!(count_id(&diags, "ids.snap_weight"), 1);
    }

    #[test]
    fn c7_invalid_snap_bbox_produces_error() {
        let data = make_snap(vec![1], vec![10], vec![0.5], vec![[5.0, -5.0, 5.0, 5.0]]);
        let diags = check_snap_data(&data);
        assert!(diags.iter().any(|d| d.check_id == "ids.snap_bbox"));
    }

    #[test]
    fn c7_zero_weight_is_valid() {
        let data = make_snap(vec![1], vec![10], vec![0.0], vec![valid_bbox()]);
        let diags = check_snap_data(&data);
        assert!(diags.is_empty(), "zero weight should be valid");
    }
}
