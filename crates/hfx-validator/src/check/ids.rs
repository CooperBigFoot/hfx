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

/// C1 — Check all catchment IDs are positive, non-zero, and unique.
///
/// Every `id` in `CatchmentsData.ids` must be > 0.  Zero and negative values
/// each produce a separate check_id so they can be filtered independently.
/// Duplicate IDs also produce a distinct diagnostic.
pub fn check_unit_ids(data: &CatchmentsData) -> Vec<Diagnostic> {
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
        } else if !seen.insert(id) {
            // id > 0 but already seen — duplicate
            dup_count += 1;
            if dup_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.catchment_duplicate",
                        Category::IdConstraint,
                        Artifact::Catchments,
                        format!(
                            "catchment id {id} at row {i} is duplicated; all IDs must be unique"
                        ),
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
            format!(
                "... and {} more zero catchment ID violations (only first {MAX_VIOLATIONS} shown)",
                zero_count - MAX_VIOLATIONS
            ),
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
    if dup_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.catchment_duplicate",
            Category::IdConstraint,
            Artifact::Catchments,
            format!("... and {} more duplicate catchment ID violations (only first {MAX_VIOLATIONS} shown)", dup_count - MAX_VIOLATIONS),
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
            format!(
                "... and {} more bbox violations (only first {MAX_VIOLATIONS} shown)",
                violation_count - MAX_VIOLATIONS
            ),
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
            Some(format!(
                "area_km2 at row {i} is not finite ({area}); must be a finite non-negative number"
            ))
        } else if area < 0.0 {
            Some(format!(
                "area_km2 {area} at row {i} is negative; must be >= 0"
            ))
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
            format!(
                "... and {} more area_km2 violations (only first {MAX_VIOLATIONS} shown)",
                violation_count - MAX_VIOLATIONS
            ),
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
            format!(
                "... and {} more zero graph ID violations (only first {MAX_VIOLATIONS} shown)",
                zero_count - MAX_VIOLATIONS
            ),
        ));
    }
    if neg_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.graph_negative",
            Category::IdConstraint,
            Artifact::Graph,
            format!(
                "... and {} more negative graph ID violations (only first {MAX_VIOLATIONS} shown)",
                neg_count - MAX_VIOLATIONS
            ),
        ));
    }
    if dup_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.graph_duplicate",
            Category::IdConstraint,
            Artifact::Graph,
            format!(
                "... and {} more duplicate graph ID violations (only first {MAX_VIOLATIONS} shown)",
                dup_count - MAX_VIOLATIONS
            ),
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
            format!(
                "... and {} more zero upstream ID violations (only first {MAX_VIOLATIONS} shown)",
                zero_count - MAX_VIOLATIONS
            ),
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

/// C7 — Check snap IDs, unit_ids, weights, and bboxes.
///
/// - `ids`: must be > 0.
/// - `unit_ids`: must be > 0.
/// - `weights`: must be finite and >= 0.
/// - `bboxes`: must be valid (see [`validate_bbox_f32`]).
pub fn check_snap_data(data: &SnapData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();
    let mut id_violation = 0usize;
    let mut dup_count = 0usize;
    let mut cid_violation = 0usize;
    let mut weight_violation = 0usize;
    let mut bbox_violation = 0usize;

    let mut seen: HashSet<i64> = HashSet::with_capacity(data.ids.len());

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
        } else if !seen.insert(id) {
            // id > 0 but already seen — duplicate
            dup_count += 1;
            if dup_count <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.snap_duplicate",
                        Category::IdConstraint,
                        Artifact::Snap,
                        format!(
                            "snap id {id} at row {i} is duplicated; all snap IDs must be unique"
                        ),
                    )
                    .at(Location::Row { index: i }),
                );
            }
        }
    }

    for (i, &cid) in data.unit_ids.iter().enumerate() {
        if cid <= 0 {
            cid_violation += 1;
            if cid_violation <= MAX_VIOLATIONS {
                diags.push(
                    Diagnostic::error(
                        "ids.snap_unit_id",
                        Category::IdConstraint,
                        Artifact::Snap,
                        format!("snap unit_id {cid} at row {i} must be > 0"),
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
            Some(format!(
                "snap weight {w} at row {i} is negative; must be >= 0"
            ))
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
        let Some([minx, miny, maxx, maxy]) = *bbox else {
            continue;
        };
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
            format!(
                "... and {} more snap ID violations (only first {MAX_VIOLATIONS} shown)",
                id_violation - MAX_VIOLATIONS
            ),
        ));
    }
    if dup_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_duplicate",
            Category::IdConstraint,
            Artifact::Snap,
            format!(
                "... and {} more duplicate snap ID violations (only first {MAX_VIOLATIONS} shown)",
                dup_count - MAX_VIOLATIONS
            ),
        ));
    }
    if cid_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_unit_id",
            Category::IdConstraint,
            Artifact::Snap,
            format!(
                "... and {} more snap unit_id violations (only first {MAX_VIOLATIONS} shown)",
                cid_violation - MAX_VIOLATIONS
            ),
        ));
    }
    if weight_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_weight",
            Category::IdConstraint,
            Artifact::Snap,
            format!(
                "... and {} more snap weight violations (only first {MAX_VIOLATIONS} shown)",
                weight_violation - MAX_VIOLATIONS
            ),
        ));
    }
    if bbox_violation > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "ids.snap_bbox",
            Category::IdConstraint,
            Artifact::Snap,
            format!(
                "... and {} more snap bbox violations (only first {MAX_VIOLATIONS} shown)",
                bbox_violation - MAX_VIOLATIONS
            ),
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
    for (name, val) in [
        ("minx", minx),
        ("miny", miny),
        ("maxx", maxx),
        ("maxy", maxy),
    ] {
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
        // Strict by default (polygons must have non-zero area).
        // For snap: non-strict inequality (line features may have zero extent in one axis).
        let strict = !matches!(artifact, Artifact::Snap);

        if strict {
            if minx >= maxx {
                out.push(
                    Diagnostic::error(
                        check_id,
                        Category::IdConstraint,
                        artifact,
                        format!(
                            "bbox at row {row} is degenerate in x: minx ({minx}) >= maxx ({maxx})"
                        ),
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
                        format!(
                            "bbox at row {row} is degenerate in y: miny ({miny}) >= maxy ({maxy})"
                        ),
                    )
                    .at(Location::Row { index: row }),
                );
            }
        } else {
            if minx > maxx {
                out.push(
                    Diagnostic::error(
                        check_id,
                        Category::IdConstraint,
                        artifact,
                        format!(
                            "bbox at row {row} is inverted in x: minx ({minx}) > maxx ({maxx})"
                        ),
                    )
                    .at(Location::Row { index: row }),
                );
            }
            if miny > maxy {
                out.push(
                    Diagnostic::error(
                        check_id,
                        Category::IdConstraint,
                        artifact,
                        format!(
                            "bbox at row {row} is inverted in y: miny ({miny}) > maxy ({maxy})"
                        ),
                    )
                    .at(Location::Row { index: row }),
                );
            }
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
