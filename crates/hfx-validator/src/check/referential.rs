//! Cross-file referential integrity checks.
//!
//! All functions accept pre-loaded column data and return a flat list of
//! [`Diagnostic`]s. They never panic and never perform I/O.
//!
//! Violation reporting is capped at 100 per direction; a summary diagnostic is
//! appended when more violations exist.

use std::collections::HashSet;

use tracing::debug;

use crate::dataset::{CatchmentsData, GraphData, SnapData};
use crate::diagnostic::{Artifact, Category, Diagnostic};

// Maximum individual violations reported per direction before emitting a summary.
const MAX_VIOLATIONS: usize = 100;

// ---------------------------------------------------------------------------
// D1: ID coverage (catchments ↔ graph)
// ---------------------------------------------------------------------------

/// D1 — Check that the set of catchment IDs equals the set of graph IDs.
///
/// Reports IDs that are in catchments but not in graph
/// (`"referential.catchment_not_in_graph"`), and IDs that are in graph but not
/// in catchments (`"referential.graph_not_in_catchments"`).
/// Each direction is capped at 100 individual diagnostics, followed by an
/// overflow summary if needed.
pub fn check_id_coverage(catchments: &CatchmentsData, graph: &GraphData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    let catchment_set: HashSet<i64> = catchments.ids.iter().copied().collect();
    let graph_set: HashSet<i64> = graph.ids.iter().copied().collect();

    // IDs in catchments but not in graph.
    let mut in_catchments_only: Vec<i64> = catchment_set.difference(&graph_set).copied().collect();
    in_catchments_only.sort_unstable();

    let catchment_overflow = in_catchments_only.len().saturating_sub(MAX_VIOLATIONS);
    for &id in in_catchments_only.iter().take(MAX_VIOLATIONS) {
        diags.push(Diagnostic::error(
            "referential.catchment_not_in_graph",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!("catchment id {id} has no corresponding entry in graph.parquet"),
        ));
    }
    if catchment_overflow > 0 {
        diags.push(Diagnostic::error(
            "referential.catchment_not_in_graph",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!(
                "... and {catchment_overflow} more catchment IDs absent from graph (only first {MAX_VIOLATIONS} shown)"
            ),
        ));
    }

    // IDs in graph but not in catchments.
    let mut in_graph_only: Vec<i64> = graph_set.difference(&catchment_set).copied().collect();
    in_graph_only.sort_unstable();

    let graph_overflow = in_graph_only.len().saturating_sub(MAX_VIOLATIONS);
    for &id in in_graph_only.iter().take(MAX_VIOLATIONS) {
        diags.push(Diagnostic::error(
            "referential.graph_not_in_catchments",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!("graph id {id} has no corresponding entry in catchments.parquet"),
        ));
    }
    if graph_overflow > 0 {
        diags.push(Diagnostic::error(
            "referential.graph_not_in_catchments",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!(
                "... and {graph_overflow} more graph IDs absent from catchments (only first {MAX_VIOLATIONS} shown)"
            ),
        ));
    }

    debug!(
        catchment_only = in_catchments_only.len(),
        graph_only = in_graph_only.len(),
        diag_count = diags.len(),
        "D1 ID coverage check complete"
    );
    diags
}

// ---------------------------------------------------------------------------
// D2: Upstream ID references
// ---------------------------------------------------------------------------

/// D2 — Check that all `upstream_ids` in the graph resolve to existing catchment IDs.
///
/// For every upstream_id entry across all graph rows, verifies the value exists
/// in the catchment ID set. Reports misses as
/// `"referential.upstream_not_in_catchments"`. Capped at 100 violations.
pub fn check_upstream_refs(catchments: &CatchmentsData, graph: &GraphData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    let catchment_set: HashSet<i64> = catchments.ids.iter().copied().collect();
    let mut violation_count = 0usize;

    for (row, upstream_list) in graph.upstream_ids.iter().enumerate() {
        for &uid in upstream_list {
            if !catchment_set.contains(&uid) {
                violation_count += 1;
                if violation_count <= MAX_VIOLATIONS {
                    diags.push(Diagnostic::error(
                        "referential.upstream_not_in_catchments",
                        Category::ReferentialIntegrity,
                        Artifact::CrossFile,
                        format!(
                            "upstream_id {uid} at graph row {row} does not exist in catchments.parquet"
                        ),
                    ));
                }
            }
        }
    }

    if violation_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "referential.upstream_not_in_catchments",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!(
                "... and {} more upstream_id references absent from catchments (only first {MAX_VIOLATIONS} shown)",
                violation_count - MAX_VIOLATIONS
            ),
        ));
    }

    debug!(
        violations = violation_count,
        diag_count = diags.len(),
        "D2 upstream refs check complete"
    );
    diags
}

// ---------------------------------------------------------------------------
// D3: Snap unit_id references
// ---------------------------------------------------------------------------

/// D3 — Check that all `unit_id` values in snap resolve to existing catchment IDs.
///
/// For every row in the snap table, verifies the `unit_id` exists in the
/// catchment ID set. Reports misses as
/// `"referential.snap_catchment_not_in_catchments"`. Capped at 100 violations.
pub fn check_snap_refs(catchments: &CatchmentsData, snap: &SnapData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    let catchment_set: HashSet<i64> = catchments.ids.iter().copied().collect();
    let mut violation_count = 0usize;

    for (row, &cid) in snap.unit_ids.iter().enumerate() {
        if !catchment_set.contains(&cid) {
            violation_count += 1;
            if violation_count <= MAX_VIOLATIONS {
                diags.push(Diagnostic::error(
                    "referential.snap_catchment_not_in_catchments",
                    Category::ReferentialIntegrity,
                    Artifact::CrossFile,
                    format!("snap unit_id {cid} at row {row} does not exist in catchments.parquet"),
                ));
            }
        }
    }

    if violation_count > MAX_VIOLATIONS {
        diags.push(Diagnostic::error(
            "referential.snap_catchment_not_in_catchments",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!(
                "... and {} more snap unit_id references absent from catchments (only first {MAX_VIOLATIONS} shown)",
                violation_count - MAX_VIOLATIONS
            ),
        ));
    }

    debug!(
        violations = violation_count,
        diag_count = diags.len(),
        "D3 snap refs check complete"
    );
    diags
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
