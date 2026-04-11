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
    let mut in_catchments_only: Vec<i64> = catchment_set
        .difference(&graph_set)
        .copied()
        .collect();
    in_catchments_only.sort_unstable();

    let catchment_overflow = in_catchments_only.len().saturating_sub(MAX_VIOLATIONS);
    for &id in in_catchments_only.iter().take(MAX_VIOLATIONS) {
        diags.push(Diagnostic::error(
            "referential.catchment_not_in_graph",
            Category::ReferentialIntegrity,
            Artifact::CrossFile,
            format!("catchment id {id} has no corresponding entry in graph.arrow"),
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
    let mut in_graph_only: Vec<i64> = graph_set
        .difference(&catchment_set)
        .copied()
        .collect();
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
// D3: Snap catchment_id references
// ---------------------------------------------------------------------------

/// D3 — Check that all `catchment_id` values in snap resolve to existing catchment IDs.
///
/// For every row in the snap table, verifies the `catchment_id` exists in the
/// catchment ID set. Reports misses as
/// `"referential.snap_catchment_not_in_catchments"`. Capped at 100 violations.
pub fn check_snap_refs(catchments: &CatchmentsData, snap: &SnapData) -> Vec<Diagnostic> {
    let mut diags: Vec<Diagnostic> = Vec::new();

    let catchment_set: HashSet<i64> = catchments.ids.iter().copied().collect();
    let mut violation_count = 0usize;

    for (row, &cid) in snap.catchment_ids.iter().enumerate() {
        if !catchment_set.contains(&cid) {
            violation_count += 1;
            if violation_count <= MAX_VIOLATIONS {
                diags.push(Diagnostic::error(
                    "referential.snap_catchment_not_in_catchments",
                    Category::ReferentialIntegrity,
                    Artifact::CrossFile,
                    format!(
                        "snap catchment_id {cid} at row {row} does not exist in catchments.parquet"
                    ),
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
                "... and {} more snap catchment_id references absent from catchments (only first {MAX_VIOLATIONS} shown)",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{CatchmentsData, GraphData, SnapData};

    fn make_catchments_with_ids(ids: Vec<i64>) -> CatchmentsData {
        let n = ids.len();
        CatchmentsData {
            row_count: n,
            ids,
            areas_km2: vec![1.0; n],
            bboxes: vec![[-10.0, -5.0, 10.0, 5.0]; n],
            up_area_null_count: 0,
            up_area_total: n,
            geometry_wkb: vec![vec![1, 3, 0, 0, 0]; n],
            row_group_sizes: vec![n],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn make_graph(ids: Vec<i64>, upstream: Vec<Vec<i64>>) -> GraphData {
        GraphData {
            ids,
            upstream_ids: upstream,
        }
    }

    fn make_snap(catchment_ids: Vec<i64>) -> SnapData {
        let n = catchment_ids.len();
        SnapData {
            row_count: n,
            ids: (1..=n as i64).collect(),
            catchment_ids,
            weights: vec![1.0; n],
            bboxes: vec![[-10.0, -5.0, 10.0, 5.0]; n],
            geometry_wkb: vec![vec![1, 1, 0, 0, 0]; n],
            row_group_sizes: vec![n],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn count_id(diags: &[Diagnostic], id: &str) -> usize {
        diags.iter().filter(|d| d.check_id == id).count()
    }

    // ========================
    // D1: check_id_coverage
    // ========================

    #[test]
    fn d1_matching_ids_produce_no_diagnostics() {
        let catchments = make_catchments_with_ids(vec![1, 2, 3]);
        let graph = make_graph(vec![1, 2, 3], vec![vec![]; 3]);
        let diags = check_id_coverage(&catchments, &graph);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn d1_catchment_id_not_in_graph_produces_error() {
        let catchments = make_catchments_with_ids(vec![1, 2, 99]);
        let graph = make_graph(vec![1, 2], vec![vec![]; 2]);
        let diags = check_id_coverage(&catchments, &graph);
        assert_eq!(
            count_id(&diags, "referential.catchment_not_in_graph"),
            1,
            "expected 1 catchment_not_in_graph diagnostic"
        );
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "referential.catchment_not_in_graph"
                    && d.message.contains("99")),
            "message should mention id 99"
        );
    }

    #[test]
    fn d1_graph_id_not_in_catchments_produces_error() {
        let catchments = make_catchments_with_ids(vec![1, 2]);
        let graph = make_graph(vec![1, 2, 88], vec![vec![]; 3]);
        let diags = check_id_coverage(&catchments, &graph);
        assert_eq!(
            count_id(&diags, "referential.graph_not_in_catchments"),
            1,
            "expected 1 graph_not_in_catchments diagnostic"
        );
        assert!(
            diags
                .iter()
                .any(|d| d.check_id == "referential.graph_not_in_catchments"
                    && d.message.contains("88")),
            "message should mention id 88"
        );
    }

    #[test]
    fn d1_both_mismatches_produce_both_error_kinds() {
        // catchments has 99 extra; graph has 88 extra
        let catchments = make_catchments_with_ids(vec![1, 2, 99]);
        let graph = make_graph(vec![1, 2, 88], vec![vec![]; 3]);
        let diags = check_id_coverage(&catchments, &graph);
        assert_eq!(count_id(&diags, "referential.catchment_not_in_graph"), 1);
        assert_eq!(count_id(&diags, "referential.graph_not_in_catchments"), 1);
    }

    #[test]
    fn d1_empty_sets_produce_no_diagnostics() {
        let catchments = make_catchments_with_ids(vec![]);
        let graph = make_graph(vec![], vec![]);
        let diags = check_id_coverage(&catchments, &graph);
        assert!(diags.is_empty());
    }

    // ========================
    // D2: check_upstream_refs
    // ========================

    #[test]
    fn d2_all_upstream_ids_exist_produces_no_diagnostics() {
        let catchments = make_catchments_with_ids(vec![1, 2, 3]);
        let graph = make_graph(vec![3], vec![vec![1, 2]]);
        let diags = check_upstream_refs(&catchments, &graph);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn d2_missing_upstream_id_produces_error() {
        let catchments = make_catchments_with_ids(vec![1, 2]);
        let graph = make_graph(vec![3], vec![vec![1, 999]]);
        let diags = check_upstream_refs(&catchments, &graph);
        assert_eq!(
            count_id(&diags, "referential.upstream_not_in_catchments"),
            1,
            "expected 1 upstream_not_in_catchments diagnostic"
        );
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("999")),
            "message should mention the missing id 999"
        );
    }

    #[test]
    fn d2_empty_upstream_lists_produce_no_diagnostics() {
        let catchments = make_catchments_with_ids(vec![1, 2, 3]);
        let graph = make_graph(vec![1, 2, 3], vec![vec![], vec![], vec![]]);
        let diags = check_upstream_refs(&catchments, &graph);
        assert!(diags.is_empty());
    }

    #[test]
    fn d2_multiple_missing_upstream_ids_each_reported() {
        let catchments = make_catchments_with_ids(vec![1]);
        let graph = make_graph(vec![2], vec![vec![888, 999]]);
        let diags = check_upstream_refs(&catchments, &graph);
        assert_eq!(count_id(&diags, "referential.upstream_not_in_catchments"), 2);
    }

    // ========================
    // D3: check_snap_refs
    // ========================

    #[test]
    fn d3_all_snap_catchment_ids_exist_produces_no_diagnostics() {
        let catchments = make_catchments_with_ids(vec![10, 20, 30]);
        let snap = make_snap(vec![10, 20, 30]);
        let diags = check_snap_refs(&catchments, &snap);
        assert!(diags.is_empty(), "expected no diagnostics, got: {diags:#?}");
    }

    #[test]
    fn d3_missing_snap_catchment_id_produces_error() {
        let catchments = make_catchments_with_ids(vec![10, 20]);
        let snap = make_snap(vec![10, 777]);
        let diags = check_snap_refs(&catchments, &snap);
        assert_eq!(
            count_id(&diags, "referential.snap_catchment_not_in_catchments"),
            1,
            "expected 1 snap_catchment_not_in_catchments diagnostic"
        );
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("777")),
            "message should mention the missing id 777"
        );
    }

    #[test]
    fn d3_empty_snap_produces_no_diagnostics() {
        let catchments = make_catchments_with_ids(vec![1, 2]);
        let snap = make_snap(vec![]);
        let diags = check_snap_refs(&catchments, &snap);
        assert!(diags.is_empty());
    }

    #[test]
    fn d3_multiple_missing_snap_catchment_ids_each_reported() {
        let catchments = make_catchments_with_ids(vec![1]);
        let snap = make_snap(vec![111, 222, 333]);
        let diags = check_snap_refs(&catchments, &snap);
        assert_eq!(
            count_id(&diags, "referential.snap_catchment_not_in_catchments"),
            3
        );
    }

    // ========================
    // Cap / overflow behaviour
    // ========================

    #[test]
    fn d1_overflow_emits_summary_diagnostic() {
        // Create 101 IDs only in catchments (none in graph).
        let ids: Vec<i64> = (1..=101).collect();
        let catchments = make_catchments_with_ids(ids);
        let graph = make_graph(vec![], vec![]);
        let diags = check_id_coverage(&catchments, &graph);

        let individual = count_id(&diags, "referential.catchment_not_in_graph");
        // Exactly MAX_VIOLATIONS individual + 1 summary = 101 total with that check_id.
        assert_eq!(individual, 101, "expected 100 individual + 1 summary");
        // Summary message contains "more".
        let summary = diags
            .iter()
            .filter(|d| d.check_id == "referential.catchment_not_in_graph")
            .last()
            .unwrap();
        assert!(
            summary.message.contains("more"),
            "summary diagnostic should mention overflow count"
        );
    }
}
