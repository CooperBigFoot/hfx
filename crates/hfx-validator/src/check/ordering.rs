//! Ordering checks for partition-friendly Parquet layout.

use std::collections::HashSet;

use crate::dataset::{CatchmentsData, GraphData};
use crate::diagnostic::{Artifact, Category, Diagnostic, Location, Severity};

pub const CATCHMENTS_LEVEL_UNSORTED: &str = "ordering.catchments.level_unsorted";
pub const GRAPH_LEVEL_UNSORTED: &str = "ordering.graph.level_unsorted";
pub const CATCHMENTS_HILBERT_UNSORTED: &str = "ordering.catchments.hilbert_unsorted";
pub const GRAPH_HILBERT_UNSORTED: &str = "ordering.graph.hilbert_unsorted";

/// Return deferred Hilbert ordering diagnostic registrations.
///
/// These diagnostics are intentionally not emitted in v0.2.1 because the spec
/// has not pinned Hilbert curve parameters yet.
pub fn deferred_hilbert_diagnostics() -> [(&'static str, Severity); 2] {
    [
        (CATCHMENTS_HILBERT_UNSORTED, Severity::Warning),
        (GRAPH_HILBERT_UNSORTED, Severity::Warning),
    ]
}

/// Check multi-level catchments are ordered by non-decreasing level.
pub fn check_catchments_ordering(catchments: &CatchmentsData) -> Vec<Diagnostic> {
    check_level_ordering(
        &catchments.levels,
        Artifact::Catchments,
        CATCHMENTS_LEVEL_UNSORTED,
        "catchments.parquet",
    )
}

/// Check multi-level graph rows are ordered by non-decreasing level.
pub fn check_graph_ordering(graph: &GraphData) -> Vec<Diagnostic> {
    check_level_ordering(
        &graph.levels,
        Artifact::Graph,
        GRAPH_LEVEL_UNSORTED,
        "graph.parquet",
    )
}

fn check_level_ordering(
    levels: &[i16],
    artifact: Artifact,
    check_id: &'static str,
    file_label: &str,
) -> Vec<Diagnostic> {
    let distinct_levels: HashSet<i16> = levels.iter().copied().collect();
    if distinct_levels.len() <= 1 {
        return Vec::new();
    }

    let Some((row, window)) = levels
        .windows(2)
        .enumerate()
        .find(|(_, pair)| pair[1] < pair[0])
    else {
        return Vec::new();
    };

    vec![
        Diagnostic::error(
            check_id,
            Category::Schema,
            artifact,
            format!(
                "{file_label} levels must be non-decreasing for multi-level datasets; \
                 row {} has level {} after level {}",
                row + 1,
                window[1],
                window[0]
            ),
        )
        .at(Location::Row { index: row + 1 }),
    ]
}

#[cfg(test)]
mod tests {
    use crate::dataset::{CatchmentsData, GraphData};
    use crate::diagnostic::{Artifact, Severity};

    use super::{
        CATCHMENTS_HILBERT_UNSORTED, GRAPH_HILBERT_UNSORTED, check_catchments_ordering,
        check_graph_ordering, deferred_hilbert_diagnostics,
    };

    fn catchments_with_levels(levels: Vec<i16>) -> CatchmentsData {
        let row_count = levels.len();
        CatchmentsData {
            row_count,
            ids: (1..=row_count as i64).collect(),
            levels,
            parent_ids: vec![None; row_count],
            areas_km2: vec![1.0; row_count],
            outlet_lons: vec![0.0; row_count],
            outlet_lats: vec![0.0; row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            up_area_null_count: row_count,
            up_area_total: row_count,
            geometry_wkb: vec![Vec::new(); row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    fn graph_with_levels(levels: Vec<i16>) -> GraphData {
        let row_count = levels.len();
        GraphData {
            ids: (1..=row_count as i64).collect(),
            levels,
            upstream_ids: vec![Vec::new(); row_count],
            bboxes: vec![[0.0, 0.0, 1.0, 1.0]; row_count],
            row_group_sizes: vec![row_count],
            row_group_has_bbox_stats: vec![true],
        }
    }

    #[test]
    fn catchments_multi_level_unsorted_emits_first_offending_row() {
        let diagnostics = check_catchments_ordering(&catchments_with_levels(vec![0, 1, 0]));

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics[0].check_id,
            "ordering.catchments.level_unsorted"
        );
        assert_eq!(diagnostics[0].artifact, Artifact::Catchments);
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn graph_multi_level_unsorted_emits_first_offending_row() {
        let diagnostics = check_graph_ordering(&graph_with_levels(vec![1, 0]));

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].check_id, "ordering.graph.level_unsorted");
        assert_eq!(diagnostics[0].artifact, Artifact::Graph);
        assert_eq!(diagnostics[0].severity, Severity::Error);
    }

    #[test]
    fn single_level_data_does_not_emit_ordering_diagnostic() {
        assert!(check_catchments_ordering(&catchments_with_levels(vec![1, 1, 1])).is_empty());
        assert!(check_graph_ordering(&graph_with_levels(vec![1, 1, 1])).is_empty());
    }

    #[test]
    fn hilbert_diagnostics_are_registered_as_deferred_warnings() {
        let registrations = deferred_hilbert_diagnostics();

        assert_eq!(
            registrations,
            [
                (CATCHMENTS_HILBERT_UNSORTED, Severity::Warning),
                (GRAPH_HILBERT_UNSORTED, Severity::Warning)
            ]
        );
    }
}
