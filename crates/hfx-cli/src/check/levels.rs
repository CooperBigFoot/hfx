//! Same-level graph consistency checks.

use std::collections::HashMap;

use crate::dataset::{CatchmentsData, GraphData};
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};

/// Check graph row levels and same-level upstream edges.
pub fn check_level_consistency(catchments: &CatchmentsData, graph: &GraphData) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let catchment_levels: HashMap<i64, i16> = catchments
        .ids
        .iter()
        .copied()
        .zip(catchments.levels.iter().copied())
        .collect();

    for (idx, (&id, &graph_level)) in graph.ids.iter().zip(&graph.levels).enumerate() {
        if let Some(&catchment_level) = catchment_levels.get(&id)
            && catchment_level != graph_level
        {
            diags.push(
                Diagnostic::error(
                    "levels.graph_level_mismatch",
                    Category::ValueConsistency,
                    Artifact::Graph,
                    format!(
                        "graph row for unit {id} has level {graph_level}, catchments row has level {catchment_level}"
                    ),
                )
                .at(Location::Row { index: idx }),
            );
        }

        for &upstream_id in graph.upstream_ids.get(idx).into_iter().flatten() {
            if let Some(&upstream_level) = catchment_levels.get(&upstream_id)
                && upstream_level != graph_level
            {
                diags.push(
                    Diagnostic::error(
                        "levels.upstream_not_same_level",
                        Category::ValueConsistency,
                        Artifact::Graph,
                        format!(
                            "graph row {id} level {graph_level} references upstream unit {upstream_id} level {upstream_level}"
                        ),
                    )
                    .at(Location::Row { index: idx }),
                );
            }
        }
    }

    diags
}
