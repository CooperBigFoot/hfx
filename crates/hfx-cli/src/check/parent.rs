//! Parent forest checks for v0.2 drainage units.

use std::collections::{HashMap, HashSet};

use crate::dataset::CatchmentsData;
use crate::diagnostic::{Artifact, Category, Diagnostic, Location};

/// Check parent references, coarser levels, and cycles.
pub fn check_parent_forest(catchments: &CatchmentsData) -> Vec<Diagnostic> {
    let mut diags = Vec::new();
    let by_id: HashMap<i64, (usize, i16)> = catchments
        .ids
        .iter()
        .copied()
        .zip(catchments.levels.iter().copied())
        .enumerate()
        .map(|(idx, (id, level))| (id, (idx, level)))
        .collect();

    for (idx, parent_id) in catchments.parent_ids.iter().enumerate() {
        let Some(parent_id) = parent_id else {
            continue;
        };
        let child_id = catchments.ids[idx];
        let child_level = catchments.levels[idx];
        let Some((_, parent_level)) = by_id.get(parent_id).copied() else {
            diags.push(
                Diagnostic::error(
                    "parent.dangling",
                    Category::ReferentialIntegrity,
                    Artifact::Catchments,
                    format!("unit {child_id} references missing parent_id {parent_id}"),
                )
                .at(Location::Row { index: idx }),
            );
            continue;
        };
        if parent_level >= child_level {
            diags.push(
                Diagnostic::error(
                    "parent.level_not_coarser",
                    Category::ValueConsistency,
                    Artifact::Catchments,
                    format!(
                        "parent {parent_id} level {parent_level} is not coarser than child {child_id} level {child_level}"
                    ),
                )
                .at(Location::Row { index: idx }),
            );
        }
    }

    let parent_by_id: HashMap<i64, i64> = catchments
        .ids
        .iter()
        .copied()
        .zip(catchments.parent_ids.iter().copied())
        .filter_map(|(id, parent)| parent.map(|p| (id, p)))
        .collect();

    for (idx, &start_id) in catchments.ids.iter().enumerate() {
        let mut seen = HashSet::new();
        let mut current = start_id;
        while let Some(&parent) = parent_by_id.get(&current) {
            if !seen.insert(current) {
                diags.push(
                    Diagnostic::error(
                        "parent.cycle_detected",
                        Category::GraphInvariant,
                        Artifact::Catchments,
                        format!("parent cycle detected while walking from unit {start_id}"),
                    )
                    .at(Location::Row { index: idx }),
                );
                break;
            }
            current = parent;
        }
    }

    diags
}
