//! Upstream adjacency graph types.

use std::collections::HashMap;

use crate::id::UnitId;
use crate::level::Level;

/// A single row in the upstream adjacency graph.
///
/// Represents one drainage unit and the set of same-level units directly upstream of it.
#[derive(Debug, Clone, PartialEq)]
pub struct AdjacencyRow {
    id: UnitId,
    level: Level,
    upstream_ids: Vec<UnitId>,
}

impl AdjacencyRow {
    /// Construct an [`AdjacencyRow`] from a unit ID, level, and upstream neighbours.
    pub fn new(id: UnitId, level: Level, upstream_ids: Vec<UnitId>) -> Self {
        Self {
            id,
            level,
            upstream_ids,
        }
    }

    /// Return the unit ID for this row.
    pub fn id(&self) -> UnitId {
        self.id
    }

    /// Return the dataset-local level for this row.
    pub fn level(&self) -> Level {
        self.level
    }

    /// Return the slice of upstream unit IDs.
    pub fn upstream_ids(&self) -> &[UnitId] {
        &self.upstream_ids
    }

    /// Return `true` if this unit has no upstream neighbours.
    pub fn is_headwater(&self) -> bool {
        self.upstream_ids.is_empty()
    }
}

/// Errors from constructing a [`DrainageGraph`].
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// Returned when the graph contains zero rows.
    #[error("drainage graph must contain at least one atom")]
    EmptyGraph,

    /// Returned when the same unit ID appears in more than one row.
    #[error("duplicate unit id {id} at row indices {first} and {second}")]
    DuplicateUnitId {
        /// The raw i64 value of the duplicated ID.
        id: i64,
        /// Index of the first occurrence.
        first: usize,
        /// Index of the second (duplicate) occurrence.
        second: usize,
    },
}

/// The complete upstream adjacency graph over drainage units.
///
/// This is the canonical in-memory representation of `graph.parquet` for
/// validation and construction purposes. The HashMap-based index provides
/// O(1) lookup by unit ID.
///
/// **Note for engine implementors:** this representation is optimized for
/// validation and random access, not traversal. A delineation engine will
/// typically convert to CSR (compressed sparse row) or another
/// traversal-optimized layout at load time, as permitted by the HFX spec.
#[derive(Debug, Clone, PartialEq)]
pub struct DrainageGraph {
    rows: Vec<AdjacencyRow>,
    index: HashMap<UnitId, usize>,
}

impl DrainageGraph {
    /// Construct a [`DrainageGraph`] from a vector of [`AdjacencyRow`]s.
    ///
    /// Builds the internal O(1) lookup index as part of construction.
    ///
    /// # Errors
    ///
    /// | Condition | Error variant |
    /// |-----------|---------------|
    /// | `rows` is empty | [`GraphError::EmptyGraph`] |
    /// | The same [`UnitId`] appears in two or more rows | [`GraphError::DuplicateUnitId`] |
    pub fn new(rows: Vec<AdjacencyRow>) -> Result<Self, GraphError> {
        if rows.is_empty() {
            return Err(GraphError::EmptyGraph);
        }

        let mut index = HashMap::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            if let Some(&first) = index.get(&row.id) {
                return Err(GraphError::DuplicateUnitId {
                    id: row.id.get(),
                    first,
                    second: i,
                });
            }
            index.insert(row.id, i);
        }

        Ok(Self { rows, index })
    }

    /// Return all rows in the graph.
    pub fn rows(&self) -> &[AdjacencyRow] {
        &self.rows
    }

    /// Return the number of rows in the graph.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return `true` if the graph contains no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Look up a row by [`UnitId`] in O(1) time.
    ///
    /// Returns `None` if no row with the given ID exists in the graph.
    pub fn get(&self, id: UnitId) -> Option<&AdjacencyRow> {
        self.index.get(&id).map(|&i| &self.rows[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_unit_id(raw: i64) -> UnitId {
        UnitId::new(raw).unwrap()
    }

    fn test_level(raw: i16) -> Level {
        Level::new(raw).unwrap()
    }

    fn headwater_row(id: i64) -> AdjacencyRow {
        AdjacencyRow::new(test_unit_id(id), test_level(0), vec![])
    }

    fn interior_row(id: i64, upstream: &[i64]) -> AdjacencyRow {
        let upstream_ids = upstream.iter().map(|&r| test_unit_id(r)).collect();
        AdjacencyRow::new(test_unit_id(id), test_level(0), upstream_ids)
    }

    #[test]
    fn single_headwater_row_has_len_one_and_is_not_empty() {
        let graph = DrainageGraph::new(vec![headwater_row(1)]).unwrap();
        assert_eq!(graph.len(), 1);
        assert!(!graph.is_empty());
    }

    #[test]
    fn empty_rows_fails_with_empty_graph_error() {
        let err = DrainageGraph::new(vec![]).unwrap_err();
        assert!(matches!(err, GraphError::EmptyGraph));
    }

    #[test]
    fn duplicate_unit_id_fails_with_duplicate_error() {
        let rows = vec![headwater_row(5), headwater_row(5)];
        let err = DrainageGraph::new(rows).unwrap_err();
        assert!(matches!(
            err,
            GraphError::DuplicateUnitId {
                id: 5,
                first: 0,
                second: 1
            }
        ));
    }

    #[test]
    fn get_returns_correct_row() {
        let row = interior_row(10, &[11, 12]);
        let graph =
            DrainageGraph::new(vec![headwater_row(11), headwater_row(12), row.clone()]).unwrap();

        let found = graph.get(test_unit_id(10)).unwrap();
        assert_eq!(found.id(), test_unit_id(10));
        assert_eq!(found.level(), test_level(0));
        assert_eq!(found.upstream_ids().len(), 2);
    }

    #[test]
    fn get_with_nonexistent_id_returns_none() {
        let graph = DrainageGraph::new(vec![headwater_row(1)]).unwrap();
        assert!(graph.get(test_unit_id(999)).is_none());
    }

    #[test]
    fn is_headwater_true_for_empty_upstream_ids() {
        let row = headwater_row(1);
        assert!(row.is_headwater());
    }

    #[test]
    fn is_headwater_false_for_nonempty_upstream_ids() {
        let row = interior_row(10, &[1, 2]);
        assert!(!row.is_headwater());
    }
}
