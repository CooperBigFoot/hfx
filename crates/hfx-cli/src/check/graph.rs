//! Graph structural checks.
//!
//! All functions accept pre-loaded column data and return a flat list of
//! [`Diagnostic`]s. They never panic and never perform I/O.

use std::collections::{HashMap, VecDeque};

use tracing::debug;

use crate::dataset::GraphData;
use crate::diagnostic::{Artifact, Category, Diagnostic};

// ---------------------------------------------------------------------------
// E1: Acyclicity (Kahn's algorithm)
// ---------------------------------------------------------------------------

/// E1 — Detect cycles in the drainage graph using Kahn's algorithm.
///
/// Each row in `data` has an `id` and a list of `upstream_ids` — the nodes
/// that drain **into** `id`.  Directed edges therefore go
/// `upstream_id → id` (water flows from upstream to downstream).
///
/// Kahn's topological sort processes nodes with in-degree 0 first and
/// decrements the in-degrees of their downstream neighbours.  After the sort,
/// any node whose in-degree is still > 0 participates in a cycle.
///
/// Reports a single `"graph.cycle_detected"` diagnostic that lists the first
/// few cycle-participating node IDs and the total count.
pub fn check_acyclicity(data: &GraphData) -> Vec<Diagnostic> {
    let n = data.ids.len();
    if n == 0 {
        debug!("E1 acyclicity check skipped — empty graph");
        return vec![];
    }

    // --- Build graph structures -------------------------------------------
    //
    // in_degree[id] = number of upstream_ids entries for that node's row.
    //   (Each upstream_id is an edge pointing TO `id`, so the in-degree of
    //    `id` is exactly the length of its upstream_ids list.)
    //
    // reverse_adj[upstream_id] = list of downstream node ids that name
    //   `upstream_id` in their upstream_ids.  We need this to know which
    //   nodes to "unlock" when we remove a source from the queue.

    let mut in_degree: HashMap<i64, usize> = HashMap::with_capacity(n);
    let mut reverse_adj: HashMap<i64, Vec<i64>> = HashMap::with_capacity(n);

    // Initialise every known node with in_degree 0 so nodes with no upstream
    // references are still present in the map.
    for &id in &data.ids {
        in_degree.entry(id).or_insert(0);
    }

    for (i, &id) in data.ids.iter().enumerate() {
        let upstreams = &data.upstream_ids[i];
        // The in-degree of `id` equals the number of upstream edges.
        *in_degree.entry(id).or_insert(0) = upstreams.len();

        for &uid in upstreams {
            // uid → id edge: `id` is a downstream of `uid`.
            reverse_adj.entry(uid).or_default().push(id);
            // Ensure uid is present in in_degree (it may not have its own row
            // if the graph is malformed, but we still handle it gracefully).
            in_degree.entry(uid).or_insert(0);
        }
    }

    // --- Kahn's BFS -------------------------------------------------------

    let mut queue: VecDeque<i64> = in_degree
        .iter()
        .filter_map(|(&id, &deg)| if deg == 0 { Some(id) } else { None })
        .collect();

    let mut processed = 0usize;

    while let Some(node) = queue.pop_front() {
        processed += 1;
        if let Some(downstream_nodes) = reverse_adj.get(&node) {
            for &downstream in downstream_nodes {
                let deg = in_degree.entry(downstream).or_insert(0);
                if *deg > 0 {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(downstream);
                    }
                }
            }
        }
    }

    // --- Cycle detection --------------------------------------------------

    let total_nodes = in_degree.len();
    if processed == total_nodes {
        debug!(
            nodes = total_nodes,
            "E1 acyclicity check passed — no cycles"
        );
        return vec![];
    }

    // Collect cycle-participating node IDs (those with remaining in-degree > 0).
    let mut cycle_nodes: Vec<i64> = in_degree
        .iter()
        .filter(|&(_, &deg)| deg > 0)
        .map(|(&id, _)| id)
        .collect();
    cycle_nodes.sort_unstable();

    let cycle_count = cycle_nodes.len();
    const DISPLAY_LIMIT: usize = 10;
    let preview: Vec<String> = cycle_nodes
        .iter()
        .take(DISPLAY_LIMIT)
        .map(|id| id.to_string())
        .collect();
    let preview_str = preview.join(", ");
    let suffix = if cycle_count > DISPLAY_LIMIT {
        format!(", ... ({} total)", cycle_count)
    } else {
        String::new()
    };

    let message = format!("{cycle_count} node(s) participate in a cycle: [{preview_str}{suffix}]");

    debug!(
        cycle_nodes = cycle_count,
        processed,
        total = total_nodes,
        "E1 acyclicity check found cycles"
    );

    vec![Diagnostic::error(
        "graph.cycle_detected",
        Category::GraphInvariant,
        Artifact::Graph,
        message,
    )]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
