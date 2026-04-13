# Post-GRIT Open Items

Surfaced during the first real dataset wrangle (GRIT Europe, 2026-04-13).
Each item needs a design decision before implementation.

---

## 1. Hilbert Curve Parameters

**Status:** Spec under-specified

**Problem:** The spec requires rows to be sorted by Hilbert curve index on centroid coordinates but does not define the curve level, coordinate space, or reference algorithm. Different adapters could produce different orderings that all claim conformance. The validator cannot enforce sort order without exact parameters.

**Needs:** Exact Hilbert curve parameters in the spec (level, coordinate normalization, reference implementation) before sort-order validation can be added.

---

## 2. Raster CRS and Extent Validation

**Status:** Implementation blocked

**Problem:** The spec requires raster CRS to match vector CRS and raster extent to fully contain the manifest bbox. Checking these requires parsing GeoTIFF GeoKeys, which the `tiff` 0.9 crate does not support.

**Options:**
- A) Add optional GDAL bindings for GeoTIFF metadata
- B) Use or build a pure-Rust GeoKeys parser
- C) Accept the gap and document it

**Current behavior:** The validator emits info-level diagnostics noting these checks are not implemented.

---

## 3. Polygon Topology Validation

**Status:** Partial implementation

**Problem:** The spec requires geometries to be valid (no self-intersections). The validator checks WKB structural validity via `geozero` but does not perform topological validation (self-intersection, ring orientation, hole containment).

**Options:**
- A) Add `geo` crate with `Validation` trait for DE-9IM topology checks
- B) Use GEOS bindings (heavy dependency)
- C) Accept structural-only validation and document the gap

**Current behavior:** WKB parse errors are caught; topological issues pass silently.

---

## 4. `up_area_km2` Computation Algorithm for DAG Fabrics

**Status:** Needs formal specification

**Problem:** The spec now clarifies that `up_area_km2` is the inclusive cumulative upstream area. When `has_up_area = false`, the engine computes it from graph traversal. However, for DAG topologies, the computation is non-trivial: shared upstream nodes must be counted exactly once (not double-counted at merge points).

**Needs:** A formally specified algorithm (e.g., topological-sort summation with visited sets) so engine implementations are deterministic and agree on values.

---

## 5. Reach-Based Snap for GRIT

**Status:** Deferred

**Problem:** The GRIT adapter uses segment lines as snap targets (150K features). Reach lines (1.9M features) would provide finer spatial resolution for outlet snapping but require cross-layer joins to inherit `is_mainstem` and drainage area from the parent segment.

**Complication:** The Europe reach layer has null `drainage_area_in`/`drainage_area_out` for all 1.9M rows and lacks `is_mainstem` entirely. A working reach-based snap requires either a different GRIT regional slice or upstream GRIT data fixes.

**When to revisit:** When a GRIT slice with populated reach metadata becomes available, or when snap resolution is identified as a bottleneck in delineation accuracy.
