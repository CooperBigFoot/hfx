# HFX — HydroFabric Exchange Specification

**Version 0.1**

---

## Overview

HFX is the canonical data contract consumed by the watershed delineation engine. It is not a native hydrofabric format. Every source fabric must be compiled into HFX by an adapter before the engine sees it.

The engine operates on HFX exclusively. It contains no fabric-specific logic.

### Terminology

A **catchment atom** is the smallest indivisible drainage unit within a compiled dataset. The atom boundary depends on the source fabric and the adapter that produced it: a hierarchically subdivided sub-basin, a stream-segment catchment, a unit catchment, etc. HFX does not prescribe how atoms are derived — only that they form a non-overlapping drainage partition over the dataset domain, and that each atom participates in a directed drainage graph that may be convergent or branching.

---

## Artifact Summary

| Artifact | Required | Purpose |
|---|---|---|
| `catchments.parquet` | Yes | Atom polygons |
| `graph.arrow` | Yes | Upstream adjacency graph |
| `snap.parquet` | No | Outlet snapping targets |
| `flow_dir.tif` | No | D8 flow direction raster for terminal refinement |
| `flow_acc.tif` | No | Flow accumulation raster for terminal refinement |
| `manifest.json` | Yes | Dataset metadata and contract declaration |

If `flow_dir.tif` or `flow_acc.tif` are absent, the engine skips raster refinement and uses the coarse terminal atom polygon directly.

---

## 1. `catchments.parquet`

One row per catchment atom.

### Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `id` | `int64` | No | Unique atom ID within this dataset |
| `area_km2` | `float32` | No | Geodesic area of this atom in km² |
| `up_area_km2` | `float32` | Yes | Inclusive cumulative upstream drainage area in km² (sum of all upstream atoms' `area_km2` plus this atom's own `area_km2`). Null if not precomputed by the adapter |
| `bbox_minx` | `float32` | No | Bounding box — western edge (degrees longitude) |
| `bbox_miny` | `float32` | No | Bounding box — southern edge (degrees latitude) |
| `bbox_maxx` | `float32` | No | Bounding box — eastern edge (degrees longitude) |
| `bbox_maxy` | `float32` | No | Bounding box — northern edge (degrees latitude) |
| `geometry` | `binary` (WKB) | No | Polygon or MultiPolygon, WGS84 (EPSG:4326) |

### Spatial Partitioning

- Rows **must** be sorted by Hilbert curve index computed on centroid coordinates.
- Row group size: 4,096–8,192 rows.
- Parquet row group statistics on `bbox_minx`, `bbox_miny`, `bbox_maxx`, `bbox_maxy` **must** be written. This enables the engine to eliminate row groups via column statistics before deserializing any geometry.

### Notes

- `id = 0` is reserved and **must not** be used. Zero is the engine's sentinel for "no downstream neighbor" (terminal/sink).
- Negative IDs are invalid.
- Geometries must be valid (no self-intersections). Run `ST_MakeValid` during ETL if source data is dirty.
- CRS must be EPSG:4326. Adapters are responsible for reprojection.
- `up_area_km2`, when present, **must** represent the inclusive cumulative upstream area: the sum of `area_km2` for this atom and all atoms reachable upstream through `graph.arrow`. For DAG-topology datasets where the source fabric partitions drainage area at bifurcations, adapters **should** set `has_up_area = false` in the manifest and leave `up_area_km2` null, allowing the engine to compute it from graph traversal.

---

## 2. `graph.arrow`

The upstream adjacency graph over catchment atoms. Contains no geometry. This is what the engine traverses during BFS.

### Schema (Apache Arrow IPC file)

| Column | Type | Nullable | Description |
|---|---|---|---|
| `id` | `int64` | No | Atom ID (FK → `catchments.parquet`) |
| `upstream_ids` | `list<int64>` | No | All atom IDs that drain directly into this one. Empty list `[]` for headwater atoms |

### Notes

- Every `id` present in `catchments.parquet` **must** have a corresponding row here, even headwaters (with `upstream_ids = []`).
- For tree-topology fabrics (`topology = "tree"`), `upstream_ids` has 0–2 entries per row.
- For DAG-topology fabrics (`topology = "dag"`), `upstream_ids` may have more entries where distributaries re-merge. The engine **must** maintain a visited set during BFS to avoid visiting shared upstream nodes more than once.
- The graph must be acyclic. Adapters must detect and break cycles (e.g., endorheic loops) during ETL.
- Arrow IPC format (not Parquet) is used for zero-copy memory mapping.

**On-disk vs. in-memory layout.** The Arrow IPC list-column schema above is the on-disk contract — all adapters must produce this layout, and the validator checks it. The engine may convert the list-column representation to CSR (flat neighbors array + offsets array) or any other layout at load time. The in-memory representation is an implementation detail, not part of this specification.

**Downstream adjacency and bifurcations.** The graph stores upstream adjacency only. Downstream adjacency is implicit: if atom A appears in the `upstream_ids` of both B and C, then A bifurcates to B and C. The engine can recover downstream neighbors by inverting the upstream map at load time if needed. Future versions may add explicit `downstream_ids` and flow partition weights (e.g., width-based fractions) at bifurcation nodes to support flow-splitting semantics beyond simple inclusive accumulation.

---

## 3. `snap.parquet`

Snapping targets used to attach an outlet point to the drainage network. **This artifact is optional.** If absent (`has_snap = false` in manifest), the engine resolves the terminal atom via point-in-polygon spatial containment on `catchments.parquet`. When present, the engine uses the tiered ranking below for higher precision outlet resolution. Snap targets are most valuable for fabrics with explicit stream-line features. For polygon-only fabrics without reach geometry, point-in-polygon is typically sufficient.

### Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `id` | `int64` | No | Unique snap feature ID |
| `catchment_id` | `int64` | No | FK → `catchments.parquet`. The atom this snap target belongs to |
| `weight` | `float32` | No | Snap ranking weight. MUST be monotonically increasing in drainage dominance: a higher weight MUST indicate the more hydrologically significant reach. Adapters typically use upstream drainage area (km² or cell count). Datasets whose weights do not satisfy this ordering are non-conformant with v0.2 snapping semantics. |
| `is_mainstem` | `bool` | No | True if this feature is on the mainstem. Used to prefer mainstem snap targets over distributaries |
| `bbox_minx` | `float32` | No | Bounding box west |
| `bbox_miny` | `float32` | No | Bounding box south |
| `bbox_maxx` | `float32` | No | Bounding box east |
| `bbox_maxy` | `float32` | No | Bounding box north |
| `geometry` | `binary` (WKB) | No | LineString (reach/segment centerline) or Point (node). WGS84 |

### Snapping Logic (engine-side)

The engine resolves an outlet point to a terminal atom via a weight-first cascade:

1. **Filter by radius.** Query snap features within a configurable search radius of the outlet point. Discard all candidates outside the radius.
2. **Rank by weight.** Prefer the candidate with the highest `weight`.
3. **Break ties by mainstem preference.** If multiple features share the winning weight (within a configurable tolerance), prefer `is_mainstem = true`.
4. **Break ties by distance.** If still tied, prefer the nearest geometry (minimum distance from outlet to feature).
5. **Break final ties by snap id.** Ascending `id` for determinism.

The engine exposes the snap strategy as a runtime configuration. Alternative strategies (distance-first tiered ranking, mainstem-biased) remain available as opt-ins for datasets whose weights are not rank-meaningful.

The winning feature's `catchment_id` is the terminal atom ID.

### Spatial Partitioning

Same Hilbert-sort and row group statistics requirements as `catchments.parquet`.

### Notes

- For polygon-only fabrics without explicit reach geometry, adapters MAY omit `snap.parquet` entirely (`has_snap = false`) and let the engine fall back to point-in-polygon on `catchments.parquet`. If the adapter does provide snap targets (e.g., for higher-precision outlet resolution), the preferred approach is outlet-directed representative geometries: skeletonized centerlines or pour-point-derived lines that approximate the drainage path within each atom. As a lower-quality fallback, the polygon centroid may be used as a Point with `weight = up_area_km2`, but centroids can be poor proxies for drainage attachment in elongated or irregular polygons.
- `is_mainstem = true` for all features in non-bifurcating fabrics.

---

## 4. `flow_dir.tif`

D8 flow direction raster. Used only for terminal atom raster refinement.

### Contract

| Property | Value |
|---|---|
| Format | Cloud-Optimized GeoTIFF (COG) |
| dtype | `uint8` |
| NoData | `255` |
| CRS | EPSG:4326 (must match all vector data) |
| Tiling | Internal tiles required (COG). 256×256 or 512×512 px |
| Encoding | Declared in `manifest.json` — either `"esri"` or `"taudem"` (see below) |

### Encoding Conventions

| Direction | ESRI (powers of 2) | TauDEM (1–8, E origin) |
|---|---|---|
| East | 1 | 1 |
| Southeast | 2 | 8 |
| South | 4 | 7 |
| Southwest | 8 | 6 |
| West | 16 | 5 |
| Northwest | 32 | 4 |
| North | 64 | 3 |
| Northeast | 128 | 2 |

The engine normalizes to its internal convention at read time based on the `flow_dir_encoding` field in `manifest.json`. Adapters do not need to re-encode.

### Notes

- The engine reads a windowed tile covering only the terminal atom's bbox. The full raster is never loaded into memory.
- Resolution should match or be finer than the source DEM used to generate `catchments.parquet`.

---

## 5. `flow_acc.tif`

Flow accumulation raster. Used alongside `flow_dir.tif` to snap the outlet to the nearest sufficiently large stream cell within the terminal atom.

### Contract

| Property | Value |
|---|---|
| Format | Cloud-Optimized GeoTIFF (COG) |
| dtype | `float32` |
| NoData | `-1.0` |
| CRS | Must match `flow_dir.tif` exactly |
| Values | Upstream cell count (not area). Area is computed by the engine from cell size and CRS |
| Tiling | Same as `flow_dir.tif` |

### Notes

- `float32` is required. `int32` overflows on large basins (e.g., Amazon mainstem exceeds 2³¹ cells at 30 m resolution).
- The engine uses `flow_acc` to determine the minimum accumulation threshold for snapping (configurable, default: 1,000 cells). This prevents snapping to ephemeral or misrouted cells at the polygon edge.

---

## 6. `manifest.json`

Sidecar metadata file. Declares the dataset identity and all parameters the engine needs to interpret the other artifacts.

The manifest describes **what the data is**, not how the engine should use it. Traversal policies (e.g., inclusive vs. mainstem-only accumulation) are engine runtime parameters, not dataset properties.

### Schema

```json
{
  "format_version": "0.1",
  "fabric_name": "example-fabric",
  "fabric_version": "2024.1",
  "fabric_level": 3,
  "crs": "EPSG:4326",
  "has_up_area": true,
  "has_rasters": true,
  "has_snap": true,
  "flow_dir_encoding": "esri",
  "terminal_sink_id": 0,
  "topology": "tree",
  "region": "europe",
  "bbox": [-25.0, 34.0, 45.0, 72.0],
  "atom_count": 194356,
  "created_at": "2025-04-10T00:00:00Z",
  "adapter_version": "0.1.0"
}
```

### Field Definitions

| Field | Type | Required | Description |
|---|---|---|---|
| `format_version` | string | Yes | HFX version this dataset targets |
| `fabric_name` | string | Yes | Source fabric identifier. Lowercase ASCII, no whitespace |
| `fabric_version` | string | No | Version of the source fabric |
| `fabric_level` | int | No | Optional hierarchical subdivision level within the source fabric (e.g., resolution tier, nesting depth) |
| `crs` | string | Yes | CRS for all vector and raster data. Must be `"EPSG:4326"` in HFX v0.1. The field exists for forward compatibility with projected CRS support in future versions |
| `has_up_area` | bool | Yes | Whether `up_area_km2` is populated in `catchments.parquet`. If false, engine computes it from graph traversal |
| `has_rasters` | bool | Yes | Whether `flow_dir.tif` and `flow_acc.tif` are present. If false, raster refinement is skipped |
| `has_snap` | bool | Yes | Whether `snap.parquet` is present. If false, engine uses point-in-polygon on `catchments.parquet` for outlet resolution |
| `flow_dir_encoding` | string | Cond. | Required if `has_rasters = true`. One of `"esri"` or `"taudem"` |
| `terminal_sink_id` | int | Yes | The ID value used to indicate no downstream neighbor. Must be `0` |
| `topology` | string | Yes | Graph topology class. `"tree"` for strictly convergent fabrics where each atom has at most one downstream neighbor. `"dag"` for fabrics with bifurcations (one atom draining to multiple downstream atoms). Informational — the engine handles both, but MAY use this for optimization hints |
| `region` | string | No | Geographic region label. Informational |
| `bbox` | float[4] | Yes | `[minx, miny, maxx, maxy]` covering all atoms. For global datasets this is the full source-fabric extent; for partial-fabric datasets it is the subset extent. Used by engine for fast pre-filtering |
| `atom_count` | int | Yes | Total number of rows in `catchments.parquet`. Sanity check |
| `created_at` | string | Yes | ISO 8601 / RFC 3339 timestamp of ETL run |
| `adapter_version` | string | Yes | Version of the adapter that produced this dataset |

---

## Deployment Patterns

An HFX dataset is the artifact bundle a single `manifest.json` describes. Every dataset covers a contiguous extent of its source fabric. That extent may be the fabric in full, or a named subset. Two deployment patterns are conformant with v0.1: **global** and **partial-fabric**. Both use the same artifact schemas and pass the same validator checks.

The deployment pattern is inferred from the manifest, not signalled by a dedicated field. The engine treats both patterns identically at query time.

### Global datasets

A global dataset covers the full extent of its source fabric. For a planetary fabric this is the whole planet; for a fabric that is inherently bounded at source (e.g., a continental hydrofabric) it is the full source extent.

- `region` **should** be omitted.
- `bbox` spans the full source-fabric extent. For a planetary dataset this is `[-180, -90, 180, 90]`; these boundary values are exact in EPSG:4326 and **must not** be padded beyond them.
- `fabric_name` and `fabric_version` identify the slice unambiguously; no further qualifier is needed.

This is the recommended pattern when callers must delineate at arbitrary query points without knowing in advance which regional slice contains the outlet.

### Partial-fabric datasets

A partial-fabric dataset covers a named subset of the source fabric. The subset boundary is an adapter concern — a continent, a Pfafstetter level-2 basin, an administrative region, a test harness fixture.

- `region` **should** be populated with a free-form label identifying the subset (e.g., `"europe"`, `"pfaf27"`, `"conus"`).
- `bbox` spans the subset extent.
- The subset **must** be closed under upstream traversal: every atom's `upstream_ids` **must** resolve to an atom present in the same dataset. Adapters producing a partial-fabric dataset are responsible for clipping the graph and dropping cross-boundary edges.

HFX does not prescribe a controlled vocabulary for `region`; the label is informational and exists to disambiguate datasets from the same source fabric.

### Format invariance

**Neither pattern changes the artifact schema or the validator contract.** Schema checks, referential integrity, and graph invariants apply identically to global and partial-fabric datasets. `region` presence or absence is the only manifest-level distinction, and it is advisory rather than schema-enforced. Engines **must not** switch behaviour based on deployment pattern.

---

## Engine Behaviour Contract (v0.1)

Version 0.1 of the engine implements **inclusive upstream accumulation** only. Given a valid HFX dataset and an outlet point, the engine:

1. **Snap** — If `has_snap = true`: query `snap.parquet` within search radius, resolve terminal atom via the weight-first cascade described in §3. If `has_snap = false`: perform point-in-polygon spatial containment query on `catchments.parquet` using bbox column statistics for row-group pruning. The containing atom is the terminal atom.
2. **Locate** — the resolved `catchment_id` is the terminal atom.
3. **Traverse** — BFS over `graph.arrow` from the terminal atom. Maintain a visited set; collect all reachable upstream atom IDs. Every upstream path is followed regardless of `is_mainstem` status (inclusive mode).
4. **Refine** (if `has_rasters = true`) — window `flow_acc.tif` and `flow_dir.tif` to the terminal atom's bbox. Snap outlet to nearest cell exceeding the accumulation threshold. Run reverse D8 trace from that cell. Convert the resulting cell mask to a polygon. Replace the terminal atom geometry with this refined sub-polygon.
5. **Fetch** — load geometries for all collected atom IDs from `catchments.parquet` using bbox row group pruning.
6. **Dissolve** — union all polygons into the final watershed boundary.
7. **Output** — return dissolved polygon with geodesic area in km².

The engine does not read `manifest.json` at query time during hot-path execution. The manifest is read once at dataset load and its parameters are baked into the engine's runtime configuration for that session.

### Future: mainstem-only traversal

A future engine version may support mainstem-only (branch) traversal, where BFS follows only the preferred upstream path at each node. This will require either edge attributes in `graph.arrow` or a supplementary lookup that identifies the mainstem parent at each bifurcation. The v0.1 graph schema is forward-compatible — additional columns can be added to `graph.arrow` without breaking existing datasets, and the engine can ignore columns it does not recognize.

---

## Validation

A conformant HFX dataset must pass the following checks (provided as a standalone validator tool):

**Snap presence check:**

- If `has_snap = true`, `snap.parquet` must be present.
- If `has_snap = false`, `snap.parquet` may be present in the directory but is ignored by the engine and not validated. Adapters should avoid shipping unused artifacts.

**Referential integrity (if `has_snap = true`):**

- All `catchment_id` values in `snap.parquet` exist in `catchments.parquet`.

**Referential integrity:**

- All `id` values in `graph.arrow` exist in `catchments.parquet`.
- All entries within `upstream_ids` in `graph.arrow` exist in `catchments.parquet`.
- Every `id` in `catchments.parquet` has a corresponding row in `graph.arrow`.

**Graph invariants:**

- No cycles exist in the graph.
- `id = 0` does not appear in `catchments.parquet`.

**Schema checks:**

- `bbox_min* < bbox_max*` (strict inequality) for every row in `catchments.parquet`.
- `bbox_min* <= bbox_max*` (non-strict inequality) for every row in `snap.parquet` (if `has_snap = true`). Snap features MAY be LineString geometries whose bounding-box extent is zero along one axis.
- `atom_count` in manifest matches row count in `catchments.parquet`.

**Raster checks (if `has_rasters = true`):**

- Both `flow_dir.tif` and `flow_acc.tif` are present.
- `flow_dir_encoding` is set in manifest.
- Raster CRS matches vector CRS declared in manifest.
- Raster spatial extent fully contains the `bbox` declared in the manifest. (Prevents edge-of-raster failures during terminal atom refinement.)

**Geometry spot-check:**

- 1% random sample of geometries in `catchments.parquet` are valid WKB polygons.

**Snap geometry type check (if `has_snap = true`):**

- All geometries in `snap.parquet` must be WKB Point or LineString.

---

*This specification is intentionally silent on adapter implementation. Any tool that produces conformant artifacts from any source fabric is a valid adapter.*
