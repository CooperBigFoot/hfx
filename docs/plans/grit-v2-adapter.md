# GRIT v2 Adapter Plan — Global HFX v0.2.1 Build

**Status:** Approved — ready for execution. Resolves the GRIT-side build architecture for HFX v0.2.1.

**Date:** 2026-05-25 (revised 2026-05-25 to fold in coordinator resolutions)

**Companion specs:** [`spec/HFX_SPEC.md`](../../spec/HFX_SPEC.md) (v0.2.1), [`spec/aux/snap/v1.md`](../../spec/aux/snap/v1.md)

**Companion adapter docs:** [`adapters/_template/build_adapter.py`](../../adapters/_template/build_adapter.py) (nine-stage contract), [`adapters/grit/GRIT_HFX_SPEC_VALIDATION.md`](../../adapters/grit/GRIT_HFX_SPEC_VALIDATION.md) (v1 friction log)

---

## 0. Resolved decisions (do not relitigate)

These were set in the planning brief and are inputs to this plan, not topics for re-debate:

| # | Decision | Implication |
|---|---|---|
| 1 | **Scope:** full HFX v0.2.1 port; two levels (`level=0` segments, `level=1` reaches); components dropped | No `component_id` column, no level=2 |
| 2 | **Packaging:** single global dataset, no per-region split in the published artifact | Publish path `s3://basin-delineations-public/grit/2.0.0/` |
| 3 | **IDs:** re-issued dense int64 starting at 1, assigned in `(level ASC, hilbert_index ASC)` order; `id=0` forbidden | Source `global_id` is preserved only via `source_id` |
| 4 | **`source_id`:** `"segment:<global_id>"` for level 0, `"reach:<global_id>"` for level 1; always populated | Always present, always populated; not a normalised HFX identity |
| 5 | **`level_label`:** `"segment"` (level 0), `"reach"` (level 1) | Unnormalised informational column |
| 6 | **Outlet:** downstream endpoint of the line geometry — uniform across segments, reaches, and all topology roles | No role branching; v0.2.1 outlet rules are satisfied by line directionality |
| 7 | **`parent_id`:** NULL for segments; reach → new_id of its parent segment via `reach.segment_id → segment.global_id → new_id` | Single-coarser-level dataset, two-tier nesting |
| 8 | **`up_area_km2`:** segments use GRIT `drainage_area_out` (km²) directly. **Reaches do NOT inherit from the parent segment**; reach `up_area_km2` is computed in a reverse-topological DP over the reach graph (see Phase 2.5). `has_up_area=true` | DAG semantics: partitioned by flow physics at bifurcations (option (a) in HFX spec §1) — documented in dataset-root `README.md` |
| 9 | **Graph:** one `graph.parquet` covering both levels; v0.2.1 bbox columns present | Reach-level upstream relation **requires source-data investigation** before implementation (see §5 risk register) |
| 10 | **Snap:** no core `snap.parquet`; two `hfx.aux.snap.v1` artifacts (`aux/snap_segments.parquet` references_levels=[0], `aux/snap_reaches.parquet` references_levels=[1]) | Reach snap inherits `weight` + `stem_role` from parent segment |
| 11 | **Stem-role classification:** `is_mainstem=1`→`mainstem`; `is_mainstem=0` with source-node `deg_out>1`→`distributary`; `is_mainstem=0` otherwise→`tributary`; unclassifiable→`unknown` | Distributary detection requires node-degree analysis over the segment graph |
| 12 | **No D8 rasters** in v1 build | Manifest omits any `hfx.aux.d8_raster.v1` entry |
| 13 | **Manifest:** `format_version`=`"0.2.1"`, `fabric_name`=`"grit"`, `fabric_version`=`"1.0.0"`, `crs`=`"EPSG:4326"`, `topology`=`"dag"`, `has_up_area`=true, `region` OMITTED, `bbox`=[-180.0,-90.0,180.0,90.0] exact (no padding), `adapter_version`=`"grit-global-2.0.0"` | Global dataset; per v0.2.1 spec §Deployment Patterns boundary values are exact for planetary datasets |
| 14 | **Layout:** `catchments.parquet`, `graph.parquet`, `manifest.json`, `aux/snap_segments.parquet`, `aux/snap_reaches.parquet` | Aux paths relative to dataset root |
| 15 | **Row ordering:** `(level ASC, hilbert_index ASC)` for catchments and graph; single-level Hilbert-only for each aux snap file | `hilbert_index` is a sort key, never stored |
| 16 | **Row groups:** 4096–8192 rows; bbox column stats written; **compression = `snappy`** throughout (NOT zstd) | v1 friction log: validator may not be compiled with zstd support |
| 17 | **Nine-stage shape:** all nine `def stage_` functions remain at module level | New per-region preprocessing lives in pre-stage helpers invoked from `stage_1` |

**Format version:** `manifest.json::format_version = "0.2.1"`.

---

## 1. Out of scope (do not touch in this workstream)

- D8 raster auxiliary (flow_dir.tif / flow_acc.tif).
- Component-level (`level=2`) catchments. `GRITv1.0_component_catchments_GLOBAL_*` is **not** read.
- Validator changes — zstd codec, JSON-mode stdout cleanup, batch-read error flooding, `--strict` ergonomics. Filed in `GRIT_HFX_SPEC_VALIDATION.md`, scheduled separately.
- Re-publication of the v1 per-region datasets at `s3://basin-delineations-public/grit/1.0.0/`. v2 ships side-by-side at `grit/2.0.0/`.
- Engine snap semantics (filter→weight→mainstem→distance→id cascade is the engine's contract, not the adapter's).

---

## 2. Build pipeline architecture

The build runs as four sequential phases mapped onto the nine-stage template. The orchestrator (`_build_dataset`) calls stages in numeric order; the template contract is satisfied because each numbered stage function exists at module scope. Phase 1 work lives in pre-stage helpers invoked from `stage_1`.

```
Phase 1   — Per-region preprocess                                (stage_1 + pre-stage helpers)
Phase 1.5 — Conditional disk-cleanup pass (only if free disk < 300 GB)
Phase 2   — Global concat, sort, ID re-issue                     (stages 2–5)
Phase 2.5 — Resolve graph edges + reverse-topo reach up_area_km2 (helpers consumed by stages 6 & 7)
Phase 3   — Write outputs                                        (stages 6–9)
Phase 4   — Validate                                             (separate CLI subcommand `validate`)
```

### Phase 1 — Per-region preprocess (Stage 1)

Repeats for each region in `("AF", "AS", "EU", "NA", "SA", "SI", "SP")`. Regions are independent and could be parallelised (see §6 Test strategy). Each region produces four intermediate Parquet shards inside a working directory; nothing in Phase 1 needs cross-region state.

Per-region steps (executed via pre-stage helpers called from `stage_1_inspect_source`):

1. Extract regional GPKGs from the 55 GB outer archive (`17435232.zip`) into the working dir (reuse `_extract_member` / `_extract_inner_gpkg` from v1).
2. **Segments layer (`GRITv1.0_segments_<R>_EPSG4326.gpkg.zip`, layer `lines`):**
   - Read with `pyogrio` (Arrow path): `global_id`, `upstream_line_ids`, `downstream_line_ids`, `is_mainstem`, `drainage_area_out`, geometry.
   - Parse CSV upstream/downstream id columns to `list[int64]` (reuse v1 `_parse_csv_int_lists`).
   - Build a per-region **segment node-degree map** by hashing segment endpoint coordinates: for each unique downstream node, count how many segments share that node as their downstream-end (`deg_out_node`). A segment whose downstream node has `deg_out_node > 1` is a candidate fork (segment is one of multiple branches diverging at that node — relevant for distributary classification of its *downstream* segments, see below).
   - Classify `stem_role` per segment:
     - `is_mainstem == 1` → `"mainstem"`.
     - `is_mainstem == 0` and the *source-node* (upstream endpoint of this segment) is a fork node (`deg_out_node > 1`) → `"distributary"`.
     - `is_mainstem == 0` otherwise → `"tributary"`.
     - Cannot classify (`is_mainstem` null) → `"unknown"`.
   - Compute outlet `(lon, lat)` from the line's downstream endpoint. Coordinate of the **last** coordinate in the line — GRIT guarantees flow direction along the line.
3. **Segment catchments (`segment_catchments__1`):**
   - Read `global_id`, `area` (km²), geometry.
   - Run `shapely.make_valid` via v1 `_coerce_to_polygonal` (drops non-polygonal shards from GeometryCollections).
   - Compute per-row bbox.
   - Inner-join on `global_id` to segments to attach `up_area_km2 = drainage_area_out`, `stem_role`, `outlet_lon/lat`, etc.
4. **Reaches layer (`GRITv1.0_reaches_<R>_EPSG4326.gpkg.zip`, layer `lines`):**
   - Read `global_id`, `segment_id` (parent segment), geometry, **plus any explicit upstream-relation column** the schema probe (§5 risk 1) identifies. If GRIT exposes `reach_upstream_ids` (or analogue), read it; if not, the build derives the reach graph from segment ordering (see Phase 2 §2).
   - `drainage_area_out` and `is_mainstem` are documented null/absent in at least the EU slice (GRIT_HFX_SPEC_VALIDATION.md). Do not read them on the reach layer.
   - Compute outlet from line downstream endpoint.
   - Join to segments via `reach.segment_id == segment.global_id` to inherit `stem_role` only (used both in catchments and in the reach snap layer) and the segment's `drainage_area_out` for use as the reach **snap weight**. **Do NOT inherit `up_area_km2`** — reach `up_area_km2` is computed in Phase 2.5 from reach graph + reach `area_km2`.
5. **Reach catchments:** read `global_id`, `area`, geometry; `make_valid`; compute bbox; inner-join to reaches on `global_id`. The reach catchment's own `area` (km²) becomes the reach row's `area_km2` and is the per-row input to the Phase 2.5 DP.
6. Per-region writes (snappy-compressed Parquet, schema fixed but row-group sizing left raw — these are intermediates, not final HFX artifacts):
   - `tmp/<region>/segments.parquet` — one row per segment catchment with `source_global_id`, `area_km2`, `up_area_km2` (= GRIT `drainage_area_out`), `stem_role`, `outlet_lon`, `outlet_lat`, `bbox_*`, `geometry_wkb`, `upstream_source_global_ids: list<int64>`.
   - `tmp/<region>/reaches.parquet` — `source_global_id`, `parent_source_global_id` (== `reach.segment_id`), `area_km2` (reach catchment area), `stem_role` (inherited from parent segment), `outlet_lon`, `outlet_lat`, `bbox_*`, `geometry_wkb`, `upstream_source_global_ids: list<int64>` (either from the source column under Path A, or empty list under Path B — filled in during Phase 2.5). **No `up_area_km2` column on this shard** — that field is materialised by the DP in Phase 2.5 and joined in at Stage 6.
   - `tmp/<region>/segment_snap.parquet` — `source_global_id`, `weight (=drainage_area_out km²)`, `stem_role`, line `geometry_wkb`, bbox.
   - `tmp/<region>/reach_snap.parquet` — same shape; `weight` and `stem_role` are the inherited parent-segment values (this is correct for snap-weight semantics, which describe drainage *dominance* of the stem the snap targets — distinct from per-unit `up_area_km2`).
7. Logging: per-region row counts, peak RSS hint, write durations.

`stage_1_inspect_source` returns a `SourceData` container holding paths to the regional shards plus aggregated row counts.

### Phase 1.5 — Conditional intermediate-cleanup pass

**Run only if `shutil.disk_usage(root).free` is below 300 GB at the end of Phase 1.** The orchestrator measures free disk after the last region's shards are written, and if the threshold trips, deletes the extracted regional GPKGs (already consumed) before Phase 2 begins. Concretely:

- Delete every `<root>/input/GRITv1.0_*_<REGION>_EPSG4326.gpkg` file.
- Keep the outer archive (`17435232.zip`) and the per-region `tmp/<region>/*.parquet` shards.

This recovers ~80–120 GB of inflated GPKG storage at the cost of having to re-extract should Phase 2 fail and need to rerun Phase 1. Skip this pass entirely when ≥300 GB free.

### Phase 2 — Global concat + Hilbert sort + dense ID re-issue (Stages 2–5)

The Phase 2 work fits into the existing stage names without renaming, with one mechanical fact to call out: **dense ID re-issue depends on Hilbert order**, so `stage_2_assign_ids` and `stage_5_hilbert_sort` form a tightly-coupled pair. The plan keeps them as separate functions for template fidelity but the orchestrator wires them so that `stage_5` runs first as a helper-call inside `stage_2`. Stages 3 and 4 collapse to no-op assertions because reprojection and geometry repair already occurred in Phase 1.

Stage-by-stage:

- **`stage_2_assign_ids(source: SourceData) -> IdAssignment`** — the heaviest stage in Phase 2:
  1. For each level in `[0, 1]`: concatenate all regional shards into one in-memory Arrow table of `(source_global_id, bbox_centroid_x, bbox_centroid_y)` rows. Compute `hilbert_index` from the centroid against the global total bounds `(-180,-90,180,90)` (use `geopandas.GeoSeries.hilbert_distance(total_bounds=GLOBAL_TOTAL_BOUNDS)` — same convention as v1 `merge_regions._build_hilbert_sort_index`).
  2. Sort each level's table by `(hilbert_index ASC, source_global_id ASC)` for determinism.
  3. Assign dense IDs in `(level ASC, hilbert_index ASC)` order: level-0 first 1..N_seg, then level-1 from N_seg+1..N_seg+N_reach.
  4. Build the canonical `(level, source_global_id) → new_id` mapping; persist it as `tmp/id_map.parquet` for later stages (and post-mortem traceability).
  5. Assert `id=0` does not appear and IDs are unique.
- **`stage_3_reproject(...)`** — assert source CRS was EPSG:4326 throughout Phase 1. No-op; surfaces as a single sanity check on the persisted shards.
- **`stage_4_make_valid(...)`** — assertion sweep on a sample of rows from the intermediate shards (re-running `make_valid` would be wasteful at 20 M rows). Logs sample counts.
- **`stage_5_hilbert_sort(...)`** — exposed as the helper used inside `stage_2`; called separately at the orchestrator level as a sanity-check on persisted ordering. Behaviour: load `tmp/id_map.parquet`, confirm rows are in `(level, hilbert_index)` order.

After Phase 2 completes:

- `tmp/id_map.parquet` holds the global ID assignments and the per-row Hilbert index.
- Per-region intermediate shards from Phase 1 are still on disk; downstream write stages stream them in Hilbert order via a sort-merge index, **avoiding loading 70–100 GB into memory** (mirrors v1 `merge_regions._stream_write_parquet`).

### Phase 2.5 — Resolve graph edges + reverse-topological reach `up_area_km2` DP

Phase 2.5 is implemented as pre-stage helpers consumed by both `stage_6_write_catchments` (which needs `up_area_km2` per reach row) and `stage_7_write_graph` (which writes the resolved edge lists). It is not a separate template stage — the executor invokes the helpers from the orchestrator between Phase 2 and Phase 3.

Two substeps:

**Substep 2.5a — Resolve graph edges per level.**

- Level 0 (segments): for each segment row, map its `upstream_source_global_ids` list through `(level=0, source_global_id) → new_id` from `tmp/id_map.parquet` to produce `upstream_ids: list<int64>` in HFX ID space. Headwaters yield `[]`. Persist as `tmp/edges_l0.parquet` columns `id`, `upstream_ids`.
- Level 1 (reaches): the source of upstream IDs depends on the Tier 0 schema-probe outcome.
  - **Path A (GRIT exposes an explicit reach upstream column):** map the source reach upstream IDs through `(level=1, source_global_id) → new_id`.
  - **Path B (derive from segment topology):** for each reach, determine ordering along its parent segment's flow path; intra-segment upstream = the immediately upstream reach within the same segment; at the segment's upstream end, the upstream reaches are the outlet reaches of segments listed in the parent segment's `upstream_line_ids`.
- Persist resolved edges as `tmp/edges_l1.parquet`.

**Substep 2.5b — Reverse-topological DP to compute reach `up_area_km2`.**

- Input: `tmp/edges_l1.parquet` (reach graph) + reach `area_km2` (already in `tmp/<region>/reaches.parquet`).
- Algorithm: standard reverse-topo accumulation. Compute reverse topological order over the level-1 DAG; iterate in that order; for each reach
  ```
  up_area_km2[id] = area_km2[id] + sum(up_area_km2[u] for u in upstream_ids[id])
  ```
  Headwater reaches (empty `upstream_ids`) get `up_area_km2 = area_km2`.
- Implementation hints:
  - Build CSR arrays for the level-1 graph for cache-friendly traversal (~20 M rows × small fan-in fits in a few GB).
  - Use `numpy.float64` for accumulation, cast to `float32` at the end (preserves precision over the depth of the network).
  - Kahn's algorithm to produce the topological order; reverse it.
- Persist as `tmp/reach_up_area.parquet` with columns `id`, `up_area_km2 (float32)`.

**Fallback.** If Path B is chosen AND derivation turns out fragile (e.g. intra-segment flow ordering cannot be reliably reconstructed from line endpoints because of multi-line reaches, zero-length reaches, or non-monotonic ordering along the segment flow path), set `up_area_km2 = NULL` for **all** reach rows instead of partially populating. Document the fallback in the dataset-root `README.md` under the DAG semantics note. **Do NOT silently inherit the segment `drainage_area_out`** — that mis-states the per-reach upstream area and silently misleads downstream consumers (within a single segment, the downstream-most reach should hold the full segment area, while the upstream reach should hold only a small fraction).

The DP pass itself is tractable: at ~20 M reach rows with average fan-in ~1.x, expect 10–30 min of wall-clock for the topological sort + accumulation pass on a workstation CPU.

### Phase 3 — Write outputs (Stages 6–9)

All writes use the global Hilbert sort index built in Phase 2 to interleave regional shards in the correct order. Each writer streams row groups of 4096–8192 rows.

- **`stage_6_write_catchments(...)`**
  - Output: `catchments.parquet`.
  - Schema: `id int64 NN`, `level int16 NN`, `parent_id int64 nullable`, `area_km2 float32 NN`, `up_area_km2 float32 nullable` (populated for both levels — segments from GRIT `drainage_area_out`, reaches from the Phase 2.5 DP; the column is nullable only because the Phase 2.5 fallback may NULL all reach rows), `outlet_lon float64 NN`, `outlet_lat float64 NN`, `bbox_* float32 NN`, `geometry binary NN` (WKB Polygon|MultiPolygon), plus optional `source_id string nullable` (always populated), `level_label string nullable` (always populated).
  - Compression: **snappy**. Not zstd.
  - For reach rows, the writer joins `tmp/reach_up_area.parquet` on `id` to attach `up_area_km2` before emitting each row group.
  - GeoParquet 1.1 metadata attached at schema construction (reuse v1 `build_geo_metadata(["Polygon", "MultiPolygon"])`).
  - Validate: assert row count, run `validate_geoparquet` post-write.
- **`stage_7_write_graph(...)`**
  - Output: `graph.parquet`.
  - Schema (v0.2.1 with bbox columns): `id int64 NN`, `level int16 NN`, `upstream_ids list<int64> NN`, `bbox_* float32 NN`.
  - Reads `tmp/edges_l0.parquet` and `tmp/edges_l1.parquet` already produced by Phase 2.5 substep 2.5a; no edge-resolution work happens in this stage itself.
  - Rows sorted by `(level ASC, hilbert_index ASC)` — index reused from `catchments.parquet`. `bbox_*` columns copied from the referenced unit. Row groups 4096–8192; bbox stats written; compression `snappy`.
  - Validate cycles: assert each per-level subgraph is acyclic via SCC (small batched check; full Tarjan can be deferred to validator).
- **`stage_8_write_snap(...)`** — writes BOTH aux files in this stage.
  - `aux/snap_segments.parquet`:
    - Schema: `id int64 NN`, `unit_id int64 NN`, `weight float32 NN`, `stem_role string nullable` (one of `mainstem|tributary|distributary|unknown`), `bbox_* float32 nullable`, `geometry binary NN` (WKB LineString).
    - `id` re-issued dense int64 starting at 1, in Hilbert order of the snap geometry's centroid.
    - `unit_id` = `(level=0, source_global_id) → new_id`.
    - `weight = drainage_area_out (km²)`.
    - Sort by snap centroid Hilbert index against global bounds.
    - Row groups 4096–8192; bbox stats written; degenerate-bbox padding reused from v1 (`inflate_degenerate_bounds`, belt-and-suspenders since v0.2 spec uses `<=`).
  - `aux/snap_reaches.parquet`: same schema, `unit_id` = `(level=1, source_global_id) → new_id`, `weight` and `stem_role` inherited from parent segment (already attached in Phase 1). The reach **snap** weight inherits from the parent segment because the snap layer expresses drainage *dominance* of the stem-line the snap targets — distinct from the per-unit `up_area_km2` column, which is computed via the Phase 2.5 DP.
  - Compression: **snappy**.
- **`stage_9_write_manifest(...)`** writes `manifest.json`:
  ```json
  {
    "format_version": "0.2.1",
    "fabric_name": "grit",
    "fabric_version": "1.0.0",
    "crs": "EPSG:4326",
    "has_up_area": true,
    "topology": "dag",
    "bbox": [-180.0, -90.0, 180.0, 90.0],
    "unit_count": <row count of catchments.parquet>,
    "created_at": "<RFC 3339 UTC at write time>",
    "adapter_version": "grit-global-2.0.0",
    "auxiliary": [
      {
        "schema": "hfx.aux.snap.v1",
        "artifacts": {"snap": "aux/snap_segments.parquet"},
        "metadata": {
          "name": "segment-stems",
          "description": "Segment-scale stems for level 0 GRIT segment catchments.",
          "references_levels": [0],
          "weight_semantics": "drainage_area_km2_partitioned"
        }
      },
      {
        "schema": "hfx.aux.snap.v1",
        "artifacts": {"snap": "aux/snap_reaches.parquet"},
        "metadata": {
          "name": "reach-stems",
          "description": "Reach-scale stems for level 1 GRIT reach catchments. Weight inherited from parent segment.",
          "references_levels": [1],
          "weight_semantics": "drainage_area_km2_partitioned"
        }
      }
    ]
  }
  ```
  - `region` is omitted (global dataset, per v0.2.1 §Deployment Patterns).
  - `bbox` is the planetary boundary exactly; no outward padding.
  - `unit_count` reads back the row count of the written `catchments.parquet` to avoid drift.

### Phase 4 — Validate

Run `cargo run -p hfx-validator -- <dataset> --format text --strict --sample-pct 100` and capture the report. Also run `--format json` to save `validator-report.json` for the bundle (v1 friction note: stdout is dirty in JSON mode; pipe with care).

Phase 4 is invoked via the adapter's `validate` subcommand (mirrors v1).

---

## 3. Stage-by-stage breakdown

The table below ties stage names to phases and read/transform/write responsibilities, with effort estimates.

| Stage | Phase | Read | Transform | Write | Effort |
|---|---|---|---|---|---|
| `stage_1_inspect_source` | 1 | 4 GPKG layers × 7 regions = 28 reads + outer-zip extraction | Per-region: parse CSV upstream/downstream ids; build node-degree map; classify stem_role; compute outlets; `make_valid` sweep; bbox + area; reach→segment join | 4 intermediate Parquet shards per region (28 files total) | **L** |
| `stage_2_assign_ids` | 2 | All segment/reach intermediate shards (centroid + source ID columns only) | Compute global Hilbert index against `(-180,-90,180,90)`; sort each level; assign dense new IDs starting at 1 | `tmp/id_map.parquet` | **M** |
| `stage_3_reproject` | 2 | Sample row metadata | Assert EPSG:4326 | (none) | **S** |
| `stage_4_make_valid` | 2 | Sample of intermediate shards | Assert sample geometries pass `is_valid` | (none) | **S** |
| `stage_5_hilbert_sort` | 2 | `tmp/id_map.parquet` | Assert ordering | (none) | **S** |
| **Phase 2.5 helpers** (edge resolution + reverse-topo reach DP) — invoked from the orchestrator between stages 5 and 6 | 2.5 | Segment/reach upstream lists + id_map + reach `area_km2` | Resolve upstream lists per level via id_map; Kahn topological sort over the reach DAG; accumulate `up_area_km2` in reverse-topo order; fallback to NULL on derivation failure | `tmp/edges_l0.parquet`, `tmp/edges_l1.parquet`, `tmp/reach_up_area.parquet` | **M** |
| `stage_6_write_catchments` | 3 | All catchment shards + id_map + `tmp/reach_up_area.parquet` (for reach rows) | Stream-merge in Hilbert order; attach reach `up_area_km2` via join; emit final rows | `catchments.parquet` | **M** |
| `stage_7_write_graph` | 3 | `tmp/edges_l0.parquet`, `tmp/edges_l1.parquet`, catchment bboxes | Attach bboxes; sort by (level, hilbert_index); write | `graph.parquet` | **S–M** |
| `stage_8_write_snap` | 3 | Segment/reach snap shards + id_map | Re-issue snap dense IDs (per file, in Hilbert order of snap centroid); inherit weight/stem_role for reaches (already in Phase 1) | `aux/snap_segments.parquet`, `aux/snap_reaches.parquet` | **M** |
| `stage_9_write_manifest` | 3 | `catchments.parquet` row count | Build manifest dict | `manifest.json` | **S** |

Effort key: S = ≤1 day, M = 2–4 days, L = ≥1 week.

---

## 4. Memory and I/O profile (per stage, order-of-magnitude)

Estimates assume the workstation budget (32–64 GB RAM, ~200 GB free disk) called out in the brief.

| Stage | Peak RAM | Disk write | Disk read | Notes |
|---|---|---|---|---|
| `stage_1_inspect_source` (per region) | 8–24 GB (driven by largest regional `reach_catchments` polygon set — AS or NA are the largest) | ~5–15 GB intermediate Parquet per region | GPKG inflation (a few × the zipped size); peak inflated GPKGs total ~80–120 GB across all 7 regions | Run one region at a time to fit memory; outer-zip already extracts on demand. Parallelism `j=2` (mirrors v1) only if RAM budget allows |
| `stage_2_assign_ids` | 4–10 GB (only centroid+id columns held globally: 2 × float64 + int64 × ~20 M rows ≈ 0.5 GB; Hilbert int64 ≈ 0.16 GB; sort buffers ~2–5×) | < 1 GB (`id_map.parquet`) | All intermediate centroid+id columns (~5 GB) | Global Hilbert sort is the memory peak; lexsort `np.lexsort` is in-place enough |
| `stage_3_reproject` | < 100 MB | 0 | tiny | No-op |
| `stage_4_make_valid` | < 100 MB | 0 | small sample | No-op |
| `stage_5_hilbert_sort` | < 1 GB | 0 | `id_map.parquet` | Assertion only |
| **Phase 2.5 helpers** (edge resolution + reach DP) | 4–8 GB (CSR arrays for level-1 graph at ~20 M nodes; float64 accumulator vector) | 1–3 GB (`tmp/edges_l*.parquet`, `tmp/reach_up_area.parquet`) | id_map + reach upstream lists | **DP wall-clock: 10–30 min** at ~20 M reach rows (Kahn topo sort + accumulation); fits comfortably in 32 GB RAM |
| `stage_6_write_catchments` | 4–8 GB (row-group buffering, geometry WKB serialisation) | 50–80 GB (`catchments.parquet`) | Sum of catchment shards ~60–100 GB + `tmp/reach_up_area.parquet` | Streams row group by row group; column-oriented |
| `stage_7_write_graph` | 4–8 GB (in-memory edge lists from `tmp/edges_l*.parquet`) | 1–3 GB (`graph.parquet`; no geometry) | Edge intermediates (~1–3 GB) | Cheapest write |
| `stage_8_write_snap` | 4–8 GB (line WKB buffering) | 10–20 GB combined for both aux files | Snap-line shards ~10–20 GB | Two passes, one per file |
| `stage_9_write_manifest` | < 100 MB | < 10 KB | Metadata only | One JSON write |
| **Phase 4 validate** | 4–10 GB (validator-internal) | small report JSON | Reads full dataset | Wall-clock is the concern; see §5 risk 3 |

Total final dataset disk footprint ≈ 70–100 GB (consistent with the brief).
Total transient disk footprint during build ≈ 200–250 GB (extracted GPKGs + intermediates + final). The 200 GB free-space precondition from v1 `run_all_regions._precondition_check` is raised to **≥300 GB** for v2; if free disk is below that threshold at the end of Phase 1, the orchestrator runs Phase 1.5 to delete the extracted regional GPKGs and recover ~80–120 GB before continuing.

---

## 5. Risk register

### Risk 1 — Reach-level upstream relation (BLOCKER, must resolve before code starts)

**Description.** The build's correctness on `graph.parquet` level 1 depends on how GRIT exposes the reach-level upstream relation. The brief lists two candidate paths:
- **Path A:** the reach `lines` layer exposes an explicit upstream-ids column (e.g. `reach_upstream_ids` or analogous).
- **Path B:** no such column; the build derives reach-level edges from segment ordering (within a segment, reaches chain along the flow direction; at the segment's upstream end, the upstream reaches are the outlet reaches of segments named in `segment.upstream_line_ids`).

The v1 friction log confirms that in the EU slice the reach `lines` layer does NOT carry `is_mainstem` and reach `drainage_area_*` columns are null — but it does not assert the absence of an upstream-ids column. **This is an open data fact.**

**Mitigation.** A schema-probe step (a few hours of GPKG inspection on EU + one large region such as AS or NA) must run before adapter code is written. Concretely:

```text
For each region in (EU, AS, NA):
  pyogrio inspect reaches GPKG layer "lines":
    enumerate columns + types + null counts
    look for: reach_upstream_ids, upstream_reach_ids, upstream_line_ids, prev_id, reach_predecessor
  spot-read 1000 rows; print fields and unique non-null counts
```

The probe's findings determine Path A vs Path B and feed into the executor brief.

**Why this is a blocker.** If Path A is wrong and Path B is also infeasible (e.g. reaches are not orderable along a segment line because the source line geometry is not split coherently), reach-level graph construction may require a custom topology pass over the reach line endpoints — a substantially larger work item. Better to know now.

**Owner:** executor pre-implementation, day 1.

### Risk 2 — Global Hilbert sort memory profile

**Description.** A ~20 M row Hilbert sort over centroids must fit in memory; the brief suggests either an in-memory sort or merge-sort of per-region sorted runs.

**Mitigation.** The id-only-plus-centroid columns at 20 M rows total ~0.7 GB (3 × float64 + int64). `np.lexsort` requires roughly 2–3× that as scratch. Net: ~3 GB peak; fits comfortably on a 32 GB workstation. **In-memory sort is acceptable**; no merge-sort fallback needed unless the row count grows by ~10×.

**Owner:** executor; verify on first cross-region dry-run.

### Risk 3 — Validator wall-clock at ~20 M rows

**Description.** The v1 friction log notes the validator is slow on large datasets. At 20 M rows, `cargo run -p hfx-validator -- --strict --sample-pct 100` could take tens of minutes to hours.

**Mitigation.**
- Build incremental validation: run validator against each artifact as it lands rather than at the end (`--skip-rasters` not relevant; per-artifact validation flags would need to be added if missing — file a follow-up issue if so).
- Smoke-test with `--sample-pct 1` first to confirm structural validity, then full pass.
- Consider running validation in the background while Phase 4 reports stream live.

**Owner:** executor; measure on EU-only smoke build.

### Risk 4 — Cross-region ID collisions

**Description.** GRIT documents `global_id` as globally unique per layer. The plan assumes this and the v1 merge_regions code already verifies it (`stage_7_concat_graph` raises on collisions). **Plan must reconfirm** by checking the union of `global_id` across all 7 regional segments and 7 regional reaches has no duplicates.

**Mitigation.** Stage 1 helper writes per-region id sets; a Phase 2 prelude step computes set-intersection. Verified before any global sort happens; fast (set ops over 20 M ints).

**Owner:** executor; embedded in `stage_2_assign_ids` preconditions.

### Risk 5 — Distributary classification correctness

**Description.** The brief defines distributary as `is_mainstem=0 AND source-node deg_out > 1`. This depends on accurate node hashing across coordinate float jitter. If GRIT segment endpoints carry sub-mm coordinate noise where they should be coincident, the node degree map will fragment and distributary detection will under-count.

**Mitigation.**
- Hash node coords at a sensible decimal precision (e.g. 7 dp ≈ 1 cm at the equator) before grouping, not as raw float tuples.
- Sanity-check that the count of distributary segments is plausible for a DAG fabric (non-zero, and a small but nonzero fraction of non-mainstem segments).

**Owner:** executor; verify on EU smoke build.

### Risk 6 — `up_area_km2` semantics for DAG bifurcations

**Description.** HFX v0.2.1 §1 (DAG paragraph) requires the producer to document one of: (a) partitioned by flow physics, (b) geometric union, (c) mainstem-routed. GRIT `drainage_area_out` is option (a). The manifest must document this; the spec allows a free-form string in `weight_semantics` for snap and a documented choice in the manifest or accompanying README for `up_area_km2`.

**Mitigation.** The plan declares option (a). The `manifest.json` `auxiliary[].metadata.weight_semantics` carries the value `"drainage_area_km2_partitioned"`. Additionally, ship a one-paragraph `README.md` in the dataset root documenting the DAG `up_area_km2` choice. This is a documentation deliverable, not code.

**Owner:** executor; one-paragraph README alongside `manifest.json`.

### Risk 7 — Outlet correctness on multi-line geometries

**Description.** Some segments/reaches may be `MultiLineString` (e.g. dateline-crossing). "Downstream endpoint of the line" is ambiguous for multi-part lines.

**Mitigation.**
- Assert in `stage_1` that segment/reach geometries are LineString (count any MultiLineString rows; log if non-zero).
- If MultiLineString rows exist (expected to be rare), pick the endpoint of the part containing the downstream-most vertex by flow direction; fail loudly if the heuristic is ambiguous.

**Owner:** executor; verify count in EU smoke build.

### Risk 8 — Reach `up_area_km2` DP correctness and fallback

**Description.** Reach-level `up_area_km2` is computed in Phase 2.5 via a reverse-topological DP over the reach graph. Correctness depends on:

1. The reach graph being correct — which inherits Risk 1 directly. Path A gives a clean explicit graph; Path B derives the graph from segment topology and is fragile if intra-segment flow ordering cannot be reliably reconstructed.
2. The graph being a true DAG (no cycles); a cycle would deadlock Kahn's algorithm.
3. Float precision accumulating across the depth of the network (~10⁴ hops deep for major basins). Accumulate in `float64`, cast to `float32` at the end.

**Mitigation.**

- Hard assert acyclicity after Phase 2.5 substep 2.5a (a cycle anywhere in the reach DAG is a build failure and must be surfaced as `MergeError` with an example cycle, not silently dropped).
- **Fallback:** if Path B is selected AND the executor judges intra-segment ordering unreliable after running it against EU+AS smoke data, set `up_area_km2 = NULL` for **all** reach rows. The catchments schema already permits this (nullable column). Document the fallback in the dataset-root `README.md`. **Never** substitute the parent segment's `drainage_area_out` for missing reach values — within a single segment, the downstream-most reach holds the full segment area while the upstream reach holds only a small fraction, so segment inheritance silently misleads consumers.
- Cross-check: for a sample of well-known basins (e.g. Amazon outlet, Mississippi outlet), the reach outlet `up_area_km2` should approximate the parent segment's `drainage_area_out` within float32 precision when the reach is the segment's downstream-most reach.

**Owner:** executor; gates Stage 6 write of any non-NULL reach `up_area_km2`.

---

## 6. Test strategy

### Tier 0 — Source schema probe
Before any adapter code: run the schema probe (Risk 1) on EU + AS + NA reach GPKGs. Output is a short report enumerating columns and null counts. Time: half a day. Output gates Path A vs Path B in `stage_7_write_graph`.

### Tier 1 — EU-only end-to-end smoke
Smallest non-trivial region (~150 K segments, ~1.9 M reaches per v1 notes). Goals:
- Exercise every stage with non-zero data.
- Confirm validator passes `--strict --sample-pct 100` against the single-region build.
- Memory and timing measurements seed §4 estimates.
- Smoke-test the v1 EU output against v2's segment-level catchments + graph: row counts match, all segment `global_id`s present (via `source_id`), per-row `area_km2` matches v1's `area` within float32 precision, segment graph upstream lists are equivalent.

The EU smoke produces a valid global-shaped HFX dataset that simply has fewer rows than the planetary one; the `bbox` would still be `[-180,-90,180,90]` only in a true global build. For the EU smoke specifically, the manifest test-builds with EU-bbox and `region="europe-smoke"` so the smoke dataset is itself conformant.

### Tier 2 — Two-region build (EU + SP)
SP (south pacific) is small and includes islands — good antipode coverage. Goals:
- Confirm cross-region ID uniqueness check fires correctly (Risk 4).
- Confirm global Hilbert sort orders correctly across distant regions.
- Confirm parent_id resolution survives concatenation.

### Tier 3 — Full 7-region planetary build
Goals:
- End-to-end timing and memory measurements vs §4 estimates.
- Full validator pass (`--strict --sample-pct 100`).
- Sample spot-checks against v1 datasets for the EU region.

### Tier 4 — Bundle publish dry-run
Before publishing to `s3://basin-delineations-public/grit/2.0.0/`:
- `aws s3 sync --dryrun` to confirm payload size matches §4 estimates.
- Manual fetch + manifest re-validation from S3 to confirm relative aux paths resolve.

---

## 7. Stage effort summary

| Stage | Effort | Cumulative |
|---|---|---|
| Tier 0 probe (pre-implementation) | S | 0.5 d |
| `stage_1_inspect_source` (incl. all per-region helpers, distributary classification, outlet derivation, reach→segment join) | **L** | 1.0–1.5 w |
| `stage_2_assign_ids` (global Hilbert + ID re-issue + id map persistence) | **M** | 2–3 d |
| `stage_3_reproject` / `stage_4_make_valid` / `stage_5_hilbert_sort` (assertion stubs) | **S** combined | 0.5 d |
| Phase 1.5 cleanup pass (conditional; small) | S | 0.5 d |
| Phase 2.5 helpers (edge resolution + reverse-topo reach DP; harder under Path B) | **M** (A) or **L** (B if topology pass needed) | 3 d–1 w |
| `stage_6_write_catchments` (streamed merge writer + reach `up_area_km2` join) | **M** | 3–4 d |
| `stage_7_write_graph` (consumes Phase 2.5 edge intermediates) | **S–M** | 2 d |
| `stage_8_write_snap` (two aux files) | **M** | 2–3 d |
| `stage_9_write_manifest` (incl. DAG README) | **S** | 1 d |
| Tier 1 EU smoke | M | 2–3 d |
| Tier 2 EU+SP build | S–M | 1–2 d |
| Tier 3 planetary build + validation | M | 3–4 d (incl. wall-clock waits) |
| Tier 4 publish dry-run | S | 0.5 d |

**Total:** ~5–7 weeks for one executor, assuming no major surprises from the schema probe.

---

## 8. Coordinator resolutions (2026-05-25)

All eight open questions raised in the draft are resolved as listed below. Executor proceeds without further coordination on these points.

| # | Question | Resolution |
|---|---|---|
| 1 | Reach upstream relation | Unknown in advance. **Tier 0 schema probe is the executor's day-1 work item**, gating all stages downstream of `stage_7_write_graph` and the Phase 2.5 DP |
| 2 | DAG `up_area_km2` semantics doc location | Dataset-root `README.md`. Manifest carries no free-form prose field for this |
| 3 | Distributary classification node | **Source-node** check (the plan's current definition) |
| 4 | Parquet compression | **`snappy`** throughout — per-region intermediates, all four final files |
| 5 | Disk budget | Assume **≥300 GB free** on the build workstation. Coordinator will confirm with user before kickoff. If user reports <300 GB, executor runs the conditional Phase 1.5 cleanup pass (delete extracted GPKGs once per-region shards are written) |
| 6 | Outer archive location | `/Users/nicolaslazaro/Desktop/grit-hfx/17435232.zip` confirmed (same as v1) |
| 7 | Adapter directory | **`adapters/grit-v2/`** (side-by-side with v1; v1 archived only after v2 is published and accepted) |
| 8 | Manifest `weight_semantics` string | **`"drainage_area_km2_partitioned"`** confirmed |

---

*This planning document is intentionally silent on file-by-file code structure. The executor should implement `adapters/grit-v2/build_adapter.py` following the nine-stage skeleton from `adapters/_template/build_adapter.py`, with per-region helpers private to that module.*
