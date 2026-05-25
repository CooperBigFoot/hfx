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
| 8 | **`up_area_km2`:** segments use GRIT `drainage_area_out` (km²) directly. **Reaches do NOT simply inherit the parent segment value**; reach `up_area_km2` is computed via a per-segment chain anchor: the outlet reach inherits `segment.drainage_area_out`, then upstream reaches walk backward subtracting local `area_km2` (see Phase 2.5). `has_up_area=true` | DAG semantics: partitioned by flow physics at bifurcations (option (a) in HFX spec §1) — documented in dataset-root `README.md` |
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
Phase 2.5 — Resolve graph edges + per-segment reach up_area_km2 (helpers consumed by stages 6 & 7)
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
   - Join to segments via `reach.segment_id == segment.global_id` to inherit `stem_role` only (used both in catchments and in the reach snap layer) and the segment's `drainage_area_out` for use as the reach **snap weight**. **Do NOT simply inherit `up_area_km2`** — reach `up_area_km2` is computed in Phase 2.5 by anchoring each segment's outlet reach to `segment.drainage_area_out` and walking upstream within that segment.
5. **Reach catchments:** read `global_id`, `area`, geometry; `make_valid`; compute bbox; inner-join to reaches on `global_id`. The reach catchment's own `area` (km²) becomes the reach row's `area_km2` and is the per-row subtraction term for the Phase 2.5 per-segment chain walk.
6. Per-region writes (snappy-compressed Parquet, schema fixed but row-group sizing left raw — these are intermediates, not final HFX artifacts):
   - `tmp/<region>/segments.parquet` — one row per segment catchment with `source_global_id`, `area_km2`, `up_area_km2` (= GRIT `drainage_area_out`), `stem_role`, `outlet_lon`, `outlet_lat`, `bbox_*`, `geometry_wkb`, `upstream_source_global_ids: list<int64>`.
   - `tmp/<region>/reaches.parquet` — `source_global_id`, `parent_source_global_id` (== `reach.segment_id`), `area_km2` (reach catchment area), `stem_role` (inherited from parent segment), `outlet_lon`, `outlet_lat`, `bbox_*`, `geometry_wkb`, `upstream_source_global_ids: list<int64>`, `downstream_source_global_ids: list<int64>` (from the source columns under Path A, or empty lists under Path B — filled in during Phase 2.5). **No `up_area_km2` column on this shard** — that field is materialised by the per-segment chain anchor in Phase 2.5 and joined in at Stage 6.
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

### Phase 2.5 — Resolve graph edges + per-segment reach `up_area_km2`

Phase 2.5 is implemented as pre-stage helpers consumed by both `stage_6_write_catchments` (which needs `up_area_km2` per reach row) and `stage_7_write_graph` (which writes the resolved edge lists). It is not a separate template stage — the executor invokes the helpers from the orchestrator between Phase 2 and Phase 3.

Two substeps:

**Substep 2.5a — Resolve graph edges per level.**

- Level 0 (segments): for each segment row, map its `upstream_source_global_ids` list through `(level=0, source_global_id) → new_id` from `tmp/id_map.parquet` to produce `upstream_ids: list<int64>` in HFX ID space. Headwaters yield `[]`. Persist as `tmp/edges_l0.parquet` columns `id`, `upstream_ids`.
- Level 1 (reaches): the source of upstream IDs depends on the Tier 0 schema-probe outcome.
  - **Path A (GRIT exposes an explicit reach upstream column):** map the source reach upstream IDs through `(level=1, source_global_id) → new_id`.
  - **Path B (derive from segment topology):** for each reach, determine ordering along its parent segment's flow path; intra-segment upstream = the immediately upstream reach within the same segment; at the segment's upstream end, the upstream reaches are the outlet reaches of segments listed in the parent segment's `upstream_line_ids`.
- Persist resolved edges as `tmp/edges_l1.parquet`.

**Substep 2.5b — Per-segment chain anchor to compute reach `up_area_km2`.**

- Input: reach `area_km2`, `parent_source_global_id`, `upstream_source_global_ids`, `downstream_source_global_ids` from `tmp/<region>/reaches.parquet` + segment `up_area_km2` from `tmp/<region>/segments.parquet`.
- GRIT invariant: bifurcations and confluences happen only at segment-boundary nodes, so reaches inside one segment form a linear chain. The adapter must not run a global additive DP over the reach DAG, because split/rejoin paths double-count shared upstream reaches and compute geometric-union semantics rather than GRIT's partitioned-flow semantics.
- Algorithm, for each segment `S`:
  1. Collect all reaches where `reach.segment_id == S.global_id`.
  2. Identify the outlet reach: the reach whose `downstream_line_ids` point to reaches outside `S`, or whose `downstream_line_ids` is empty for terminal segments.
  3. Walk `upstream_line_ids` from outlet to headwater, keeping only upstream reaches inside `S`. The walk must cover exactly all reaches in `S`.
  4. Assign:
     ```
     up_area_km2[outlet] = S.drainage_area_out
     up_area_km2[r_i]    = up_area_km2[r_{i-1}] - area_km2[r_{i-1}]
     ```
     where `r_i` is one step upstream of `r_{i-1}`.
  5. Assert emitted values are finite and non-negative. Log near-zero values as a data smell, but do not fail solely on near-zero.
- Segments with one reach are trivial: the reach gets `S.drainage_area_out`.
- Segments whose `drainage_area_out` is null emit NULL `up_area_km2` for every reach in that segment.
- Persist as `tmp/reach_up_area.parquet` with columns `id`, `up_area_km2 (float32 nullable)`.

**Fallback.** If a segment's chain walk fails (multiple outlet candidates, no outlet candidate, multiple in-segment upstream candidates at any step, partial coverage, or negative/non-finite results), set `up_area_km2 = NULL` for **that segment's reaches** and document the fallback count/examples in the dataset-root `README.md` under the DAG semantics note. **Do NOT silently inherit the segment `drainage_area_out` for every reach** — that mis-states the per-reach upstream area and silently misleads downstream consumers.

The chain-anchor pass is O(total reach count), independent per segment, and parallelisable.

### Phase 3 — Write outputs (Stages 6–9)

All writes use the global Hilbert sort index built in Phase 2 to interleave regional shards in the correct order. Each writer streams row groups of 4096–8192 rows.

- **`stage_6_write_catchments(...)`**
  - Output: `catchments.parquet`.
  - Schema: `id int64 NN`, `level int16 NN`, `parent_id int64 nullable`, `area_km2 float32 NN`, `up_area_km2 float32 nullable` (populated for both levels — segments from GRIT `drainage_area_out`, reaches from the Phase 2.5 per-segment chain anchor; the column is nullable because segment-local fallback may NULL affected reach rows), `outlet_lon float64 NN`, `outlet_lat float64 NN`, `bbox_* float32 NN`, `geometry binary NN` (WKB Polygon|MultiPolygon), plus optional `source_id string nullable` (always populated), `level_label string nullable` (always populated).
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
  - `aux/snap_reaches.parquet`: same schema, `unit_id` = `(level=1, source_global_id) → new_id`, `weight` and `stem_role` inherited from parent segment (already attached in Phase 1). The reach **snap** weight inherits from the parent segment because the snap layer expresses drainage *dominance* of the stem-line the snap targets — distinct from the per-unit `up_area_km2` column, which is computed via the Phase 2.5 per-segment chain anchor.
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
| **Phase 2.5 helpers** (edge resolution + per-segment reach up-area chain anchor) — invoked from the orchestrator between stages 5 and 6 | 2.5 | Segment/reach upstream/downstream lists + id_map + reach `area_km2` + segment `drainage_area_out` | Resolve upstream lists per level via id_map; within each segment identify the outlet reach, walk upstream, and subtract local reach areas from the segment outlet anchor; fallback to NULL on segment chain-walk failure | `tmp/edges_l0.parquet`, `tmp/edges_l1.parquet`, `tmp/reach_up_area.parquet` | **M** |
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
| **Phase 2.5 helpers** (edge resolution + reach chain anchor) | 4–8 GB (reach grouping by parent segment plus edge lists) | 1–3 GB (`tmp/edges_l*.parquet`, `tmp/reach_up_area.parquet`) | id_map + reach upstream/downstream lists + segment drainage areas | O(total reach count); per-segment work is independent and parallelisable |
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

### Risk 8 — Reach `up_area_km2` chain-anchor correctness and fallback

**Description.** Reach-level `up_area_km2` is computed in Phase 2.5 via a per-segment chain anchor. Correctness depends on:

1. The reach graph being correct — which inherits Risk 1 directly. Path A gives explicit upstream/downstream reach lists; Path B derives them from segment topology.
2. GRIT's segment-internal linearity invariant holding: bifurcations and confluences happen only at segment-boundary nodes, so reaches inside one segment form a single chain.
3. Segment `drainage_area_out` being populated for consumed segments.

**Mitigation.**

- Hard assert same-level graph acyclicity after Phase 2.5 substep 2.5a for `graph.parquet` correctness.
- For reach upstream areas, process each parent segment independently: identify one outlet reach, walk upstream within the segment, assign the outlet to `segment.drainage_area_out`, and subtract each downstream reach's local `area_km2` while walking upstream.
- **Fallback:** if any segment's chain walk fails (multiple outlet candidates, partial coverage, multiple in-segment upstream candidates, null segment drainage, or negative/non-finite results), set `up_area_km2 = NULL` for that segment's reaches. The catchments schema already permits this (nullable column). Document fallback counts/examples in the dataset-root `README.md`. **Never** substitute the parent segment's `drainage_area_out` for every reach in the segment — within a single segment, the downstream-most reach holds the full segment area while the upstream reach should hold only a smaller upstream remainder.
- Cross-check: sampled outlet reaches should equal their parent segment's `drainage_area_out`; sampled reaches should satisfy `area_km2 <= up_area_km2 <= parent segment drainage_area_out` when non-null.

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
| Phase 2.5 helpers (edge resolution + per-segment reach chain anchor; harder under Path B) | **M** (A) or **L** (B if topology pass needed) | 3 d–1 w |
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
| 1 | Reach upstream relation | Unknown in advance. **Tier 0 schema probe is the executor's day-1 work item**, gating all stages downstream of `stage_7_write_graph` and the Phase 2.5 chain anchor |
| 2 | DAG `up_area_km2` semantics doc location | Dataset-root `README.md`. Manifest carries no free-form prose field for this |
| 3 | Distributary classification node | **Source-node** check (the plan's current definition) |
| 4 | Parquet compression | **`snappy`** throughout — per-region intermediates, all four final files |
| 5 | Disk budget | Assume **≥300 GB free** on the build workstation. Coordinator will confirm with user before kickoff. If user reports <300 GB, executor runs the conditional Phase 1.5 cleanup pass (delete extracted GPKGs once per-region shards are written) |
| 6 | Outer archive location | `/Users/nicolaslazaro/Desktop/grit-hfx/17435232.zip` confirmed (same as v1) |
| 7 | Adapter directory | **`adapters/grit-v2/`** (side-by-side with v1; v1 archived only after v2 is published and accepted) |
| 8 | Manifest `weight_semantics` string | **`"drainage_area_km2_partitioned"`** confirmed |

---

*This planning document is intentionally silent on file-by-file code structure. The executor should implement `adapters/grit-v2/build_adapter.py` following the nine-stage skeleton from `adapters/_template/build_adapter.py`, with per-region helpers private to that module.*
