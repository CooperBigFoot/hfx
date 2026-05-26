# MERIT v2 Adapter Plan — Global HFX v0.2.1 Build

**Status:** Draft — ready for coordinator review. Resolves the MERIT-side build architecture for HFX v0.2.1.

**Date:** 2026-05-26

**Companion specs:** [`spec/HFX_SPEC.md`](../../spec/HFX_SPEC.md) (v0.2.1), [`spec/aux/snap/v1.md`](../../spec/aux/snap/v1.md), [`spec/aux/d8_raster/v1.md`](../../spec/aux/d8_raster/v1.md)

**Companion adapter docs:** [`adapters/_template/build_adapter.py`](../../adapters/_template/build_adapter.py) (nine-stage contract), [`adapters/merit/`](../../adapters/merit/) (v0.1 reference, HFX v0.1), [`adapters/grit-v2/`](../../adapters/grit-v2/) (just-shipped v0.2.1 reference at planetary scale)

---

## 0. Resolved decisions (do not relitigate)

These are inputs to this plan, not topics for re-debate. The brief locks the items in the top half of this table; the lower half resolves the open items from the brief.

### Locked by the planning brief

| # | Decision | Implication |
|---|---|---|
| 1 | **Scope:** full HFX v0.2.1 port of MERIT-Basins + MERIT Hydro rasters; single drainage-unit level (`level=0`) | No segment/reach hierarchy, no `parent_id` chains — all `parent_id` are NULL |
| 2 | **Adapter location:** new `adapters/merit-v2/` (side-by-side with v1; v1 stays untouched) | Mirrors the `grit/` → `grit-v2/` pattern |
| 3 | **Build strategy:** independent per-Pfaf-L2 builds (60 basins after exclusions) → cross-basin preflight → global concat into one HFX dataset | Mirrors GRIT v2; final manifest omits `region`, `bbox = [-180.0, -90.0, 180.0, 90.0]` exact |
| 4 | **Topology:** `tree` (MERIT-Basins has a single downstream pointer per atom) | `stem_role ∈ {mainstem, tributary, unknown}` only — no `distributary` |
| 5 | **`up_area_km2` source:** MERIT-Basins `riv.uparea` field directly, joined on `COMID` | `has_up_area=true`; no reverse-topo DP needed — major simplification over GRIT |
| 6 | **Auxiliary data:** one global `hfx.aux.snap.v1` ("stems") from per-basin `riv_pfaf_<NN>` polylines + sixty `hfx.aux.d8_raster.v1` entries, one per Pfaf basin ("pfaf-11" … "pfaf-91") | 61 aux entries total; D8 rasters stay per-basin to keep individual files manageable |
| 7 | **Publish path:** `s3://basin-delineations-public/merit/0.2.0/` | Adapter writes to a local staging directory; publish is a separate coordinator action |
| 8 | **Format version:** `manifest.json::format_version = "0.2.1"` | Hard cut from v0.1 |

### Resolved here (planner calls)

| # | Decision | Resolution | Why |
|---|---|---|---|
| 9 | **Atom ID source** | `id = COMID` (cast to `int64`); reject `id ≤ 0` and duplicates; preflight gate (`verify_cross_basin.py`) confirms uniqueness across all 60 basins before global concat | MERIT-Basins documents `COMID` as globally unique across Pfaf basins. v1 already trusts this within a basin; the preflight makes that trust empirical at planetary scale. The brief asked us to "decide dense-id re-issue order"; we instead skip the re-issue entirely because COMIDs are already positive, dense-enough, and globally unique. Re-issuing would lose traceability and provide no gain. `source_id` is set to `"merit:<COMID>"` for audit symmetry with GRIT v2 — same value as `id` but namespaced |
| 10 | **Outlet point derivation** | Downstream endpoint of the matching `riv_pfaf_<NN>` LineString, joined to catchment on `COMID`. Same uniform rule for all topology roles (flow-through, terminal sink, headwater) | MERIT-Basins ships 1:1 catchments and reaches with consistent flow direction along the LineString (encoded by `NextDownID`). This is option (b) from the brief — intersection of polygon boundary with riv polyline at downstream end — but cheaper than a geometric intersection because the polyline endpoint coordinate IS the outlet. For terminal sinks the endpoint may lie strictly inside the polygon; this is conformant per v0.2.1 §4 ("outlet lies inside the unit polygon for interior drainage or lakes"). v1 did not write outlet columns; v0.2.1 makes them required, so this is a net-new derivation |
| 11 | **Snap stem classification** | `mainstem` / `tributary` only. Rule: at each confluence (each unique `NextDownID > 0`), the child reach with the largest `uparea` is `mainstem`; all other children at the same confluence are `tributary`. Tie-break by larger `COMID` for determinism. Headwaters (no upstream) and terminal sinks (`NextDownID == 0`) default to `mainstem` if they are sole children of their downstream, else `tributary`. `unknown` reserved for rows where `uparea` is NULL | This is the same algorithm v1 uses for its `is_mainstem` boolean (see `adapters/merit/build_adapter.py` stage_8). v0.2.1's `stem_role` enum is strictly more expressive, so the v1 logic ports directly: `is_mainstem == True` → `"mainstem"`, `is_mainstem == False` → `"tributary"`. v1 verified the count is plausible on pfaf-27 (1,147 mainstem / 826 tributary out of 1,973) |
| 12 | **Snap weight semantics** | `weight = uparea` (km²), cast to `float32`. Documented in `manifest.auxiliary[0].metadata.weight_semantics` as `"drainage_area_km2"` (note: no `_partitioned` suffix because MERIT topology is a tree — no flow partitioning happens at bifurcations because there are no bifurcations) | Mirrors v1 (`weight = rivers["uparea"]`). The HFX spec asks the producer to document interpretation in a free-form string; `"drainage_area_km2"` is unambiguous |
| 13 | **Raster transcoding** | Re-encode to Cloud-Optimized GeoTIFF (tiled 512×512, deflate-compressed, with overviews) **per Pfaf basin** at adapter time. This is mandatory: `hfx.aux.d8_raster.v1` specifies COG as the required format (not "GeoTIFF"). v1 ships per-basin transcoded COGs already; we keep the per-basin layout for v2 instead of stitching to one planetary mosaic | Keeps individual artifacts well under the R2 single-object 5 TB ceiling and below practical engine query sizes (the planetary mosaic in v1 is ~28 GB for flowdir + ~45 GB for flow_acc, see merit v1 README "Known limitations"). 60 per-basin pairs are more streamable than two monolithic planetary rasters. v1's `build_global_rasters.py` is retained as a future-work tool but is NOT in the v2 build path |
| 14 | **Aux file layout** | `aux/snap_stems.parquet` (single global snap file, flat under `aux/`); `aux/d8/pfaf_<NN>/flow_dir.tif` and `aux/d8/pfaf_<NN>/flow_acc.tif` (per-basin D8 directories) | Snap path is flat (`aux/snap_stems.parquet`) for consistency with GRIT v2's `aux/snap_segments.parquet` / `aux/snap_reaches.parquet` — that is the only other v0.2.1 reference dataset and we mirror its convention. D8 sub-path `aux/d8/pfaf_<NN>/` chosen because (a) `hfx.aux.d8_raster.v1` does not prescribe any directory layout — only that artifact paths are relative and do not escape the dataset root, and (b) grouping each `flow_dir`/`flow_acc` pair under one `pfaf_<NN>/` directory makes the 60 aux entries human-readable and easy to inspect with `aws s3 ls`. The 60 manifest aux entries each declare `artifacts.flow_dir = "aux/d8/pfaf_<NN>/flow_dir.tif"` and `artifacts.flow_acc = "aux/d8/pfaf_<NN>/flow_acc.tif"`. There is one snap file because the brief calls for one global stems entry, not per-basin |
| 15 | **Cross-basin merge safety preflight** | New `verify_cross_basin.py` (sibling to `build_adapter.py`). Checks: (a) COMID set uniqueness across all 60 per-basin `cat_pfaf_<NN>_*.shp` + `riv_pfaf_<NN>_*.shp` shapefiles; (b) `NextDownID` does NOT cross basin boundaries (every non-zero `NextDownID` must resolve to a COMID inside the same basin); (c) raster CRS and bounds sanity per basin; (d) shapefile schema drift — column names and types match the reference (pfaf-27) | Mirrors GRIT's `verify_cross_region.py`. Hard-fail on any collision. (b) is the new MERIT-specific check: GRIT had explicit cross-region edge audit, MERIT needs the same on cross-basin `NextDownID` because if any reach points downstream into another Pfaf basin, the global graph cannot be constructed by simple concat and the build strategy is invalidated |
| 16 | **Per-basin build orchestration** | Port v1's `run_missing_basins.py` to `adapters/merit-v2/run_all_basins.py`. Bounded parallelism via `ProcessPoolExecutor`, default `-j 3` (mirrors v1 — pfaf-7/-8/-AS-class basins peak at ~6 GB RSS per worker, so j=3 fits a 24 GB budget). Sequential download phase, parallel build phase. Per-basin timeout 3 h. Calibration run on pfaf-42 (medium size) before the full batch | Higher parallelism (j=10 or j=60) is possible but the workstation RAM budget is the binding constraint, not CPU. v1's j=3 is already validated on the same hardware class. Cross-basin builds are independent so the orchestrator does not need to be smarter |
| 17 | **Planetary bbox handling** | `bbox = [-180.0, -90.0, 180.0, 90.0]` **exact**, `region` OMITTED. No outward padding | v0.2.1 §Deployment Patterns says the boundary values are exact and **must not** be padded. v1 padded by `1e-4`, which is non-conformant under v0.2.1 |
| 18 | **pfaf-35 anti-meridian handling** | EXCLUDE pfaf-35 from the global build. The catchment polygons wrap past 180°E, the mghydro raster is clipped to 180°E, and HFX v0.2.1 EPSG:4326 longitudes are bounded by `[-180, 180]`. The catchment vector geometries themselves can be repaired with `shapely.make_valid` and clipped at the antimeridian, but the per-basin raster cannot be straightforwardly extended past 180°E. We exclude the entire basin for v2 and file a follow-up to revisit when MERIT Hydro 5° source tiles can be re-mosaicked with antimeridian awareness | v1 also excludes pfaf-35 from the global raster mosaic (`PFAF_SKIP = frozenset({35})` in `build_global_rasters.py`). Excluding it from the entire dataset is stricter than v1 (which kept the vector data and dropped only the raster) but produces an internally consistent dataset where every catchment has a paired D8 raster aux. The dataset's documentation will state the exclusion explicitly |
| 19 | **pfaf-87, pfaf-88 (Antarctic) handling** | EXCLUDE both basins from the global build. The mghydro raster rehost returns HTTP 404 for both `flowdir87.tif` and `flowdir88.tif` (and their `accum` equivalents). We do not have D8 rasters for them, and the planning brief mandates one D8 aux entry per basin | Same rationale as pfaf-35: a global dataset where every basin has its D8 pair is simpler than maintaining "some basins have rasters and some don't" branching in engines. Documented in the dataset README |
| 20 | **Final basin count** | 60 basins (61 valid Pfaf-L2 codes minus pfaf-35 plus pfaf-87 plus pfaf-88, but per v1 `VALID_PFAF_CODES` we have 61 codes already excluding 87/88, so we go from 61 → 60 by dropping pfaf-35 only) | v1's `VALID_PFAF_CODES` is the source of truth. The final build set is `VALID_PFAF_CODES - {35} = 60 basins` |
| 21 | **Compression** | `snappy` for all Parquet (`catchments.parquet`, `graph.parquet`, `aux/snap_stems.parquet`); `deflate` for COG TIFs with `PREDICTOR=2` for `flow_dir` (uint8) and `PREDICTOR=3` for `flow_acc` (float32) | Matches the v0.2.1 friction note that drove GRIT v2 to snappy (validator may not be compiled with zstd support). COG predictors match v1's choices |
| 22 | **`source_id` and `level_label`** | `source_id = "merit:<COMID>"` (always populated); `level_label = "merit-basins"` (always populated) | Mirrors GRIT v2's traceability pattern. `level_label` is unnormalised and only consumed for audit |
| 23 | **`fabric_name`, `fabric_version`, `adapter_version`** | `fabric_name = "merit_basins"`, `fabric_version = "v0.7_bugfix1"`, `adapter_version = "0.2.0"` | `fabric_name` drops the per-Pfaf suffix v1 used (`merit_basins_pfaf27`) because this is a single global dataset. `fabric_version` matches the upstream Lin et al. 2019 release tag |
| 24 | **Nine-stage shape** | All nine `def stage_*` functions remain at module level for template fidelity. Per-basin preprocessing lives in pre-stage helpers invoked from `stage_1_inspect_source`. Mirrors GRIT v2's structure | Keeps the `_template/build_adapter.py` contract satisfied without bending the adapter to fit it |

---

## 1. Out of scope (do not touch in this workstream)

- HFX spec changes. v0.2.1 is fixed for this workstream; if MERIT exposes a real spec gap (e.g. a need to attribute the rehost provenance in a structured manifest field) file it as a separate task.
- Re-deriving D8 rasters from the canonical Yamazaki 2019 5° MERIT Hydro tiles. Use the mghydro per-basin rehost as v1 did. The dependency is documented as a known source-fetch fragility.
- Multi-level units. MERIT is single-level (`level=0`).
- DAG support. MERIT is tree.
- Re-publication of the v1 per-Pfaf datasets at `s3://basin-delineations-public/merit/<pfaf>/`. v2 ships at `s3://basin-delineations-public/merit/0.2.0/` as one global dataset.
- Engine snap semantics — the weight→stem_role→distance→id cascade is the engine's contract, not the adapter's.
- A planet-wide D8 mosaic. We keep per-basin D8 because (a) v1 already proves it works for engine consumers, (b) total per-basin bytes are smaller than v1's planetary mosaic, and (c) per-basin aux entries fit the HFX v0.2.1 multi-aux pattern naturally. v1's `build_global_rasters.py` stays available for future use.
- Validator changes (zstd codec, batch-read error flooding, `--strict` ergonomics — track separately as in GRIT v2).
- Re-evaluating the mghydro license posture or seeking an alternate rehost.

---

## 2. Build pipeline architecture

The build runs as four sequential phases mapped onto the nine-stage template. The orchestrator (`_build_dataset`) calls stages in numeric order; the template contract is satisfied because each numbered stage function exists at module scope. Phase 1 work lives in pre-stage helpers invoked from `stage_1_inspect_source`.

```
Phase 0   — Cross-basin preflight (separate CLI subcommand: `preflight`)
Phase 1   — Per-basin preprocess + raster transcode                (stage_1 + pre-stage helpers)
Phase 2   — Global concat, dense identity (COMID stays), Hilbert sort (stages 2–5)
Phase 3   — Write outputs                                           (stages 6–9)
Phase 4   — Validate                                                (separate CLI subcommand: `validate`)
```

### Phase 0 — Cross-basin preflight

`verify_cross_basin.py` runs once before any per-basin build is dispatched. Fast (set ops over ~3M COMIDs total, plus a per-basin schema probe). Outputs:

- `preflight/cross_basin_report.json` — machine-readable: `total_unique_ids`, `id_collisions: []`, `cross_basin_next_down_refs: []`, `schema_drift: []`, `antimeridian_basins: [35]`.
- `preflight/cross_basin_report.txt` — human-readable summary.

Hard-fail conditions:

- Any duplicate COMID across the 60 selected basins.
- Any `NextDownID > 0` resolving to a COMID in a different basin (would invalidate the simple-concat graph strategy).
- Any column name or dtype drift in `cat_pfaf_<NN>_*.shp` or `riv_pfaf_<NN>_*.shp` against the reference (pfaf-27).

Soft warnings (logged, not fatal):

- Catchment/reach COMID set drift within a basin (v1 already tolerates this with a 95% coverage threshold; preflight reports the worst basin).
- Antimeridian basins beyond pfaf-35 (none are expected — pfaf-35 is the only known wrap).

### Phase 1 — Per-basin preprocess + raster transcode (Stage 1)

Repeats for each of 60 basins. Basins are independent and parallelisable. Each basin writes its final COG pair **directly into the dataset's `aux/d8/pfaf_<NN>/` directory** (no intermediate location, no later copy stage) plus three intermediate Parquet shards under `tmp/`:

Per-basin steps (executed via pre-stage helpers called from `stage_1_inspect_source`):

1. **Download (idempotent)** — call out to `rclone`/`curl` for missing inputs. Mirrors v1 `run_missing_basins.py`. Skip if present and valid (gdalinfo round-trip).
2. **Load catchments and rivers** — read `cat_pfaf_<NN>_*.shp` and `riv_pfaf_<NN>_*.shp` with `pyogrio`. Force CRS to EPSG:4326 (shapefiles ship without `.prj`).
3. **Verify cat ↔ riv COMID coverage** — inner-join on COMID; if cat/riv set drift exceeds 5% on either side, hard-fail. v1's tolerance is the same. The 1:1 join is what makes the outlet derivation possible.
4. **Compute outlets** — for each catchment row, take the downstream endpoint of the matching reach LineString (the last coordinate, since MERIT-Basins encodes flow direction along the line). Reuse a `_downstream_endpoint(geom)` helper (vendor from GRIT v2). For `MultiLineString` rows (rare; flag a warning), pick the part whose endpoint has the lowest flow_acc-equivalent ranking; if ambiguous, hard-fail. The brief asks us to "decide" the outlet rule; this codifies the choice and the multi-part fallback.
5. **Classify stem roles** — at each unique confluence node (each unique `NextDownID > 0` value), the child with maximum `uparea` is `mainstem`; others are `tributary`. Headwaters with no upstream are `mainstem` if they are sole children of their downstream, else `tributary`. Terminal sinks (`NextDownID == 0`) are `mainstem`. Rows with NULL `uparea` get `unknown`. Same algorithm as v1, ported from boolean to enum string.
6. **`shapely.make_valid` sweep** — repair catchment polygons. Reuse `_coerce_to_polygonal` to keep `Polygon`/`MultiPolygon` only. Log invalid count before/after.
7. **Compute per-row bbox** (`bbox_minx/miny/maxx/maxy`).
8. **Transcode rasters directly to dataset path** — read `flowdir<NN>.tif` and `accum<NN>.tif` from mghydro, crop to catchment bbox + 10-px pad, remap NoData (`flow_dir`: source byte 247 / MERIT int8 -9 → 255; `flow_acc`: source 0 → -1.0), cast to `uint8` / `float32`, write as COG with `BLOCKSIZE=512`, `COMPRESS=DEFLATE`, `OVERVIEWS=AUTO`, `BIGTIFF=YES`. Validate with `rio_cogeo.cog_validate`. **Output paths are the final dataset paths**: `<dataset_root>/aux/d8/pfaf_<NN>/flow_dir.tif` and `<dataset_root>/aux/d8/pfaf_<NN>/flow_acc.tif`. The orchestrator creates `<dataset_root>/aux/d8/pfaf_<NN>/` before this step. There is no intermediate `<basin_workdir>` COG location and no later copy/move/symlink stage — Stage 8 will never touch these files. Rationale: symlinks break `aws s3 sync` (either errors or silently dereferences with stale provenance), and a separate copy stage doubles the disk footprint for no benefit.
9. **Per-basin Parquet intermediates** (snappy-compressed, schema fixed but row-group sizing left loose; these stay under `tmp/` until Phase 3 consumes them):
   - `tmp/pfaf_<NN>/units.parquet` — one row per catchment: `id (=COMID)`, `area_km2 (=unitarea)`, `up_area_km2 (=uparea)`, `outlet_lon`, `outlet_lat`, `bbox_*`, `geometry_wkb`, `source_id`, `level_label`. **No `parent_id` column** because all rows have null parent.
   - `tmp/pfaf_<NN>/edges.parquet` — `id`, `upstream_ids: list<int64>` derived by inverting `NextDownID`. Headwaters yield `[]`. Same algorithm as v1 stage_7.
   - `tmp/pfaf_<NN>/snap.parquet` — `id (=COMID, placeholder)`, `unit_id (=COMID)`, `weight (=uparea, float32)`, `stem_role`, `bbox_*`, `geometry_wkb` (line WKB), plus `unit_bbox_minx/miny/maxx/maxy` — the catchment's bbox carried alongside so that Stage 8 can sort snap rows by the catchment-centroid Hilbert key without re-joining. One row per reach (1:1 with catchment).
10. **Per-basin logging** — row counts, peak RSS, wall-clock, final COG file sizes (from `aux/d8/pfaf_<NN>/`).

`stage_1_inspect_source` returns a `SourceData` container holding paths to the per-basin `tmp/` intermediates. The COGs are not in `SourceData` because they are already in their final dataset location; stages 2–8 do not touch them.

**Implication for failure recovery:** if Phase 1 fails partway, the partially-built dataset's `aux/d8/` directory is partial. The orchestrator must idempotently skip basins whose `aux/d8/pfaf_<NN>/{flow_dir,flow_acc}.tif` already exist and pass `cog_validate`. This mirrors v1's idempotency posture for raw downloads.

### Phase 1.5 — Conditional disk-cleanup pass (safety valve; expected to be a no-op)

**Run only if `shutil.disk_usage(root).free` falls below 100 GB at the end of Phase 1.** Because Phase 1 writes COGs directly to the dataset's final `aux/d8/pfaf_<NN>/` location (no intermediate COG staging), the transient disk footprint is materially smaller than it was in the previous plan revision. On the workstation budget (≥250 GB free at kickoff) this trigger is expected NOT to fire, and Phase 1.5 should be a no-op. If it does fire (e.g. user ran with <250 GB), the orchestrator deletes the per-basin extracted source vectors and source rasters (already consumed in Phase 1) to recover ~50 GB. Skip this pass entirely when ≥100 GB free at the threshold.

### Phase 2 — Global concat + Hilbert sort (Stages 2–5)

The Phase 2 work fits into the existing stage names without renaming. Because MERIT-Basins COMIDs are already globally unique positive int64 values (verified in Phase 0), **no dense re-issue happens** — we keep COMIDs as HFX `id`s directly. This is a simplification over GRIT v2.

Stage-by-stage:

- **`stage_2_assign_ids(source: SourceData) -> Path`** — concatenate all per-basin `tmp/pfaf_<NN>/units.parquet` shards into one in-memory Arrow table of `(id=COMID, bbox_minx, bbox_miny, bbox_maxx, bbox_maxy)` rows. Compute `hilbert_index` against the planetary total bounds `(-180, -90, 180, 90)`. Sort by `(hilbert_index ASC, id ASC)` for determinism. Persist as `tmp/id_map.parquet` — columns `id`, `hilbert_index` — for downstream stages. Assert: no duplicate COMID, no `id ≤ 0`, no NULL.
- **`stage_3_reproject(source: SourceData) -> None`** — assert source CRS was EPSG:4326 throughout Phase 1 (Phase 1 forces it at load time; this is a sanity check on the persisted shards). No-op.
- **`stage_4_make_valid(source: SourceData) -> None`** — assertion sweep on a sample of rows; re-running `make_valid` at ~3 M rows would be wasteful. Logs sample counts.
- **`stage_5_hilbert_sort(source: SourceData) -> None`** — load `tmp/id_map.parquet`, confirm rows are in `(hilbert_index, id)` order. Assertion only.

After Phase 2 completes, `tmp/id_map.parquet` holds the global Hilbert-sorted ID assignments. Phase 1 per-basin shards remain on disk; Phase 3 writers stream them in Hilbert order via the sort-merge index.

### Phase 3 — Write outputs (Stages 6–9)

All writes use the global Hilbert sort index from Phase 2 to interleave per-basin shards in the correct order. Each writer streams row groups of 4096–8192 rows.

- **`stage_6_write_catchments(source: SourceData, out_dir: Path) -> None`**
  - Output: `catchments.parquet`.
  - Schema: `id int64 NN`, `level int16 NN` (= 0 for every row), `parent_id int64 nullable` (= NULL for every row), `area_km2 float32 NN`, `up_area_km2 float32 nullable` (= `uparea`, will be non-null for every row but column is nullable per spec), `outlet_lon float64 NN`, `outlet_lat float64 NN`, `bbox_* float32 NN`, `geometry binary NN` (WKB Polygon|MultiPolygon), `source_id string nullable` (always populated `"merit:<COMID>"`), `level_label string nullable` (always populated `"merit-basins"`).
  - Compression: snappy. Row groups 4096–8192. GeoParquet 1.1 metadata attached.
  - Streams from per-basin `tmp/pfaf_<NN>/units.parquet` shards in `(hilbert_index, id)` order via the sort index.
  - Validate: assert row count matches the sum of per-basin counts, run `validate_geoparquet` post-write.
- **`stage_7_write_graph(source: SourceData, out_dir: Path) -> None`**
  - Output: `graph.parquet` (NOT `graph.arrow` — that's v0.1).
  - Schema (v0.2.1 with bbox columns): `id int64 NN`, `level int16 NN` (= 0), `upstream_ids list<int64> NN`, `bbox_* float32 NN`.
  - Reads `tmp/pfaf_<NN>/edges.parquet` from each basin and concatenates. Since `NextDownID` does not cross basins (Phase 0 guarantee), no cross-basin edge resolution is needed.
  - Rows sorted by `(hilbert_index ASC, id ASC)` — index reused from `catchments.parquet`. `bbox_*` columns copied from the referenced unit (joined on `id`).
  - Row groups 4096–8192; bbox stats written; compression snappy.
  - Validate cycles: per-basin acyclicity is already enforced in Phase 1; assert per-basin and overall via a single iterative DFS over the merged edges.
- **`stage_8_write_snap(source: SourceData, out_dir: Path) -> None`** — snap-only. The 60 D8 COG pairs are already in their final location under `aux/d8/pfaf_<NN>/` (written in Phase 1 step 8). This stage does not touch them.
  - Output: `aux/snap_stems.parquet` (single global file, flat path under `aux/` for consistency with GRIT v2's `aux/snap_segments.parquet` / `aux/snap_reaches.parquet`).
  - Schema: `id int64 NN`, `unit_id int64 NN`, `weight float32 NN`, `stem_role string nullable` (one of `mainstem|tributary|unknown`), `bbox_* float32 nullable`, `geometry binary NN` (WKB LineString).
  - Concatenates from per-basin `tmp/pfaf_<NN>/snap.parquet` shards.
  - **Sort key: catchment-centroid Hilbert index, not snap-centroid Hilbert index.** Each `tmp/pfaf_<NN>/snap.parquet` row carries the matching catchment's bbox (`unit_bbox_*`, added in Phase 1 step 9). The catchment centroid is computed from those bbox columns, then a Hilbert index against planetary bounds. Sort all snap rows by `(catchment_hilbert_index ASC, unit_id ASC)`. Because MERIT snap is 1:1 with catchments (one reach per catchment via COMID), this sort means row N of `snap_stems.parquet` pairs with row N of `catchments.parquet` — paired window reads in engines become a sequential scan over both files in lockstep. Sorting by snap geometry's own centroid would produce a slightly different order due to line-vs-polygon centroid drift and break that lockstep.
  - `id` re-issued dense int64 starting at 1, assigned in the catchment-Hilbert-sorted order. `unit_id = COMID` (the catchment's id; join to `catchments.parquet` by `unit_id` is identity since `id = COMID`).
  - Row groups 4096–8192; bbox stats written; compression snappy.
- **`stage_9_write_manifest(source: SourceData, out_dir: Path) -> None`** writes `manifest.json`:
  ```json
  {
    "format_version": "0.2.1",
    "fabric_name": "merit_basins",
    "fabric_version": "v0.7_bugfix1",
    "crs": "EPSG:4326",
    "has_up_area": true,
    "topology": "tree",
    "bbox": [-180.0, -90.0, 180.0, 90.0],
    "unit_count": <row count of catchments.parquet>,
    "created_at": "<RFC 3339 UTC at write time>",
    "adapter_version": "0.2.0",
    "auxiliary": [
      {
        "schema": "hfx.aux.snap.v1",
        "artifacts": {"snap": "aux/snap_stems.parquet"},
        "metadata": {
          "name": "stems",
          "description": "MERIT-Basins reach centerlines (riv_pfaf_<NN>), 1:1 with catchments. weight = uparea (km^2). stem_role derived by largest-uparea descent at each confluence.",
          "references_levels": [0],
          "weight_semantics": "drainage_area_km2"
        }
      },
      {
        "schema": "hfx.aux.d8_raster.v1",
        "artifacts": {
          "flow_dir": "aux/d8/pfaf_11/flow_dir.tif",
          "flow_acc": "aux/d8/pfaf_11/flow_acc.tif"
        },
        "metadata": {"flow_dir_encoding": "esri", "name": "pfaf-11"}
      },
      // ... 59 more d8_raster.v1 entries, one per basin ...
    ]
  }
  ```
  - `region` is omitted (global dataset).
  - `bbox` is the planetary boundary exactly; no outward padding.
  - `unit_count` reads back the row count of the written `catchments.parquet` to avoid drift.
  - **Note on the `name` field in d8 metadata:** `spec/aux/d8_raster/v1.md` does not list `name` as required, but it allows additive metadata fields. We add `name: "pfaf-<NN>"` so that the 60 d8 entries are distinguishable by humans when scanning the manifest. The validator does not enforce this field, but it is conformant.
  - Also write a dataset-root `README.md`. The README must include the **`## Coverage` section verbatim as specified below** plus additional sections covering the per-basin layout of `aux/d8/`, the mghydro rehost dependency (with Yamazaki 2019 + Heberger 2023 citations), the COMID-as-`id` choice, and the stem_role classification rule.

    **Required `## Coverage` section (executor copies this verbatim into `<dataset_root>/README.md`):**

    ```
    ## Coverage

    This dataset covers 60 of the 63 MERIT-Basins Pfaf-L2 basins. The
    following are deliberately excluded:

    - pfaf-87, pfaf-88 (Antarctic): no MERIT Hydro raster coverage at
      source (mghydro returns 404 for flowdir87/88.tif). MERIT-Basins
      itself has no Antarctic rivers.
    - pfaf-35 (New Zealand / Pacific antimeridian): the catchment
      polygons wrap past 180°E and the per-basin D8 raster is clipped
      at the antimeridian. Excluded to preserve the invariant that
      every catchment has a paired D8 raster aux.

    The manifest declares planetary bbox [-180, -90, 180, 90] for
    catalog-discoverability; actual data coverage is the 60 included
    basins listed above.
    ```

    This is non-negotiable because the manifest's planetary bbox would otherwise lead consumers to assume full global coverage. The exclusion list MUST be the first content section after the title.

### Phase 4 — Validate

Run `cargo run -p hfx-validator -- <dataset> --format text --strict --sample-pct 100` and capture the report. Also run `--format json` to save `validator-report.json` for the bundle.

Phase 4 is invoked via the adapter's `validate` subcommand (mirrors v1).

---

## 3. Stage-by-stage breakdown

The table below ties stage names to phases and read/transform/write responsibilities, with effort estimates.

| Stage | Phase | Read | Transform | Write | Effort |
|---|---|---|---|---|---|
| `verify_cross_basin.py` (preflight) | 0 | All 60 cat/riv shapefiles (COMID + NextDownID columns only) | Set unions; cross-basin NextDownID resolution; schema-drift check | `preflight/cross_basin_report.{json,txt}` | **S** |
| `stage_1_inspect_source` | 1 | 60 × {cat.shp, riv.shp, flowdir.tif, accum.tif} | Per-basin: load+CRS-force; cat↔riv COMID join; outlet from riv endpoint; stem_role classification; make_valid; bbox; raster transcode to COG written **directly to the final dataset path** | 60 × `tmp/pfaf_<NN>/{units,edges,snap}.parquet` + 60 × `<dataset_root>/aux/d8/pfaf_<NN>/{flow_dir,flow_acc}.tif` (final location) | **L** |
| `stage_2_assign_ids` | 2 | All per-basin unit shards (centroid + COMID columns only) | Compute global Hilbert index; sort | `tmp/id_map.parquet` | **M** |
| `stage_3_reproject` | 2 | Sample row metadata | Assert EPSG:4326 | (none) | **S** |
| `stage_4_make_valid` | 2 | Sample of unit shards | Assert sample geometries pass `is_valid` | (none) | **S** |
| `stage_5_hilbert_sort` | 2 | `tmp/id_map.parquet` | Assert ordering | (none) | **S** |
| `stage_6_write_catchments` | 3 | All unit shards + id_map | Stream-merge in Hilbert order | `catchments.parquet` | **M** |
| `stage_7_write_graph` | 3 | All edge shards + catchment bboxes | Concat (no cross-basin edges per Phase 0 guarantee); attach bboxes; sort by hilbert_index | `graph.parquet` | **S–M** |
| `stage_8_write_snap` | 3 | All snap shards (with `unit_bbox_*` carried from Phase 1) | Stream-merge snap rows in catchment-centroid Hilbert order; re-issue snap dense IDs in that order | `aux/snap_stems.parquet` (snap-only; the 60 COG pairs are already in their final location from Phase 1) | **S–M** |
| `stage_9_write_manifest` | 3 | `catchments.parquet` row count + raster paths | Build manifest dict with 61 aux entries; write README | `manifest.json`, `README.md` | **S** |

Effort key: S = ≤1 day, M = 2–4 days, L = ≥1 week.

---

## 4. Memory and I/O profile (per stage, order-of-magnitude)

Estimates assume the workstation budget (32–64 GB RAM, ~250 GB free disk).

| Stage | Peak RAM | Disk write | Disk read | Notes |
|---|---|---|---|---|
| Phase 0 preflight | < 2 GB (COMID set ops over ~3 M ids; per-basin column reads only) | < 10 MB (preflight reports) | All 60 × {cat.shp, riv.shp} headers/COMID columns ~2 GB | Single pass, no per-basin geometry I/O |
| `stage_1_inspect_source` (per basin, j=3 parallel) | 4–8 GB per worker (largest basins are Amazon-class pfaf-62, Mississippi pfaf-74; raster crop windows can hit ~2 GB of float32 cells) | ~1 GB Parquet intermediates per basin (under `tmp/`) + ~1.5 GB COG pair per basin (written **directly to `<dataset_root>/aux/d8/pfaf_<NN>/`**, no staging copy). Across 60 basins: ~60 GB tmp + ~90 GB dataset-resident COGs | Sum of raw vectors + rasters ~50 GB on disk read | j=3 fits ~24 GB RAM budget. Wall-clock per basin: 5–15 min typical, 30–90 min for Amazon-class. Total Phase 1 with j=3: 4–10 h |
| `stage_2_assign_ids` | 2–4 GB (centroid+id columns held globally: 3 M rows × 5 × 8 bytes ≈ 0.12 GB; Hilbert int64 ≈ 0.024 GB; sort buffers 2–3×) | < 100 MB (`id_map.parquet`) | All per-basin centroid+id columns ~0.5 GB | In-memory sort is fine — 3 M rows is small |
| `stage_3_reproject` | < 100 MB | 0 | tiny | No-op |
| `stage_4_make_valid` | < 100 MB | 0 | small sample | No-op |
| `stage_5_hilbert_sort` | < 1 GB | 0 | `id_map.parquet` | Assertion only |
| `stage_6_write_catchments` | 4–8 GB (row-group buffering, geometry WKB serialisation) | ~20–40 GB (`catchments.parquet`) | Sum of unit shards ~20–40 GB | Streams row group by row group; column-oriented |
| `stage_7_write_graph` | 2–4 GB (in-memory edge lists) | 0.5–1 GB (`graph.parquet`; no geometry) | Edge intermediates ~0.5 GB + catchment bboxes ~0.2 GB | Cheapest write |
| `stage_8_write_snap` | 4–8 GB (line WKB buffering for the global snap file) | ~5–10 GB snap | Snap shards ~5–10 GB | Snap-only — the 60 D8 COG pairs are already in their final dataset location from Phase 1, this stage does not touch them |
| `stage_9_write_manifest` | < 100 MB | < 50 KB | Metadata only | One JSON write + one README |
| **Phase 4 validate** | 4–10 GB (validator-internal, 60 raster aux entries each get COG headers loaded) | small report JSON | Reads full dataset including 60 COG pairs | Wall-clock: 30–60 min estimated; see §5 risk 4 |

Total **final dataset disk footprint** ≈ 115–140 GB:
- `catchments.parquet`: 20–40 GB
- `graph.parquet`: 0.5–1 GB
- `aux/snap_stems.parquet`: 5–10 GB
- `aux/d8/pfaf_<NN>/`: 60 pairs × ~1.5 GB avg = ~90 GB (range 0.2 GB for small basins like pfaf-27, ~3 GB for Amazon-class)

Total **transient disk footprint during build** ≈ 150–200 GB:
- Raw inputs (vectors + rasters) ~50 GB (deletable in Phase 1.5 if pressed)
- Per-basin Parquet intermediates ~60 GB under `tmp/` (consumed by stages 6/7/8, can be deleted post-Phase 3)
- Final dataset under `<dataset_root>/` ~140 GB (which includes the 60 COG pairs already in place after Phase 1, accumulating throughout the build rather than appearing all at once at the end)

There is no separate ~100 GB "per-basin COG staging" line item because Phase 1 writes COGs directly to the final dataset path — `<dataset_root>/aux/d8/pfaf_<NN>/` and the workspace `tmp/` share the same filesystem and the COGs never get duplicated.

The brief calls for ≥200 GB free disk. We recommend **≥250 GB** to comfortably hold both source and output simultaneously, with Phase 1.5 cleanup of source vectors+rasters as a relief valve if free drops below 100 GB mid-build. (Previous plan revision recommended ≥300 GB because it double-counted the COG staging; the direct-write architecture shaves ~100 GB off the transient budget.)

---

## 5. Risk register

### Risk 1 — COMID collisions across Pfaf basins (BLOCKER; resolved by Phase 0)

**Description.** Build correctness on `catchments.parquet` (id uniqueness) and `graph.parquet` (edge resolution) depends on COMID being globally unique across all 60 basins. MERIT-Basins documents this, but it has not been empirically verified at the union of 60 basins on this hardware.

**Mitigation.** Phase 0 `verify_cross_basin.py` is the gate. Hard-fail on any collision. Mirrors GRIT v2's `verify_cross_region.py` posture. Fast (set ops over ~3 M ints).

**Fallback if collisions are found.** Re-issue dense IDs in `(hilbert_index, source_id)` order, just like GRIT v2 does. This would add an `id_map.parquet`-style mapping and slow Phase 2 by ~1 hour but is mechanically straightforward. Document the choice in the README. The executor should escalate to coordinator before making this change because it changes the contract documented in §0.

**Owner:** executor; Phase 0, day 1.

### Risk 2 — Cross-basin `NextDownID` references

**Description.** If any reach in basin A has `NextDownID` pointing to a reach in basin B, the global graph cannot be built by simple per-basin concat — we would need to either drop those edges (lossy) or do a global graph resolution pass like GRIT v2's per-region edge map.

**Mitigation.** Phase 0 preflight check (b) — every non-zero `NextDownID` must resolve to a COMID inside the same basin. Hard-fail on violation.

**Fallback if violations are found.** Build a global COMID → basin index in Phase 1, then resolve cross-basin edges in Phase 2.5 (new phase). Adds ~1–2 days of work. Escalate to coordinator.

**Owner:** executor; Phase 0, day 1.

### Risk 3 — Shapefile `.prj` missing causes silent CRS mis-assignment

**Description.** MERIT-Basins shapefiles ship without `.prj`. v1 forces CRS to EPSG:4326 at load time. This is correct (Lin et al. 2019 explicitly publishes in WGS84) but the act of forcing CRS could mask a real CRS bug if the source ever ships rotated coordinates.

**Mitigation.** Phase 0 preflight check (c) — for each basin, read the catchment shapefile, force CRS to EPSG:4326, then assert that `total_bounds` falls within the expected Pfaf-L2 extent (the brief's planetary `[-180, -90, 180, 90]` envelope is too loose — use the per-basin centroid lookup from the Pfafstetter Level-2 documentation as a sanity check, or simply assert `[-180, -90] <= total_bounds <= [180, 90]` after the CRS force, which would catch e.g. a UTM zone slip).

Also verify the matching raster for each basin has EPSG:4326 in its CRS metadata.

**Owner:** executor; preflight + stage 1.

### Risk 4 — 61 aux entries in manifest exceeds validator scale

**Description.** v0.2.1 manifests in the wild typically have 1–2 aux entries (GRIT v2 has 2 snap entries). 61 aux entries (1 snap + 60 d8) is an order of magnitude beyond what the validator has been exercised against. Possible issues: linear scans, JSON pretty-printing, repeated COG-header reads.

**Mitigation.**
- Pre-test on a small subset: build a 3-basin smoke (pfaf-27 + pfaf-42 + pfaf-91, picked for varying size) with 3 d8 aux entries, run the validator, measure wall-clock and verify all 3 d8 entries are validated.
- If validator wall-clock exceeds 60 min on the full planetary build, file a validator follow-up but do NOT block the v2 publish — the dataset is conformant regardless of validator speed.

**Owner:** executor; Tier 2 smoke first, then planetary.

### Risk 5 — Total D8 raster bytes hit a filesystem or S3 limit

**Description.** ~90 GB of total COG bytes across 60 directories. Individual COGs are 0.2–3 GB, well within the R2 5 TB single-object limit. The aggregate dataset directory bytes will be ~150–200 GB. Risk is bounded; document for the publish step.

**Mitigation.**
- Confirm with the user before kickoff that the R2 bucket has at least 300 GB free for staging (the staging area receives the dataset before `aws s3 sync` flips the public path).
- Per-basin COG sizes: spot-check at smoke build and extrapolate to planetary using the v1 per-basin reference dataset sizes.

**Owner:** executor reports per-basin sizes after Phase 1; coordinator verifies bucket capacity before publish.

### Risk 6 — mghydro rehost is non-canonical and may disappear

**Description.** The MERIT Hydro flow_dir / flow_acc rasters are served by mghydro.com as a convenience rehost of Yamazaki 2019's 5° tiles, basin-merged. We do not control the host. If mghydro.com goes down mid-build or if individual TIFs change byte-by-byte, the build is impacted.

**Mitigation.**
- Cache all 60 (× 2 = 120) TIFs locally before Phase 1 starts. v1's `run_missing_basins.py download` subcommand already does this idempotently.
- Document the rehost dependency in the dataset README with a citation block (Yamazaki 2019 primary source, Heberger 2023 mghydro derivation).
- File a future-work issue to re-derive D8 from the Yamazaki 2019 5° canonical source. Out of scope for v2.

**Owner:** executor for caching; coordinator for the future-work follow-up.

### Risk 7 — `uparea` semantics mismatch with HFX `up_area_km2`

**Description.** HFX `up_area_km2` is defined as inclusive cumulative upstream drainage area in km² at the unit's level. MERIT-Basins `uparea` is documented as upstream drainage area in km². The brief flags this as a risk because units and definition need verification.

**Mitigation.**
- Read the MERIT-Basins paper (Lin et al. 2019) and confirm `uparea` is in km², not km² × 10³ or m² or upstream-cell-count.
- Spot-check at smoke build: pfaf-27 (Iceland) has a known largest atom (Hvítá outlet, drainage ≈ 287.5 km² per v1's pyshed round-trip). Confirm the corresponding `uparea` row matches within a few percent (pixel-grid quantisation).
- If units mismatch, document the conversion and apply at load time.

**Owner:** executor; verify on pfaf-27 smoke.

### Risk 8 — Outlet derivation produces points outside the polygon

**Description.** The chosen outlet rule (downstream endpoint of the matching riv LineString) may produce a coordinate slightly outside the catchment polygon due to coordinate precision differences between the catchment polygon and the reach line. The validator's outlet topology-role check (v0.2.1 §4) may flag this.

**Mitigation.**
- v0.2.1 §Reference Validator Coverage says outlet topology-role position checks "are reserved for follow-up validator releases" — they are conformance requirements but may not be enforced now. So this risk is most likely silent in the v0.2.1 validator.
- Belt-and-suspenders: in Phase 1, assert `polygon.distance(Point(outlet_lon, outlet_lat)) < 1e-6` (1 µdeg ≈ 11 cm). If the distance exceeds the threshold, snap the outlet to the nearest boundary point on the polygon. Log the count.
- Track the count of snapped outlets per basin and surface in the dataset README.

**Owner:** executor; verify on pfaf-27 smoke.

### Risk 9 — Validator wall-clock at ~3 M rows + 60 raster aux entries

**Description.** GRIT v2 validator runs took up to an hour for 20 M rows. MERIT at ~3 M rows is smaller, but the 60 COG aux entries add per-aux overhead the validator hasn't seen at this scale.

**Mitigation.**
- Smoke-test with `--sample-pct 1` first to confirm structural validity.
- Run the full validate in the background while preparing the publish staging.
- Capture wall-clock metrics and report back to coordinator.

**Owner:** executor; Tier 3 measurement.

### Risk 10 — COG transcode wall-clock dominates Phase 1

**Description.** Per-basin COG transcode (with overviews) can take 5–15 min per basin for medium basins, 30–60 min for Amazon-class. Across 60 basins at j=3 that's 2–10 hours just for raster work, possibly exceeding the per-basin timeout.

**Mitigation.**
- Per-basin timeout 3 h (v1's default). Calibration run on pfaf-42 sets expectations.
- Consider `--skip-overviews` in development builds; production publish must include overviews per spec (COG implies overviews).

**Owner:** executor; calibration step.

### Risk 11 — Disk fill mid-build leaves dataset in inconsistent state

**Description.** 150–200 GB transient footprint (post direct-write COG architecture) risks filling the workstation disk on machines with under 250 GB free.

**Mitigation.**
- Hard precondition: orchestrator checks free disk before Phase 1 starts (`>= 250 GB`) and refuses to launch otherwise.
- Phase 1.5 conditional cleanup of source vectors+rasters if free drops below 100 GB at the Phase 1 → Phase 2 boundary.
- Per-basin checkpointing: if a build fails partway, the orchestrator skips basins whose `aux/d8/pfaf_<NN>/{flow_dir,flow_acc}.tif` already exist and pass `cog_validate` (mirrors v1's `--retry-failed` posture and is the natural consequence of writing COGs to final paths).

**Owner:** executor; coordinator confirms with user before kickoff.

---

## 6. Test strategy

### Tier 0 — Cross-basin preflight
Run `verify_cross_basin.py` against all 60 basins. Outputs a short JSON+TXT report. Half a day. Output gates the rest of the build.

### Tier 1 — Single-basin smoke (pfaf-27, Iceland)
Smallest basin (~1,973 atoms in v1's count). Goals:
- Exercise every stage with non-zero data.
- Confirm validator passes `--strict --sample-pct 100` against a **proper single-basin HFX v0.2.1 dataset** (not a fake "wrapped global"). The Tier 1 smoke produces a real partial-fabric dataset per v0.2.1 §Deployment Patterns:
  - `region = "pfaf-27"` (free-form label per spec — kebab-case, matches the basin code)
  - `bbox` = actual pfaf-27 polygon-union extent (computed from `cat_pfaf_27_*.shp` total bounds, NOT the planetary box). For Iceland this is roughly `[-25, 63, -13, 67]`; use whatever the source data produces.
  - One `hfx.aux.d8_raster.v1` aux entry named `"pfaf-27"` at `aux/d8/pfaf_27/{flow_dir,flow_acc}.tif`.
  - One `hfx.aux.snap.v1` aux entry named `"stems"` at `aux/snap_stems.parquet` covering just pfaf-27.
  - `unit_count` = pfaf-27 row count only.
  This is the conformant way to ship a partial dataset and exercises every code path the planetary build does, minus the cross-basin concat. The orchestrator's single-basin mode must produce this shape (not a planetary stub).
- Memory and timing seed §4 estimates.
- Spot-check `uparea` vs the known Hvítá ≈ 287.5 km² target (v1 verified this via pyshed round-trip).

### Tier 2 — Multi-basin smoke (3 basins: pfaf-27 small, pfaf-42 medium, pfaf-91 small)
Goals:
- Exercise cross-basin concat with non-trivial Hilbert sort across distant regions.
- Manifest for Tier 2 is also a partial-fabric dataset: `region = "smoke-3basin"` (or similar free-form label), `bbox` = actual union of the three polygons' extents, three `hfx.aux.d8_raster.v1` entries + one snap entry.
- Confirm 3 d8 aux entries validate correctly.
- Confirm the cross-basin COMID uniqueness preflight fires correctly.
- Measure end-to-end wall-clock and extrapolate to planetary.

### Tier 3 — Full 60-basin planetary build
Goals:
- End-to-end timing and memory measurements vs §4 estimates.
- Full validator pass (`--strict --sample-pct 100`).
- Sample spot-checks against v1 per-Pfaf datasets: row counts match per basin (modulo pfaf-35 exclusion), `id` (=COMID) sets match, `uparea` round-trips through `up_area_km2` exactly, `is_mainstem` (v1 boolean) maps to `stem_role` (v2 enum) consistently.

### Tier 4 — Publish dry-run
Before publishing to `s3://basin-delineations-public/merit/0.2.0/`:
- `aws s3 sync --dryrun` to confirm payload size matches §4 estimates.
- Manual fetch + manifest re-validation from S3 to confirm relative aux paths (especially the `aux/d8/pfaf_<NN>/`) resolve.
- Spot-check that engines consuming `hfx.aux.d8_raster.v1` can iterate over all 60 entries.

---

## 7. Stage effort summary

| Stage | Effort | Cumulative |
|---|---|---|
| Tier 0 preflight (incl. `verify_cross_basin.py`) | S | 0.5 d |
| `stage_1_inspect_source` (incl. per-basin orchestration, outlet derivation, stem_role classification, COG transcode) | **L** | 1.0–1.5 w |
| `stage_2_assign_ids` (global Hilbert + persistence; no dense re-issue) | **S–M** | 1 d |
| `stage_3` / `stage_4` / `stage_5` (assertion stubs) | **S** combined | 0.5 d |
| Phase 1.5 cleanup pass (conditional) | S | 0.5 d |
| `stage_6_write_catchments` (streamed merge writer) | **M** | 2–3 d |
| `stage_7_write_graph` (concat from per-basin edges + bbox attach) | **S** | 1 d |
| `stage_8_write_snap` (snap concat only; COGs already in place from Phase 1) | **S–M** | 1–2 d |
| `stage_9_write_manifest` (61 aux entries + README) | **S–M** | 1–2 d |
| Tier 1 pfaf-27 smoke | M | 1–2 d |
| Tier 2 3-basin smoke | S–M | 1–2 d |
| Tier 3 planetary build + validation | M | 2–4 d (incl. wall-clock waits) |
| Tier 4 publish dry-run | S | 0.5 d |

**Total:** ~3.5–5 weeks for one executor, assuming Phase 0 preflight passes cleanly. If preflight finds collisions or cross-basin edges (Risks 1, 2) add 1 week for fallback work.

---

## 8. Coordinator resolutions

| # | Question | Resolution |
|---|---|---|
| 1 | pfaf-81 raster bounds reported at half-pixel west of -180 in Phase 0 — false positive from over-strict bounds check, or real antimeridian/CRS issue? | False positive. MERIT Hydro uses cell-centered grids where cell EDGES extend 1/2-pixel outside the cell-center extent. -180.000417 = -180 - 1/2400 deg = exactly half a 3-arcsec pixel west. Preflight bounds check loosened to tolerate +/-1 pixel (1/1200 deg) to absorb the canonical grid offset while still catching real CRS slips. Raster passes through to Phase 1 unmodified. |

---

## 9. Open questions for coordinator

The planner has made defensible calls for every decision in the brief, but the following items deserve a coordinator second-look before the executor starts. None are blocking — proceeding with the planner's calls is safe — but the coordinator may want to override.

1. **No dense COMID re-issue (Decision #9).** GRIT v2 re-issues dense IDs in Hilbert order for traceability and to forbid `id=0`. We instead keep COMIDs verbatim because (a) MERIT-Basins COMIDs are already positive int64, (b) the value of preserving source IDs for audit is high, and (c) re-issuing buys us nothing because the COMIDs are already globally unique (verified in Phase 0). If the coordinator wants symmetry with GRIT v2, we should re-issue and store `source_id = "merit:<COMID>"` separately. The §3 / §4 effort estimates assume we do NOT re-issue.

2. **Per-basin D8 vs planetary mosaic (Decision #13).** The brief specifies "sixty `hfx.aux.d8_raster.v1` entries, one per Pfaf basin". This is what we plan. An alternative would be a single planet-wide D8 mosaic (which v1's `build_global_rasters.py` produces). The mosaic approach has fewer aux entries (1 vs 60) but produces two ~30 GB COGs that are harder to stream selectively. The brief's choice is the right one for engines that delineate within a single basin at a time; flag if a multi-basin engine consumer needs the mosaic.

3. **pfaf-35 / pfaf-87 / pfaf-88 exclusions (Decisions #18, #19).** v1 keeps pfaf-35 in the vector dataset (just excludes from the mosaic). v2 excludes it from the dataset entirely so that every catchment has a D8 pair. This is a stricter cut than v1. If the coordinator wants pfaf-35 catchments included without paired D8 raster, we can declare 60 d8 aux entries (no pfaf-35) and include pfaf-35 catchments in `catchments.parquet`. This breaks the implicit invariant that every catchment is covered by some d8 aux entry, and engines would need to handle the gap. Recommend keeping the exclusion as planned.

4. **Workstation disk budget (Risk 11).** The plan now assumes ≥250 GB free (down from ≥300 GB in the previous plan revision, because the direct-write COG architecture removed a ~100 GB staging copy). The brief said ≥200 GB; we're slightly above. Coordinator confirm with user before kickoff. If 100–250 GB, the executor relies on Phase 1.5 cleanup; if <100 GB the build cannot run safely.

5. **mghydro rehost trust (Risk 6).** No mitigation in v2 beyond what v1 already does (local cache + idempotent download). If the coordinator wants higher provenance assurance, file a separate workstream to re-derive D8 from Yamazaki 2019 5° tiles. Out of scope for v2.

6. **Manifest `weight_semantics` string (Decision #12).** We chose `"drainage_area_km2"`. GRIT v2 used `"drainage_area_km2_partitioned"` (DAG semantics). MERIT is tree, so there's no partitioning happening — the unqualified string is correct, but the executor should not mechanically copy GRIT v2's value.

---

*This planning document is intentionally silent on file-by-file code structure. The executor should implement `adapters/merit-v2/build_adapter.py` (plus `verify_cross_basin.py` and `run_all_basins.py` helpers) following the nine-stage skeleton from `adapters/_template/build_adapter.py`, with per-basin helpers private to that module. Reuse from `adapters/merit/` (raster transcoding, snap stem classification, outlet handling helpers) and `adapters/grit-v2/` (the just-shipped global concat pattern) is encouraged.*
