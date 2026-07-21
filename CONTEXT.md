# Project domain context

Canonical glossary for HFX domain language, shared by humans and cold agent
sessions. Seeded from `spec/HFX_SPEC.md` and `docs/adapter/index.md`.

## Canonical terms

| Term | Meaning |
|---|---|
| HFX (HydroFabric Exchange) | The canonical compiled data contract a delineation engine consumes. Not a native hydrofabric format; every source fabric must be compiled into HFX by an adapter first. |
| Source hydrofabric / fabric | A source dataset describing a region's river network (e.g. HydroBASINS, GRIT, MERIT-Basins) that an adapter compiles into HFX. Identified in the manifest by `fabric_name` / `fabric_version`. |
| Adapter | A one-way, offline compile step that turns a source hydrofabric into conformant HFX artifacts. Named `adapters/<source>/`; the engine has no fabric-specific logic. |
| Consumer / engine | A program that reads an HFX dataset and delineates watersheds from it (e.g. `pourpoint`). Reads HFX exclusively. |
| HFX dataset | The artifact bundle described by a single `manifest.json`: `catchments.parquet`, `graph.parquet`, `manifest.json`, plus any declared auxiliaries. Covers one contiguous extent of its source fabric. |
| Drainage unit | One polygonal hydrologic unit at one dataset-local level; exactly one row of `catchments.parquet`. |
| Level | A dataset-local resolution tier. `level = 0` is the recommended coarsest tier; higher is finer. Values are dataset-local and carry no cross-dataset meaning. |
| Parent | The containing coarser drainage unit named by `parent_id`. Following `parent_id` reaches a root (null `parent_id`) in finitely many steps; the parent relation is an acyclic forest. |
| Unit outlet | The single EPSG:4326 coordinate where water exits a drainage unit (`outlet_lon` / `outlet_lat`). Distinct from snap features; every unit has exactly one. |
| Headwater | A drainage unit with no same-level upstream neighbors. |
| Topology tree | A strictly convergent network where each unit has at most one downstream same-level neighbor. |
| Topology dag | A network with bifurcations, where one unit may drain to multiple downstream units. Declared as `topology = "dag"`. |
| `up_area_km2` | Inclusive cumulative upstream area for same-level graph traversal. Deliberately non-comparable across levels. `has_up_area` claims column-level coverage, not every row populated. |
| Hilbert sort | Row ordering by Hilbert curve index on centroid coordinates (multi-level: `level ASC, hilbert_index ASC`). A sort key only, never stored as a column; lets a consumer skip row groups. |
| Bounding-box covering | A GeoParquet 1.1 non-nullable `bbox` struct column (`xmin/ymin/xmax/ymax`, `float32`) holding each row's bounds for spatial predicate pushdown. |
| Manifest | `manifest.json`: declares dataset identity, `topology`, row counts, and auxiliary declarations. Describes what the data is, not how an engine uses it. |
| Auxiliary schema | A declared non-core data contract, e.g. `hfx.aux.d8_raster.v1` (D8 rasters) or `hfx.aux.snap.v2` (snap features). Declared under manifest `auxiliary[]` with a relative artifact path. |
| Snap features | Optional `hfx.aux.snap.v2` stream "stems" an engine uses to snap a query point onto the channel network before delineation. Absent → engine falls back to point-in-polygon on `catchments.parquet`. |
| Global / planetary dataset | An HFX dataset covering the full source-fabric extent. `region` omitted; planetary `bbox` is exactly `[-180, -90, 180, 90]`. |
| Partial-fabric dataset | An HFX dataset covering a named subset. `region` set (free-form label); the subset must be closed under upstream traversal at each level. |
| Antimeridian coordinate overshoot | A polygon vertex marginally beyond a domain edge (e.g. lon > +180° from source float precision, au/Fiji ~180.0006). A coordinate-domain conformance issue fixed by the coordinate-domain clamp. Not a true wrap. |
| Coordinate-domain clamp | Tolerance-bounded normalization at each adapter load boundary that snaps a vertex marginally outside `[-180,180]×[-90,90]` onto the domain edge and reports it with a warning (layer, vertex count, source IDs). Overshoot beyond one source grid cell (15 arc-seconds, 0.004166…°) is a fatal error, never clamped. |
| Antimeridian wrap unit | A drainage unit whose true geographic extent crosses the antimeridian (single-unit span > 180°), requiring geometric unwrap/split, not clamping. Zero observed in both global builds (2026-07-16 single-level, 2026-07-20 nested Pfaf 1-12). |
| `format_version` | The SPEC-track version of the HFX data contract (currently `0.3.0`), distinct from the toolkit `hfx`/`hfx-cli` crate version and from source `fabric_version`. |
| Level range | The single contiguous Pfafstetter level range (source numbering 1–12) a HydroBASINS build compiles, default the full `1-12`. Arbitrary non-contiguous level sets are not legal selections. |
| Declarative auxiliary | An auxiliary schema that describes its source data as-is — CRS, encoding, and units are declared in manifest metadata — instead of requiring adapter normalization. `hfx.aux.d8_raster.v2` is the first; core artifacts remain normalized. |
| Header-authoritative property | A physical raster property (dtype, nodata) whose source of truth is the raster file header. The spec constrains its allowed values; the manifest never duplicates it. |

## Aliases to avoid

| Avoid | Use instead | Why |
|---|---|---|
| atom | drainage unit | `atom` is v0.1 legacy naming; v0.2+ renamed the concept to "drainage unit". |
| catchment (as the unit concept) | drainage unit | `catchment` survives only in the fixed filename `catchments.parquet` and consumer prose; the normalized HFX identity term is "drainage unit". |
| graph.arrow | graph.parquet | `graph.arrow` is the legacy v0.1 artifact; not valid in v0.2+. |
| root-level snap.parquet | `hfx.aux.snap.v2` auxiliary | A root `snap.parquet` is legacy v0.2; snap features must be declared as the `hfx.aux.snap.v2` auxiliary. |
| has_rasters / has_snap | auxiliary declaration | These v0.1 manifest flags were removed; presence is declared via `auxiliary[]` entries instead. |

## Relationships

| Concepts | Relationship |
|---|---|
| Adapter → HFX dataset | An adapter compiles a source hydrofabric into an HFX dataset offline; the compile is one-way. |
| Engine → HFX dataset | A consumer reads HFX exclusively and contains no fabric-specific logic. |
| Drainage unit → parent | `parent_id` references a strictly coarser containing unit; the parent relation is an acyclic forest with perfect nesting within validator tolerance. |
| `graph.parquet` → level | Graph edges are same-level upstream adjacency only; cross-level relationships are carried by `parent_id`, never by graph edges. |
| Drainage unit ↔ graph / outlet | Every unit has exactly one graph row and exactly one outlet coordinate. |
| Snap features → drainage unit | Snap stems reference the drainage-unit IDs assigned in `catchments.parquet`. Coverage is per-reach, not per-unit: reach-less units (small coastal/island/headwater sub-basins that thinned HydroRIVERS does not reach) are intentionally left without stems. This is safe because the engine's snap search is spatial and crosses unit boundaries — a query snaps to the nearest qualifying stem within `search_radius` (engine default 1000 m) regardless of which unit that stem belongs to. `NoSnapCandidates` therefore fires only for genuinely off-network points farther than the radius from any channel. |
| HydroRIVERS reach → HydroBASINS drainage unit | A HydroRIVERS reach attaches to the unit whose `id` equals the reach's `HYBAS_L12` attribute (an authoritative HydroSHEDS attribute join, not a spatial join, because both layers are delineated from the same DEM). Reaches whose `HYBAS_L12` is absent from the merged unit set are dropped, never spatially reassigned. |
| Coordinate-domain clamp → antimeridian wrap unit | The clamp only moves vertices within one source grid cell of a domain edge, so a true wrap unit (in-domain vertices, span > 180°) is never altered by it; wrap detection remains the separate span guard. Clamping applies uniformly to all three load boundaries: unit polygons, pour points, and reach linestrings. |
| HydroBASINS Pfaf level ↔ HFX level | Within a build's level range, Pfafstetter level N maps to HFX `level = N − min(range)`; the coarsest selected level is HFX level 0 with null `parent_id` (full range: Pfaf 1 → level 0 … Pfaf 12 → level 11). Snap stems remain keyed to Pfaf 12 only (`references_levels = [12 − min]`); a build whose range excludes Pfaf 12 cannot include snap (`--rivers` is then a fatal error). Engines reach coarser levels via `parent_id`, never via per-level stem duplication. |
| HydroBASINS `parent_id` ↔ `PFAF_ID` prefix | A unit's parent is the next-coarser selected level's unit whose `PFAF_ID` is the child's code with its last digit dropped (the Pfafstetter hierarchy, analogous to USGS HUC nesting) — a per-region attribute join, never spatial containment. An unresolved or ambiguous parent is a fatal build error with no spatial fallback. |
| HydroBASINS pour points → unit outlet (per level) | Every selected level sources outlets from its own HydroBASINS Pour Points file (`hybas_pour_lev<NN>_v1`), joined by `HYBAS_ID`; the join is total per level, and the deterministic nearest-centroid multi-point collapse applies at every level. Coarser outlets are never derived from finest-level descendants. |
| Manifest bbox → bounding-box covering | A partial-fabric (regional) `manifest.bbox` is the **float32** covering union, component-wise `float32(total_bounds)`. The validator's enclosure check recomputes the union from the float32 covering. Round-to-nearest can push a covering leaf one ulp beyond the float64 bounds, so a float64 manifest bbox sits *inside* the union and fails. Writing the float32 cast makes the manifest bbox equal the covering union and enclose it by equality. A global/planetary dataset uses `manifest["bbox"] = list(PLANETARY_BBOX)`, whose fixed value is `[-180.0, -90.0, 180.0, 90.0]` and encloses any in-domain covering. |

## Ambiguities

| Topic | Current interpretation | Resolution condition |
|---|---|---|
| `up_area_km2` at DAG bifurcations | Producer-defined; one of (a) flow-partitioned area, (b) geometric union, or (c) mainstem-routed accumulation, and it must be documented. | Producer states the chosen convention in the manifest or an accompanying README; cross-dataset comparison reconciles conventions first. |
