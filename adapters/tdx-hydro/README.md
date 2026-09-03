# TDX-Hydro to HFX Adapter

This adapter compiles one pristine NGA TDX-Hydro processing basin into an HFX v0.3.0 dataset. It writes `catchments.parquet`, `graph.parquet`, `manifest.json`, and an `hfx.aux.snap.v2` artifact at `aux/snap_stems.parquet`. The output contains one HFX level, uses `topology = "tree"`, and carries inclusive upstream area.

## Inputs and source fabric

`build` requires the two NGA GeoPackages for the same TDX-Hydro processing basin:

- `--basins` points to the basin polygons GeoPackage. Its single vector layer must contain `streamID` and Polygon or MultiPolygon geometry.
- `--streamnet` points to the stream network GeoPackage. Its single vector layer must contain `LINKNO`, `DSLINKNO`, `DSContArea`, and LineString geometry.

Each path must name an existing `.gpkg` file containing exactly one non-empty vector layer with one active geometry column and a declared CRS. The adapter transforms a non-EPSG:4326 input to EPSG:4326 before compiling it. Topology identifiers must be integral. `basins.streamID` and `streamnet.LINKNO` must be non-negative; `streamnet.DSLINKNO` may also use the native `-1` terminal sentinel. `DSContArea` must be finite and positive.

TDX-Hydro source data may encode a zero-length reach as a two-coordinate LineString whose two two-dimensional coordinate tuples are exactly identical. The adapter accepts that one otherwise-invalid geometry shape as a documented source-data convention. It preserves both source vertices and does not repair, extend, perturb, or replace the geometry. Null, empty, Z, wrong-type, and every other invalid geometry remain fatal; in particular, a LineString with three identical coordinates is not admitted by this exception.

The source fabric is the pristine NGA distribution, not the modified GEOGLOWS v2 derivative. `fabric_name` is `tdx_hydro`; `--fabric-version` supplies the NGA product version recorded in the manifest. See [the pristine-NGA source decision](../../docs/decisions/2026-07-21-tdx-hydro-pristine-nga-source.md). NGA's observed download pattern is `https://earth-info.nga.mil/php/download.php?file=<processing-basin-id>-<product>-gpkg`, where `<product>` is `basins` or `streamnet`.

## Manifest facts

| Property | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | tdx_hydro |
| `fabric_version` | supplied by `--fabric-version` |
| `adapter_version` | 0.1.0 |
| `topology` | tree |
| CRS | EPSG:4326 |
| HFX levels | level 0 only |
| `parent_id` | null for every unit |
| `has_up_area` | true |
| `region` | supplied by `--processing-basin-id` |

## Global LINKNO identifiers

Native `LINKNO` values are unique only within a TDX-Hydro processing basin. Every emitted drainage-unit ID uses the GEOGLOWS Global LINKNO convention:

```text
id = LINKNO + header_number * 10_000_000
```

The transform applies consistently to catchment `id`, graph edge endpoints, and snap `unit_id`. The native `-1` downstream sentinel is never transformed.

The processing-basin-to-header-number crosswalk is vendored at [`data/tdx_header_numbers.json`](data/tdx_header_numbers.json) and is loaded locally at build time. Builds never refresh or fetch it. [`data/README.md`](data/README.md) records its GEOGLOWS source repository, pinned source commit, retrieval date, source URL, and SHA-256 digest. The crosswalk is a numbering aid only; the geometry, topology, and attributes remain pristine NGA TDX-Hydro. See [the Global LINKNO decision](../../docs/decisions/2026-07-21-tdx-hydro-global-linkno-unit-ids.md).

## Drainage units and graph topology

The adapter emits level-0 units only. A unit exists exactly when a basin polygon joins by attribute equality:

```text
basins.streamID = streamnet.LINKNO
```

Every basin polygon must join to one streamnet row. Missing or duplicate identities are fatal. There is no spatial fallback. Polygon-less streamnet links do not become drainage units, and `parent_id` is null for every emitted unit.

The native same-level relation is `LINKNO -> DSLINKNO`. For each polygon-bearing link, the adapter follows `DSLINKNO` through zero or more polygon-less links until it reaches the first polygon-bearing downstream link or `-1`. The former creates one contracted same-level edge; the latter makes the unit a root. The report distinguishes `contracted_edge_count`, `contracted_root_count`, and the total `contracted_link_traversal_count`.

Degenerate reaches do not change unit selection or contraction. A polygon-bearing degenerate reach still emits its drainage unit. A polygon-less degenerate reach remains a native topology pass-through and is dropped from emitted units and snap stems exactly like any other polygon-less reach. Polygon-bearing status comes only from the attribute join; there is no spatial fallback or per-basin allowlist.

Tree topology is enforced, not inferred from the manifest declaration. The adapter rejects native self-links, missing downstream targets, cycles, duplicate `LINKNO` rows, and duplicate rows that imply a bifurcation. It then verifies that every emitted unit has at most one downstream edge and that the contracted emitted relation is acyclic. A violation is a fatal build error.

Catchment and graph bbox columns are identical float32 coverings rounded outward from each catchment's exact float64 geometry bounds. Each per-basin manifest bbox is the component-wise union of the emitted catchment covering columns, with the selected float32 values widened exactly to float64 for JSON. The manifest therefore equals the validator's catchment-column union while enclosing the source geometries.

## Reach orientation and outlets

For every healthy native reach whose `DSLINKNO` names a successor, the adapter compares both endpoints of the reach with the distinct endpoint coordinates of that immediate native successor. Orientation ambiguity is decided from the reach's own endpoints: if matches use exactly one reach endpoint, that exact source coordinate is the proven downstream endpoint. The successor endpoint need not be unique. TDX-Hydro contains many nonzero reaches shorter than twice the default `0.001`-degree coincidence tolerance, approximately 222 m, so one endpoint of a predecessor can legitimately coincide within tolerance with both endpoints of its short successor. This successor-side multi-match is accepted and reported; it does not make the predecessor's orientation ambiguous.

No current-endpoint match is a fatal non-coincidence error. If matches use both endpoints of a healthy current reach, its direction is not discriminated by coincidence. For a non-root reach whose current endpoint separation is at most `3 * --endpoint-tolerance`, the adapter may preserve TDX/TauDEM native vertex order and use the final source vertex as its downstream endpoint without changing geometry. This reported native-order trust convention is allowed only when current `DSContArea` is strictly less than successor `DSContArea` and source endpoint 1 matches the already-resolved upstream endpoint of the healthy non-root successor. Tied or contradictory area, an unresolved successor endpoint side, or separation beyond that bound is fatal. The adapter does not snap an outlet to a successor, repair geometry, or use a spatial fallback.

A degenerate successor has one distinct coordinate even though its preserved LineString stores that coordinate twice. A sole current-endpoint match to that point still discriminates a healthy current reach and is counted as proven by endpoint coincidence. Duplicate storage of the successor point is handled only by the degenerate-reach diagnostics and is not counted as a short-successor resolution.

**TRUST ASSUMPTION: a TDX-Hydro exactly degenerate reach has no directional axis, so its orientation is vacuous and is never counted as proven. Its single distinct source coordinate is treated as its downstream endpoint. A non-root degenerate reach must still coincide with its immediate native successor; non-coincidence remains fatal. This is a source-convention trust assumption, not spatial inference or an orientation proof.** A polygon-bearing degenerate reach uses that coordinate as its unit outlet. A degenerate root uses the same rule whether or not it has predecessors and is not counted as predecessor-orientation-proven, short-successor-resolved, reach-side-near-degenerate-resolved, or native-order trusted isolated.

For a healthy native root with predecessors, uniquely identified predecessor-to-root endpoint matches identify the root's upstream endpoint. The opposite root endpoint is therefore the downstream endpoint. Definite predecessor matches to both root endpoints are fatal. A predecessor that matches both endpoints of a short root contributes no definite endpoint fact and therefore does not create a spurious conflict. If another predecessor supplies one definite endpoint, that fact proves the root orientation. If every predecessor match is endpoint-indeterminate, the root remains limited to the `2 * --endpoint-tolerance` near-degenerate bound; after strictly downstream-increasing `DSContArea` is confirmed, the adapter preserves native order, uses the root's final source vertex, and reports bounded near-degenerate trust instead of calling the root predecessor-proven.

**TRUST ASSUMPTION: a healthy isolated native root has no successor and no predecessor topology, so its orientation cannot be proven by endpoint coincidence. For those reaches only, the adapter trusts TauDEM/TDX native vertex order and treats the final LineString vertex as the outlet. This is a trust assumption, never an orientation proof.** The report keeps trusted healthy isolated roots separate from endpoint-coincidence-proven links, predecessor-orientation-proven roots, and bounded near-degenerate resolutions, and separately identifies the trusted isolated roots that bear polygons.

Each drainage unit's `outlet_lon` and `outlet_lat` are the downstream endpoint resolved for its own native polygon-bearing reach. They are not polygon centroids, spatial joins, repaired geometries, or the downstream endpoint of a synthetic geometry assembled across contracted links. The proof is performed against the immediate native `DSLINKNO` successor even when graph contraction later skips polygon-less links.

Pilot processing basin `7020000010` exposed 6,178 successor-side multi-match pairs at the default tolerance. Of these, 296 use exactly degenerate successors already covered by the zero-length convention and 5,882 use nonzero successors no longer than twice the tolerance. Only 85 pairs also match both endpoints of the current reach. These are pilot magnitudes, not production constants. Planetary build #107 must re-check the dedicated counts and native LINKNO lists independently for every processing basin. A non-root reach-side case beyond the wider DSContArea-guarded bound is fatal, and an indeterminate-root case remains fatal beyond `2 * --endpoint-tolerance`.

## Inclusive upstream area and coordinate normalization

`up_area_km2` is the polygon-bearing reach's source-authoritative inclusive `DSContArea`, converted to square kilometres only according to the empirically selected source unit. The adapter never rescales `DSContArea` toward catchment-polygon area, substitutes a polygon-derived value, or fabricates an area attribute. Snap `weight` uses the same raw converted source value.

The empirical check verifies the `DSContArea` **unit**, not equality between raster drainage area and vector catchment area. At build time the adapter computes WGS84 geodesic area for every basin polygon, accumulates those polygon areas through the complete native `LINKNO -> DSLINKNO` tree, and compares polygon-bearing samples against both raw-unit hypotheses. It rejects an exact numerical tie and also fails unless the losing candidate's aggregate absolute relative error is at least 1,000 times the winning candidate's error. After decisive unit selection, a separate 100 percent aggregate absolute relative-divergence ceiling remains as a generous gross fabric/compile sanity check; it is not the unit-discrimination threshold. The selected unit, both candidate errors, and signed aggregate, absolute aggregate, and maximum per-reach relative-divergence diagnostics are recorded in the report. Positive signed divergence means raw `DSContArea` exceeds the accumulated polygon reference.

For pilot processing basin `7020000010`, TauDEM raster-derived `DSContArea` systematically exceeds the inclusive sum of geodesic vector catchment-polygon areas: signed aggregate divergence is about 12.1 percent and absolute aggregate divergence is about 12.9 percent. The effect is already present at headwaters, with about +8.3 percent signed and 10.3 percent absolute divergence, so it is intrinsic raster-versus-vector source-fabric divergence rather than an error introduced by topology accumulation. Per-reach absolute divergence in the pilot has p50 about 10.6 percent, p90 about 19.9 percent, p99 about 23.7 percent, and maximum about 42.4 percent. The m2 hypothesis nevertheless wins decisively (`0.1289` error versus `1.12e6` for km2), so the empirical unit check runs and passes while preserving raw source areas.

Planetary build #107 must re-check and retain these diagnostics independently for every processing basin. Divergence magnitude is basin-specific; a basin whose aggregate divergence approaches the 100 percent sanity ceiling, or whose maximum per-reach diagnostic is unusually large, deserves source and compile scrutiny, never automatic rescaling.

Both source layers follow the repository's coordinate-domain clamp discipline at the normalized EPSG:4326 load boundary. Longitude must lie in `[-180, 180]` and latitude in `[-90, 90]`. A marginal overshoot of at most one 0.4-arcsecond TDX source cell, `0.4 / 3600` degrees, is clamped to the exact domain edge before identifiers, areas, outlets, ordering, or bounds are derived. A larger overshoot is a fatal source-contract error. Every clamp reports the altered vertex count and sorted native IDs, and a nonzero clamp emits a warning.

The one-cell envelope is grounded in NGA's description of TDX-Hydro as a nominal 12 m hydrography suite derived from TanDEM-X input and in the DLR TanDEM-X DEM Product Specification mapping the standard global 12 m-at-the-equator product to 0.4-arcsecond spacing:

- NGA TDX-Hydro product page: https://earth-info.nga.mil/index.php?action=geosciences&dir=geosci
- DLR TanDEM-X DEM Product Specification, table 1: https://tandemx-science.dlr.de/pdfs/TD-GS-PS-0021_DEM-Product-Specification_v3.2.pdf
- Repository clamp discipline: [adapter coordinate-domain clamp decision](../../docs/decisions/2026-07-17-adapter-coordinate-domain-clamp.md)

## Snap stems

The adapter always writes `aux/snap_stems.parquet` and declares it as `hfx.aux.snap.v2` with `metadata.name = "stems"` and `references_levels = [0]`. Only polygon-bearing native reaches are included. Polygon-less reaches are excluded without a spatial fallback, and the snap path reuses the topology model's `polygonless_dropped_reach_count`; it does not compute a second drop definition.

Each selected stem preserves its normalized source LineString. Its `unit_id` is the corresponding Global LINKNO. Stems are ordered by centroid Hilbert distance over the fixed EPSG:4326 world domain `[-180, -90, 180, 90]`, with `unit_id` as the deterministic tie-break, and receive sequential `id` values from 1 through N after ordering.

Snap bbox columns are float32 coverings rounded outward from exact float64 geometry bounds. For a polygon-bearing degenerate reach, the snap row preserves the original two identical vertices. The writer then expands each dimension that was degenerate in the exact bounds by the existing float32 metadata epsilon so the covering remains ordered; that bbox operation does not alter the WKB geometry and is not a geometry repair.

`weight` is the same empirically normalized inclusive `DSContArea` in km2 used by `up_area_km2`, stored as float32. The manifest records this exact producer definition:

```text
weight_semantics = "Drainage-area weight equals inclusive DSContArea in km2; higher values indicate stronger drainage dominance."
```

`stem_role` is null for every row because this adapter does not fabricate mainstem roles. The artifact uses EPSG:4326 LineString WKB and a GeoParquet 1.1 float32 `bbox` covering.

## Diagnostics report and warnings

`--report` is required and must point outside `--out`; a report path equal to or inside the dataset root is rejected before compilation. The dataset and report are staged and published together. The JSON report contains:

- `build_identity.processing_basin_id`
- `build_identity.fabric_name`
- `build_identity.fabric_version`
- `build_identity.created_at`
- `build_identity.adapter_version`
- `build_identity.dataset_root`
- `diagnostics.ingestion.basins_clamp.altered_vertex_count`
- `diagnostics.ingestion.basins_clamp.altered_native_ids`
- `diagnostics.ingestion.streamnet_clamp.altered_vertex_count`
- `diagnostics.ingestion.streamnet_clamp.altered_native_ids`
- `diagnostics.ingestion.dscontarea.source_unit`
- `diagnostics.ingestion.dscontarea.checked_polygon_bearing_link_count`
- `diagnostics.ingestion.dscontarea.geodesic_upstream_area_sum_m2`
- `diagnostics.ingestion.dscontarea.dscontarea_sum_raw`
- `diagnostics.ingestion.dscontarea.m2_relative_error`
- `diagnostics.ingestion.dscontarea.km2_relative_error`
- `diagnostics.ingestion.dscontarea.selected_relative_error`
- `diagnostics.ingestion.dscontarea.signed_aggregate_relative_divergence`
- `diagnostics.ingestion.dscontarea.absolute_aggregate_relative_divergence`
- `diagnostics.ingestion.dscontarea.max_absolute_relative_divergence`
- `diagnostics.streamnet.polygon_bearing_link_count`
- `diagnostics.streamnet.polygonless_dropped_reach_count`
- `diagnostics.streamnet.degenerate_reach_count`
- `diagnostics.streamnet.degenerate_reach_native_linknos`
- `diagnostics.streamnet.degenerate_polygon_bearing_reach_count`
- `diagnostics.streamnet.degenerate_polygon_bearing_reach_native_linknos`
- `diagnostics.streamnet.degenerate_polygonless_reach_count`
- `diagnostics.streamnet.degenerate_polygonless_reach_native_linknos`
- `diagnostics.streamnet.short_successor_resolved_reach_count`
- `diagnostics.streamnet.short_successor_resolved_reach_native_linknos`
- `diagnostics.streamnet.reach_side_near_degenerate_resolved_reach_count`
- `diagnostics.streamnet.reach_side_near_degenerate_resolved_reach_native_linknos`
- `diagnostics.streamnet.root_count`
- `diagnostics.streamnet.contracted_edge_count`
- `diagnostics.streamnet.contracted_root_count`
- `diagnostics.streamnet.contracted_link_traversal_count`
- `diagnostics.streamnet.endpoint_coincidence_proven_link_count`
- `diagnostics.streamnet.predecessor_orientation_proven_root_count`
- `diagnostics.streamnet.trusted_orientation_isolated_root_count`
- `diagnostics.streamnet.trusted_orientation_isolated_root_native_linknos`
- `diagnostics.streamnet.trusted_orientation_polygon_bearing_isolated_root_count`
- `diagnostics.streamnet.trusted_orientation_polygon_bearing_isolated_root_native_linknos`
- `diagnostics.streamnet.orientation_tolerance`
- `diagnostics.memory.target_bytes`
- `diagnostics.memory.measurement_available`
- `diagnostics.memory.unavailable_reason`
- `diagnostics.memory.observed_peak_rss_bytes`
- `diagnostics.memory.high_water_rss_bytes`
- `diagnostics.memory.sample_interval_ms`
- `diagnostics.memory.measurement_method`
- `diagnostics.memory.peak_scratch_bytes`
- `diagnostics.memory.scratch_high_water_bytes`
- `diagnostics.memory.scratch_measurement_available`
- `diagnostics.memory.scratch_unavailable_reason`
- `diagnostics.memory.basins_rows`
- `diagnostics.memory.streamnet_rows`
- `diagnostics.memory.basins_geometry_count`
- `diagnostics.memory.streamnet_geometry_count`
- `diagnostics.memory.basins_coordinate_count`
- `diagnostics.memory.streamnet_coordinate_count`
- `diagnostics.memory.basins_input_bytes`
- `diagnostics.memory.streamnet_input_bytes`
- `diagnostics.memory.selected_dtypes.native_id`
- `diagnostics.memory.selected_dtypes.downstream_native_id`
- `diagnostics.memory.selected_dtypes.global_id`
- `diagnostics.memory.selected_dtypes.dscontarea`
- `diagnostics.memory.selected_dtypes.hilbert`
- `diagnostics.memory.selected_dtypes.flags`

The phase keys are `basins_load`, `streamnet_load`, `source_validate`,
`basins_clamp`, `streamnet_clamp`, `source_post_clamp_validate`,
`dscontarea_infer`, `topology`, `catchment_run_creation`,
`catchment_graph_merge_write`, `snap_run_creation`, and `snap_merge_write`.
Every `diagnostics.memory.phases.<phase>` object has exactly
`start_rss_bytes`, `end_rss_bytes`, `peak_rss_bytes`,
`allocation_delta_bytes`, `max_intra_phase_increase_bytes`, and
`sample_count`.

RSS and scratch measurement can be unavailable on non-Linux developer
platforms. Missing evidence is represented by `null` numeric leaves plus a
concrete unavailable reason, never by zero. `manifest.json` construction and
write are outside these phases and the memory block is report-only.

The adapter emits a warning for each nonzero clamp, including the layer, altered vertex count, and native IDs. After compilation it emits separate warnings for nonzero `contracted_edge_count`, `contracted_root_count`, `contracted_link_traversal_count`, and `polygonless_dropped_reach_count`. It emits three dedicated nonzero degenerate-reach warnings, each with a count and sorted native LINKNOs: all degenerate reaches, the polygon-bearing subset, and the polygon-less subset. It emits dedicated warnings with sorted native LINKNOs for nonzero `short_successor_resolved_reach_count` and `reach_side_near_degenerate_resolved_reach_count`; these fields exclude exact M4-S2 degeneracy and do not redefine M4-S3 area diagnostics. It also warns separately for nonzero trusted healthy isolated-root counts, including native LINKNO values, both for all healthy isolated native roots and for the polygon-bearing subset. The empirical area-unit decision, candidate-error ratio, visible raster-versus-vector divergence metrics, and complete streamnet summary are informational logs. Validity outside the exact two-identical-vertex source convention, native-successor non-coincidence, non-root reach-side ambiguity beyond the wider DSContArea-guarded bound, indeterminate-root ambiguity beyond `2 * --endpoint-tolerance`, conflicting definite root predecessor matches, and other contract failures raise errors rather than warnings.

## CLI usage

From `adapters/tdx-hydro`, install the locked environment:

```bash
uv sync
```

Build one processing basin. The report is a sibling of the dataset directory so it remains outside the dataset root:

```bash
uv run python build_adapter.py build \
  --basins /path/to/7020000010-basins.gpkg \
  --streamnet /path/to/7020000010-streamnet.gpkg \
  --out ./out/7020000010 \
  --report ./out/7020000010-build-report.json \
  --processing-basin-id 7020000010 \
  --fabric-version <NGA_PRODUCT_VERSION>
```

`--basins`, `--streamnet`, `--out`, `--report`, `--processing-basin-id`, and `--fabric-version` are required. `--endpoint-tolerance <degrees>` is optional and defaults to `0.001`; it controls endpoint-coincidence orientation checks and scales the guarded non-root reach-side limit and the separate `2 *` indeterminate-root limit.

Assemble explicit compiled processing-basin roots into one HFX dataset:

```bash
uv run python build_adapter.py assemble \
  --input ./out/7020000010 \
  --input ./out/7020014250 \
  --out ./out/assembled
```

Repeat `--input` once per compiled per-basin HFX root. Assembly never discovers
inputs by scanning a directory. `--out` must be absent or an empty directory.
Assembly writes into a temporary sibling and publishes the complete dataset by
rename only after all checks and writes succeed. A failure leaves no partial
artifacts and preserves a caller-supplied empty directory as empty.

Catchments and graph rows, and snap rows, are merged in bounded memory.
Ordering is recomputed from WKB as centroid Hilbert distance over the fixed
world bounds `[-180, -90, 180, 90]` in EPSG:4326, with deterministic Global
LINKNO tie-breaks. `_hilbert` is transient and is not stored. Graph rows remain
paired with catchments, while snap IDs are reassigned sequentially from 1
through N.

Inputs must have compatible identity and fabric versions, exact core and snap
schemas, matching artifact counts, one known and unique processing-basin region
per root, monotonic world-domain ordering, complete snap-to-catchment
references, and the declared snap auxiliary contract. Attribution files are
staged byte-identically.

Only the exact set of all 62 processing-basin crosswalk entries omits `region`
and claims the independently exact planetary declaration
`bbox = [-180, -90, 180, 90]`. Every proper subset receives
`tdx-hydro-partial-<digest>`, where the digest is the first 12 hexadecimal
characters of SHA-256 over the sorted region keys joined by commas, with no
trailing separator. Its manifest bbox is the exact union of child
covering-union manifest bboxes and therefore exactly equals the union of the
copied float32 catchment columns. For example,
regions `7020000010,7020014250` produce digest `afd4ffb0b736` and region
`tdx-hydro-partial-afd4ffb0b736`.

Validate all dataset layers with strict, 100-percent HFX validation plus GeoParquet 1.1 checks. `--hfx-binary` is optional and defaults to `hfx`:

```bash
uv run python build_adapter.py validate ./out/assembled \
  --hfx-binary ../../target/release/hfx
```

The underlying Rust command is
`<absolute-path>/target/release/hfx <dataset> --strict --sample-pct 100 --format text`;
`<dataset>` is its sole positional argument. The adapter also validates
`catchments.parquet` and `aux/snap_stems.parquet` as GeoParquet 1.1 and verifies
that `graph.parquet` has the expected non-GeoParquet classification.

## Local basin adjudication

The adapter can deterministically adjudicate the seven absent TDX-Hydro
processing basins from two local, read-only evidence trees:

```bash
uv run --project adapters/tdx-hydro python adapters/tdx-hydro/build_adapter.py adjudicate \
  --acquired-evidence-root tdx-m5-seven-acquire-evidence \
  --historical-evidence-root tdx-m5-planetary-evidence \
  --current-disposition 1020018110-duplicate-identity.json \
  --current-disposition 5020049720-duplicate-identity.json
```

Each `--current-disposition` argument names one document written by the
`adjudicate-duplicate-identity` command described below. The flag is optional
and repeatable; at most one document per processing basin is accepted.

The fixed processing-basin order is `1020018110`, `2020003440`, `2020065840`,
`2020071190`, `4020050470`, `5020049720`, and `6020000010`. Acquired products
and state are resolved without discovery at
`salvage/downloads/<processing-basin-id>-{basins,streamnet}.gpkg` and
`salvage/state/basins/<processing-basin-id>/current.json`. Historical state is
resolved at `mirror/state/basins/<processing-basin-id>/current.json`.

Source-defect and adapter-strictness verdicts come only from acquired source
geometry, never a preserved traceback. A transfer-failure verdict comes from
the historical campaign exhaustion record, with a later clean acquisition
supplying resolution evidence. Missing, unsafe, malformed, or mismatched
evidence refuses the complete result before stdout receives any JSON. A
successful run emits one canonical, seven-item JSON document to stdout and has
no output-file option.

The adjudication reproduces adapter version `0.1.0` at git revision
`bca87d8adb0651d130bde9c7dfcf3947427cfa24`. For processing basin `2020071190`,
the governing pinned-adapter refusal is `cannot determine the upstream endpoint
of root successor LINKNO 1104039` in the reverse-topological loop. The later
`successor_conflict` ordering is historical-campaign context; the acquired
feature does not reach that branch.

The command never compiles, writes, uploads, contacts NGA, or reads S3. The
canonical generated ledger is
[`seven-basin-verdicts.json`](seven-basin-verdicts.json), preserved byte-for-byte
from adjudicator stdout. Its `adapter.adapter_version` value `0.1.0` and
`adapter.git_revision` value
`bca87d8adb0651d130bde9c7dfcf3947427cfa24` identify the examined adapter build
under which the seven processing basins are absent. The adjudicator at commit
`bd2606c1dd268eee8f87327008411bf73a08d1b7` derived the schema 1 ledger; the
adjudicator at commit `97fcd89` regenerated it as schema 2 with the two
duplicate-identity documents named above, each produced by the same commit.

The `1020018110` duplicate `streamID` finding is an encoding inconsistency
between NGA products. Every processing basin that compiles stores its `basins`
layer with GeoPackage geometry type `Unknown`, mixing `Polygon` and
`MultiPolygon` rows, with multipart catchments of up to 266 parts and one row
per `streamID`. The `1020018110` and `5020049720` products store the layer with
geometry type `Polygon` and one row per part: `1020018110` holds 924,556 rows,
122,259 `streamID` values carried by more than one row, and 515,435 streamnet
reaches; `5020049720` holds 1,453,118 rows, 211,758 `streamID` values carried by
more than one row, and 933,991 reaches. The two `streamID 9` rows are one single
cell and an 8.40 km2 catchment that share a vertex, and `streamID 24` in
`5020049720` is 16 diagonal cells touching a 5.79 km2 catchment; the parts are
pairwise disjoint in both cases. The identifiers are consistent within each
product, so the finding is a single-part catchment encoding that the adapter
unions into one `MultiPolygon` unit per `streamID`, refusing only on interior
overlap. No source-defect report is warranted, and none was sent. The geometry
evidence remains in [`seven-basin-verdicts.json`](seven-basin-verdicts.json).

The acquired geometry sources and feature identities are:

- `salvage/downloads/1020018110-basins.gpkg`, the smallest duplicated
  `streamID` found by an attribute-only index of the layer (`streamID 9`, the
  identity the compile refusal names), with
  `salvage/downloads/1020018110-streamnet.gpkg` supplying the `LINKNO 9`
  feature count; the evidence `layer` counts record that 122,259 of the
  layer's `streamID` values are duplicated across its 924,556 polygon rows
- `salvage/downloads/2020003440-streamnet.gpkg`, `LINKNO 148956` and its
  `DSLINKNO` successor
- `salvage/downloads/2020071190-streamnet.gpkg`, root `LINKNO 1104039` and every
  feature whose `DSLINKNO` is `1104039`

The transfer sources pair each historical state with the later acquired state
and products:

- `mirror/state/basins/2020065840/current.json` with
  `salvage/state/basins/2020065840/current.json`,
  `salvage/downloads/2020065840-basins.gpkg`, and
  `salvage/downloads/2020065840-streamnet.gpkg`
- `mirror/state/basins/4020050470/current.json` with
  `salvage/state/basins/4020050470/current.json`,
  `salvage/downloads/4020050470-basins.gpkg`, and
  `salvage/downloads/4020050470-streamnet.gpkg`
- `mirror/state/basins/5020049720/current.json` with
  `salvage/state/basins/5020049720/current.json`,
  `salvage/downloads/5020049720-basins.gpkg`, and
  `salvage/downloads/5020049720-streamnet.gpkg`
- `mirror/state/basins/6020000010/current.json` with
  `salvage/state/basins/6020000010/current.json`,
  `salvage/downloads/6020000010-basins.gpkg`, and
  `salvage/downloads/6020000010-streamnet.gpkg`

The ledger records every raw measurement consumed by the adjudicators and is
the single verdict authority. The duplicate record's machine-readable
`derivation` object supplies its preconditions, consistency rule, branches, and
selected branch. For the non-root record, `adapter strictness` applies exactly
when `endpoint_separation <= non_root_near_degenerate_limit`, `DSContArea <
successor_DSContArea`, and any emitted tolerance match has
`current_endpoint_index == 1`; every other measured combination yields `source
defect`. For the root record, `adapter strictness` applies exactly when
`endpoint_separation <= root_near_degenerate_limit` and every emitted
predecessor `DSContArea` is less than the root `DSContArea`; every other measured
combination yields `source defect`. For a transfer record, validated exhaustion
of exactly one historical product plus successful later acquisition identities
for both products yields `transfer failure`.

Both evidence trees remained read-only, and no bulk evidence is committed.

### Ledger schema 2: historical reason and current disposition

Every ledger entry carries two distinct fields:

- `historical_absence` records why the processing basin was absent from the
  55-basin campaign: `verdict`, `evidence_kind`, and `evidence`, exactly as the
  schema 1 entry did.
- `current_disposition` records the basin's current source-backed disposition,
  or `null` when this ledger holds none. `null` means no duplicate-identity
  document was supplied for that basin; the historical reason remains its
  latest classification. A non-null disposition carries `verdict`,
  `evidence_kind` (always `acquired source geometry`), `adjudication_kind`
  (`duplicate identity`), and `evidence` with the document's `source`,
  `stream_id_selection`, `duplicated_stream_ids`, and one `identities` entry
  per duplicated identity. Its `verdict` is `source defect` when any identity
  is a source defect and `adapter strictness` otherwise.

Before a document is accepted, its `source` `bytes`, `sha256`, and
`layer_name` must equal the acquired basins evidence attested in
`salvage/state/basins/<processing-basin-id>/current.json`, so a disposition
about other bytes refuses the whole ledger.

In the committed ledger, `1020018110` and `5020049720` carry non-null current
dispositions. `5020049720` was historically absent because its transfer failed
(`historical_absence.verdict` is `transfer failure`); the later compile of the
acquired source refused `duplicate unit identity for streamID 24`, and its
`current_disposition.verdict` is `source defect` under
`duplicate-ground-equality-v1`: seventeen valid polygons carry `streamID 24`
(sixteen single-cell polygons along a diagonal at about 142.108 E, 10.082 S
plus one 1,009-vertex catchment of about 5.79 km² whose bounding box encloses
them), they are pairwise disjoint (`overlap_area_km2` is 0.0), and the
streamnet carries `LINKNO 24` exactly once. The recorded `layer` counts show
the duplication is systemic in that product: 211,758 of its 933,755 distinct
`streamID` values are carried by more than one of its 1,453,118 polygon rows,
while the streamnet carries 933,991 unique `LINKNO` values. The `1020018110`
product shows the same pattern at 122,259 duplicated identities. Whether
disjoint single-part pieces of one catchment should be unioned into one
drainage unit is a generic adapter decision that this ledger records but does
not take. The
other five basins carry `null` because their orientation refusals are
adjudicated separately.

### Duplicate identity adjudication

Any duplicated `streamID` in any processing basin's `basins` product can be
adjudicated from source geometry alone, without campaign state:

```bash
uv run --project adapters/tdx-hydro python adapters/tdx-hydro/build_adapter.py \
  adjudicate-duplicate-identity \
  --basins <path>/5020049720-basins.gpkg \
  --processing-basin-id 5020049720 \
  --streamnet <path>/5020049720-streamnet.gpkg \
  > 5020049720-duplicate-identity.json
```

In both modes the command first indexes the layer's duplicated `streamID`
values and their feature IDs in one attribute-only pass over the feature ID
and `streamID` columns, run through the standard library `sqlite3` module in
read-only, immutable mode, so no geometry is loaded and no journal file is
created; the counts are recorded under `layer`. Without
`--stream-id`, every discovered duplicated `streamID` is adjudicated, and the
command refuses when none is duplicated. With `--stream-id N`, only that
identity is adjudicated, and the command refuses when fewer than two features
carry it. Only the features carrying an adjudicated identity are ever read
from the layer, by feature ID, so a basin with many duplicated identities
should be examined one requested identity at a time. The optional `--streamnet` argument records,
per identity, how many streamnet features carry the matching `LINKNO`; it
never changes the verdict. Both files
must be regular single-layer GeoPackages whose layer is `basins` or
`TDX_streamnet_<processing-basin-id>_01`; a symlink, a multi-layer file, or a
layer for another basin refuses.

Rule `duplicate-ground-equality-v1` applies to every feature carrying the
identity, however many there are: when every feature is spatially equal to the
first, the features cover the same ground and the verdict is `adapter
strictness`; otherwise they cover different ground under one identity and the
verdict is `source defect`. A structurally unusable, invalid, or non-finite
geometry refuses, as does a measurement set where coordinate sequences are
equal but geometries are not. Each feature is recorded with its `identifier`,
`vertex_count`, `bbox`, geodesic `area_km2`, and full `coordinates`; the
identity also records `union_area_km2` and `overlap_area_km2` so a reader can
see whether the features overlap without recomputing geometry, and the
`layer` counts so one identity is never mistaken for the only one.

The command emits one canonical JSON document to stdout with `adjudication_kind`
`duplicate identity`, `schema_version` 1, the source identity, the `layer`
counts, the selection mode (`discovered` or `requested`), the adjudicated
identities, and one verdict
per identity in the same evidence shape as the ledger's historical duplicate
record. It never writes files, compiles, or contacts NGA.

## Campaign notes

The processing basin `7020000010` pilot ran on 2026-07-22 from VM
checkout `433040a606c467351a0e6eb018c6c28ad4ee5a98`.

| Product | Bytes | SHA-256 | Layer |
|---|---:|---|---|
| basins | 5907767296 | `6d75b56428227824749a497b279a5d0891fd67d4c6a82df1b746f620add6da1a` | `basins` |
| streamnet | 1676398592 | `ae3e5c881cad7c4e3b85a594b4e59889e698f5262e18466a1748c13af6948ca9` | `TDX_streamnet_7020000010_01` |

The source identity was `fabric_version = "NGA-TDX-Hydro-20230126"`.
No explicit label or Last-Modified header was available; the latest
`gpkg_contents.last_change` value was streamnet's
2023-01-26T21:57:00.865Z (basins: 2022-06-30T20:53:53.843Z), yielding the
20230126 suffix.

The build ran once with external report
`/mnt/hfx/work/tdx-hydro-7020000010-build-report.json`. The empirical area
check selected `m2`, relative error
0.1289, over 331263 links.
Orientation recorded 322344 coincidence-proven links,
1702 predecessor-proven roots, and
1767 trusted isolated roots.

Report counts:

- contracted edges: `246`
- contracted roots: `0`
- contracted traversals: `246`
- polygon-less dropped reaches: `123`
- basins clamp vertices: `0`
- streamnet clamp vertices: `0`
- trusted polygon-bearing isolated roots: `1767`

Release strict 100-percent validation and the adapter HFX/GeoParquet wrapper
both passed. Seven dataset/report/attribution objects were verified at
`s3://pourpoint-hfx/scratch/tdx-hydro-pilot/2026-07-23T092027Z/`, outside
`hfx/`. Default teardown removed both exact campaign
resources; independent listings confirmed zero footprint.

Full record:
[`scripts/hetzner/CAMPAIGN-tdx-hydro-7020000010.md`](../../scripts/hetzner/CAMPAIGN-tdx-hydro-7020000010.md).
