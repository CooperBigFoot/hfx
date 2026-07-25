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

No current-endpoint match is a fatal non-coincidence error. If matches use both endpoints of a healthy current reach, its direction is not discriminated by coincidence. When the current endpoint separation is at most `2 * --endpoint-tolerance`, the reach is bounded near-degenerate at the compile's coincidence resolution. The adapter preserves TDX/TauDEM native vertex order and uses the final source vertex as its downstream endpoint without changing geometry. This is a reported native-order trust convention, not an endpoint-coincidence proof. A reach-side ambiguity whose endpoint separation exceeds that bound remains a fatal build error. The adapter never widens the bound, snaps an outlet to a successor, repairs geometry, or uses a spatial fallback.

A degenerate successor has one distinct coordinate even though its preserved LineString stores that coordinate twice. A sole current-endpoint match to that point still discriminates a healthy current reach and is counted as proven by endpoint coincidence. Duplicate storage of the successor point is handled only by the degenerate-reach diagnostics and is not counted as a short-successor resolution.

**TRUST ASSUMPTION: a TDX-Hydro exactly degenerate reach has no directional axis, so its orientation is vacuous and is never counted as proven. Its single distinct source coordinate is treated as its downstream endpoint. A non-root degenerate reach must still coincide with its immediate native successor; non-coincidence remains fatal. This is a source-convention trust assumption, not spatial inference or an orientation proof.** A polygon-bearing degenerate reach uses that coordinate as its unit outlet. A degenerate root uses the same rule whether or not it has predecessors and is not counted as predecessor-orientation-proven, short-successor-resolved, reach-side-near-degenerate-resolved, or native-order trusted isolated.

For a healthy native root with predecessors, uniquely identified predecessor-to-root endpoint matches identify the root's upstream endpoint. The opposite root endpoint is therefore the downstream endpoint. Definite predecessor matches to both root endpoints are fatal. A predecessor that matches both endpoints of a short root contributes no definite endpoint fact and therefore does not create a spurious conflict. If another predecessor supplies one definite endpoint, that fact proves the root orientation. If every predecessor match is endpoint-indeterminate, the root necessarily lies within the `2 * --endpoint-tolerance` near-degenerate bound; the adapter preserves native order, uses the root's final source vertex, and reports the same bounded near-degenerate trust convention instead of calling the root predecessor-proven.

**TRUST ASSUMPTION: a healthy isolated native root has no successor and no predecessor topology, so its orientation cannot be proven by endpoint coincidence. For those reaches only, the adapter trusts TauDEM/TDX native vertex order and treats the final LineString vertex as the outlet. This is a trust assumption, never an orientation proof.** The report keeps trusted healthy isolated roots separate from endpoint-coincidence-proven links, predecessor-orientation-proven roots, and bounded near-degenerate resolutions, and separately identifies the trusted isolated roots that bear polygons.

Each drainage unit's `outlet_lon` and `outlet_lat` are the downstream endpoint resolved for its own native polygon-bearing reach. They are not polygon centroids, spatial joins, repaired geometries, or the downstream endpoint of a synthetic geometry assembled across contracted links. The proof is performed against the immediate native `DSLINKNO` successor even when graph contraction later skips polygon-less links.

Pilot processing basin `7020000010` exposed 6,178 successor-side multi-match pairs at the default tolerance. Of these, 296 use exactly degenerate successors already covered by the zero-length convention and 5,882 use nonzero successors no longer than twice the tolerance. Only 85 pairs also match both endpoints of the current reach. These are pilot magnitudes, not production constants. Planetary build #107 must re-check the dedicated counts and native LINKNO lists independently for every processing basin, and any reach-side case beyond the bounded near-degenerate rule must remain fatal.

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

The adapter emits a warning for each nonzero clamp, including the layer, altered vertex count, and native IDs. After compilation it emits separate warnings for nonzero `contracted_edge_count`, `contracted_root_count`, `contracted_link_traversal_count`, and `polygonless_dropped_reach_count`. It emits three dedicated nonzero degenerate-reach warnings, each with a count and sorted native LINKNOs: all degenerate reaches, the polygon-bearing subset, and the polygon-less subset. It emits dedicated warnings with sorted native LINKNOs for nonzero `short_successor_resolved_reach_count` and `reach_side_near_degenerate_resolved_reach_count`; these fields exclude exact M4-S2 degeneracy and do not redefine M4-S3 area diagnostics. It also warns separately for nonzero trusted healthy isolated-root counts, including native LINKNO values, both for all healthy isolated native roots and for the polygon-bearing subset. The empirical area-unit decision, candidate-error ratio, visible raster-versus-vector divergence metrics, and complete streamnet summary are informational logs. Validity outside the exact two-identical-vertex source convention, native-successor non-coincidence, reach-side ambiguity beyond the bounded near-degenerate rule, conflicting definite root predecessor matches, and other contract failures raise errors rather than warnings.

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

`--basins`, `--streamnet`, `--out`, `--report`, `--processing-basin-id`, and `--fabric-version` are required. `--endpoint-tolerance <degrees>` is optional and defaults to `0.001`; it controls only endpoint-coincidence orientation checks.

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
