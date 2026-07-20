# HydroBASINS to HFX Adapter

This adapter is a reusable, guarded single- or multi-region compiler for the HydroBASINS standard, without-lakes product. It writes the HFX v0.3.0 core artifacts `catchments.parquet`, `graph.parquet`, and `manifest.json` for one contiguous source Pfafstetter level range, defaulting to all source Pfaf levels `1-12`. Supplying `--rivers` optionally adds an `hfx.aux.snap.v2` HydroRIVERS layer when source Pfaf level 12 is selected.

## Inputs

Pass the canonical HydroBASINS `extract/` directory to `--basins`. For every selected region and every selected source Pfaf level, the adapter reads `extract/hybas_<region>/hybas_<region>_lev<NN>_v1c.shp`, where `<NN>` is zero-padded from `01` through `12`. Pass the global `extract/pour/` directory to `--pour-points`; the adapter reads `extract/pour/hybas_pour_lev<NN>_v1.shp` once for each selected source Pfaf level. Pour points are global per-level inputs, not per-region inputs.

The nine standard region codes are `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, and `si`. Polygon and pour-point inputs are independently normalized to EPSG:4326 at each selected level. The canonical layout is:

```text
<source-root>/
└── extract/
    ├── hybas_af/
    │   ├── hybas_af_lev01_v1c.shp
    │   ├── ...
    │   └── hybas_af_lev12_v1c.shp
    ├── hybas_gr/
    │   ├── hybas_gr_lev01_v1c.shp
    │   ├── ...
    │   └── hybas_gr_lev12_v1c.shp
    └── pour/
        ├── hybas_pour_lev01_v1.shp
        ├── ...
        └── hybas_pour_lev12_v1.shp
```

The paths are direct contract paths. Basin filenames use the standard-product `v1c` suffix, while the ancillary pour-point filenames use `v1`. Multi-region operation shares each global per-level pour layer across the selected regional polygon layers.

### Optional HydroRIVERS input

Pass `--rivers <dir>` to emit HydroRIVERS reaches as an HFX snap auxiliary. A single-region build recursively resolves exactly one `*.shp` anywhere below the supplied directory:

```text
<rivers>/HydroRIVERS_<region>.shp
```

For `--regions` and `--all-regions`, the adapter appends each selected region code and recursively resolves exactly one shapefile below `--rivers/<code>/`:

```text
<rivers>/af/HydroRIVERS_af.shp
<rivers>/eu/HydroRIVERS_eu.shp
```

The resolver accepts any shapefile name, but each applicable root must contain exactly one recursively discoverable `*.shp`; zero or multiple matches are errors. Omitting `--rivers` preserves the core-only build and emits only the existing catchments, graph, and manifest without a snap auxiliary declaration.

HydroRIVERS input must contain `HYRIV_ID`, `HYBAS_L12`, `UPLAND_SKM`, `NEXT_DOWN`, and one LineString geometry column. The adapter normalizes it to EPSG:4326.

Source Pfaf levels 1 through 12 are supported as one singleton or contiguous range. The with-lakes variant remains unsupported because its matching HydroBASINS Pour Points ancillary product is unavailable.

## CLI Reference

Run the adapter from `adapters/hydrobasins` with `uv run python build_adapter.py`.

```text
build (--region <code> | --regions <code> [<code> ...] | --all-regions) --basins <extract-dir> --pour-points <extract/pour-dir> --out <dir> [--levels <N|N-M>] [--rivers <dir>] [--planetary] [--strict-build]
extract --region <code> --basins <extract-dir> --pour-points <extract/pour-dir>
validate <dataset> [--report-dir <dir>]
```

Exactly one build selector is required, and the three selectors are mutually exclusive. `--regions` accepts one or more tokens and splits them on commas or whitespace, so comma-separated, whitespace-separated, and mixed lists are accepted. Selection order is preserved, and duplicate codes are rejected. Explicit `--region` and `--regions` values are resolved through their corresponding filenames rather than parser-validated against the standard-code list.

`--levels` uses source Pfaf numbering and accepts either one level, such as `--levels 12`, or one inclusive contiguous ascending range, such as `--levels 6-12`. Omitting it selects the default source range `1-12`. The option may be specified only once. Comma lists, whitespace lists, descending ranges, values outside 1 through 12, and malformed ranges are rejected. For example, `--levels 1,3`, `--levels "1 3"`, `--levels 12-6`, `--levels 0`, and `--levels 1-13` are invalid.

HFX level numbering is zero-based relative to the selected source range: `HFX level = source Pfaf level - min(selected range)`. In the default `1-12` build, source Pfaf levels 1 and 12 become HFX levels 0 and 11. In a `6-12` build, source Pfaf levels 6 and 12 become HFX levels 0 and 6. A singleton `--levels 12` build contains HFX level 0.

`extract` remains a source Pfaf-12-only, single-region inspection command and does not accept `--levels`. It reads `hybas_<region>_lev12_v1c.shp` from the canonical regional directory and `hybas_pour_lev12_v1.shp` from the global pour directory, reports their paths, feature counts, CRS information, basin-column presence, and pour-point join key, and writes no HFX artifacts.

`--all-regions` scans for the nine standard basin layers in fixed order, `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, `si`, and selects the regions that are present for the selected source Pfaf range. It does not require all nine, but errors if none is present. `--planetary` and `--strict-build` are independent optional build flags.

`validate` takes the dataset path positionally. Its optional `--report-dir` defaults to `<dataset>/validation`. It persists text and JSON validator reports and the corresponding stderr files when stderr is present.

```bash
# Inspect one region at source Pfaf level 12; extract does not accept --levels.
uv run python build_adapter.py extract \
  --region gr \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour

# Build one region with the default source Pfaf range 1-12.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --out ./out/gr-pfaf1-12

# Build one region as a source Pfaf-12 singleton, producing HFX level 0.
uv run python build_adapter.py build \
  --region gr \
  --levels 12 \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --out ./out/gr-pfaf12

# Build a partial nested range; source Pfaf 6 becomes HFX level 0.
uv run python build_adapter.py build \
  --region gr \
  --levels 6-12 \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --out ./out/gr-pfaf6-12

# Build two regions with the default range and region-specific HydroRIVERS roots.
uv run python build_adapter.py build \
  --regions af,eu \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --rivers /path/to/hydrorivers \
  --out ./out/af-eu-pfaf1-12-snap

# Build every present standard region with planetary metadata and snap.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/extract \
  --pour-points /path/to/hydrobasins/extract/pour \
  --rivers /path/to/hydrorivers \
  --out ./out/global-pfaf1-12-snap \
  --planetary \
  --strict-build

# Validate and write reports under <dataset>/validation.
uv run python build_adapter.py validate ./out/global-pfaf1-12-snap

# Validate with an explicit report directory.
uv run python build_adapter.py validate ./out/global-pfaf1-12-snap \
  --report-dir ./out/global-pfaf1-12-snap-validation
```

## Build and Merge Semantics

The adapter iterates selected source Pfaf levels in ascending order. For each level, it loads and normalizes every selected region's polygon layer, loads that level's global pour-point layer, assigns each polygon's outlet by `HYBAS_ID`, and merges the selected regions into one same-level union. It rejects duplicate `HYBAS_ID` values within that union and again across all selected levels.

For every level above the coarsest selected source Pfaf level, the adapter assigns `parent_id` within each region by dropping the final decimal digit from the child's `PFAF_ID` and requiring that prefix to identify exactly one polygon in the immediately preceding selected source Pfaf level. The coarsest selected source Pfaf level maps to HFX level 0 and has null `parent_id`. Missing or ambiguous parent joins are fatal and have no spatial fallback.

After concatenating all selected levels, the adapter performs one deterministic global sort by `(level ASC, hilbert ASC)`, with `id` as the final tie-break. Both `catchments.parquet` and `graph.parquet` use that order. The antimeridian guard checks the complete combined unit set before each core artifact is written once, and `catchments.parquet` is structurally validated as GeoParquet 1.1 immediately after writing.

Both `catchments.parquet` and `graph.parquet` use balanced row groups. Files with at least 4,096 units have row-group sizes from 4,096 through 8,192 rows inclusive; this permits one balanced group for 4,096 through 8,192 units. Files with fewer than 4,096 units use one row group.

### HydroRIVERS snap auxiliary

`--rivers` requires the selected source Pfaf range to include source Pfaf level 12. A range that excludes it fails before source loading with `--rivers requires --levels to include source Pfaf level 12`.

Snap keying remains unchanged: each HydroRIVERS reach joins by `HYBAS_L12` to the unit from source Pfaf level 12. This remains true when source Pfaf level 12 maps to a nonzero HFX level. Reaches whose `HYBAS_L12` is absent from the merged source Pfaf-12 unit set are dropped without a spatial fallback, and the adapter logs `dropped %d HydroRIVERS reaches with HYBAS_L12 absent from the unit set`.

The auxiliary declaration uses the zero-based HFX level containing source Pfaf-12 units. Its formula is `references_levels = [12 - min(selected range)]`: `[11]` for the default `1-12` range, `[6]` for `6-12`, and `[0]` for the singleton `12` range. The `unit_id` values remain equal to `HYBAS_L12` in every case.

The adapter writes `aux/snap_stems.parquet` with these columns:

| Column | Meaning |
|---|---|
| `id` | Sequential signed 64-bit integer from 1 through N after global ordering |
| `unit_id` | Signed 64-bit joined HydroBASINS unit ID, equal to `HYBAS_L12` |
| `weight` | `UPLAND_SKM` as float32, declared as `drainage_area_km2` |
| `stem_role` | `mainstem`, `tributary`, or `unknown` from `NEXT_DOWN` confluences |
| `bbox` | Required `xmin`, `ymin`, `xmax`, `ymax` GeoParquet covering struct |
| `geometry` | Original, unclipped HydroRIVERS LineString WKB in EPSG:4326 |

The artifact carries GeoParquet 1.1 metadata mapping the `bbox` covering to `geometry` and is written with row-group statistics. The covering is required by the consumer's spatial query path.

Roles are derived within each regional HydroRIVERS layer. At each confluence, the contributor with the largest `UPLAND_SKM` continues as `mainstem`; other contributors are `tributary`. Equal weights use the larger `HYRIV_ID` as the deterministic winner. A reach whose positive `NEXT_DOWN` is outside the loaded regional layer produces `unknown`. Terminal reaches and reaches without a competing sibling remain `mainstem`, and the adapter never emits `distributary`.

For multiple regions, the adapter concatenates regional reaches, filters them against the merged source Pfaf-12 unit IDs, applies one global centroid-Hilbert order using the merged units' total bounds, and then assigns sequential snap IDs. Region index and source order provide deterministic tie-breaks.

## Outlets and Graph Boundary

Every selected source Pfaf level gets outlets from its own DEM-derived HydroBASINS Pour Points ancillary layer, joined to polygons by `HYBAS_ID`. The join is total at each level: every selected polygon must have at least one matching point, and a missing match is a hard build error. A coarser unit's outlet is never derived from a finest-level descendant, polygon geometry, or HydroRIVERS.

When a coastal unit has multiple points at its source Pfaf level, the adapter selects the candidate nearest the unit centroid. Equal-distance ties are broken by the lowest `(lon, lat)` pair.

Each HFX level has a same-level, tree-shaped, acyclic graph with one row per unit from that level's source layer. `NEXT_DOWN = 0` is the terminal sink sentinel. A unit with `ENDO = 2` is terminal even when `NEXT_DOWN` is nonzero, so its virtual outgoing edge is cut.

Graph targets resolve across the complete merged regional union at the same HFX level. A non-endorheic `NEXT_DOWN` edge can cross a selected region boundary. Any nonzero, non-endorheic downstream target absent from that complete same-level union is a hard build error; it is not converted into an implicit terminal or boundary cut.

## Antimeridian Guard

For each combined EPSG:4326 unit, the build computes the raw stored longitude extent as `maxx - minx`. Any polygon whose extent is strictly greater than 180 degrees is an antimeridian-wrap candidate. Default builds warn and continue writing artifacts. With `--strict-build`, the same candidates cause a build error before artifacts are written.

The guard detects and reports candidates only; it does not unwrap or split geometry. Operational resolution, including unwrapping or splitting and the actual full global operation, remains a separate operator action.

## Manifest Behavior

For a normal non-planetary build, `manifest.json` uses the selected codes joined by commas in selection order as `region`, such as `af,eu`, and uses the combined geometry bounds. For `--all-regions`, the label follows the fixed standard-code scan order for the regions actually present.

`--planetary` omits `region` and writes the exact bbox `[-180, -90, 180, 90]`. Planetary mode changes manifest metadata only. It does not download or compile missing regions.

For the default source Pfaf range `1-12`, supplying `--rivers` adds this auxiliary declaration:

```json
{
  "auxiliary": [
    {
      "schema": "hfx.aux.snap.v2",
      "artifacts": {
        "snap": "aux/snap_stems.parquet"
      },
      "metadata": {
        "name": "stems",
        "description": "Unclipped HydroRIVERS reach centerlines for HydroBASINS Pfaf-12 snapping. HydroRIVERS and HydroBASINS are HydroSHEDS products covered by the HydroSHEDS License Agreement. weight = UPLAND_SKM (km^2). stem_role = mainstem/tributary derived from NEXT_DOWN confluences.",
        "references_levels": [11],
        "weight_semantics": "drainage_area_km2"
      }
    }
  ]
}
```

When `--rivers` is omitted, `auxiliary` is absent and no snap artifact is written.

## Attribute-Join Evidence

The committed `attribute_join_scan_report.json` records a passing geometry-free scan of the canonical global extracts. It evaluated 3,786,218 child joins across 99 adjacent-level pairs for the `PFAF_ID` parent-prefix contract, covered 3,786,228 basins across 12 pour-point levels for the per-level `HYBAS_ID` outlet join, and checked 3,786,228 basins across 108 regional level layers for global `HYBAS_ID` collision freedom. All three checks reported zero findings.

## Validation and Conformance

Authoritative HFX conformance uses `hfx-cli --strict --sample-pct 100`. The adapter's `validate` subcommand runs text and JSON modes from the repository root and persists their output:

```bash
cargo run -p hfx-cli -- <dataset> --format text --strict --sample-pct 100
cargo run -p hfx-cli -- <dataset> --format json --strict --sample-pct 100
```

GeoParquet 1.1 structural validation of `catchments.parquet` occurs during `build`; `validate` does not repeat it. Test and conformance coverage includes nested default and partial-range builds, same-level graph behavior, parent and outlet joins, source Pfaf-12 snap gating, and two-region merge behavior. The committed attribute scan is evidence for global join assumptions; this documentation step does not run a source-data global build.

For an operator check, build otherwise equivalent snap-enabled and core-only datasets with the canonical roots and default source Pfaf range `1-12`, then use the same coordinate on or near a known HydroRIVERS reach. Strictly validate the snap-enabled dataset from the HFX repository root:

```bash
cargo run -p hfx-cli -- \
  adapters/hydrobasins/out/<REGION>-pfaf1-12-snap \
  --strict \
  --sample-pct 100
```

From the pourpoint repository root, delineate against the snap-enabled dataset without `--no-refine`:

```bash
cargo run --release -- delineate \
  --dataset <ABSOLUTE_PATH_TO_REGION_SNAP_DATASET> \
  --lat <LAT> \
  --lon <LON> \
  --format geojson
```

The GeoJSON FeatureCollection's successful feature has `resolution_method` beginning with `snap(` and `refinement` beginning with `best_effort_skipped(` whose provenance contains `NoD8AuxDeclared`. The same coordinate against a core-only dataset has `resolution_method` beginning with `pip(`. The default snap radius is 1000 m; `--snap-radius <METRES>` overrides it. See [WORKFLOW.md](WORKFLOW.md#verify-snap-and-polygon-resolution) for the complete two-dataset recipe.

## Licensing

HydroBASINS, HydroBASINS Pour Points, and HydroRIVERS are HydroSHEDS products covered by the same HydroSHEDS License Agreement. The agreement permits commercial, non-commercial, and internal use, but prohibits public or open redistribution of the licensed materials as a stand-alone product. Internal use of the source and compiled HFX output is permitted; do not treat either the source layers or compiled output as freely redistributable stand-alone material.

## Limitations and Deferred Work

- Planetary mode changes manifest metadata only; selecting all present source regions and performing the operational global build remain separate operator actions.
- Antimeridian detection is implemented; operational unwrapping or splitting remains a separate operator action.
- Snap dispatch is dataset-level. When `hfx.aux.snap.v2` is declared, every outlet query uses the snap resolver with no per-query point-in-polygon fallback. With the default 1000 m radius, a point farther than 1000 m from every channel returns `NoSnapCandidates`.
- The with-lakes product is unsupported because its matching pour-points product is unavailable.
