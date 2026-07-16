# HydroBASINS Adapter Workflow

This operator workflow covers single-region, selected multi-region, all-present-regions, strict-build, and planetary-manifest operation for the HydroBASINS standard, without-lakes Pfafstetter level 12 product, including optional HydroRIVERS snap builds. The adapter normalizes all inputs to EPSG:4326 and writes one HFX v0.3.0 dataset.

## Prepare Inputs and Environment

Every selected region needs the HydroBASINS layers `hybas_<region>_lev12_v1.shp` and `hybas_pour_lev12_v1.shp`. The nine standard region codes are `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, and `si`. HydroRIVERS is optional.

For a single-region `--region` build or `extract`, the adapter recursively resolves exactly one pour-point layer anywhere below the supplied `--pour-points` directory. For `--regions` and `--all-regions`, it requires one exactly resolved layer below a per-code subdirectory:

```text
/path/to/hydrobasins/
├── standard/
│   ├── hybas_af_lev12_v1.shp
│   └── hybas_eu_lev12_v1.shp
├── pour-points/
    ├── af/
    │   └── hybas_pour_lev12_v1.shp
    ├── eu/
    │   └── hybas_pour_lev12_v1.shp
    └── gr/
        └── hybas_pour_lev12_v1.shp
└── rivers/
    ├── af/
    │   └── HydroRIVERS_af.shp
    ├── eu/
    │   └── HydroRIVERS_eu.shp
    └── gr/
        └── HydroRIVERS_gr.shp
```

Multi-region operation does not use a root-wide pour-point layer or infer regions from point attributes.

Pass `--rivers <dir>` to include HydroRIVERS. For a single-region build, the supplied directory is the resolver root. For `--regions` and `--all-regions`, each resolver root is `--rivers/<code>/`. The adapter recursively searches each applicable root and requires exactly one `*.shp`; zero or multiple matches are errors. The filename itself is unrestricted. HydroRIVERS input must contain `HYRIV_ID`, `HYBAS_L12`, `UPLAND_SKM`, `NEXT_DOWN`, and one LineString geometry column.

From `adapters/hydrobasins`, prepare the environment:

```bash
uv sync
```

Do not generate or commit `uv.lock` as part of a documentation-only change.

## Inspect One Region

`extract` accepts only the single `--region` selector; it does not accept `--regions` or `--all-regions`. It reports both source paths, feature counts, CRS information, required basin-column presence, and the pour-point join key, and writes no HFX artifacts.

```bash
# Inspect one region; this writes no HFX files.
uv run python build_adapter.py extract \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points/gr
```

## Select and Compile Regions

The `build` command requires exactly one of `--region`, `--regions`, or `--all-regions`; these selectors are mutually exclusive. `--planetary` is an independent optional manifest flag, and `--strict-build` is an independent optional guard flag. Either optional flag may accompany any one selector.

`--regions` accepts comma-separated, whitespace-separated, or mixed code lists, preserves their authored order, and rejects duplicates. Explicit codes are resolved through filenames rather than parser-validated against the standard-code list. `--all-regions` scans `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, `si` in that fixed order and selects the basin layers that are present; it errors only when none is present.

### Single region

```bash
# Build one region with a HydroRIVERS snap layer. The pour-point and optional river layers are resolved from their supplied roots.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points/gr \
  --rivers /path/to/hydrorivers/gr \
  --out ./out/gr-snap
```

Leaving out `--rivers` produces the unchanged core-only dataset with no snap artifact or manifest declaration.

### Selected regions

```bash
# Build two regions. Each pour-point and HydroRIVERS layer is below <root>/<code>/.
uv run python build_adapter.py build \
  --regions af,eu \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --rivers /path/to/hydrorivers \
  --out ./out/af-eu-snap

# The equivalent whitespace-list form is also accepted.
uv run python build_adapter.py build \
  --regions af eu \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --rivers /path/to/hydrorivers \
  --out ./out/af-eu-snap
```

Leaving out `--rivers` produces the unchanged core-only dataset with no snap artifact or manifest declaration.

### All present standard regions

```bash
# Build every standard region layer present under --basins.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/all-present-regions
```

### Strict antimeridian handling

```bash
# Reject antimeridian-wrap candidates instead of warning.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/all-present-regions-strict \
  --strict-build
```

### Planetary manifest metadata

```bash
# Exercise planetary manifest semantics with the selected regions.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/planetary-code-path \
  --planetary \
  --strict-build
```

`--planetary` changes manifest metadata only. It does not select, download, or compile missing regions.

## Build Sequence

The build executes in this order:

1. Resolve selected regions in authored order, or in fixed standard-code scan order for `--all-regions`.
2. For each region independently, load and normalize polygons to EPSG:4326, load and normalize its pour points, perform the total `HYBAS_ID` join, and deterministically assign one outlet. When `--rivers` is present, also load and normalize that region's HydroRIVERS LineStrings to EPSG:4326 and derive stem roles within the regional layer from `NEXT_DOWN`.
3. Concatenate the assigned regional frames into one combined frame and reject any `HYBAS_ID` duplicated anywhere in the union.
4. Apply a fresh global centroid-Hilbert sort with `id` as the deterministic tie-break. This replaces region-block ordering.
5. Run the antimeridian guard over the combined union.
6. Write `catchments.parquet` once with balanced row groups and immediately run its GeoParquet 1.1 structural validation.
7. Construct the combined-union graph, check its tree shape and acyclicity, and write `graph.parquet` once with balanced row groups.
8. When `--rivers` is present, concatenate regional reaches, drop reaches whose `HYBAS_L12` is absent from the merged unit set while logging the exact count, apply one global centroid-Hilbert order using the merged units' total bounds, and assign signed 64-bit sequential IDs from 1 through N. Region index and source order provide deterministic tie breaks.
9. When `--rivers` is present, write `aux/snap_stems.parquet` with the required GeoParquet covering and row-group statistics.
10. Write `manifest.json` last, adding the `hfx.aux.snap.v2` declaration only when snap data is present.

An ordinary manifest includes the selected codes joined by commas in selection order, such as `af,eu`, and the combined geometry bbox. An `--all-regions` label follows fixed scan order for the regions present. A planetary manifest omits `region` and uses the exact bbox `[-180, -90, 180, 90]`.

When snap data is present, the manifest includes this exact auxiliary declaration:

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
        "references_levels": [0],
        "weight_semantics": "drainage_area_km2"
      }
    }
  ]
}
```

The `auxiliary` key is absent when `--rivers` is omitted.

For both Parquet artifacts, a file with fewer than 4,096 units has one row group. A file with at least 4,096 units has balanced groups from 4,096 through 8,192 rows inclusive. Counts from 4,096 through 8,192 validly produce one balanced group.

## Validate the Dataset

Run strict, 100-percent HFX validation in text and JSON modes:

```bash
# Validate and write reports under <dataset>/validation.
uv run python build_adapter.py validate ./out/af-eu

# Validate with an explicit report directory.
uv run python build_adapter.py validate ./out/af-eu \
  --report-dir ./out/af-eu-validation
```

By default, reports are written under `./out/af-eu/validation` as `validator-report.text` and `validator-report.json`. With `--report-dir`, they are written to the selected directory. A corresponding `validator-report.text.stderr` or `validator-report.json.stderr` file is written when that mode emits stderr. The subcommand invokes the repository validator with `--strict --sample-pct 100`; it does not repeat the GeoParquet validation performed during `build`.

Run the unchanged unit-test suite from `adapters/hydrobasins` with:

```bash
uv run python -m unittest discover -p "test_*.py"
```

Test and conformance coverage includes a two-region merge smoke for clean concatenation and global `HYBAS_ID` uniqueness. This workflow does not claim that a source-data global build has run.

## Output and Validation Checklist

- `catchments.parquet`, `graph.parquet`, and `manifest.json` are present.
- There is one catchments row and one graph row per merged HydroBASINS unit.
- Every `HYBAS_ID` is positive and globally unique across the merged union.
- Every unit resolves through its region's total pour-point join.
- `outlet_lon` and `outlet_lat` come from the DEM-derived highest-flow-accumulation point, not polygon geometry or HydroRIVERS.
- Multiple coastal candidates collapse to the point nearest the sub-basin centroid, with the lowest `(lon, lat)` pair breaking equal-distance ties.
- Catchments and graph rows have the same deterministic global Hilbert order with `id` tie-breaking.
- A non-endorheic cross-region `NEXT_DOWN` edge resolves when its target exists anywhere in the combined union.
- A nonzero, non-endorheic target absent from the complete union is a hard error, not an implicit terminal or boundary cut.
- `ENDO = 2` cuts the virtual outgoing edge even when `NEXT_DOWN` is nonzero, and `NEXT_DOWN = 0` is the terminal sink sentinel.
- The same-level graph is tree-shaped and acyclic.
- Both Parquet artifacts follow the balanced row-group rules.
- `catchments.parquet` passed GeoParquet 1.1 structural validation during the build.
- HFX validation passed in strict mode with a 100-percent sample.
- The two-region smoke covers clean concatenation and global ID uniqueness.
- With `--rivers`, `aux/snap_stems.parquet` is present with exactly `id`, `unit_id`, `weight`, `stem_role`, `bbox`, and `geometry`.
- Snap `id` is a sequential signed 64-bit integer from 1 through N after global ordering; `unit_id` is signed 64-bit and equals `HYBAS_L12`; `weight` is `UPLAND_SKM` as float32.
- Snap `geometry` is the original, unclipped HydroRIVERS LineString encoded as WKB after normalization to EPSG:4326.
- Snap `bbox` is the required GeoParquet covering struct with `xmin`, `ymin`, `xmax`, and `ymax` leaves, mapped to `geometry` by GeoParquet 1.1 metadata. Row-group statistics are present for the consumer's spatial query path.
- Every retained `unit_id` refers to a level-0 unit in the complete merged dataset. Reaches with unresolved `HYBAS_L12` are dropped, with the adapter logging `dropped %d HydroRIVERS reaches with HYBAS_L12 absent from the unit set`; there is no spatial fallback join.
- `stem_role` is only `mainstem`, `tributary`, or `unknown`; `distributary` is never emitted. At a regional confluence, the largest `UPLAND_SKM` continues as `mainstem`, with larger `HYRIV_ID` breaking a weight tie. A positive `NEXT_DOWN` outside the loaded regional reach set is `unknown`; terminal reaches and reaches without a competing sibling remain `mainstem`.
- Multi-region reaches are concatenated, filtered against the merged units, globally centroid-Hilbert ordered using the merged units' total bounds, and assigned sequential IDs only afterward. Region index and source order provide deterministic tie breaks.
- A snap-enabled manifest declares schema `hfx.aux.snap.v2`, artifact `{"snap": "aux/snap_stems.parquet"}`, name `stems`, `references_levels: [0]`, and `weight_semantics: "drainage_area_km2"`.
- A core-only build has no `aux/snap_stems.parquet` and no `auxiliary` manifest key.

## Verify Snap and Polygon Resolution

Choose a small region and a query coordinate on or near a known HydroRIVERS reach. Build two otherwise equivalent datasets from `adapters/hydrobasins`: one with the snap input and one without it.

```bash
# Run from hfx/adapters/hydrobasins.
uv run python build_adapter.py build \
  --region <REGION> \
  --basins <HYDROBASINS_DIR> \
  --pour-points <POUR_POINTS_REGION_DIR> \
  --rivers <HYDRORIVERS_REGION_DIR> \
  --out ./out/<REGION>-snap

uv run python build_adapter.py build \
  --region <REGION> \
  --basins <HYDROBASINS_DIR> \
  --pour-points <POUR_POINTS_REGION_DIR> \
  --out ./out/<REGION>-core
```

Strictly validate the snap-enabled dataset from the HFX repository root:

```bash
cargo run -p hfx-cli -- \
  adapters/hydrobasins/out/<REGION>-snap \
  --strict \
  --sample-pct 100
```

From the pourpoint repository root, delineate the on-network coordinate against the snap-enabled dataset. The `--snap-radius 1000` spelling is optional because 1000 m is the default, but showing it makes the tested radius explicit. Do not pass `--no-refine` in this check because that flag changes the refinement result to `Disabled`.

```bash
cargo run --release -- delineate \
  --dataset <ABSOLUTE_PATH_TO_REGION_SNAP_DATASET> \
  --lat <LAT> \
  --lon <LON> \
  --format geojson \
  --snap-radius 1000
```

Inspect the successful feature in the returned GeoJSON FeatureCollection. It must contain a watershed geometry, a `resolution_method` beginning with `snap(`, and a `refinement` value beginning with `best_effort_skipped(` whose provenance contains `NoD8AuxDeclared`. HydroBASINS has no D8 auxiliary, so that terminal refinement result is expected.

Run the same coordinate against the core-only dataset:

```bash
cargo run --release -- delineate \
  --dataset <ABSOLUTE_PATH_TO_REGION_CORE_DATASET> \
  --lat <LAT> \
  --lon <LON> \
  --format geojson
```

This result must also contain a watershed, but its `resolution_method` begins with `pip(` because the dataset has no `hfx.aux.snap.v2` declaration.

Snap selection is dataset-level. A snap-enabled dataset does not fall back to point-in-polygon for an individual query. With the default 1000 m radius, a point farther than 1000 m from every channel returns `NoSnapCandidates`; use an on-network or near-network point for this verification.

## Licensing

HydroBASINS, HydroBASINS Pour Points, and HydroRIVERS are HydroSHEDS products covered by the same HydroSHEDS License Agreement. The agreement permits commercial, non-commercial, and internal use, but prohibits public or open redistribution of the licensed materials as a stand-alone product. Internal use of the source and compiled HFX output is permitted; do not treat either the source layers or compiled output as freely redistributable stand-alone material.

## Operational Cautions

- The guard computes each combined EPSG:4326 unit's raw stored longitude extent as `maxx - minx`; an extent strictly greater than 180 degrees is an antimeridian-wrap candidate. Default mode warns and continues writing, while `--strict-build` fails before artifacts are written.
- Antimeridian detection does not unwrap or split geometry. Operational resolution and the actual global compile and hosting remain deferred to Effort #34.
- Only standard without-lakes Pfafstetter level 12 inputs are supported. The with-lakes product is unsupported because its matching ancillary pour-points product is unavailable.
- Snap dispatch is dataset-level, so a dataset declaring `hfx.aux.snap.v2` has no per-query point-in-polygon fallback. With the default 1000 m radius, a point farther than 1000 m from every channel returns `NoSnapCandidates`.
- Planetary mode changes metadata only. The actual planetary compile and hosting remain Effort #34.
