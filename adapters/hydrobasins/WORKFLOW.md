# HydroBASINS Adapter Workflow

This operator workflow covers single-region, selected multi-region, all-present-regions, strict-build, and planetary-manifest operation for the HydroBASINS standard, without-lakes Pfafstetter level 12 product. The adapter normalizes all inputs to EPSG:4326 and writes one HFX v0.3.0 dataset.

## Prepare Inputs and Environment

Every selected region needs `hybas_<region>_lev12_v1.shp` and its `hybas_pour_lev12_v1.shp`. The nine standard region codes are `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, and `si`.

For a single-region `--region` build or `extract`, the adapter recursively resolves exactly one pour-point layer anywhere below the supplied `--pour-points` directory. For `--regions` and `--all-regions`, it requires one exactly resolved layer below a per-code subdirectory:

```text
/path/to/hydrobasins/
├── standard/
│   ├── hybas_af_lev12_v1.shp
│   └── hybas_eu_lev12_v1.shp
└── pour-points/
    ├── af/
    │   └── hybas_pour_lev12_v1.shp
    ├── eu/
    │   └── hybas_pour_lev12_v1.shp
    └── gr/
        └── hybas_pour_lev12_v1.shp
```

Multi-region operation does not use a root-wide pour-point layer or infer regions from point attributes.

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
# Build one region. Its pour-point layer is resolved from the supplied root.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points/gr \
  --out ./out/gr
```

### Selected regions

```bash
# Build two regions. Each pour-point layer is below <root>/<code>/.
uv run python build_adapter.py build \
  --regions af,eu \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/af-eu

# The equivalent whitespace-list form is also accepted.
uv run python build_adapter.py build \
  --regions af eu \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/af-eu
```

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
2. For each region independently, load and normalize polygons to EPSG:4326, load and normalize its pour points, perform the total `HYBAS_ID` join, and deterministically assign one outlet.
3. Concatenate the assigned regional frames into one combined frame and reject any `HYBAS_ID` duplicated anywhere in the union.
4. Apply a fresh global centroid-Hilbert sort with `id` as the deterministic tie-break. This replaces region-block ordering.
5. Run the antimeridian guard over the combined union.
6. Write `catchments.parquet` once with balanced row groups and immediately run its GeoParquet 1.1 structural validation.
7. Construct the combined-union graph, check its tree shape and acyclicity, and write `graph.parquet` once with balanced row groups.
8. Write `manifest.json` last.

An ordinary manifest includes the selected codes joined by commas in selection order, such as `af,eu`, and the combined geometry bbox. An `--all-regions` label follows fixed scan order for the regions present. A planetary manifest omits `region` and uses the exact bbox `[-180, -90, 180, 90]`.

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

## Operational Cautions

- The guard computes each combined EPSG:4326 unit's raw stored longitude extent as `maxx - minx`; an extent strictly greater than 180 degrees is an antimeridian-wrap candidate. Default mode warns and continues writing, while `--strict-build` fails before artifacts are written.
- Antimeridian detection does not unwrap or split geometry. Operational resolution and the actual global compile and hosting remain deferred to Effort #34.
- Only standard without-lakes Pfafstetter level 12 inputs are supported. The with-lakes product is unsupported because its matching ancillary pour-points product is unavailable.
- The HydroSHEDS License Agreement permits commercial, non-commercial, and internal use, but prohibits public/open redistribution of the Licensed Materials as a stand-alone product. It covers both polygon and HydroBASINS Pour Points layers; neither inputs nor compiled outputs should be assumed freely redistributable or promised for public hosting.
- Snap features and HydroRIVERS integration remain Effort #33. HydroRIVERS is not an adapter input.
- Planetary mode changes metadata only. The actual planetary compile and hosting remain Effort #34.
