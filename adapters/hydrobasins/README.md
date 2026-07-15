# HydroBASINS to HFX Adapter

This adapter is a reusable, guarded single- or multi-region compiler for the HydroBASINS standard, without-lakes Pfafstetter level 12 product. It writes the HFX v0.3.0 core artifacts `catchments.parquet`, `graph.parquet`, and `manifest.json` as one level in EPSG:4326.

## Inputs

Every selected region requires two source layers:

- `hybas_<region>_lev12_v1.shp`, directly under the directory supplied to `--basins`.
- `hybas_pour_lev12_v1.shp`, resolved recursively from the applicable pour-point directory.

The nine standard region codes are `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, and `si`. Polygon and pour-point inputs are independently normalized to EPSG:4326 for each selected region.

For `--region`, the directory supplied to `--pour-points` is itself passed to an exactly-one-layer recursive resolver. It may contain exactly one `hybas_pour_lev12_v1.shp` at any depth. For `--regions` and `--all-regions`, the adapter appends each selected code and requires exactly one matching layer below `--pour-points/<code>`. A conventional multi-region layout is:

```text
<basins>/hybas_af_lev12_v1.shp
<basins>/hybas_eu_lev12_v1.shp
<pour-points>/af/hybas_pour_lev12_v1.shp
<pour-points>/eu/hybas_pour_lev12_v1.shp
```

A multi-region build does not read one root-wide pour-point shapefile or infer region membership from point attributes.

Only Pfafstetter level 12 and the standard without-lakes product are supported. The with-lakes variant is unsupported because its matching HydroBASINS Pour Points ancillary product is unavailable.

## CLI Reference

Run the adapter from `adapters/hydrobasins` with `uv run python build_adapter.py`.

```text
build (--region <code> | --regions <code> [<code> ...] | --all-regions) --basins <dir> --pour-points <dir> --out <dir> [--planetary] [--strict-build]
extract --region <code> --basins <dir> --pour-points <dir>
validate <dataset> [--report-dir <dir>]
```

Exactly one build selector is required, and the three selectors are mutually exclusive. `--regions` accepts one or more tokens and splits them on commas or whitespace, so comma-separated, whitespace-separated, and mixed lists are accepted. Selection order is preserved, and duplicate codes are rejected. Explicit `--region` and `--regions` values are resolved through their corresponding filenames rather than parser-validated against the standard-code list.

`--all-regions` scans for the nine standard basin layers in fixed order, `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, `si`, and selects the layers that are present. It does not require all nine, but errors if none is present. `--planetary` and `--strict-build` are independent optional build flags.

`extract` remains a single-region inspection command. It requires `--region`, `--basins`, and `--pour-points`; reports both source paths, feature counts, CRS information, basin-column presence, and the pour-point join key; and writes no HFX artifacts.

`validate` takes the dataset path positionally. Its optional `--report-dir` defaults to `<dataset>/validation`. It persists text and JSON validator reports and the corresponding stderr files when stderr is present.

```bash
# Inspect one region; this writes no HFX files.
uv run python build_adapter.py extract \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points/gr

# Build one region. Its pour-point layer is resolved from the supplied root.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points/gr \
  --out ./out/gr

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

# Build every standard region layer present under --basins.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/all-present-regions

# Reject antimeridian-wrap candidates instead of warning.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/all-present-regions-strict \
  --strict-build

# Exercise planetary manifest semantics with the selected regions.
uv run python build_adapter.py build \
  --all-regions \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/planetary-code-path \
  --planetary \
  --strict-build

# Validate and write reports under <dataset>/validation.
uv run python build_adapter.py validate ./out/af-eu

# Validate with an explicit report directory.
uv run python build_adapter.py validate ./out/af-eu \
  --report-dir ./out/af-eu-validation
```

## Build and Merge Semantics

For every selected region, the adapter independently loads and normalizes the polygons to EPSG:4326, loads and normalizes that region's pour points, and assigns outlets. It concatenates the assigned regional frames into one combined frame, rejects any `HYBAS_ID` duplicated anywhere in the merged union, and performs a fresh global centroid-Hilbert sort with `id` as the deterministic tie-break. The final order is this global re-Hilbert order, not region-block order.

The antimeridian guard then checks the combined units before the adapter writes each artifact once. `catchments.parquet` is structurally validated as GeoParquet 1.1 immediately after writing.

Both `catchments.parquet` and `graph.parquet` use balanced row groups. Files with at least 4,096 units have row-group sizes from 4,096 through 8,192 rows inclusive; this permits one balanced group for 4,096 through 8,192 units. Files with fewer than 4,096 units use one row group.

## Outlets and Graph Boundary

`outlet_lon` and `outlet_lat` come from the DEM-derived HydroBASINS Pour Points ancillary layer at the highest-flow-accumulation cell, joined to polygons by `HYBAS_ID`. They are not geometrically derived from polygons and do not depend on HydroRIVERS. The join is total within every selected region: every unit must have at least one matching point, and a missing match is a hard build error.

When a coastal unit has multiple points, the adapter selects the candidate nearest the sub-basin centroid. Equal-distance ties are broken by the lowest `(lon, lat)` pair.

`graph.parquet` is a same-level, tree-shaped, acyclic graph with one row per unit. `NEXT_DOWN = 0` is the terminal sink sentinel. A unit with `ENDO = 2` is terminal even when `NEXT_DOWN` is nonzero, so its virtual outgoing edge is cut.

The graph is constructed over the entire combined merged union. A non-endorheic `NEXT_DOWN` edge in one selected region can therefore resolve to a unit in another selected region. Any nonzero, non-endorheic downstream target absent from the complete merged union is a hard build error; it is not converted into an implicit terminal or boundary cut.

## Antimeridian Guard

For each combined EPSG:4326 unit, the build computes the raw stored longitude extent as `maxx - minx`. Any polygon whose extent is strictly greater than 180 degrees is an antimeridian-wrap candidate. Default builds warn and continue writing artifacts. With `--strict-build`, the same candidates cause a build error before artifacts are written.

The guard detects and reports candidates only; it does not unwrap or split geometry. Operational resolution, including unwrapping or splitting and the actual full global operation, remains deferred to Effort #34.

## Manifest Behavior

For a normal non-planetary build, `manifest.json` uses the selected codes joined by commas in selection order as `region`, such as `af,eu`, and uses the combined geometry bounds. For `--all-regions`, the label follows the fixed standard-code scan order for the regions actually present.

`--planetary` omits `region` and writes the exact bbox `[-180, -90, 180, 90]`. Planetary mode changes manifest metadata only. It does not download or compile missing regions; the actual global compile and hosting remain deferred to Effort #34.

## Validation and Conformance

Authoritative HFX conformance uses `hfx-cli --strict --sample-pct 100`. The adapter's `validate` subcommand runs text and JSON modes from the repository root and persists their output:

```bash
cargo run -p hfx-cli -- <dataset> --format text --strict --sample-pct 100
cargo run -p hfx-cli -- <dataset> --format json --strict --sample-pct 100
```

GeoParquet 1.1 structural validation of `catchments.parquet` occurs during `build`; `validate` does not repeat it. Test and conformance coverage includes a two-region merge smoke that exercises clean concatenation and global `HYBAS_ID` uniqueness. This documentation step does not run a source-data global build.

## Licensing

The HydroSHEDS License Agreement permits commercial, non-commercial, and internal use, but prohibits public/open redistribution of the Licensed Materials as a stand-alone product. The same license covers the HydroBASINS Pour Points ancillary layer. Neither the inputs nor compiled output should be assumed freely redistributable or promised for public hosting.

## Limitations and Deferred Work

- The actual planetary compile and hosting remain Effort #34.
- Antimeridian detection is implemented; operational unwrapping or splitting remains Effort #34.
- Snap features and HydroRIVERS integration are Effort #33. HydroRIVERS is not an adapter input.
- The with-lakes product is unsupported because its matching pour-points product is unavailable.
- Only Pfafstetter level 12 is supported.
