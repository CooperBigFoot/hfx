# HydroBASINS Adapter Workflow

This operator workflow compiles one region of the HydroBASINS standard, without-lakes Pfafstetter level 12 product into HFX v0.3.0.

## Prepare Inputs and Environment

Arrange both required layers:

- `hybas_<region>_lev12_v1.shp` under the basin directory.
- `hybas_pour_lev12_v1.shp` under the pour-point directory.

Select one region code from `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, or `si`. From `adapters/hydrobasins`, prepare the environment:

```bash
uv sync
```

## Inspect the Sources

Run `extract` before compilation. It reports the paths, feature counts, CRS information, required basin-column presence, and pour-point join key without writing HFX files.

```bash
# Inspect one region's two required source layers; this writes no HFX files.
uv run python build_adapter.py extract \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points
```

## Compile One Region

```bash
# Compile one regional HFX dataset.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/gr
```

The build executes in this order:

1. Load and normalize the regional polygons to EPSG:4326.
2. Load and normalize the ancillary pour points.
3. Perform a total `HYBAS_ID` join and deterministically collapse multiple candidates to one outlet. The nearest point to the sub-basin centroid wins, with the lowest `(lon, lat)` pair breaking equal-distance ties.
4. Write `catchments.parquet` and immediately run its GeoParquet 1.1 structural check.
5. Write the same-level `graph.parquet`, cutting outgoing edges for `ENDO = 2` units and checking acyclicity.
6. Write `manifest.json` with the requested region and regional geometry bounds.

The optional planetary manifest path can be exercised on a one-region build:

```bash
# Exercise the existing planetary manifest code path on the one-region build.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/planetary-code-path \
  --planetary
```

`--planetary` omits `region` and writes the exact manifest bbox `[-180, -90, 180, 90]`. It changes manifest semantics only; it is not a multi-region or global compile.

## Validate the Dataset

Run strict, 100-percent HFX validation in both text and JSON formats:

```bash
# Validate an existing dataset and write reports to the default directory.
uv run python build_adapter.py validate ./out/gr

# Validate and select the report directory explicitly.
uv run python build_adapter.py validate ./out/gr \
  --report-dir ./out/gr-validation
```

By default, reports are written under `./out/gr/validation` as `validator-report.text` and `validator-report.json`. With `--report-dir`, they are written to the selected directory. A corresponding `.stderr` file is also written for each format when stderr is present. The subcommand invokes the repository validator with `--strict --sample-pct 100`; it does not run the GeoParquet validator, which already runs during `build`.

Run the unchanged unit-test suite with:

```bash
uv run python -m unittest discover -p "test_*.py"
```

## Output and Validation Checklist

- `catchments.parquet`, `graph.parquet`, and `manifest.json` are present.
- There is one catchments row and one graph row per HydroBASINS unit.
- Ancillary `outlet_lon` and `outlet_lat` values are finite and in range.
- Every unit resolved through the total pour-point `HYBAS_ID` join.
- The same-level graph is tree-shaped and acyclic.
- Units with `ENDO = 2` are terminal even when `NEXT_DOWN` is nonzero.
- `NEXT_DOWN = 0` is treated as the terminal sink sentinel.
- HFX validation passed in strict mode with a 100-percent sample.
- `catchments.parquet` passed GeoParquet 1.1 structural validation during the build.

A nonzero downstream ID that is absent from the selected region is rejected rather than silently cut.

## Operational Cautions

- Only standard without-lakes Pfafstetter level 12 inputs are accepted. The with-lakes product is unsupported because the ancillary pour points exist only for the standard variant.
- The pour points are DEM-derived at the highest-flow-accumulation cell. They are not derived from polygon geometry and are not HydroRIVERS data; HydroRIVERS is not an adapter input.
- The HydroSHEDS License Agreement permits commercial, non-commercial, and internal use, but prohibits public/open redistribution of the Licensed Materials as a stand-alone product. This restriction applies to both the polygon and HydroBASINS Pour Points layers, so neither inputs nor compiled outputs should be assumed freely redistributable.
- `--planetary` changes manifest semantics but is not a global compile. The full global compile and Dropbox hosting were not run and are deferred to Effort #34.
- The antimeridian guard and its operational resolution are deferred to Effort #34; no guard is implemented at `3979f69`.
- Snap features and HydroRIVERS integration are deferred to Effort #33.
- Multi-region ingestion is deferred to a later milestone; the current CLI compiles one region and has no `--regions` or `--all-regions` flag.
