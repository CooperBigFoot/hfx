# HydroBASINS to HFX Adapter

This adapter compiles the HydroBASINS standard, without-lakes Pfafstetter level 12 product into HFX v0.3.0 core artifacts: `catchments.parquet`, `graph.parquet`, and `manifest.json`. The result is a single-level dataset in EPSG:4326. At the current milestone, each build compiles one HydroBASINS region.

## Inputs

Every regional build requires both source layers:

- `hybas_<region>_lev12_v1.shp`, under the directory supplied to `--basins`.
- `hybas_pour_lev12_v1.shp`, under the directory supplied to `--pour-points`.

The accepted HydroBASINS region codes are `af`, `ar`, `as`, `au`, `eu`, `gr`, `na`, `sa`, and `si`. Both inputs are normalized to EPSG:4326.

Only Pfafstetter level 12 and the standard without-lakes product are supported. The with-lakes variant is unsupported because the HydroBASINS Pour Points ancillary product exists only for the standard variant.

## CLI Reference

Run the adapter from `adapters/hydrobasins` with `uv run python build_adapter.py`.

- `extract` requires `--region`, `--basins`, and `--pour-points`. It inspects the two source layers and reports their paths, feature counts, CRS information, required basin-column presence, and the pour-point join key. It writes no HFX files.
- `build` requires `--region`, `--basins`, `--pour-points`, and `--out`, and optionally accepts `--planetary`. It compiles one region.
- `validate` requires positional `<dataset>` and optionally accepts `--report-dir`. It persists `validator-report.text` and `validator-report.json`, plus the corresponding `.stderr` files when stderr is present. The default report directory is `<dataset>/validation`.

There is no implemented `--regions` or `--all-regions` flag. Multi-region ingestion belongs to a later milestone.

```bash
# Inspect one region's two required source layers; this writes no HFX files.
uv run python build_adapter.py extract \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points

# Compile one regional HFX dataset.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/gr

# Exercise the existing planetary manifest code path on the one-region build.
uv run python build_adapter.py build \
  --region gr \
  --basins /path/to/hydrobasins/standard \
  --pour-points /path/to/hydrobasins/pour-points \
  --out ./out/planetary-code-path \
  --planetary

# Validate an existing dataset and write reports to the default directory.
uv run python build_adapter.py validate ./out/gr

# Validate and select the report directory explicitly.
uv run python build_adapter.py validate ./out/gr \
  --report-dir ./out/gr-validation
```

## Build Sequence

The adapter performs these stages in order:

1. Load and normalize the selected regional polygon layer.
2. Load the ancillary pour-points layer.
3. Join the layers and deterministically assign one outlet per unit.
4. Write `catchments.parquet`.
5. Write `graph.parquet`.
6. Write `manifest.json`.

`catchments.parquet` is structurally validated as GeoParquet 1.1 immediately after it is written.

## Source Mapping and Topology

`outlet_lon` and `outlet_lat` come from the HydroBASINS Pour Points ancillary layer, `hybas_pour_lev12_v1.shp`, joined to sub-basins by `HYBAS_ID`. The pour points are DEM-derived at the highest-flow-accumulation cell. Outlets are not geometrically derived from the polygon and do not depend on HydroRIVERS. The join is total: every unit must resolve to at least one pour point, and a missing join is a build error.

Coastal units can have multiple pour points for one `HYBAS_ID`. The adapter emits one outlet by selecting the candidate nearest the sub-basin centroid. Equal distances are tie-broken by the lowest `(lon, lat)` pair.

`graph.parquet` is a same-level tree with one row per unit and is checked for acyclicity. `NEXT_DOWN = 0` is the terminal sink sentinel. Virtual endorheic edges are cut: a unit with `ENDO = 2` is terminal even if it has a nonzero `NEXT_DOWN`, so its outgoing edge is omitted. A nonzero downstream ID outside the selected region is not silently cut; the single-region build rejects that reference.

## Manifest Behavior

A normal regional build includes the requested `region` and uses the regional geometry bounds. `--planetary` exercises an existing manifest code path that omits `region` and writes the exact bbox `[-180, -90, 180, 90]`. It does not make the current CLI ingest multiple regions and does not mean that a full global build has been run.

## Validation and Conformance

Authoritative HFX conformance uses `hfx-cli --strict --sample-pct 100`. The adapter's `validate` subcommand runs these commands from the repository root and persists their output:

```bash
cargo run -p hfx-cli -- <dataset> --format text --strict --sample-pct 100
cargo run -p hfx-cli -- <dataset> --format json --strict --sample-pct 100
```

GeoParquet 1.1 structural validation of `catchments.parquet` is the second validation layer and occurs during `build`. The `validate` subcommand does not invoke the GeoParquet validator.

## Licensing

The HydroSHEDS License Agreement permits commercial, non-commercial, and internal use, but prohibits public/open redistribution of the Licensed Materials as a stand-alone product. The same license covers the HydroBASINS Pour Points ancillary layer. Neither the source data nor compiled output should be treated as freely redistributable or promised for public hosting.

## Limitations and Deferred Work

- Planetary mode is a code path only. The full global compile and Dropbox hosting are Effort #34 and were not run in this effort.
- The antimeridian guard and its operational resolution are deferred to Effort #34. This adapter delivers the current code path; no antimeridian guard is implemented at `3979f69`.
- Snap features and HydroRIVERS integration are Effort #33. HydroRIVERS is not an input to this adapter.
- The with-lakes product is unsupported because pour points exist only for the standard variant.
- Only Pfafstetter level 12 is supported.
- Multi-region ingestion is a later milestone and is not part of the current single-region CLI.
