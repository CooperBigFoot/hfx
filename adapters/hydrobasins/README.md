# HydroBASINS to HFX Adapter

Adapter shell for the HydroBASINS standard, without-lakes Pfafstetter Level 12 product, targeting HFX v0.3.0.

## Current M1 contract

M1 accepts one polygon layer named `hybas_<region>_lev12_v1.shp` for one region. The `--basins` argument names the directory containing that single regional layer.

```bash
uv run python build_adapter.py build --region <r> --basins <dir> --out <dir>
```

The command writes a single-region, Hilbert-sorted `<out>/catchments.parquet`.
The GeoParquet 1.1 file contains WKB geometry, a non-nullable `bbox` covering,
min/max statistics on every bbox leaf, and balanced row groups between 4,096 and
8,192 rows (with one row group for smaller inputs).

**M1 outlet warning:** `outlet_lon` and `outlet_lat` are non-nullable `float64`
columns containing `NaN` placeholders. This is intentionally non-conformant at
the HFX semantic level. M2 replaces these placeholders with coordinates from the
HydroBASINS pour-points layer.

GeoParquet 1.1 structural validation runs immediately after writing. Full
`hfx-cli --strict` validation is deferred until real outlets, `graph.parquet`,
and `manifest.json` exist.

## Milestone boundaries

- M1 handles one `hybas_<region>_lev12_v1.shp` polygon layer and writes the
  catchments slice with balanced row groups.
- M2 adds pour-point inputs and real HFX outlet coordinates.
- M3 adds `graph.parquet`.
- M4 adds `manifest.json` and planetary behavior.
- M6 adds multi-region builds and the antimeridian guard.
