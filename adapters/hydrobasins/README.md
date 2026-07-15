# HydroBASINS to HFX Adapter

Adapter shell for the HydroBASINS standard, without-lakes Pfafstetter Level 12 product, targeting HFX v0.3.0.

## Current M1 contract

M1 accepts one polygon layer named `hybas_<region>_lev12_v1.shp` for one region. The `--basins` argument names the directory containing that single regional layer.

```bash
uv run python build_adapter.py build --region <r> --basins <dir> --out <dir>
```

This command is a scaffold until M1-S2 implements polygon ingestion. It does not currently compile or validate a dataset.

## Milestone boundaries

- M1 handles one `hybas_<region>_lev12_v1.shp` polygon layer only.
- M2 adds pour-point inputs and real HFX outlet coordinates.
- M3 adds `graph.parquet`.
- M4 adds `manifest.json` and planetary behavior.
- M6 adds multi-region builds, the antimeridian guard, and row-group balancing.
