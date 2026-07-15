# HydroBASINS Adapter Workflow

This workflow applies only to the HydroBASINS standard, without-lakes Pfafstetter Level 12 product.

## Operator sequence

From `adapters/hydrobasins`:

1. Prepare the Python environment:

   ```bash
   uv sync
   ```

2. Inspect the command shell:

   ```bash
   uv run python build_adapter.py --help
   ```

3. Invoke the build-shaped command for one regional polygon layer named `hybas_<region>_lev12_v1.shp`:

   ```bash
   uv run python build_adapter.py build --region <r> --basins <dir> --out <dir>
   ```

   `--basins` names the directory containing that one regional layer. The invocation is intentionally non-functional until M1-S2 implements polygon ingestion.

## Milestone boundaries

- M1 handles one `hybas_<region>_lev12_v1.shp` polygon layer only.
- M2 adds pour-point inputs and real HFX outlet coordinates.
- M3 adds `graph.parquet`.
- M4 adds `manifest.json` and planetary behavior.
- M6 adds multi-region builds, the antimeridian guard, and row-group balancing.
