# HydroBASINS Adapter Workflow

This workflow applies only to the HydroBASINS standard, without-lakes Pfafstetter Level 12 product.

## Operator sequence

From `adapters/hydrobasins`:

1. Prepare the Python environment:

   ```bash
   uv sync
   ```

2. Verify the adapter imports:

   ```bash
   uv run python -c "import build_adapter"
   ```

3. Run the synthetic write-path suite:

   ```bash
   uv run python -m unittest discover -p "test_*.py"
   ```

The suite creates a real temporary shapefile, invokes the command dispatch path,
and verifies the resulting GeoParquet file without requiring HydroBASINS data.

For an available regional polygon layer named
`hybas_<region>_lev12_v1.shp`, the command is:

   ```bash
   uv run python build_adapter.py build --region <r> --basins <dir> --out <dir>
   ```

`--basins` names the directory containing that one regional layer. The command
writes a Hilbert-sorted `<out>/catchments.parquet` with WKB geometry, a
non-nullable bbox covering and bbox-leaf statistics, and balanced row groups.

**M1 outlet warning:** the non-nullable `float64` `outlet_lon` and `outlet_lat`
columns contain only `NaN` placeholders. They intentionally do not satisfy full
HFX outlet semantics. M2 replaces them with HydroBASINS pour-point coordinates.
GeoParquet 1.1 structural validation runs during the build. A real-region build
is not an M1-S3 completion gate because source data is unavailable locally.
`hfx-cli --strict` is also deferred until real outlets, `graph.parquet`, and
`manifest.json` exist.

## Milestone boundaries

- M1 handles one `hybas_<region>_lev12_v1.shp` polygon layer and writes the
  catchments slice with balanced row groups.
- M2 adds pour-point inputs and real HFX outlet coordinates.
- M3 adds `graph.parquet`.
- M4 adds `manifest.json` and planetary behavior.
- M6 adds multi-region builds and the antimeridian guard.
