# MERIT Adapter Workflow

Step-by-step runbook for compiling one Pfafstetter Level-2 basin into HFX. Examples use pfaf-27 (Iceland) because it is small (~2k atoms) and fast to validate.

## Pre-flight checklist

- [ ] `rclone` installed and configured with a `GoogleDrive:` remote pointing at your Google account.
- [ ] The MERIT-Basins v0.7 / v1.0_bugfix1 share accepted into your Google Drive "Shared with me" so `--drive-shared-with-me` can see it.
- [ ] Chosen Pfafstetter Level-2 code `<NN>` in `11`..`91`.
- [ ] `uv` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`).
- [ ] `hfx` validator CLI on `PATH`. From the repo root: `cargo install --path crates/hfx-validator`.

## 1. Download MERIT-Basins vectors

```bash
rclone copy --drive-shared-with-me \
  "GoogleDrive:MERIT-Hydro_v07_Basins_v01_bugfix1/pfaf_level_02/" \
  ~/data/merit_basins/pfaf_level_02/ \
  --include "*pfaf_<NN>_*"
```

This populates `cat_pfaf_<NN>_*.{shp,shx,dbf,cpg}` and `riv_pfaf_<NN>_*.{shp,shx,dbf,cpg}`. The shapefiles ship without a `.prj`; the adapter forces CRS to EPSG:4326 on load.

## 2. Download MERIT Hydro rasters

Basin-merged flow-direction and flow-accumulation, rehost by M. Heberger:

```bash
mkdir -p ~/data/merit_hydro_rasters/flow_dir_basins ~/data/merit_hydro_rasters/accum_basins

curl -o ~/data/merit_hydro_rasters/flow_dir_basins/flowdir<NN>.tif \
  https://mghydro.com/watersheds/rasters/flow_dir_basins/flowdir<NN>.tif
curl -o ~/data/merit_hydro_rasters/accum_basins/accum<NN>.tif \
  https://mghydro.com/watersheds/rasters/accum_basins/accum<NN>.tif
```

Filenames have the basin code appended directly (no underscore).

## 3. Build

```bash
uv sync

# Optional: inspect the inputs first without writing anything
uv run python build_adapter.py extract \
  --merit-basins ~/data/merit_basins/pfaf_level_02 \
  --rasters ~/data/merit_hydro_rasters \
  --pfaf 27

# Full build into ./out/hfx
uv run python build_adapter.py build \
  --merit-basins ~/data/merit_basins/pfaf_level_02 \
  --rasters ~/data/merit_hydro_rasters \
  --pfaf 27 \
  --out ./out
```

The `build` subcommand runs the nine stages plus raster transcoding, then invokes validation automatically (step 4). It also asserts GeoParquet 1.1 conformance on `catchments.parquet` and `snap.parquet` via `geoparquet-io`.

## 4. Validate

To re-run validation on an existing build:

```bash
hfx ./out/hfx --strict --sample-pct 100 --format text
```

Expected advisories on small basins (pfaf-27 and similar):

- `schema.catchments.rg_size` — row group below the 4,096 strict-mode floor because the basin is small.
- `schema.snap.rg_size` — same reason for the snap table.
- `raster.crs_extent_not_implemented` — validator gap for GeoTIFF GeoKey parsing (see `docs/decisions/2026-04-13-post-grit-open-items.md`, open item 2).

All three are advisory and do not fail the build. Any other warning or error is a real issue.

## 5. Try it with pyshed

Round-trip verification — pyshed 0.1.7 consumes the built dataset and delineates without fabric-specific code:

```bash
pip install pyshed
```

```python
import pyshed

engine = pyshed.Engine("./out/hfx")
result = engine.delineate(lat=64.10, lon=-21.82)  # Hvítá, Iceland
print(f"drainage area: {result.area_km2:.1f} km2")
# Expected: 287.5 km2
```

## When you are done

- [ ] All three artifacts (`catchments.parquet`, `graph.arrow`, `manifest.json`) plus the two optional pairs (`snap.parquet`, `flow_dir.tif`, `flow_acc.tif`) are in `./out/hfx/`.
- [ ] `hfx --strict --sample-pct 100` exits zero with only the three expected advisories for small basins.
- [ ] Citations in [`README.md`](README.md) still match the source versions you downloaded (MERIT-Basins v0.7 / v1.0_bugfix1, MERIT Hydro via mghydro).

## Troubleshooting

- **`hfx` behaves like an older build after editing the validator.** The `cargo install --path crates/hfx-validator` command installs to `~/.cargo/bin/`; re-run it from the repo root after pulling or editing validator code so the CLI on `PATH` matches the workspace.
- **`rclone` reports no files for the MERIT-Basins share.** The share must first be accepted into your Google Drive "Shared with me"; `--drive-shared-with-me` reads that namespace, not public links. Open the share URL once in a browser while signed in to the same Google account, then retry.
- **`curl` returns HTML instead of a GeoTIFF.** mghydro serves a plain directory listing for unknown paths. Double-check `<NN>` and that the two parent directories (`flow_dir_basins/`, `accum_basins/`) exist locally before writing.
