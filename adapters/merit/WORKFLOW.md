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

- `raster.crs_extent_not_implemented` — validator gap for GeoTIFF GeoKey parsing (see `docs/decisions/2026-04-13-post-grit-open-items.md`, open item 2).

This advisory does not fail the build. Any other warning or error is a real issue.

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
- [ ] `hfx --strict --sample-pct 100` exits zero with only the expected raster advisory for small basins.
- [ ] Citations in [`README.md`](README.md) still match the source versions you downloaded (MERIT-Basins v0.7 / v1.0_bugfix1, MERIT Hydro via mghydro).

## Troubleshooting

- **`hfx` behaves like an older build after editing the validator.** The `cargo install --path crates/hfx-validator` command installs to `~/.cargo/bin/`; re-run it from the repo root after pulling or editing validator code so the CLI on `PATH` matches the workspace.
- **`rclone` reports no files for the MERIT-Basins share.** The share must first be accepted into your Google Drive "Shared with me"; `--drive-shared-with-me` reads that namespace, not public links. Open the share URL once in a browser while signed in to the same Google account, then retry.
- **`curl` returns HTML instead of a GeoTIFF.** mghydro serves a plain directory listing for unknown paths. Double-check `<NN>` and that the two parent directories (`flow_dir_basins/`, `accum_basins/`) exist locally before writing.

## Batch build all basins

`run_missing_basins.py` is the batch orchestrator for Phase 2. It downloads any missing raw inputs (vectors via rclone, rasters via curl) and runs `build_adapter.py build` for each of the 61 Pfaf-L2 basins with bounded parallelism.

### Quick start

```bash
# See which basins are complete/missing:
uv run --directory /Users/nicolaslazaro/Desktop/work/hfx/adapters/merit \
  python run_missing_basins.py list

# Dry-run to inspect commands for a subset before committing:
uv run --directory /Users/nicolaslazaro/Desktop/work/hfx/adapters/merit \
  python run_missing_basins.py run \
    --pfaf-codes missing \
    --skip-downloads \
    --dry-run

# Full batch: download missing inputs + build all missing basins at j=3:
uv run --directory /Users/nicolaslazaro/Desktop/work/hfx/adapters/merit \
  python run_missing_basins.py run \
    --pfaf-codes missing \
    --parallelism 3
```

### Calibration run

Before launching the full batch, run a calibration step against pfaf-42 to measure per-basin wall-clock and peak RSS, then extrapolate total batch time:

```bash
uv run --directory /Users/nicolaslazaro/Desktop/work/hfx/adapters/merit \
  python run_missing_basins.py run \
    --calibrate \
    --calibration-pfaf 42 \
    --pfaf-codes missing
# Prints estimated batch time, then exits unless --calibrate-auto-continue is passed.
```

### Key flags

| Flag | Default | Effect |
|---|---|---|
| `--pfaf-codes` | `all` | `all`, `missing`, or `11,42,91` |
| `--parallelism` / `-j` | `3` | Concurrent `ProcessPoolExecutor` workers |
| `--force` | off | Overwrite already-complete output dirs |
| `--dry-run` | off | Print commands; touch nothing |
| `--skip-downloads` | off | Assume raw inputs already present |
| `--retry-failed` | off | Re-run only failed basins from a prior `--run-id` |
| `--per-basin-timeout-sec` | `10800` | SIGTERM → SIGKILL after this many seconds |
| `--output-root` | `~/Desktop/merit-hfx/per-basin` | HFX output root |

Env vars `HFX_MERIT_OUTPUT_ROOT` and `HFX_MERIT_PARALLELISM` override the respective defaults.

### Output layout

Each successful basin lands at `<output-root>/merit-hfx-pfaf<NN>/`:

```
<output-root>/
  merit-hfx-pfaf11/
    catchments.parquet
    graph.arrow
    manifest.json
    snap.parquet
    flow_dir.tif
    flow_acc.tif
  merit-hfx-pfaf12/
    ...
```

The orchestrator refuses to overwrite an existing `merit-hfx-pfaf<NN>/` that already contains a valid `manifest.json` unless `--force` is passed.

Per-run logs land at `adapters/merit/batch_logs/<run_id>/` (gitignored):

```
batch_logs/<run_id>/
  orchestrator.log      # top-level orchestrator log
  downloads.log         # rclone/curl output
  summary.json          # machine-readable per-basin results
  summary.txt           # human-readable table
  pfaf<NN>.log          # stdout + stderr from adapter for basin NN
  pfaf<NN>.time.txt     # /usr/bin/time -l output (wall-clock + peak RSS)
```

### Rehoming the pfaf-27 reference dataset

If pfaf-27 was already built into `~/Desktop/merit-hfx/merit-hfx-pfaf27/` (the legacy location), the orchestrator detects it there and logs a warning recommending a rehome. This is an operator step — move it manually:

```bash
mv ~/Desktop/merit-hfx/merit-hfx-pfaf27 ~/Desktop/merit-hfx/per-basin/merit-hfx-pfaf27
```

After that `list` will show pfaf-27 as complete at the canonical location.

## 6. Build the global raster mosaic (Phase 2 only)

`build_global_rasters.py` assembles all MERIT Hydro 5-degree tiles into two
planet-wide Cloud-Optimized GeoTIFFs:

- `<output-dir>/flow_dir.tif` — uint8, ESRI D8, NoData=255
- `<output-dir>/flow_acc.tif` — float32, upstream area km², NoData=-1.0

This script does **not** touch `catchments.parquet`, `graph.arrow`,
`snap.parquet`, or `manifest.json`. Those belong to `build_adapter.py` /
`run_missing_basins.py`. After the two TIFs land, re-run `merge_basins.py`
with `--rasters-ready --force` to flip `has_rasters=true` in the manifest.

### 6.1 Register and download MERIT Hydro 5° tiles

1. Visit https://global-hydrodynamics.github.io/MERIT_Hydro/ and fill in the
   Google Form to register. You will receive a download password by email.
2. Download the `dir_*.tar` and `upa_*.tar` tile packages using the provided
   link and password.
3. Unpack into a flat directory layout (no subdirectories inside `dir/` or
   `upa/`):

   ```bash
   mkdir -p ~/data/merit_hydro_5deg/{dir,upa}

   for t in ~/Downloads/merit_hydro/dir_*.tar; do
     tar -xf "$t" -C ~/data/merit_hydro_5deg/dir/ --strip-components=1
   done

   for t in ~/Downloads/merit_hydro/upa_*.tar; do
     tar -xf "$t" -C ~/data/merit_hydro_5deg/upa/ --strip-components=1
   done
   ```

   Expected result: ~400 `*_dir.tif` tiles under `dir/` and ~400 `*_upa.tif`
   tiles under `upa/`. Ocean-only 5° boxes are not distributed by the authors,
   so the exact count is lower than the full 72×36 global grid.

4. Verify the layout:

   ```bash
   ls ~/data/merit_hydro_5deg/dir/ | head -5
   # n00e005_dir.tif  n00e010_dir.tif  ...
   ls ~/data/merit_hydro_5deg/upa/ | head -5
   # n00e005_upa.tif  n00e010_upa.tif  ...
   ```

### 6.2 Run the mosaic build

**Smoke-test first** (validates tool setup against 4 tiles; exits before COG
translate — safe to run even without all tiles downloaded):

```bash
cd adapters/merit
uv run python build_global_rasters.py \
  --source-dir ~/data/merit_hydro_5deg \
  --output-dir ./out/hfx-merit-global \
  --smoke-test \
  --dry-run
```

**Full build** (~1 hour, ~60 GB peak disk; ensure sufficient free space first):

```bash
uv run python build_global_rasters.py \
  --source-dir ~/data/merit_hydro_5deg \
  --output-dir ./out/hfx-merit-global
```

**Optional flags:**

| Flag | Effect |
|---|---|
| `--tmp-dir PATH` | Scratch space (default: `<output-dir>/_tmp_rasters`) |
| `--reference-dir PATH` | Sanity-check global COGs against a known pfaf region |
| `--skip-overviews` | Skip overview pyramid (faster, not fully spatially indexed) |
| `--gdal-cachemax MB` | GDAL cache in MB (default: 4096) |
| `--log-level LEVEL` | DEBUG / INFO / WARNING / ERROR (default: INFO) |

### 6.3 Integrate with merge_basins.py

`build_global_rasters.py` writes **only** the two TIFs. After they land
adjacent to a manifest previously written by `merge_basins.py` (with
`has_rasters=false`), re-run:

```bash
uv run python merge_basins.py \
  --rasters-ready \
  --force \
  --output-root ./out/hfx-merit-global
```

This flips `has_rasters=true` without re-running the full basin merge.

### 6.4 Troubleshooting

- **`Source directory does not exist` or `No *_dir.tif tiles found`** — The
  registration / untar step (§6.1) has not been completed. Follow §6.1 exactly.
- **`Grid alignment check FAILED`** — The tiles may be from a different MERIT
  Hydro version or were unpacked with subdirectory stripping missing. Verify
  the tile origins with `gdalinfo <tile>` and compare to another tile. Redownload
  if in doubt.
- **`COG validation failed`** — Capture the GDAL version (`gdalinfo --version`)
  and re-run with `--log-level DEBUG` to see the full gdal_translate stderr.
  Requires GDAL ≥ 3.5.
- **`sanity check FAILED`** — A pixel-offset mismatch between the global mosaic
  and the reference sub-region. Run `gdalinfo` on both files and compare `Origin`
  and `Pixel Size` fields to detect a systematic shift.
