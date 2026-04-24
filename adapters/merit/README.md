# MERIT Adapter

Python adapter that compiles [MERIT-Basins](https://www.reachhydro.org/home/params/merit-basins) vector catchments and reach centerlines together with [MERIT Hydro](http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/) flow-direction and flow-accumulation rasters into one HFX dataset per Pfafstetter Level-2 basin.

## Status

Working. GRIT is the canonical reference adapter; MERIT is the second validated adapter. End-to-end verified on pfaf-27 (Iceland, 1,973 atoms), both with `hfx --strict --sample-pct 100` and against pyshed 0.1.7 delineation.

## Inputs

Two inputs are needed per Pfaf-L2 basin. `<NN>` is the two-digit Pfafstetter Level-2 code (`11`..`91`).

**Vectors — MERIT-Basins v0.7 / v1.0_bugfix1** (Lin et al. 2019). Distributed as a Google Drive share under CC BY-NC-SA 4.0. The shapefiles ship without a `.prj`; stage 1 forces the CRS to EPSG:4326 on load.

**Rasters — MERIT Hydro flow direction and flow accumulation**, basin-merged rehost by M. Heberger at mghydro.com, derived from Yamazaki et al. 2019. Licensing is dual CC BY-NC 4.0 / ODbL 1.0. The mghydro rehost is a non-canonical convenience: the original MERIT Hydro tiles are 5° squares, which would require a separate basin-merge step; mghydro ships the merge result.

## Runbook

### 1. Pre-flight

- [ ] `rclone` installed and configured with a `GoogleDrive:` remote pointing at your Google account.
- [ ] The MERIT-Basins v0.7 / v1.0_bugfix1 share accepted into your Google Drive "Shared with me" so `--drive-shared-with-me` can see it.
- [ ] Chosen Pfafstetter Level-2 code `<NN>` in `11`..`91`.
- [ ] `uv` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`).
- [ ] `hfx` validator CLI on `PATH`. From the repo root: `cargo install --path crates/hfx-validator`.

### 2. Download MERIT-Basins vectors

```bash
rclone copy --drive-shared-with-me \
  "GoogleDrive:MERIT-Hydro_v07_Basins_v01_bugfix1/pfaf_level_02/" \
  ~/data/merit_basins/pfaf_level_02/ \
  --include "*pfaf_<NN>_*"
```

This populates `cat_pfaf_<NN>_*.{shp,shx,dbf,cpg}` and `riv_pfaf_<NN>_*.{shp,shx,dbf,cpg}`. The shapefiles ship without a `.prj`; the adapter forces CRS to EPSG:4326 on load.

### 3. Download MERIT Hydro rasters

Basin-merged flow-direction and flow-accumulation, rehost by M. Heberger:

```bash
mkdir -p ~/data/merit_hydro_rasters/flow_dir_basins ~/data/merit_hydro_rasters/accum_basins

curl -o ~/data/merit_hydro_rasters/flow_dir_basins/flowdir<NN>.tif \
  https://mghydro.com/watersheds/rasters/flow_dir_basins/flowdir<NN>.tif
curl -o ~/data/merit_hydro_rasters/accum_basins/accum<NN>.tif \
  https://mghydro.com/watersheds/rasters/accum_basins/accum<NN>.tif
```

Filenames have the basin code appended directly (no underscore).

### 4. Build one basin

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

The `build` subcommand runs the nine stages plus raster transcoding, then invokes validation automatically (step 5). It also asserts GeoParquet 1.1 conformance on `catchments.parquet` and `snap.parquet` via `geoparquet-io`.

### 5. Validate

To re-run validation on an existing build:

```bash
hfx ./out/hfx --strict --sample-pct 100 --format text
```

Expected advisories on small basins (pfaf-27 and similar):

- `raster.crs_extent_not_implemented` — validator gap for GeoTIFF GeoKey parsing (see `docs/decisions/2026-04-13-post-grit-open-items.md`, open item 2).

This advisory does not fail the build. Any other warning or error is a real issue.

### 6. Try it with pyshed

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

When you are done:

- [ ] All three artifacts (`catchments.parquet`, `graph.arrow`, `manifest.json`) plus the two optional pairs (`snap.parquet`, `flow_dir.tif`, `flow_acc.tif`) are in `./out/hfx/`.
- [ ] `hfx --strict --sample-pct 100` exits zero with only the expected raster advisory for small basins.
- [ ] Citations in [Citations](#citations) still match the source versions you downloaded (MERIT-Basins v0.7 / v1.0_bugfix1, MERIT Hydro via mghydro).

## Batch: build all 61 basins

[`run_missing_basins.py`](run_missing_basins.py) is the Phase 2 batch orchestrator. It downloads any missing raw inputs (vectors via rclone, rasters via curl) and runs `build_adapter.py build` for each of the 61 Pfaf-L2 basins with bounded parallelism (`ProcessPoolExecutor`, default `-j 3`). Output datasets land at `~/Desktop/merit-hfx/per-basin/merit-hfx-pfaf<NN>/`. Per-run logs (including `summary.json` and `summary.txt`) are written to `adapters/merit/batch_logs/<run-id>/` (gitignored).

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

## Global raster mosaic (Phase 2)

`build_global_rasters.py` mosaics the 60 mghydro per-basin TIFs (the same rasters used by `build_adapter.py`) into two planet-wide Cloud-Optimized GeoTIFFs:

- `<output-dir>/flow_dir.tif` — uint8, ESRI D8, NoData=255
- `<output-dir>/flow_acc.tif` — float32, upstream pixel count, NoData=-1.0

**Why polygon-masking before VRT-stitching**: mghydro's per-basin rasters are clipped to each basin's bounding box, not its polygon. Adjacent basins overlap in their bboxes and encode `0` both for "outside polygon" and for valid D8 sinks. The script rasterizes each basin's catchment polygon to disambiguate before stitching, so `gdalbuildvrt` sees strictly non-overlapping valid data.

This script does **not** touch `catchments.parquet`, `graph.arrow`, `snap.parquet`, or `manifest.json`. Those belong to `build_adapter.py` / `run_missing_basins.py`. After the two TIFs land, re-run `merge_basins.py` with `--rasters-ready --force` to flip `has_rasters=true` in the manifest.

### Inputs

The inputs are the same data already on disk from the [Runbook](#runbook) §2–3:

- Vectors: `~/data/merit_basins/pfaf_level_02/cat_pfaf_<NN>_*.shp`
- Rasters: `~/data/merit_hydro_rasters/flow_dir_basins/flowdir<NN>.tif` and `~/data/merit_hydro_rasters/accum_basins/accum<NN>.tif`

No additional registration or download step is required.

### Run

**Smoke-test first** (single basin, ~30 s; confirms tool setup without touching the full ~90 GB of intermediate data):

```bash
cd adapters/merit
uv run python build_global_rasters.py \
  --output-dir /tmp/hfx-merit-global-smoke \
  --basins 27 \
  --skip-overviews
```

**Full build** (~2 hours, ~90 GB peak disk; ensure sufficient free space first):

```bash
uv run python build_global_rasters.py \
  --output-dir ./out/hfx-merit-global
```

**Key flags:**

| Flag | Default | Effect |
|---|---|---|
| `--raster-root PATH` | `~/data/merit_hydro_rasters` | mghydro rasters root |
| `--merit-basins-root PATH` | `~/data/merit_basins/pfaf_level_02` | Catchment shapefiles |
| `--tmp-dir PATH` | `<output-dir>/_tmp_rasters` | Scratch space for masked TIFs |
| `--reference-dir PATH` | per-basin pfaf42 dir | Sanity-check reference |
| `--basins LIST` | all 60 | Comma-separated subset, e.g. `27` |
| `--parallelism` / `-j` | `4` | Parallel masking workers |
| `--skip-overviews` | off | Skip COG overview pyramid |
| `--skip-sanity-check` | off | Skip sanity check against reference |
| `--gdal-cachemax MB` | `4096` | GDAL cache in MB |
| `--log-level LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |

### Integrate

`build_global_rasters.py` writes **only** the two TIFs. After they land adjacent to a manifest previously written by `merge_basins.py` (with `has_rasters=false`), re-run:

```bash
uv run python merge_basins.py \
  --rasters-ready \
  --force \
  --output-root ./out/hfx-merit-global
```

This flips `has_rasters=true` without re-running the full basin merge.

### Troubleshooting

- **`Required input directory missing`** — Verify that `~/data/merit_hydro_rasters` and `~/data/merit_basins/pfaf_level_02` exist (from Runbook §2–3). Pass explicit paths via `--raster-root` and `--merit-basins-root` if they live elsewhere.
- **`flowdir TIF missing` or `accum TIF missing`** — The mghydro curl step (Runbook §3) has not been completed for that basin. Run `run_missing_basins.py download`.
- **`COG validation failed`** — Capture `gdalinfo --version` and re-run with `--log-level DEBUG`. Requires GDAL ≥ 3.5.
- **`sanity check FAILED`** — A pixel-offset mismatch between the global mosaic and the reference. Run `gdalinfo` on both files and compare `Origin` and `Pixel Size`. If the reference was built by an older adapter version (without matching NoData handling), pass `--skip-sanity-check`.
- **pfaf-35 skipped** — Expected. pfaf-35 wraps past 180°E and mghydro clips to 180°E; the script detects the extent mismatch and skips with a WARN.

## Schema and topology decisions

- **Granularity**: one HFX dataset per Pfaf-L2 basin. MERIT-Basins is already partitioned this way upstream; there is no global build.
- **`fabric_name`**: pattern `merit_basins_pfaf{NN}` (e.g. `merit_basins_pfaf27`). Lowercase ASCII, no whitespace.
- **`topology = "tree"`**: MERIT Hydro is strict D8, no distributaries. Contrast with GRIT's `"dag"`.
- **`has_up_area = true`**: `up_area_km2` is joined from `rivers.uparea` (inclusive cumulative km²), which matches HFX semantics exactly. This is clean by construction, unlike GRIT's bifurcation-partitioned area that forced `has_up_area = false`.
- **`has_snap = true`, `has_rasters = true`**: both optional artifacts ship.
- **`is_mainstem`** derivation: MERIT has no native mainstem flag. At each confluence the child with the largest `uparea` wins (tie-break: larger COMID); other children become tributaries. On pfaf-27 this yields 1,147 mainstem / 826 tributary.
- **Terminal sinks**: MERIT's `NextDownID = 0` marks basin outlets. The graph writer skips those edges so the affected atom simply has no downstream parent — the HFX terminal-sink sentinel (id `0`) never appears as an atom id.

## Nine-stage mapping

| Stage | Purpose | MERIT-specific logic |
|---|---|---|
| 1 Inspect source | Load inputs; validate bounds and CRS | Glob `cat_pfaf_<NN>_*.shp` / `riv_pfaf_<NN>_*.shp`; force CRS to EPSG:4326 (no `.prj`); verify cat/riv COMID sets match 1:1 and both rasters cover the catchment bbox. |
| 2 Assign IDs | Produce `int64` positive unique `id` | `id = COMID`; reject zeros, duplicates, and nulls. |
| 3 Reproject | Ensure EPSG:4326 | Validation-only — MERIT-Basins is already WGS84. |
| 4 Make valid | Repair topology | `shapely.make_valid` then coerce any `GeometryCollection` byproducts back to `Polygon` / `MultiPolygon`. |
| 5 Hilbert sort | Enable row-group pruning | Hilbert distance on centroids over `total_bounds`; secondary sort on `id` for determinism. |
| 6 Write catchments | `catchments.parquet` per spec §1 | Join `rivers.uparea` on `COMID` to populate `up_area_km2`; write GeoParquet 1.1 with hand-crafted `geo` metadata and balanced row groups. |
| 7 Write graph | `graph.arrow` adjacency | Invert reach `NextDownID` into per-atom upstream lists; `NextDownID = 0` marks terminal outlets (no parent); iterative DFS cycle check before write. |
| 8 Write snap | `snap.parquet` per spec §3 | Reach LineStrings, 1:1 with catchments; `weight = uparea`; `is_mainstem` derived by largest-uparea descent at each confluence; Hilbert-sorted in the same frame as catchments. |
| 9 Write manifest | `manifest.json` per spec §6 | `fabric_name = "merit_basins_pfaf{NN}"`; `topology = "tree"`; `flow_dir_encoding = "esri"`; bbox padded outward by `1e-4`. |

Raster transcoding runs between stages 8 and 9: flow direction is remapped to uint8 with NoData 255 (MERIT's int8 `-9` reads back as uint8 `247`; anything outside the valid D8 set is folded to NoData), flow accumulation is cast to float32 with NoData `-1.0`, and both are written as COGs cropped to the catchment bbox.

## Rasters

Source is the mghydro basin-merged rehost (see licensing above). The adapter crops each raster to the catchment bbox plus a 10-pixel pad, then emits HFX-conformant Cloud-Optimized GeoTIFFs with Deflate compression and internal 256×256 tiles:

- `flow_dir.tif` — uint8, NoData 255, encoding `"esri"` (MERIT's 1/2/4/8/16/32/64/128 convention).
- `flow_acc.tif` — float32, NoData `-1.0`.

Both pass `rio_cogeo.cog_validate` post-write.

## Troubleshooting

- **`hfx` behaves like an older build after editing the validator.** The `cargo install --path crates/hfx-validator` command installs to `~/.cargo/bin/`; re-run it from the repo root after pulling or editing validator code so the CLI on `PATH` matches the workspace.
- **`rclone` reports no files for the MERIT-Basins share.** The share must first be accepted into your Google Drive "Shared with me"; `--drive-shared-with-me` reads that namespace, not public links. Open the share URL once in a browser while signed in to the same Google account, then retry.
- **`curl` returns HTML instead of a GeoTIFF.** mghydro serves a plain directory listing for unknown paths. Double-check `<NN>` and that the two parent directories (`flow_dir_basins/`, `accum_basins/`) exist locally before writing.

## Known limitations and open items

- `snap.parquet` **is** Hilbert-sorted in MERIT. This closes the conformance gap flagged for GRIT in the GRIT adapter docs.
- Reach-based snap is out of scope by construction: MERIT-Basins reaches are 1:1 with catchments, so the GRIT segment-vs-reach distinction does not apply. Each catchment has exactly one snap LineString.
- Small basins (fewer than 4,096 atoms, e.g. pfaf-27 at 1,973) ship as a single row group and are fully conformant under HFX v0.2.17+.
- Raster CRS and extent containment are checked by the adapter but not by the validator (see [`docs/decisions/2026-04-13-post-grit-open-items.md`](../../docs/decisions/2026-04-13-post-grit-open-items.md), open item 2). The validator emits `raster.crs_extent_not_implemented` as an info-level advisory.
- **pfaf-35 anti-meridian wrap** — the catchment bbox maxx=190.3° wraps past 180°E; mghydro's raster is clipped to 180°E. Basin excluded from the global mosaic. Revisit when MERIT Hydro 5° source tiles or a wrap-aware raster assembly is on the table.
- **pfaf-87 / pfaf-88 missing from mghydro** — Antarctic sub-basins, HTTP 404 for both flowdir and accum at mghydro's directory. Vector shapefiles may still exist in the Lin et al. 2019 release; the global dataset excludes these two basins.
- **`flow_acc.tif` size** — the global raster is 45 GB because raw float32 upstream pixel counts compress poorly. A km² conversion or int32 encoding would roughly halve the file. Revisit once a shed consumer profiles query cost.

For HFX adapter authoring guidance generally, see [`docs/ADAPTER_GUIDE.md`](../../docs/ADAPTER_GUIDE.md).

## Citations

- Lin, P., Pan, M., Beck, H. E., et al. (2019). Global reconstruction of naturalized river flows at 2.94 million reaches. *Water Resources Research*, 55(8), 6499–6516. (MERIT-Basins)
- Yamazaki, D., Ikeshima, D., Sosa, J., Bates, P. D., Allen, G. H., & Pavelsky, T. M. (2019). MERIT Hydro: A high-resolution global hydrography map based on latest topography dataset. *Water Resources Research*, 55(6), 5053–5073.
- Heberger, M. (2023). Basin-merged MERIT Hydro rasters and the `delineator` project. https://mghydro.com/watersheds/
