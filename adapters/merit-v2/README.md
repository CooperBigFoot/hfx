# MERIT v2 → HFX Adapter

Python adapter that compiles [MERIT-Basins](https://www.reachhydro.org/home/params/merit-basins) vector hydrography and MERIT Hydro D8 rasters into HFX v0.3.0 datasets — one dataset per Pfafstetter Level-2 basin, or multi-basin and planetary builds.

## Status

Current reference implementation. There is **no hosted MERIT dataset** — the only hosted HFX reference dataset is GRIT 2.0.0 (see the root [README](../../README.md#datasets)). MERIT HFX datasets are locally reproducible: anyone can compile them from the public sources below with this adapter.

This adapter doubles as the **worked example** for authoring new adapters — for instance the most-wanted HydroBASINS adapter. Copy [`adapters/_template/`](../_template/), follow this adapter end to end, and see [`docs/ADAPTER_GUIDE.md`](../../docs/ADAPTER_GUIDE.md).

## Manifest Facts

| Property | Value |
|---|---|
| `format_version` | 0.3.0 |
| `fabric_name` | merit_basins |
| `fabric_version` | v0.7_bugfix1 |
| `adapter_version` | 0.2.0 |
| `topology` | tree |
| CRS | EPSG:4326 |
| `has_up_area` | true |

D8 rasters are transcoded to Cloud-Optimized GeoTIFFs at `aux/d8/pfaf_<NN>/flow_dir.tif` and `aux/d8/pfaf_<NN>/flow_acc.tif`, declared as `hfx.aux.d8_raster.v1` auxiliary entries with `flow_dir_encoding = "esri"`. Reach centerlines ship as one `hfx.aux.snap.v2` index (`stems`) at `aux/snap_stems.parquet`.

## Inputs (public sources)

**Vectors — MERIT-Basins v0.7 / v1.0_bugfix1** (Lin et al. 2019). Distributed as a Google Drive share via [reachhydro.org](https://www.reachhydro.org/home/params/merit-basins) under CC BY-NC-SA 4.0. The shapefiles ship without a `.prj`; the adapter forces the CRS to EPSG:4326 on load.

**Rasters — MERIT Hydro flow direction and flow accumulation**, basin-merged rehost by M. Heberger at [mghydro.com](https://mghydro.com/watersheds/), derived from Yamazaki et al. 2019.

## Valid Basins

MERIT coverage is a **sparse** set of Pfafstetter Level-2 codes, not a dense `11`–`91` range (`VALID_PFAF_CODES` in `build_adapter.py`):

| Group | Codes |
|---|---|
| 1x | 11–18 |
| 2x | 21–29 |
| 3x | 31–36 |
| 4x | 41–49 |
| 5x | 51–57 |
| 6x | 61–67 |
| 7x | 71–78 |
| 8x | 81–86 |
| 9x | 91 |

Codes 87 and 88 (Antarctic) are absent from the mghydro raster distribution. pfaf-35 is additionally excluded from planetary builds because it crosses the antimeridian raster cut.

## How to Run

```bash
uv run python build_adapter.py build \
  --merit-basins ~/data/merit_basins/pfaf_level_02 \
  --rasters ~/data/merit_hydro_rasters \
  --pfaf 27 \
  --out ./out
```

`build` flags:

| Flag | Effect |
|---|---|
| `--merit-basins <dir>` | Directory containing the `cat_pfaf_<NN>_*` / `riv_pfaf_<NN>_*` shapefiles |
| `--rasters <dir>` | Directory containing `flow_dir_basins/` and `accum_basins/` |
| `--pfaf <NN>` | Build a single Pfaf-L2 basin |
| `--pfaf-codes <NN,NN,...>` | Comma-separated multi-basin build, e.g. `27,42,91` |
| `--all-basins` | Build all 60 included basins (`VALID_PFAF_CODES` minus {35}); mutually exclusive with `--pfaf`/`--pfaf-codes` |
| `--planetary` | Emit a planetary-mode dataset: literal planet bbox, no `region` key, coverage README |
| `--out <dir>` | Output root |
| `--force` | Replace an existing output directory |
| `--log-level <LEVEL>` | Python logging level (default `INFO`) |

Validate an existing dataset with the adapter's own subcommand:

```bash
uv run python build_adapter.py validate <dataset>
```

## Cross-Basin Preflight

Before multi-basin or planetary compilation, run [`verify_cross_basin.py`](verify_cross_basin.py) — it verifies cross-basin COMID uniqueness, `NextDownID` references, schema consistency, and raster availability (optionally downloading missing inputs with `--download-missing`). It is covered by [`test_verify_cross_basin.py`](test_verify_cross_basin.py).

```bash
uv run python verify_cross_basin.py \
  --merit-basins ~/data/merit_basins/pfaf_level_02 \
  --rasters ~/data/merit_hydro_rasters
```

## Validate Locally

```bash
cargo run -p hfx-cli -- /path/to/dataset --strict
```

## Citations

- Lin, P., Pan, M., Beck, H. E., et al. (2019). Global reconstruction of naturalized river flows at 2.94 million reaches. *Water Resources Research*, 55(8), 6499–6516. (MERIT-Basins, CC BY-NC-SA 4.0)
- Yamazaki, D., Ikeshima, D., Sosa, J., Bates, P. D., Allen, G. H., & Pavelsky, T. M. (2019). MERIT Hydro: A high-resolution global hydrography map based on latest topography dataset. *Water Resources Research*, 55(6), 5053–5073.
- Heberger, M. (2023). Basin-merged MERIT Hydro rasters and the `delineator` project. https://mghydro.com/watersheds/ (dual CC BY-NC 4.0 / ODbL 1.0)
