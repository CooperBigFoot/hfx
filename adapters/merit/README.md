# MERIT Adapter

Python adapter that compiles [MERIT-Basins](https://www.reachhydro.org/home/params/merit-basins) vector catchments and reach centerlines together with [MERIT Hydro](http://hydro.iis.u-tokyo.ac.jp/~yamadai/MERIT_Hydro/) flow-direction and flow-accumulation rasters into one HFX dataset per Pfafstetter Level-2 basin.

## Status

Working. GRIT is the canonical reference adapter; MERIT is the second validated adapter. End-to-end verified on pfaf-27 (Iceland, 1,973 atoms), both with `hfx --strict --sample-pct 100` and against pyshed 0.1.7 delineation.

## Starting point

Two inputs are needed per Pfaf-L2 basin. `<NN>` is the two-digit Pfafstetter Level-2 code (`11`..`91`).

**Vectors — MERIT-Basins v0.7 / v1.0_bugfix1** (Lin et al. 2019). Distributed as a Google Drive share under CC BY-NC-SA 4.0:

```bash
rclone copy --drive-shared-with-me \
  "GoogleDrive:MERIT-Hydro_v07_Basins_v01_bugfix1/pfaf_level_02/" \
  ~/data/merit_basins/pfaf_level_02/ \
  --include "*pfaf_<NN>_*"
```

The shapefiles ship without a `.prj`; stage 1 forces the CRS to EPSG:4326 on load.

**Rasters — MERIT Hydro flow direction and flow accumulation**, basin-merged rehost by M. Heberger at mghydro.com, derived from Yamazaki et al. 2019. Licensing is dual CC BY-NC 4.0 / ODbL 1.0:

```bash
curl -o ~/data/merit_hydro_rasters/flow_dir_basins/flowdir<NN>.tif \
  https://mghydro.com/watersheds/rasters/flow_dir_basins/flowdir<NN>.tif
curl -o ~/data/merit_hydro_rasters/accum_basins/accum<NN>.tif \
  https://mghydro.com/watersheds/rasters/accum_basins/accum<NN>.tif
```

Basin code is appended directly, no underscore. The mghydro rehost is a non-canonical convenience: the original MERIT Hydro tiles are 5° squares, which would require a separate basin-merge step; mghydro ships the merge result.

## Running the adapter

```bash
uv sync
uv run python build_adapter.py build \
  --merit-basins ~/data/merit_basins/pfaf_level_02 \
  --rasters ~/data/merit_hydro_rasters \
  --pfaf 27 \
  --out ./out
```

See [`WORKFLOW.md`](WORKFLOW.md) for the full runbook including download, validation, and end-to-end verification with pyshed.

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

## Schema and topology decisions

- **Granularity**: one HFX dataset per Pfaf-L2 basin. MERIT-Basins is already partitioned this way upstream; there is no global build.
- **`fabric_name`**: pattern `merit_basins_pfaf{NN}` (e.g. `merit_basins_pfaf27`). Lowercase ASCII, no whitespace.
- **`topology = "tree"`**: MERIT Hydro is strict D8, no distributaries. Contrast with GRIT's `"dag"`.
- **`has_up_area = true`**: `up_area_km2` is joined from `rivers.uparea` (inclusive cumulative km²), which matches HFX semantics exactly. This is clean by construction, unlike GRIT's bifurcation-partitioned area that forced `has_up_area = false`.
- **`has_snap = true`, `has_rasters = true`**: both optional artifacts ship.
- **`is_mainstem`** derivation: MERIT has no native mainstem flag. At each confluence the child with the largest `uparea` wins (tie-break: larger COMID); other children become tributaries. On pfaf-27 this yields 1,147 mainstem / 826 tributary.
- **Terminal sinks**: MERIT's `NextDownID = 0` marks basin outlets. The graph writer skips those edges so the affected atom simply has no downstream parent — the HFX terminal-sink sentinel (id `0`) never appears as an atom id.

## Rasters

Source is the mghydro basin-merged rehost (see licensing above). The adapter crops each raster to the catchment bbox plus a 10-pixel pad, then emits HFX-conformant Cloud-Optimized GeoTIFFs with Deflate compression and internal 256×256 tiles:

- `flow_dir.tif` — uint8, NoData 255, encoding `"esri"` (MERIT's 1/2/4/8/16/32/64/128 convention).
- `flow_acc.tif` — float32, NoData `-1.0`.

Both pass `rio_cogeo.cog_validate` post-write.

## Known limitations and open items

- `snap.parquet` **is** Hilbert-sorted in MERIT. This closes the conformance gap flagged for GRIT in `adapters/grit/WORKFLOW.md`.
- Reach-based snap is out of scope by construction: MERIT-Basins reaches are 1:1 with catchments, so the GRIT segment-vs-reach distinction does not apply. Each catchment has exactly one snap LineString.
- Small basins (fewer than 4,096 atoms, e.g. pfaf-27 at 1,973) trigger advisory `schema.catchments.rg_size` and `schema.snap.rg_size` warnings because a single row group falls below the strict-mode floor. This is unavoidable given the Pfaf-L2 granularity and is accepted as a known deviation.
- Raster CRS and extent containment are checked by the adapter but not by the validator (see [`docs/decisions/2026-04-13-post-grit-open-items.md`](../../docs/decisions/2026-04-13-post-grit-open-items.md), open item 2). The validator emits `raster.crs_extent_not_implemented` as an info-level advisory.

For HFX adapter authoring guidance generally, see [`docs/ADAPTER_GUIDE.md`](../../docs/ADAPTER_GUIDE.md).

## Citations

- Lin, P., Pan, M., Beck, H. E., et al. (2019). Global reconstruction of naturalized river flows at 2.94 million reaches. *Water Resources Research*, 55(8), 6499–6516. (MERIT-Basins)
- Yamazaki, D., Ikeshima, D., Sosa, J., Bates, P. D., Allen, G. H., & Pavelsky, T. M. (2019). MERIT Hydro: A high-resolution global hydrography map based on latest topography dataset. *Water Resources Research*, 55(6), 5053–5073.
- Heberger, M. (2023). Basin-merged MERIT Hydro rasters and the `delineator` project. https://mghydro.com/watersheds/
