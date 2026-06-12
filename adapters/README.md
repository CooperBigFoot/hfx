# Adapters

HFX adapters compile a source hydrofabric into the canonical HFX artifacts (manifest, catchments, graph, optional snap and rasters). Each subdirectory is one adapter. See [`../docs/ADAPTER_GUIDE.md`](../docs/ADAPTER_GUIDE.md) for the authoring guide.

| Path | Status | Purpose |
|---|---|---|
| [`_template/`](_template/) | scaffold | Starting point for new adapters — copy and fill in the nine numbered stages. |
| [`grit-v2/`](grit-v2/) | current reference adapter | Produced the hosted `grit/2.0.0` dataset — `format_version` 0.2.1, `adapter_version` grit-global-2.0.0; planetary dag with segment (level 0) and reach (level 1) drainage units. |
| [`merit-v2/`](merit-v2/) | current reference implementation / worked example | Compiles MERIT-Basins + MERIT Hydro into HFX 0.2.1 locally — tree topology, D8 rasters as `hfx.aux.d8_raster.v1` COGs; per-basin, multi-basin, or planetary builds. |
| [`grit/`](grit/) | historical (v1, format_version 0.1) | Compiled GRIT v1.0 regional segment catchments into the original HFX 0.1 layout. |
| [`merit/`](merit/) | historical (v1, format_version 0.1) | Compiled MERIT-Basins v0.7 / v1.0_bugfix1 vectors plus MERIT Hydro rasters into per-basin HFX 0.1 datasets. |

## Building a New Adapter

The v2 adapters are the current reference implementations and target `format_version` 0.2.1; the v1 `grit/` and `merit/` adapters produced 0.1 datasets and are kept for historical reference only. The MERIT v2 adapter is the worked example to follow when authoring a new adapter: it covers vectors, tree topology, snap stems, and D8 raster auxiliaries end to end, and its output is fully reproducible from public sources.

**HydroBASINS is the most-wanted next adapter.** To build it (or any other adapter): copy [`_template/`](_template/), use [`merit-v2/`](merit-v2/) as the worked example, and see [`../docs/ADAPTER_GUIDE.md`](../docs/ADAPTER_GUIDE.md).
