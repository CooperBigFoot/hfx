# Adapters

HFX adapters compile a source hydrofabric into the canonical HFX artifacts (manifest, catchments, graph, optional snap and rasters). Each subdirectory is one adapter. See [`../docs/ADAPTER_GUIDE.md`](../docs/ADAPTER_GUIDE.md) for the authoring guide.

| Path | Status | Purpose |
|---|---|---|
| [`_template/`](_template/) | scaffold | Starting point for new adapters — copy and fill in the nine numbered stages. |
| [`grit/`](grit/) | validated scratch adapter (Europe slice) | Compiles GRIT v1.0 Europe segment catchments into HFX; uses `geoparquet-io==1.0.0b2` and passes `hfx --strict --sample-pct 100`. |
| [`merit/`](merit/) | reserved, not yet implemented | Planned MERIT Hydro raster-to-HFX adapter. |
