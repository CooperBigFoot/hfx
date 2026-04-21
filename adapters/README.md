# Adapters

HFX adapters compile a source hydrofabric into the canonical HFX artifacts (manifest, catchments, graph, optional snap and rasters). Each subdirectory is one adapter. See [`../docs/ADAPTER_GUIDE.md`](../docs/ADAPTER_GUIDE.md) for the authoring guide.

| Path | Status | Purpose |
|---|---|---|
| [`_template/`](_template/) | scaffold | Starting point for new adapters — copy and fill in the nine numbered stages. |
| [`grit/`](grit/) | validated scratch adapter (Europe slice) | Compiles GRIT v1.0 Europe segment catchments into HFX; uses `geoparquet-io==1.0.0b2` and passes `hfx --strict --sample-pct 100`. |
| [`merit/`](merit/) | working adapter (pfaf-27 tested) | Compiles MERIT-Basins v0.7/v1.0_bugfix1 (Lin et al. 2019) vectors plus MERIT Hydro basin-merged rasters (mghydro.com rehost of Yamazaki et al. 2019) into HFX per Pfafstetter Level-2 basin (`fabric_name = "merit_basins_pfaf{NN}"`); tree topology, `has_up_area=true`, `has_snap=true`, `has_rasters=true`; validated on pfaf-27 (Iceland, 1973 atoms) and consumed by pyshed 0.1.7. |
