# MERIT Adapter

Status: reserved placeholder. Not yet implemented. MERIT Hydro (Yamazaki et al., 2019) is a planned source fabric — a 3-arcsec global D8 flow-direction + flow-accumulation raster plus derived RWDB reach vectors — and when implemented, the adapter will live here.

## Getting started

Read [`../../docs/ADAPTER_GUIDE.md`](../../docs/ADAPTER_GUIDE.md) for the authoring guide. The canonical worked example is [`../grit/`](../grit/), which covers every production stage from raw input to a dataset that passes `hfx --strict --sample-pct 100`.

## Planned approach

- **Atoms by raster-based partitioning**: sub-basins extracted from the MERIT flow-direction + upstream-area grid at a chosen pour-point density. MERIT ships no pre-vectorized catchment layer; polygon boundaries must be derived from the raster directly.
- **Topology `"tree"`**: D8 fabric has no distributaries, so the graph is a strict tree. Contrast with GRIT, which is `"dag"` due to bifurcations.
- **Snap**: reach vector lines from the RWDB dataset; snap weight set to the reach's upstream drainage area in km², satisfying the v0.2 MUST-monotonic weight contract.
- **`has_up_area = true`**: inclusive upstream area computed directly from the MERIT `uparea` raster per atom, summing accumulation cells within each derived sub-basin.
- **Paired COG rasters**: ship `flow_dir.tif` + `flow_acc.tif` so the engine can refine terminal atoms at sub-atom resolution without re-ingesting the full MERIT source.
- **Scoped first pass**: initial build targets one regional tile (continental subdivision) to keep the build tractable before scaling to global coverage.

To contribute, open a PR that adds a scratch adapter script, a `WORKFLOW.md`, and a `pyproject.toml` following the pattern in [`../../docs/ADAPTER_GUIDE.md`](../../docs/ADAPTER_GUIDE.md).
