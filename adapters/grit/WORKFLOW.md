# GRIT Europe Scratch Workflow

This adapter is intentionally a scratch ETL for validating the HFX spec against the first real GRIT input.

## Environment

Use `uv` from the repo root. The outer GRIT archive path must be supplied via `--outer-archive` or the `GRIT_OUTER_ARCHIVE` environment variable:

```bash
export GRIT_OUTER_ARCHIVE=/path/to/17435232.zip

uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py extract
uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py build
uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py validate
```

Or pass it inline:

```bash
uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py \
    --outer-archive /path/to/17435232.zip build
```

## Input

The script reads the outer GRIT archive supplied via `--outer-archive PATH` or the `GRIT_OUTER_ARCHIVE` environment variable, and extracts only these Europe `EPSG:4326` members into `<root>/input`:

- `GRITv1.0_segments_EU_EPSG4326.gpkg.zip`
- `GRITv1.0_segment_catchments_EU_EPSG4326.gpkg.zip`
- `GRITv1.0_reaches_EU_EPSG4326.gpkg.zip`

## Mapping

### `catchments.parquet`

- HFX atom = GRIT segment catchment
- `id` = `global_id`
- `area_km2` = `area`
- `up_area_km2` = null for all rows
- bbox columns = geometry bounds
- geometry = WKB polygon or multipolygon

### `graph.arrow`

- `id` = GRIT segment `global_id`
- `upstream_ids` = parsed `upstream_line_ids`

### `snap.parquet`

- HFX snap row = GRIT segment line in the current working validation build
- `id` = segment `global_id`
- `catchment_id` = segment `global_id`
- `weight` = segment `drainage_area_out` — satisfies the v0.2 requirement that weight MUST be monotonically increasing in drainage dominance (higher weight = more hydrologically significant reach)
- `is_mainstem` = segment `is_mainstem == 1`
- geometry = segment WKB linestring

The engine default snap strategy (v0.2) is a weight-first cascade: filter by radius → rank by weight (highest preferred) → tie-break by mainstem preference → tie-break by distance → tie-break by snap id ascending.

## Known deliberate choices

- `has_up_area = false` because published GRIT drainage-area fields are partitioned at bifurcations and do not represent HFX inclusive upstream area.
- `has_rasters = false` for this exercise.
- Reach-based snap was inspected but not used for the final working Europe build because the reach layer lacks `is_mainstem` and its drainage-area columns are null in this slice.
- The validated working fallback is segment-line snap targets.
- `catchments.parquet` is Hilbert-sorted by centroid. `snap.parquet` is currently written in source order; the report must call that out as a conformance gap because the current validator does not enforce Hilbert sorting there.
