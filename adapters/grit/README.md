# GRIT → HFX Adapter

Scratch Python adapter that compiles [GRIT](https://www.reachhydro.org/home/params/grit) hydrofabric data into HFX format.

## Status

Validated against the Europe EPSG:4326 slice (150,325 segment catchments). Produces a strictly valid HFX dataset as confirmed by `hfx validate --strict --sample-pct 100`.

This is a single-script scratch adapter for spec validation, not a production pipeline.

## How to Run

Supply the outer GRIT archive via `--outer-archive` or the `GRIT_OUTER_ARCHIVE` environment variable:

```bash
# Using the flag
uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py \
    --outer-archive /path/to/17435232.zip build

# Using the environment variable
export GRIT_OUTER_ARCHIVE=/path/to/17435232.zip
uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py build
uv run --project adapters/grit python adapters/grit/build_grit_eu_hfx.py validate
```

See `WORKFLOW.md` for the full step-by-step workflow.

## Requirements

- Python >= 3.11
- Dependencies managed via `uv` (see `pyproject.toml`)
- Key libraries: geopandas, polars, pyarrow, pyogrio, shapely

## Mapping Summary

### HFX Atom = GRIT Segment Catchment

GRIT organizes its data at two granularity levels: **segments** (~150K in Europe) and **reaches** (~1.9M in Europe). Each segment has exactly one catchment polygon. The HFX adapter maps each GRIT segment catchment to one HFX catchment atom, using the segment `global_id` as the HFX atom `id`.

### Graph: Segment `upstream_line_ids`

The GRIT segment table carries an `upstream_line_ids` field (CSV-encoded list of segment `global_id` values). These map directly to HFX `graph.arrow` `upstream_ids`. The graph topology is `"dag"` because GRIT includes bifurcations (distributaries).

### Snap: Segment Lines (not Reach Lines)

The adapter uses **segment lines** as snap targets rather than reach lines. This was a pragmatic choice:

| Property | Segment Lines | Reach Lines |
|----------|--------------|-------------|
| Count (Europe) | 150,325 | 1,922,187 |
| `is_mainstem` | Available | **Missing** in Europe slice |
| `drainage_area_km2` | Available via `drainage_area_out` | **Null for all rows** in Europe slice |
| Cross-layer join needed | No (1:1 with catchments) | Yes (must join via `segment_id`) |

Using segment lines provides a direct one-layer mapping: `segment.global_id == catchment.global_id`, with `drainage_area_out` as weight and `is_mainstem` directly available.

Reach-based snap would provide finer spatial resolution but requires cross-layer joins to inherit metadata from the parent segment. This remains a future improvement.

### `up_area_km2` = null

GRIT drainage area attributes are **partitioned at bifurcations** — they do not represent the HFX concept of inclusive cumulative upstream area. The adapter sets `up_area_km2 = null` for all rows and declares `has_up_area = false` in the manifest, allowing the engine to compute inclusive upstream area from graph traversal.

## Producer Workarounds

These workarounds were required to pass strict validation:

- **Degenerate snap bboxes**: Horizontal or vertical `LineString` features produce bounding boxes where `minx == maxx` or `miny == maxy`. The adapter pads these by epsilon (`1e-4`) via `inflate_degenerate_bounds()`. *(Spec has been updated to allow `<=` for snap bboxes.)*
- **Manifest bbox padding**: Direct geometry bounds fail enclosure checks due to floating-point rounding. The adapter pads the manifest bbox outward by epsilon (`1e-4`) via `outward_bbox()`.
- **Row group balancing**: Strict mode requires row groups in the range [4,096, 8,192]. The adapter uses `balanced_row_group_bounds()` to distribute rows evenly.
- **Compression**: Written without compression (`compression=None`) to work around a validator codec gap. *(Now fixed — validator supports zstd, snappy, lz4, gzip.)*

## Files

| File | Purpose |
|------|---------|
| `build_grit_eu_hfx.py` | ETL script: GRIT GPKG → HFX artifacts |
| `WORKFLOW.md` | Step-by-step commands for running the adapter |
| `GRIT_HFX_SPEC_VALIDATION.md` | Historical record of findings from the first validation pass |
| `pyproject.toml` | Python dependencies (uv-managed) |

## Adapter Guide

This adapter is the **canonical worked example** for the HFX adapter development guide. For authoring a new adapter against a different source hydrofabric, refer to `../../docs/ADAPTER_GUIDE.md` (created in Phase 3 of the adapter refactor). That guide generalizes the patterns established here — Hilbert sorting, row-group balancing, snap weight conformance, and manifest construction — into reusable guidance for any HFX adapter author.
