# GRIT → HFX Adapter

Scratch Python adapter that compiles [GRIT](https://zenodo.org/records/17435232) hydrofabric data into HFX format.

## Status

Validated initially against the Europe EPSG:4326 slice. The adapter is now parameterized for the seven published GRIT regions: `AF`, `AS`, `EU`, `NA`, `SA`, `SI`, and `SP`.

The global build has also been validated: all seven regions merged into one strict-valid HFX dataset with 1,767,065 atoms.

## How to Run

Use `uv` from the repo root. Supply the outer GRIT archive via `--outer-archive` or the `GRIT_OUTER_ARCHIVE` environment variable, and select a region with `--region`.

```bash
# Using the flag
uv run --project adapters/grit python adapters/grit/build_adapter.py \
    --region EU \
    --outer-archive /path/to/17435232.zip \
    build

# Using the environment variable
export GRIT_OUTER_ARCHIVE=/path/to/17435232.zip
uv run --project adapters/grit python adapters/grit/build_adapter.py --region EU extract
uv run --project adapters/grit python adapters/grit/build_adapter.py --region EU build
uv run --project adapters/grit python adapters/grit/build_adapter.py --region EU validate
```

By default, outputs are written under `/Users/nicolaslazaro/Desktop/grit-hfx/per-region/grit-hfx-<region_lower>/`. Final HFX artifacts are placed directly in that per-region directory:

- `catchments.parquet`
- `graph.arrow`
- `manifest.json`
- `snap.parquet`

## Global Build Example

The global workflow mirrors the MERIT adapter: preflight source ids, build each region, merge the regional HFX outputs, then validate and sanity-check the merged artifact.

```bash
# 1. Verify global ids and graph references before building.
uv run --project adapters/grit python adapters/grit/verify_cross_region.py \
    --outer-archive /Users/nicolaslazaro/Desktop/grit-hfx/17435232.zip \
    --root /Users/nicolaslazaro/Desktop/grit-hfx

# 2. Build all seven regional HFX outputs.
uv run --project adapters/grit python adapters/grit/run_all_regions.py \
    run \
    --outer-archive /Users/nicolaslazaro/Desktop/grit-hfx/17435232.zip \
    --root /Users/nicolaslazaro/Desktop/grit-hfx

# 3. Merge regional outputs into one global HFX dataset.
uv run --project adapters/grit python adapters/grit/merge_regions.py \
    --inputs-root /Users/nicolaslazaro/Desktop/grit-hfx/per-region \
    --output /Users/nicolaslazaro/Desktop/grit-hfx/global/grit-hfx-global

# 4. Save a JSON validator report and sanity-check the global manifest.
cargo run -p hfx-validator -- \
    /Users/nicolaslazaro/Desktop/grit-hfx/global/grit-hfx-global \
    --strict --sample-pct 100 --skip-rasters --format json \
    > /Users/nicolaslazaro/Desktop/grit-hfx/global/validator-report.json
python -c 'import json,pathlib; m=json.loads(pathlib.Path("/Users/nicolaslazaro/Desktop/grit-hfx/global/grit-hfx-global/manifest.json").read_text()); assert m["atom_count"] == 1767065; assert m["bbox"] == [-180.0, -90.0, 180.0, 90.0]; assert "region" not in m'
```

The 2026-04-30 global run produced `/Users/nicolaslazaro/Desktop/grit-hfx/global/grit-hfx-global` with 1,767,065 atoms. Preflight scanned `AF`, `AS`, `EU`, `NA`, `SA`, `SI`, and `SP` with zero id collisions, zero resolved cross-region references, zero unresolved references, and zero antimeridian regions. The final manifest uses the exact bbox `[-180.0, -90.0, 180.0, 90.0]` and omits `region`.

## Requirements

- Python >= 3.11
- Dependencies managed via `uv` (see `pyproject.toml`)
- Key libraries: geopandas, polars, pyarrow, pyogrio, shapely

## Input

The script extracts the selected region's `EPSG:4326` members into `<root>/per-region/grit-hfx-<region_lower>/input`:

- `GRITv1.0_segments_<CODE>_EPSG4326.gpkg.zip`
- `GRITv1.0_segment_catchments_<CODE>_EPSG4326.gpkg.zip`
- `GRITv1.0_reaches_<CODE>_EPSG4326.gpkg.zip`

The reaches archive remains extractable for inspection and future work, but the current build uses segment lines for snap targets.

## Mapping Summary

### HFX Atom = GRIT Segment Catchment

GRIT organizes its data at two granularity levels: **segments** and **reaches**. Each segment has exactly one catchment polygon. The HFX adapter maps each GRIT segment catchment to one HFX catchment atom, using the segment `global_id` as the HFX atom `id`.

### `catchments.parquet`

- HFX atom = GRIT segment catchment
- `id` = `global_id`
- `area_km2` = `area`
- `up_area_km2` = null for all rows
- bbox columns = geometry bounds
- geometry = WKB polygon or multipolygon

### Graph: Segment `upstream_line_ids`

The GRIT segment table carries an `upstream_line_ids` field (CSV-encoded list of segment `global_id` values). These map directly to HFX `graph.arrow` `upstream_ids`. The graph topology is `"dag"` because GRIT includes bifurcations (distributaries).

### `graph.arrow`

- `id` = GRIT segment `global_id`
- `upstream_ids` = parsed `upstream_line_ids`

### Snap: Segment Lines (not Reach Lines)

The adapter uses **segment lines** as snap targets rather than reach lines. This was a pragmatic choice:

| Property | Segment Lines | Reach Lines |
|----------|--------------|-------------|
| Count | One per segment | Many per region |
| `is_mainstem` | Available in the validated Europe slice | Missing in the validated Europe reach layer |
| `drainage_area_km2` | Available via `drainage_area_out` | Null in the validated Europe reach layer |
| Cross-layer join needed | No (1:1 with catchments) | Yes (must join via `segment_id`) |

Using segment lines provides a direct one-layer mapping: `segment.global_id == catchment.global_id`, with `drainage_area_out` as weight and `is_mainstem` directly available.

Reach-based snap would provide finer spatial resolution but requires cross-layer joins to inherit metadata from the parent segment. This remains a future improvement.

### `snap.parquet`

- HFX snap row = GRIT segment line
- `id` = segment `global_id`
- `catchment_id` = segment `global_id`
- `weight` = segment `drainage_area_out`
- `is_mainstem` = segment `is_mainstem == 1`
- geometry = segment WKB linestring

The engine default snap strategy is a weight-first cascade: filter by radius, rank by weight, tie-break by mainstem preference, tie-break by distance, then tie-break by snap id ascending.

### `up_area_km2` = null

GRIT drainage area attributes are **partitioned at bifurcations** — they do not represent the HFX concept of inclusive cumulative upstream area. The adapter sets `up_area_km2 = null` for all rows and declares `has_up_area = false` in the manifest, allowing the engine to compute inclusive upstream area from graph traversal.

## Producer Workarounds

These workarounds were required to pass strict validation:

- **Degenerate snap bboxes**: Horizontal or vertical `LineString` features produce bounding boxes where `minx == maxx` or `miny == maxy`. The adapter pads these by epsilon (`1e-4`) via `inflate_degenerate_bounds()`. *(Spec has been updated to allow `<=` for snap bboxes.)*
- **Manifest bbox padding**: Direct geometry bounds fail enclosure checks due to floating-point rounding. The adapter pads the manifest bbox outward by epsilon (`1e-4`) via `outward_bbox()`.
- **Row group balancing**: Strict mode requires row groups in the range [4,096, 8,192]. The adapter uses `balanced_row_group_bounds()` to distribute rows evenly.
- **Compression**: Written without compression (`compression=None`) to work around a validator codec gap. *(Now fixed — validator supports zstd, snappy, lz4, gzip.)*

## Known Deliberate Choices

- `has_up_area = false` because published GRIT drainage-area fields are partitioned at bifurcations and do not represent HFX inclusive upstream area.
- `has_rasters = false` for this exercise.
- Reach-based snap is not used by the current build; segment-line snap targets are the validated fallback.
- `catchments.parquet` is Hilbert-sorted by centroid. `snap.parquet` is currently written in source order.

## Files

| File | Purpose |
|------|---------|
| `build_adapter.py` | ETL script: GRIT GPKG → HFX artifacts |
| `verify_cross_region.py` | Preflight scanner for duplicate ids, graph references, and antimeridian indicators |
| `run_all_regions.py` | Batch runner for the seven regional GRIT builds |
| `merge_regions.py` | Global merger for regional HFX outputs |
| `GRIT_HFX_SPEC_VALIDATION.md` | Historical record of findings from the first validation pass |
| `pyproject.toml` | Python dependencies (uv-managed) |

## Adapter Guide

This adapter is the **canonical worked example** for the HFX adapter development guide. For authoring a new adapter against a different source hydrofabric, refer to `../../docs/ADAPTER_GUIDE.md` (created in Phase 3 of the adapter refactor). That guide generalizes the patterns established here — Hilbert sorting, row-group balancing, snap weight conformance, and manifest construction — into reusable guidance for any HFX adapter author.
