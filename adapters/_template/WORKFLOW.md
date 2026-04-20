# Adapter Workflow

This document describes the steps to run an adapter built from the HFX template.  Before running any command, complete the setup tasks in the section below.

## Before this template can run

- [ ] Set `FABRIC_NAME` to a lowercase ASCII string matching `^[a-z][a-z0-9_-]*$`
- [ ] Set `ADAPTER_VERSION` to a version string (e.g. `"0.1.0"`)
- [ ] Decide `TOPOLOGY`: `"tree"` for strictly convergent fabrics, `"dag"` for fabrics with bifurcations
- [ ] Decide `HAS_UP_AREA`: set `True` if the adapter computes inclusive cumulative upstream area; `False` lets the engine compute it from graph traversal
- [ ] Decide `HAS_SNAP`: set `True` if the adapter produces `snap.parquet`; otherwise the engine falls back to point-in-polygon on `catchments.parquet`
- [ ] Decide `HAS_RASTERS`: set `True` if `flow_dir.tif` and `flow_acc.tif` are included
- [ ] Implement stages 1 through 9 (replace each `raise NotImplementedError(...)`)
- [ ] Vendor `balanced_row_group_bounds` from `adapters/grit/build_grit_eu_hfx.py` if your dataset has more than 4096 atoms — stage 6 requires it

## Commands

```bash
uv sync

# Inspect and load the source inputs
uv run python build_adapter.py extract --input <src>

# Build the full HFX dataset (runs all stages, then validates)
uv run python build_adapter.py build --input <src> --out ./out

# Re-run validation on an existing build
uv run python build_adapter.py validate --out ./out
```

## What validate does

Validation runs two independent layers.  Both must pass for the dataset to be considered conformant.

The first layer is the authoritative HFX validator CLI.  Running `hfx <dataset-path> --strict --sample-pct 100 --format text` reads the full dataset directory — manifest, catchments, graph, and optional snap/rasters — and checks all spec rules: schema correctness, referential integrity, graph acyclicity, row-group statistics presence, atom count, and a geometry spot-check.

The second layer is GeoParquet 1.1 structural validation via `validate_geoparquet` (from `geoparquet-io`).  It checks that the `geo` metadata in `catchments.parquet` (and `snap.parquet` if `HAS_SNAP = True`) conforms to the GeoParquet 1.1 specification — geometry encoding, column metadata structure, and version declaration.

## Debugging cheat sheet

- **`bbox_*` column statistics missing in Parquet row groups** — ensure `pq.ParquetWriter` is opened with `write_statistics=True` (the default) and that `bbox_minx`, `bbox_miny`, `bbox_maxx`, `bbox_maxy` are top-level `float32` columns.  If they are nested inside a struct column, Parquet statistics are not written for them and the engine cannot prune row groups.

- **`hfx: command not found`** — install the validator with `cargo install hfx-validator`, or run it from the repo root with `cargo run -p hfx-validator -- <dataset-path> --strict`.

- **Row-group size outside `[4096, 8192]`** — vendor the `balanced_row_group_bounds` helper from `adapters/grit/build_grit_eu_hfx.py`.  It computes evenly distributed row-group boundaries that satisfy the strict-mode size constraint.

- **`validate_geoparquet` complains about geometry column metadata** — verify that `build_geo_metadata(...)` was called and the result was attached to the Arrow schema via `schema.with_metadata(...)` *before* `pq.ParquetWriter` is opened.  Attaching metadata after the writer is open, or per-chunk, does not update the file-level schema metadata.

- **`referential.upstream_not_in_catchments`** — `stage_7_write_graph` emitted an upstream ID that does not appear in `catchments.parquet`.  Every ID in `upstream_ids` must exist as an `id` in the catchments table.  Check your source graph for dangling references before writing.

## When you are done

- Remove all `TODO` comments and placeholder values (`"todo-fabric-name"`, `"todo-adapter-version"`).
- Replace `ADAPTER_VERSION` with the actual release version of your adapter.
- If you deviated from the spec intentionally (e.g. a different snap weight convention, a known conformance gap), document the deviation in your adapter's `README.md` with a link to the relevant entry in `docs/decisions/`.
- Add a `WORKFLOW.md` specific to your adapter describing its inputs, source-specific mapping decisions, and any known workarounds — following the pattern in `adapters/grit/WORKFLOW.md`.
