#!/usr/bin/env python3
"""HFX adapter skeleton.

Copy this directory to adapters/<your-source>/ and implement the nine stage
stubs.  Fill in the TODO constants below, then implement each stage function.

Usage
-----
    uv sync
    uv run python build_adapter.py extract --input <path-to-source>
    uv run python build_adapter.py build   --input <path-to-source> --out ./out
    uv run python build_adapter.py validate --out ./out
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import geopandas as gpd
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
from geoparquet_io.core.validate import validate_geoparquet


# ---------------------------------------------------------------------------
# Module-level configuration — fill these in before implementing the stages.
# ---------------------------------------------------------------------------

FABRIC_NAME = "todo-fabric-name"       # TODO: lowercase ASCII matching ^[a-z][a-z0-9_-]*$
ADAPTER_VERSION = "todo-adapter-version"  # TODO: e.g. "0.1.0"
TOPOLOGY = "tree"                      # TODO: "tree" or "dag"
HAS_UP_AREA = False                    # TODO: True if adapter computes up_area_km2
HAS_RASTERS = False                    # TODO: True if flow_dir.tif / flow_acc.tif are included
HAS_SNAP = False                       # TODO: True if snap.parquet is produced

ROW_GROUP_MIN = 4096
ROW_GROUP_MAX = 8192


# ---------------------------------------------------------------------------
# Source data container
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceData:
    """Holds raw inputs loaded from the source hydrofabric."""

    catchments: gpd.GeoDataFrame
    snap_lines: gpd.GeoDataFrame | None = None
    flow_dir_path: Path | None = None
    flow_acc_path: Path | None = None


# ---------------------------------------------------------------------------
# Implemented helpers
# ---------------------------------------------------------------------------

def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    """Build GeoParquet 1.1 ``geo`` metadata for embedding into an Arrow schema.

    No ``crs`` key is written — GeoParquet 1.1 defaults to OGC:CRS84 when
    absent, which is semantically equivalent to EPSG:4326 for lon/lat data.
    Writing a plain string ``"EPSG:4326"`` would violate the spec (requires
    PROJJSON dict or null), so the key is omitted entirely.

    This metadata dict must be attached to the Arrow schema *before*
    ``pq.ParquetWriter`` is opened.  Do not attach it per-chunk.

    Parameters
    ----------
    geometry_types:
        GeoParquet geometry type strings, e.g. ``["Polygon", "MultiPolygon"]``
        or ``["LineString", "MultiLineString"]``.

    Returns
    -------
    dict[bytes, bytes]
        Ready to pass to ``schema.with_metadata()``.
    """
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
            },
        },
    }
    return {b"geo": json.dumps(geo).encode("utf-8")}


# ---------------------------------------------------------------------------
# Stage stubs — implement each in your adapter copy
# ---------------------------------------------------------------------------

def stage_1_inspect_source(input_path: Path) -> SourceData:
    """Load and inspect the source hydrofabric at ``input_path``.

    Read the source data and return a ``SourceData`` instance.  Validate
    expected row counts, required columns, and any source-specific invariants
    here so that downstream stages can rely on clean inputs.
    """
    raise NotImplementedError(
        "stage_1_inspect_source not implemented — see spec/HFX_SPEC.md "
        "§Artifact Summary and docs/ADAPTER_GUIDE.md §Inspect Source"
    )


def stage_2_assign_ids(source: SourceData) -> gpd.GeoDataFrame:
    """Map source identifiers to HFX atom IDs (int64 > 0).

    ``id = 0`` is reserved as the terminal-sink sentinel and MUST NOT appear
    in the output.  Negative IDs are invalid.  The mapping must be stable
    across builds so downstream consumers can cache IDs.

    Returns a GeoDataFrame with an ``id`` column of dtype int64.
    """
    raise NotImplementedError(
        "stage_2_assign_ids not implemented — see spec/HFX_SPEC.md "
        "§1. catchments.parquet and docs/ADAPTER_GUIDE.md §Assign IDs"
    )


def stage_3_reproject(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Reproject geometries to EPSG:4326 (WGS 84).

    All HFX vector and raster data must share CRS EPSG:4326.  If the source
    is already in EPSG:4326 this stage is a no-op, but it must still be
    called so the pipeline contract is satisfied.
    """
    raise NotImplementedError(
        "stage_3_reproject not implemented — see spec/HFX_SPEC.md "
        "§1. catchments.parquet §Notes and docs/ADAPTER_GUIDE.md §Reproject"
    )


def stage_4_make_valid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix invalid geometries using ``shapely.make_valid``.

    The spec requires that all geometries in ``catchments.parquet`` are valid
    (no self-intersections).  Call ``shapely.make_valid`` on every row during
    ETL rather than relying on the source fabric to be clean.
    """
    raise NotImplementedError(
        "stage_4_make_valid not implemented — see spec/HFX_SPEC.md "
        "§1. catchments.parquet §Notes and docs/ADAPTER_GUIDE.md §Make Valid"
    )


def stage_5_hilbert_sort(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Sort rows by Hilbert curve index computed on centroid coordinates.

    The spec requires Hilbert-curve ordering for spatial locality; this
    enables the engine to prune row groups via ``bbox_*`` column statistics.

    Open item 1 — Hilbert curve parameters (resolution, bounding box used for
    normalisation) are not yet frozen in the spec.  Each adapter must document
    its specific Hilbert-sort parameters in its own README.md so that the
    choice is reproducible and auditable.

    A common approach:
        centroids = gdf.geometry.centroid
        gdf["hilbert_index"] = centroids.hilbert_distance(
            total_bounds=gdf.total_bounds
        )
        return gdf.sort_values("hilbert_index").reset_index(drop=True)
    """
    raise NotImplementedError(
        "stage_5_hilbert_sort not implemented — see spec/HFX_SPEC.md "
        "§1. catchments.parquet §Spatial Partitioning "
        "and docs/ADAPTER_GUIDE.md §Hilbert Sort"
    )


def stage_6_write_catchments(gdf: gpd.GeoDataFrame, out_dir: Path) -> None:
    """Write ``catchments.parquet`` conformant with HFX spec §1.

    Canonical pattern
    -----------------
    1. Build the Arrow schema with columns: id (int64), area_km2 (float32),
       up_area_km2 (float32, nullable), bbox_minx/miny/maxx/maxy (float32),
       geometry (binary / WKB).
    2. Attach ``build_geo_metadata(["Polygon", "MultiPolygon"])`` to the
       schema via ``schema.with_metadata(...)`` — this MUST happen before
       ``pq.ParquetWriter`` is opened, not per chunk.
    3. Open ``pq.ParquetWriter(out_path, schema, write_statistics=True)``.
    4. Compute ``balanced_row_group_bounds(len(gdf))`` (vendor this helper
       from ``adapters/grit/build_grit_eu_hfx.py`` if needed) and write each
       chunk as a separate row group via ``writer.write_table(table)``.
    5. Close the writer, then call
       ``validate_geoparquet(out_path, target_version="1.1")``.

    Row group size must fall in [ROW_GROUP_MIN, ROW_GROUP_MAX] = [4096, 8192].
    The four ``bbox_*`` columns must be top-level float32 columns so that
    Parquet row-group statistics are written for them.  Do NOT absorb them into
    a struct — the engine prunes row groups via these statistics.
    """
    raise NotImplementedError(
        "stage_6_write_catchments not implemented — see spec/HFX_SPEC.md "
        "§1. catchments.parquet and docs/ADAPTER_GUIDE.md §Write Catchments"
    )


def stage_7_write_graph(
    ids: Iterable[int],
    upstream: dict[int, list[int]],
    out_dir: Path,
) -> None:
    """Write ``graph.arrow`` as an Apache Arrow IPC file.

    Schema: id (int64, not nullable), upstream_ids (list<int64>, not nullable).
    Headwater atoms have an empty list ``[]`` for ``upstream_ids``.

    Every id present in ``catchments.parquet`` must have a corresponding row
    here, even headwaters.  The graph must be acyclic — detect and break any
    cycles (e.g. endorheic loops) during ETL.

    Arrow IPC format (not Parquet) is used for zero-copy memory mapping by
    the engine.
    """
    raise NotImplementedError(
        "stage_7_write_graph not implemented — see spec/HFX_SPEC.md "
        "§2. graph.arrow and docs/ADAPTER_GUIDE.md §Write Graph"
    )


def stage_8_write_snap(snap_gdf: gpd.GeoDataFrame, out_dir: Path) -> None:
    """Write ``snap.parquet`` conformant with HFX spec §3.

    Schema: id (int64), catchment_id (int64), weight (float32),
    is_mainstem (bool), bbox_minx/miny/maxx/maxy (float32), geometry (binary).

    v0.2 snap contract — weight MUST be monotonically increasing in drainage
    dominance: a higher weight value MUST indicate the more hydrologically
    significant reach.  Adapters typically use upstream drainage area (km² or
    cell count) as the weight.  Datasets whose weights do not satisfy this
    ordering are non-conformant with v0.2 snapping semantics.

    Bbox inequality for snap features uses non-strict ``<=`` (not ``<``)
    because LineString features may be axis-aligned, producing a bounding box
    with zero extent along one axis.

    Same Hilbert-sort and row-group statistics requirements as
    ``catchments.parquet`` apply here.
    """
    raise NotImplementedError(
        "stage_8_write_snap not implemented — see spec/HFX_SPEC.md "
        "§3. snap.parquet and docs/ADAPTER_GUIDE.md §Write Snap"
    )


def stage_9_write_manifest(
    out_dir: Path,
    bbox: tuple[float, float, float, float],
    atom_count: int,
) -> None:
    """Write ``manifest.json`` conformant with HFX spec §6.

    Key invariants
    --------------
    - ``fabric_name`` must match ``^[a-z][a-z0-9_-]*$``.
    - ``atom_count`` must equal the row count of ``catchments.parquet``.
    - ``created_at`` is an RFC 3339 UTC timestamp generated at write time via
      ``datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")``.
    - ``terminal_sink_id`` must be ``0``.
    - ``flow_dir_encoding`` is required when ``HAS_RASTERS = True``.
    - ``bbox`` should be padded outward by a small epsilon (e.g. 1e-4) to
      survive floating-point rounding in the validator's enclosure check.
    """
    raise NotImplementedError(
        "stage_9_write_manifest not implemented — see spec/HFX_SPEC.md "
        "§6. manifest.json and docs/ADAPTER_GUIDE.md §Write Manifest"
    )


# ---------------------------------------------------------------------------
# Implemented validate helper
# ---------------------------------------------------------------------------

def validate(out_dir: Path) -> int:
    """Run both validation layers against the built HFX dataset.

    Layer 1 — authoritative HFX validator CLI:
        Calls ``hfx <out_dir>/hfx --strict --sample-pct 100 --format text``
        via subprocess and captures the exit code.

    Layer 2 — GeoParquet 1.1 structural validation:
        Calls ``validate_geoparquet`` on ``catchments.parquet`` (and on
        ``snap.parquet`` if ``HAS_SNAP`` is True).  Raises ``RuntimeError``
        if ``is_valid`` is False.

    Both layers must pass for the dataset to be considered conformant.

    Returns the exit code from the ``hfx`` CLI (0 = valid, 1 = invalid).
    """
    dataset_path = out_dir / "hfx"
    exit_code = subprocess.call(
        [
            "hfx",
            str(dataset_path),
            "--strict",
            "--sample-pct",
            "100",
            "--format",
            "text",
        ]
    )

    catchments_path = dataset_path / "catchments.parquet"
    result = validate_geoparquet(str(catchments_path), target_version="1.1")
    if not result.is_valid:
        failures = [c for c in result.checks if c.status.value == "failed"]
        raise RuntimeError(
            f"GeoParquet 1.1 validation failed for {catchments_path}: "
            + "; ".join(f"{c.name}: {c.message}" for c in failures)
        )

    if HAS_SNAP:
        snap_path = dataset_path / "snap.parquet"
        snap_result = validate_geoparquet(str(snap_path), target_version="1.1")
        if not snap_result.is_valid:
            failures = [c for c in snap_result.checks if c.status.value == "failed"]
            raise RuntimeError(
                f"GeoParquet 1.1 validation failed for {snap_path}: "
                + "; ".join(f"{c.name}: {c.message}" for c in failures)
            )

    return exit_code


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HFX adapter template — extract, build, and validate an HFX dataset.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        metavar="PATH",
        help="path to the source hydrofabric input (required for extract and build)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("./out"),
        metavar="PATH",
        help="working directory for built HFX output (default: ./out)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("extract", help="inspect and extract the source inputs")
    subparsers.add_parser("build", help="build the HFX dataset into <out>/hfx")
    subparsers.add_parser("validate", help="validate an existing <out>/hfx dataset")

    return parser.parse_args()


def main() -> int:
    """Dispatch subcommands and orchestrate the build pipeline."""
    args = _parse_args()
    out_dir: Path = args.out.resolve()
    hfx_dir = out_dir / "hfx"
    hfx_dir.mkdir(parents=True, exist_ok=True)

    def _run_stage(name: str, fn, *fn_args):
        try:
            return fn(*fn_args)
        except NotImplementedError as exc:
            print(f"[{name}] not implemented: {exc}")
            raise SystemExit(2)

    if args.command == "extract":
        if args.input is None:
            print("error: --input PATH is required for the extract subcommand")
            return 2
        _run_stage("stage_1_inspect_source", stage_1_inspect_source, args.input.resolve())
        return 0

    if args.command == "build":
        if args.input is None:
            print("error: --input PATH is required for the build subcommand")
            return 2
        input_path = args.input.resolve()

        source = _run_stage("stage_1_inspect_source", stage_1_inspect_source, input_path)
        gdf = _run_stage("stage_2_assign_ids", stage_2_assign_ids, source)
        gdf = _run_stage("stage_3_reproject", stage_3_reproject, gdf)
        gdf = _run_stage("stage_4_make_valid", stage_4_make_valid, gdf)
        gdf = _run_stage("stage_5_hilbert_sort", stage_5_hilbert_sort, gdf)
        _run_stage("stage_6_write_catchments", stage_6_write_catchments, gdf, hfx_dir)

        ids = gdf["id"].tolist()
        upstream: dict[int, list[int]] = {}
        _run_stage("stage_7_write_graph", stage_7_write_graph, ids, upstream, hfx_dir)

        if HAS_SNAP and source.snap_lines is not None:
            _run_stage("stage_8_write_snap", stage_8_write_snap, source.snap_lines, hfx_dir)

        total_bounds = tuple(float(v) for v in gdf.total_bounds)
        bbox = (total_bounds[0], total_bounds[1], total_bounds[2], total_bounds[3])
        _run_stage(
            "stage_9_write_manifest",
            stage_9_write_manifest,
            hfx_dir,
            bbox,
            len(gdf),
        )

        return validate(out_dir)

    if args.command == "validate":
        return validate(out_dir)

    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
