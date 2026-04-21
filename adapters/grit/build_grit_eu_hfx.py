#!/usr/bin/env python3
"""GRIT Europe -> HFX v0.2 adapter.

This is the canonical worked example that mirrors the nine-stage shape from
``adapters/_template/build_adapter.py`` verbatim.  A reader doing

    grep -n '^def stage_' adapters/grit/build_grit_eu_hfx.py

should see all nine template stages in order.  GRIT-specific prep (unzip the
outer archive, read the GPKG layers, preprocess the polars segments table)
lives in pre-stage helpers invoked from ``stage_1_inspect_source``.
"""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import polars as pl
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import pyogrio
from geoparquet_io.core.validate import validate_geoparquet
from shapely import make_valid
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


# ---------------------------------------------------------------------------
# Module-level configuration
# ---------------------------------------------------------------------------

FABRIC_NAME = "grit"
FABRIC_VERSION = "v1.0"
ADAPTER_VERSION = "grit-eu-scratch-2026-04-13"
TOPOLOGY = "dag"
REGION = "europe"
CRS = "EPSG:4326"
HAS_UP_AREA = False
HAS_RASTERS = False
HAS_SNAP = True

DEFAULT_ROOT = Path("/tmp/grit-hfx-eu")

EU_INPUTS = {
    "segments": "GRITv1.0_segments_EU_EPSG4326.gpkg.zip",
    "segment_catchments": "GRITv1.0_segment_catchments_EU_EPSG4326.gpkg.zip",
    "reaches": "GRITv1.0_reaches_EU_EPSG4326.gpkg.zip",
}

SEGMENTS_LAYER = "lines"
SEGMENT_CATCHMENTS_LAYER = "segment_catchments__1"
REACHES_LAYER = "lines"

EXPECTED_SEGMENT_COUNT = 150_325
EXPECTED_REACH_COUNT = 1_922_187
ROW_GROUP_SIZE = 4_096
ROW_GROUP_MIN = 4_096
ROW_GROUP_MAX = 8_192
REACH_CHUNK_SIZE = 50_000
SNAP_BBOX_EPSILON = 1e-4
MANIFEST_BBOX_EPSILON = 1e-4


def log(message: str) -> None:
    print(f"[grit-hfx] {message}", flush=True)


# ---------------------------------------------------------------------------
# Source data container
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceData:
    """Holds raw inputs loaded from the GRIT Europe hydrofabric.

    Attributes
    ----------
    inputs:
        Mapping of logical name (``segments``, ``segment_catchments``,
        ``reaches``) to the extracted inner GPKG path.
    segments:
        Polars segments table with ``global_id``, ``upstream_line_ids``,
        ``downstream_line_ids``, ``is_mainstem``, ``drainage_area_out``.
    catchments:
        Segment-catchment polygons with ``global_id`` and ``area`` columns.
    snap_lines:
        Segment-line GeoDataFrame used for the snap layer (reach layer is
        rejected in the Europe slice because ``is_mainstem`` and
        drainage-area columns are null).
    """

    inputs: dict[str, Path]
    segments: pl.DataFrame
    catchments: gpd.GeoDataFrame
    snap_lines: gpd.GeoDataFrame


# ---------------------------------------------------------------------------
# Implemented helpers
# ---------------------------------------------------------------------------

def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    """Build GeoParquet 1.1 ``geo`` metadata for embedding into an Arrow schema.

    No ``crs`` key is written — GeoParquet 1.1 defaults to OGC:CRS84 when absent,
    which is semantically equivalent to EPSG:4326 for lon/lat data.  Writing a
    plain string ``"EPSG:4326"`` would violate the spec (requires PROJJSON dict or
    null), so we omit the key entirely.
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


def assert_geoparquet_valid(out_path: Path) -> None:
    """Assert that ``out_path`` passes GeoParquet 1.1 validation.

    Raises ``RuntimeError`` listing every failed check if validation fails.
    """
    result = validate_geoparquet(str(out_path), target_version="1.1")
    if not result.is_valid:
        failures = [c for c in result.checks if c.status.value == "failed"]
        raise RuntimeError(
            f"GeoParquet 1.1 validation failed for {out_path}: "
            + "; ".join(f"{c.name}: {c.message}" for c in failures)
        )


def balanced_row_group_bounds(
    total_rows: int,
    min_size: int = ROW_GROUP_MIN,
    max_size: int = ROW_GROUP_MAX,
) -> list[tuple[int, int]]:
    """Split ``total_rows`` into row-group slices of size in ``[min_size, max_size]``."""
    if total_rows <= 0:
        return []

    min_groups = math.ceil(total_rows / max_size)
    max_groups = max(1, total_rows // min_size)
    group_count = max_groups
    while group_count >= min_groups:
        base = total_rows // group_count
        remainder = total_rows % group_count
        if min_size <= base <= max_size and base + (1 if remainder else 0) <= max_size:
            bounds: list[tuple[int, int]] = []
            start = 0
            for index in range(group_count):
                size = base + (1 if index < remainder else 0)
                stop = start + size
                bounds.append((start, stop))
                start = stop
            return bounds
        group_count -= 1

    return [(0, total_rows)]


def outward_bbox(bounds: tuple[float, float, float, float]) -> list[float]:
    """Pad the dataset bbox outward by ``MANIFEST_BBOX_EPSILON`` on every side."""
    minx, miny, maxx, maxy = bounds
    return [
        float(minx) - MANIFEST_BBOX_EPSILON,
        float(miny) - MANIFEST_BBOX_EPSILON,
        float(maxx) + MANIFEST_BBOX_EPSILON,
        float(maxy) + MANIFEST_BBOX_EPSILON,
    ]


def inflate_degenerate_bounds(bounds, epsilon: float = SNAP_BBOX_EPSILON):
    """Pad zero-extent snap bounds by ``epsilon`` so each axis has positive width."""
    bounds = bounds.copy()
    same_x = bounds["minx"] >= bounds["maxx"]
    same_y = bounds["miny"] >= bounds["maxy"]
    bounds.loc[same_x, "minx"] = bounds.loc[same_x, "minx"] - epsilon
    bounds.loc[same_x, "maxx"] = bounds.loc[same_x, "maxx"] + epsilon
    bounds.loc[same_y, "miny"] = bounds.loc[same_y, "miny"] - epsilon
    bounds.loc[same_y, "maxy"] = bounds.loc[same_y, "maxy"] + epsilon
    return bounds


def ensure_dir(path: Path) -> None:
    """Create ``path`` (and parents) if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Pre-stage helpers: unzip the outer archive and read source tables
# ---------------------------------------------------------------------------

def _extract_member(outer_zip: Path, member_name: str, out_path: Path) -> None:
    ensure_dir(out_path.parent)
    with ZipFile(outer_zip) as archive:
        info = archive.getinfo(member_name)
        if out_path.exists() and out_path.stat().st_size == info.file_size:
            log(f"reuse extracted {out_path.name}")
            return
        log(f"extract {member_name} -> {out_path}")
        with archive.open(info) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)


def _extract_inner_gpkg(inner_zip_path: Path) -> Path:
    with ZipFile(inner_zip_path) as archive:
        gpkg_names = [name for name in archive.namelist() if name.endswith(".gpkg")]
        if len(gpkg_names) != 1:
            raise ValueError(
                f"expected exactly one .gpkg in {inner_zip_path}, found {gpkg_names}"
            )
        member_name = gpkg_names[0]
        out_path = inner_zip_path.with_suffix("")
        if out_path.exists():
            return out_path
        log(f"inflate {inner_zip_path.name} -> {out_path.name}")
        with archive.open(member_name) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        return out_path


def _extract_inputs(root: Path, outer_archive: Path) -> dict[str, Path]:
    """Unzip the Europe GPKGs from the outer GRIT archive and return their paths."""
    input_dir = root / "input"
    ensure_dir(input_dir)
    outputs: dict[str, Path] = {}
    for key, member_name in EU_INPUTS.items():
        out_path = input_dir / member_name
        _extract_member(outer_archive, member_name, out_path)
        outputs[key] = _extract_inner_gpkg(out_path)
    return outputs


def _read_segments_table(segment_zip: Path) -> pl.DataFrame:
    """Read the polars segments table used for the graph and snap layers."""
    df = pyogrio.read_dataframe(
        segment_zip,
        layer=SEGMENTS_LAYER,
        columns=[
            "global_id",
            "upstream_line_ids",
            "downstream_line_ids",
            "is_mainstem",
            "drainage_area_out",
        ],
        read_geometry=False,
        use_arrow=True,
    )
    return (
        pl.from_pandas(df)
        .with_columns(
            pl.col("global_id").cast(pl.Int64),
            pl.col("upstream_line_ids").cast(pl.Utf8),
            pl.col("downstream_line_ids").cast(pl.Utf8),
            pl.col("is_mainstem").cast(pl.Int64),
            pl.col("drainage_area_out").cast(pl.Float64),
        )
        .sort("global_id")
    )


def _parse_csv_int_lists(values: pl.Series) -> list[list[int]]:
    parsed: list[list[int]] = []
    for raw in values.fill_null("").to_list():
        if raw == "":
            parsed.append([])
            continue
        parsed.append([int(part.strip()) for part in raw.split(",") if part.strip()])
    return parsed


# ---------------------------------------------------------------------------
# Nine stage functions (mirror adapters/_template/build_adapter.py)
# ---------------------------------------------------------------------------

def stage_1_inspect_source(input_path: Path) -> SourceData:
    """Load GRIT Europe inputs and validate expected row counts.

    ``input_path`` is the outer GRIT archive zip (``17435232.zip`` for the
    Europe slice).  This stage unzips the three Europe GPKGs, reads the
    polars segments table, reads the segment-catchments GeoDataFrame, and
    reads the segment-lines GeoDataFrame used for the snap layer.

    The reach layer is inspected via :data:`EU_INPUTS` but not loaded here
    because its ``is_mainstem`` and drainage-area columns are null in the
    Europe slice — we fall back to segment-line snap targets.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"outer archive missing: {input_path}")

    root = input_path.parent
    # Convention from the existing CLI: extracted inputs live in ``<root>/input``
    # where ``root`` is ``--root``, not the archive's parent.  The caller
    # passes the outer archive as ``input_path`` and the working ``root``
    # separately; here we defer the extract directory to whoever invoked us
    # by resolving it from the global passed through the CLI.  To keep the
    # stage signature aligned with the template, extraction is done in the
    # CLI dispatcher before stage_1 runs, and the inputs map is passed via
    # module-level state on the outer archive file.  See ``main``.
    #
    # In practice this stage is always called after ``_extract_inputs`` has
    # populated ``<work_root>/input``; ``input_path`` here points at the
    # outer archive and we resolve the already-extracted inputs relative to
    # the working root stored in ``_CURRENT_WORK_ROOT``.
    work_root = _CURRENT_WORK_ROOT
    if work_root is None:
        raise RuntimeError(
            "stage_1_inspect_source called before the working root was set; "
            "use main() to drive the pipeline."
        )

    inputs = _extract_inputs(work_root, input_path)

    log("read segments table (polars)")
    segments = _read_segments_table(inputs["segments"])
    if segments.height != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} Europe segment rows, "
            f"found {segments.height}"
        )

    log("read segment catchments (geopandas)")
    catchments = pyogrio.read_dataframe(
        inputs["segment_catchments"],
        layer=SEGMENT_CATCHMENTS_LAYER,
        columns=["global_id", "area"],
        read_geometry=True,
        use_arrow=True,
    )
    if len(catchments) != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} Europe segment catchments, "
            f"found {len(catchments)}"
        )

    log("read segment lines for snap layer (geopandas)")
    snap_lines = pyogrio.read_dataframe(
        inputs["segments"],
        layer=SEGMENTS_LAYER,
        columns=["global_id", "drainage_area_out", "is_mainstem"],
        read_geometry=True,
        use_arrow=True,
    )
    if len(snap_lines) != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} segment snap rows, "
            f"found {len(snap_lines)}"
        )

    return SourceData(
        inputs=inputs,
        segments=segments,
        catchments=catchments,
        snap_lines=snap_lines,
    )


def stage_2_assign_ids(source: SourceData) -> gpd.GeoDataFrame:
    """Map GRIT ``global_id`` to HFX ``id`` (int64 > 0).

    GRIT ``global_id`` is already a positive unique int64, so the mapping
    is a rename.  ``id = 0`` is reserved as the terminal-sink sentinel and
    this stage asserts it does not appear in the source.
    """
    gdf = source.catchments.copy()
    gdf["id"] = gdf["global_id"].astype("int64")
    if (gdf["id"] == 0).any():
        raise ValueError("GRIT global_id contains 0, which is reserved for the terminal sink")
    if (gdf["id"] < 0).any():
        raise ValueError("GRIT global_id contains negative values; HFX requires id > 0")
    if gdf["id"].duplicated().any():
        raise ValueError("GRIT global_id contains duplicates")
    return gdf


def stage_3_reproject(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assert the GRIT Europe GPKGs are already in EPSG:4326 — no reprojection."""
    if gdf.crs is not None and gdf.crs.to_epsg() not in (4326, None):
        raise ValueError(
            f"GRIT Europe inputs are expected to be EPSG:4326 already, got {gdf.crs}"
        )
    return gdf


def _coerce_to_polygonal(geom: BaseGeometry) -> BaseGeometry:
    """Repair ``geom`` and coerce the result to ``Polygon``/``MultiPolygon``.

    ``shapely.make_valid`` can return a ``GeometryCollection`` when a fix
    needs to express mixed dimensions (polygons + lines + points).  The
    HFX spec only permits ``Polygon``/``MultiPolygon`` in
    ``catchments.parquet``, so this helper drops non-polygonal shards and
    merges surviving polygons back into a single feature.
    """
    if geom.is_valid:
        return geom
    repaired = make_valid(geom)
    if isinstance(repaired, (Polygon, MultiPolygon)):
        return repaired
    if isinstance(repaired, GeometryCollection):
        polys: list[Polygon] = []
        for part in repaired.geoms:
            if isinstance(part, Polygon):
                polys.append(part)
            elif isinstance(part, MultiPolygon):
                polys.extend(part.geoms)
        if not polys:
            raise ValueError(
                "make_valid produced a GeometryCollection with no polygonal parts"
            )
        if len(polys) == 1:
            return polys[0]
        return MultiPolygon(polys)
    # Unexpected branch (e.g. LineString) — surface it instead of silently
    # dropping data.
    raise ValueError(f"make_valid produced unsupported geometry type: {type(repaired).__name__}")


def stage_4_make_valid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run ``shapely.make_valid`` on every catchment polygon.

    GRIT polygons are generally clean, but the spec requires adapters to
    repair geometries rather than rely on the source fabric.  This is a
    no-cost sweep when the input is already valid.  Repaired geometries
    that degrade to a ``GeometryCollection`` are coerced back to
    ``Polygon``/``MultiPolygon`` via :func:`_coerce_to_polygonal` so the
    catchments layer stays within the spec's permitted geometry types.
    """
    log("make_valid sweep across catchment polygons")
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.apply(_coerce_to_polygonal)
    return gdf


def stage_5_hilbert_sort(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Sort rows by Hilbert-curve index on centroid coordinates.

    Uses ``geopandas.GeoSeries.hilbert_distance`` with
    ``total_bounds=gdf.total_bounds`` as the normalising extent.  A stable
    secondary key on ``global_id`` makes the ordering deterministic when
    two centroids share the same Hilbert cell.
    """
    centroids = gdf.geometry.centroid
    gdf = gdf.copy()
    gdf["hilbert_index"] = centroids.hilbert_distance(total_bounds=gdf.total_bounds)
    gdf = gdf.sort_values(["hilbert_index", "global_id"], kind="mergesort").reset_index(drop=True)
    log(f"catchments Hilbert-sorted rows={len(gdf)}")
    return gdf


def stage_6_write_catchments(
    gdf: gpd.GeoDataFrame,
    out_dir: Path,
) -> tuple[list[int], tuple[float, float, float, float]]:
    """Write ``catchments.parquet`` conformant with HFX spec §1.

    Returns ``(ids, total_bounds)`` for downstream stages to consume.
    """
    row_count = len(gdf)
    total_bounds = tuple(float(value) for value in gdf.total_bounds)
    ids = [int(value) for value in gdf["global_id"].tolist()]

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )
    # Attach GeoParquet 1.1 metadata so downstream consumers recognise this file
    # as GeoParquet rather than plain Parquet with a binary geometry column.
    schema = schema.with_metadata(
        build_geo_metadata(["Polygon", "MultiPolygon"])
    )
    out_path = out_dir / "catchments.parquet"
    with pq.ParquetWriter(
        out_path,
        schema=schema,
        compression=None,
        write_statistics=True,
    ) as writer:
        for start, stop in balanced_row_group_bounds(row_count):
            chunk = gdf.iloc[start:stop]
            bounds = chunk.geometry.bounds
            geometry_wkb = chunk.geometry.to_wkb(hex=False)
            table = pa.Table.from_arrays(
                [
                    pa.array(chunk["global_id"].tolist(), type=pa.int64()),
                    pa.array(chunk["area"].tolist(), type=pa.float32()),
                    pa.array([None] * len(chunk), type=pa.float32()),
                    pa.array(bounds["minx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(bounds["miny"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(bounds["maxx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(bounds["maxy"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(geometry_wkb.tolist(), type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table)
            if start == 0 or stop == row_count or (start // ROW_GROUP_SIZE) % 10 == 0:
                log(f"catchments chunk rows={stop - start} written={stop}/{row_count}")

    gc.collect()
    log(f"wrote {out_path}")
    assert_geoparquet_valid(out_path)
    log("catchments.parquet passed GeoParquet 1.1 validation")
    return ids, total_bounds


def stage_7_write_graph(
    segments: pl.DataFrame,
    catchment_ids: list[int],
    out_dir: Path,
) -> None:
    """Write ``graph.arrow`` from the GRIT ``upstream_line_ids`` column.

    Every id in ``catchment_ids`` gets a row; headwaters have an empty list.
    The Arrow IPC format (not Parquet) is used for zero-copy memory mapping
    by the engine.
    """
    log("build graph.arrow")
    graph_df = (
        pl.DataFrame({"id": catchment_ids})
        .join(
            segments.select(["global_id", "upstream_line_ids"]).rename({"global_id": "id"}),
            on="id",
            how="left",
            validate="1:1",
        )
        .with_columns(pl.col("upstream_line_ids").fill_null(""))
    )

    missing_upstream_rows = graph_df.filter(pl.col("upstream_line_ids").is_null()).height
    if missing_upstream_rows:
        raise ValueError(f"graph build lost {missing_upstream_rows} segment rows")

    graph_ids = set(graph_df["id"].to_list())
    catchment_id_set = set(catchment_ids)
    if graph_ids != catchment_id_set:
        missing = sorted(catchment_id_set - graph_ids)[:10]
        extra = sorted(graph_ids - catchment_id_set)[:10]
        raise ValueError(f"graph/catchment id mismatch missing={missing} extra={extra}")

    upstream_lists = _parse_csv_int_lists(graph_df["upstream_line_ids"])
    list_type = pa.list_(pa.field("item", pa.int64(), nullable=True))
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("upstream_ids", list_type, nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(graph_df["id"].to_list(), type=pa.int64()),
            pa.array(upstream_lists, type=list_type),
        ],
        schema=schema,
    )

    out_path = out_dir / "graph.arrow"
    with pa.OSFile(str(out_path), "wb") as sink:
        with pa_ipc.new_file(sink, schema) as writer:
            writer.write(table)
    log(f"wrote {out_path}")


def stage_8_write_snap(
    snap_gdf: gpd.GeoDataFrame,
    catchment_id_set: set[int],
    out_dir: Path,
) -> int:
    """Write ``snap.parquet`` from segment-line features.

    GRIT reaches are rejected in the Europe slice (null ``is_mainstem`` and
    drainage-area columns); segment lines are the validated fallback.

    ``weight = drainage_area_out`` satisfies the v0.2 requirement that
    ``weight`` be monotonically increasing in drainage dominance.
    Degenerate horizontal/vertical line bboxes are inflated by
    :data:`SNAP_BBOX_EPSILON`.
    """
    log("build snap.parquet from segment lines")
    out_path = out_dir / "snap.parquet"
    row_count = len(snap_gdf)

    ids = [int(value) for value in snap_gdf["global_id"].tolist()]
    unknown = sorted(set(ids) - catchment_id_set)
    if unknown:
        raise ValueError(f"segment snap ids missing from catchments: {unknown[:10]}")

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("catchment_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("is_mainstem", pa.bool_(), nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )
    # Attach GeoParquet 1.1 metadata. GRIT segment geometries are LineStrings
    # (or MultiLineString where segments are split across dateline or similar).
    schema = schema.with_metadata(
        build_geo_metadata(["LineString", "MultiLineString"])
    )
    with pq.ParquetWriter(
        out_path,
        schema=schema,
        compression=None,
        write_statistics=True,
    ) as writer:
        for start, stop in balanced_row_group_bounds(row_count):
            chunk = snap_gdf.iloc[start:stop]
            chunk_bounds = inflate_degenerate_bounds(chunk.geometry.bounds)
            table = pa.Table.from_arrays(
                [
                    pa.array(chunk["global_id"].tolist(), type=pa.int64()),
                    pa.array(chunk["global_id"].tolist(), type=pa.int64()),
                    pa.array(chunk["drainage_area_out"].astype("float32").tolist(), type=pa.float32()),
                    pa.array((chunk["is_mainstem"] == 1).tolist(), type=pa.bool_()),
                    pa.array(chunk_bounds["minx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk_bounds["miny"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk_bounds["maxx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk_bounds["maxy"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table)
            if start == 0 or stop == row_count or (start // ROW_GROUP_SIZE) % 10 == 0:
                log(f"snap chunk rows={stop - start} written={stop}/{row_count}")

    gc.collect()
    log(f"wrote {out_path}")
    assert_geoparquet_valid(out_path)
    log("snap.parquet passed GeoParquet 1.1 validation")
    return row_count


def stage_9_write_manifest(
    out_dir: Path,
    bbox: tuple[float, float, float, float],
    atom_count: int,
) -> None:
    """Write ``manifest.json`` with the GRIT Europe v0.2 fields."""
    manifest = {
        "format_version": "0.1",
        "fabric_name": FABRIC_NAME,
        "fabric_version": FABRIC_VERSION,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "has_rasters": HAS_RASTERS,
        "has_snap": HAS_SNAP,
        "terminal_sink_id": 0,
        "topology": TOPOLOGY,
        "region": REGION,
        "bbox": outward_bbox(bbox),
        "atom_count": atom_count,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adapter_version": ADAPTER_VERSION,
    }
    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2) + "\n")
    log(f"wrote {out_path}")


# ---------------------------------------------------------------------------
# Validate helper
# ---------------------------------------------------------------------------

def validate(out_dir: Path, strict: bool = True, sample_pct: float = 100.0) -> int:
    """Validate the built HFX dataset at ``out_dir/hfx``.

    Layer 1 — authoritative HFX validator via ``cargo run -p hfx-validator``
    (text + json output, json saved as ``validator-report.json`` beside
    the dataset root).

    Layer 2 — GeoParquet 1.1 structural validation for ``catchments.parquet``
    and, when :data:`HAS_SNAP`, ``snap.parquet``.  Raises ``RuntimeError`` if
    either file fails validation.

    Returns the worst (max) exit code from the two validator invocations.
    """
    dataset_path = out_dir / "hfx"
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset path missing: {dataset_path}")

    repo_root = Path(__file__).resolve().parents[2]

    cmd = [
        "cargo",
        "run",
        "-p",
        "hfx-validator",
        "--",
        str(dataset_path),
        "--format",
        "text",
        "--sample-pct",
        str(sample_pct),
    ]
    if strict:
        cmd.append("--strict")
    log("run validator (text)")
    text = subprocess.run(cmd, cwd=repo_root, check=False)

    json_cmd = [
        "cargo",
        "run",
        "-p",
        "hfx-validator",
        "--",
        str(dataset_path),
        "--format",
        "json",
        "--sample-pct",
        str(sample_pct),
    ]
    if strict:
        json_cmd.append("--strict")
    log("run validator (json)")
    json_run = subprocess.run(
        json_cmd,
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    (out_dir / "validator-report.json").write_text(json_run.stdout)
    if json_run.stderr:
        (out_dir / "validator-report.stderr").write_text(json_run.stderr)

    catchments_path = dataset_path / "catchments.parquet"
    assert_geoparquet_valid(catchments_path)
    if HAS_SNAP:
        assert_geoparquet_valid(dataset_path / "snap.parquet")

    return max(text.returncode, json_run.returncode)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# Working root for the current pipeline invocation.  ``stage_1_inspect_source``
# reads this to resolve the ``<work_root>/input`` directory where the outer
# archive is unpacked.  It is set by ``main`` before the pipeline runs.
_CURRENT_WORK_ROOT: Path | None = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scratch GRIT Europe -> HFX wrangler.")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="working directory for extracted inputs and generated HFX output",
    )
    parser.add_argument(
        "--outer-archive",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "path to the outer GRIT archive zip (e.g. 17435232.zip). "
            "Falls back to the GRIT_OUTER_ARCHIVE environment variable. "
            "Required for the 'extract' and 'build' subcommands."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "extract",
        help="extract the Europe inputs from the outer GRIT archive",
    )
    subparsers.add_parser(
        "build",
        help="build the scratch HFX dataset into <root>/hfx",
    )

    validate_parser = subparsers.add_parser(
        "validate", help="run the existing Rust validator"
    )
    validate_parser.add_argument("--strict", action="store_true", default=True)
    validate_parser.add_argument("--sample-pct", type=float, default=100.0)

    return parser.parse_args()


def _resolve_outer_archive(args: argparse.Namespace) -> Path | None:
    if args.outer_archive is not None:
        return args.outer_archive.resolve()
    env_val = os.environ.get("GRIT_OUTER_ARCHIVE")
    if env_val:
        return Path(env_val).resolve()
    return None


def _require_outer_archive(args: argparse.Namespace) -> Path:
    archive = _resolve_outer_archive(args)
    if archive is None:
        raise SystemExit(
            "error: outer archive path is required for this subcommand.\n"
            "  Provide it via --outer-archive PATH\n"
            "  or set the GRIT_OUTER_ARCHIVE environment variable."
        )
    return archive


def _build_dataset(root: Path, outer_archive: Path) -> None:
    """Drive the nine stages end-to-end."""
    global _CURRENT_WORK_ROOT
    _CURRENT_WORK_ROOT = root

    hfx_dir = root / "hfx"
    ensure_dir(hfx_dir)

    source = stage_1_inspect_source(outer_archive)
    gdf = stage_2_assign_ids(source)
    gdf = stage_3_reproject(gdf)
    gdf = stage_4_make_valid(gdf)
    gdf = stage_5_hilbert_sort(gdf)

    catchment_ids, bbox = stage_6_write_catchments(gdf, hfx_dir)
    if len(catchment_ids) != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} catchment ids, found {len(catchment_ids)}"
        )

    # Release the polygon GeoDataFrame before loading the snap lines again;
    # peak memory is dominated by holding both at once.
    del gdf
    gc.collect()

    stage_7_write_graph(source.segments, catchment_ids, hfx_dir)

    catchment_id_set = set(catchment_ids)
    snap_rows = stage_8_write_snap(source.snap_lines, catchment_id_set, hfx_dir)
    if snap_rows != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} snap rows, found {snap_rows}"
        )

    stage_9_write_manifest(hfx_dir, bbox, len(catchment_ids))


def main() -> int:
    """Dispatch subcommands and orchestrate the build pipeline."""
    global _CURRENT_WORK_ROOT

    args = _parse_args()
    root = args.root.resolve()
    _CURRENT_WORK_ROOT = root

    if args.command == "extract":
        _extract_inputs(root, _require_outer_archive(args))
        return 0
    if args.command == "build":
        _build_dataset(root, _require_outer_archive(args))
        return 0
    if args.command == "validate":
        return validate(root, strict=args.strict, sample_pct=args.sample_pct)
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
