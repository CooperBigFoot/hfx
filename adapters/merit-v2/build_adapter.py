#!/usr/bin/env python3
"""Build MERIT-Basins HFX v0.2.1 datasets."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from geoparquet_io.core.validate import validate_geoparquet
from rasterio.windows import Window, from_bounds
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles
from shapely import make_valid
from shapely.geometry import GeometryCollection, LineString, MultiLineString, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import nearest_points


FABRIC_NAME = "merit_basins"
FABRIC_VERSION = "v0.7_bugfix1"
ADAPTER_VERSION = "0.2.0"
FORMAT_VERSION = "0.2.1"
CRS = "EPSG:4326"
TOPOLOGY = "tree"
HAS_UP_AREA = True

FLOW_DIR_ENCODING = "esri"
FLOW_DIR_NODATA_OUT = 255
FLOW_ACC_NODATA_OUT = -1.0
MERIT_FLOWDIR_UNDEFINED_AS_UINT8 = 247
ROW_GROUP_MIN = 4096
ROW_GROUP_MAX = 8192
TMP_ROW_GROUP_SIZE = 65_536
SNAP_BBOX_EPSILON = 1e-4
OUTLET_SNAP_TOLERANCE_DEGREES = 1e-6

DEFAULT_MERIT_BASINS_ROOT = Path("~/data/merit_basins/pfaf_level_02").expanduser()
DEFAULT_RASTERS_ROOT = Path("~/data/merit_hydro_rasters").expanduser()
DEFAULT_OUT = Path("/Users/nicolaslazaro/Desktop/merit-hfx-v2/tier1")
PLANETARY_BBOX = [-180.0, -90.0, 180.0, 90.0]

# Vendored from adapters/merit/run_missing_basins.py. Codes 87 and 88
# are absent from the mghydro raster distribution; pfaf-35 is excluded
# from planetary v2 builds because it crosses the antimeridian raster cut.
VALID_PFAF_CODES: tuple[int, ...] = (
    11, 12, 13, 14, 15, 16, 17, 18,
    21, 22, 23, 24, 25, 26, 27, 28, 29,
    31, 32, 33, 34, 35, 36,
    41, 42, 43, 44, 45, 46, 47, 48, 49,
    51, 52, 53, 54, 55, 56, 57,
    61, 62, 63, 64, 65, 66, 67,
    71, 72, 73, 74, 75, 76, 77, 78,
    81, 82, 83, 84, 85, 86,
    91,
)
EXCLUDED_PLANETARY_PFAF_CODES = frozenset({35})
ALL_INCLUDED_PFAF_CODES = tuple(
    code for code in VALID_PFAF_CODES if code not in EXCLUDED_PLANETARY_PFAF_CODES
)

logger = logging.getLogger("merit-v2")


class AdapterError(RuntimeError):
    """Raised when MERIT v2 adapter preconditions fail."""


@dataclass(frozen=True)
class SourceData:
    """Hold loaded MERIT-Basins inputs for one Pfaf-L2 basin."""

    pfaf: int | None
    pfaf_codes: tuple[int, ...]
    catchments: gpd.GeoDataFrame
    rivers: gpd.GeoDataFrame
    flow_dir_path: Path
    flow_acc_path: Path
    raster_paths: dict[int, tuple[Path, Path]] = field(default_factory=dict)


@dataclass
class BuildContext:
    """Carry build paths, metrics, and source-derived state across stages."""

    pfaf: int
    out_dir: Path
    tmp_dir: Path
    pfaf_codes: tuple[int, ...] = field(default_factory=tuple)
    timings: dict[str, dict[str, float]] = field(default_factory=dict)
    outlet_snap_count: int = 0
    max_outlet_drift_degrees: float = 0.0
    stem_role_counts: dict[str, int] = field(default_factory=dict)
    spec_d8_bounds_note: str = ""
    bbox: tuple[float, float, float, float] | None = None
    basin_bboxes: dict[int, tuple[float, float, float, float]] = field(default_factory=dict)
    per_basin_counts: dict[int, int] = field(default_factory=dict)
    hilbert_monotonic: bool = False
    d8_entries_written: list[str] = field(default_factory=list)
    ids: list[int] = field(default_factory=list)
    planetary: bool = False


def configure_logging(level: str) -> None:
    """Configure adapter logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def ensure_dir(path: Path) -> None:
    """Create a directory and its parents."""
    path.mkdir(parents=True, exist_ok=True)


def pd_concat(frames: list[gpd.GeoDataFrame]) -> pd.DataFrame:
    """Concatenate non-empty GeoDataFrames without reindex surprises."""
    if not frames:
        raise AdapterError("no frames to concatenate")
    return pd.concat(frames, ignore_index=True, copy=False)


def peak_rss_mb() -> float:
    """Return current process peak RSS in MB on macOS/Linux."""
    value = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    if sys.platform == "darwin":
        return value / 1024.0 / 1024.0
    return value / 1024.0


def run_stage(ctx: BuildContext, label: str, fn, *args):
    """Run a stage and record wall-clock plus peak RSS."""
    start = time.perf_counter()
    logger.info("%s start", label)
    result = fn(*args)
    elapsed = time.perf_counter() - start
    ctx.timings[label] = {
        "wall_seconds": elapsed,
        "peak_rss_mb": peak_rss_mb(),
    }
    logger.info("%s done wall=%.2fs peak_rss=%.1f MB", label, elapsed, ctx.timings[label]["peak_rss_mb"])
    return result


def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    """Build GeoParquet 1.1 metadata for an Arrow schema."""
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": geometry_types}},
    }
    return {b"geo": json.dumps(geo).encode("utf-8")}


def assert_geoparquet_valid(out_path: Path) -> None:
    """Raise when a Parquet file fails GeoParquet 1.1 validation."""
    result = validate_geoparquet(str(out_path), target_version="1.1")
    if result.is_valid:
        return
    failures = [check for check in result.checks if check.status.value == "failed"]
    details = "; ".join(f"{check.name}: {check.message}" for check in failures)
    raise AdapterError(f"GeoParquet validation failed for {out_path}: {details}")


def balanced_row_group_bounds(
    total_rows: int,
    min_size: int = ROW_GROUP_MIN,
    max_size: int = ROW_GROUP_MAX,
) -> list[tuple[int, int]]:
    """Split rows into validator-strict row-group slices."""
    if total_rows <= 0:
        return []
    if total_rows < min_size:
        return [(0, total_rows)]

    min_groups = math.ceil(total_rows / max_size)
    max_groups = max(1, total_rows // min_size)
    group_count = max_groups
    while group_count >= min_groups:
        base = total_rows // group_count
        remainder = total_rows % group_count
        largest = base + (1 if remainder else 0)
        if min_size <= base <= max_size and largest <= max_size:
            bounds = []
            start = 0
            for index in range(group_count):
                size = base + (1 if index < remainder else 0)
                stop = start + size
                bounds.append((start, stop))
                start = stop
            return bounds
        group_count -= 1
    return [(0, total_rows)]


def _basins_dir(root: Path) -> Path:
    candidate = root / "pfaf_level_02"
    return candidate if candidate.is_dir() else root


def _single_match(root: Path, pattern: str) -> Path:
    matches = sorted(root.glob(pattern))
    if len(matches) != 1:
        raise AdapterError(f"expected exactly one match for {pattern} under {root}, found {len(matches)}")
    return matches[0]


def _coerce_to_polygonal(geom: BaseGeometry) -> BaseGeometry:
    if geom.is_valid and isinstance(geom, (Polygon, MultiPolygon)):
        return geom
    repaired = make_valid(geom)
    if isinstance(repaired, (Polygon, MultiPolygon)):
        return repaired
    if isinstance(repaired, GeometryCollection):
        polygons: list[Polygon] = []
        for part in repaired.geoms:
            if isinstance(part, Polygon):
                polygons.append(part)
            elif isinstance(part, MultiPolygon):
                polygons.extend(part.geoms)
        if not polygons:
            raise AdapterError("make_valid produced no polygonal parts")
        return polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
    raise AdapterError(f"unsupported repaired geometry type: {type(repaired).__name__}")


def _line_coords(geom: BaseGeometry) -> list[tuple[float, float]]:
    if isinstance(geom, LineString):
        return [(float(x), float(y)) for x, y in geom.coords]
    if isinstance(geom, MultiLineString):
        raise AdapterError("MultiLineString outlet disambiguation is not implemented for Tier 1")
    raise AdapterError(f"expected LineString, got {geom.geom_type}")


def _downstream_endpoint(geom: BaseGeometry) -> tuple[float, float]:
    return _line_coords(geom)[-1]


def _bbox_frame(gdf: gpd.GeoDataFrame, inflate_degenerate: bool = False) -> gpd.GeoDataFrame:
    bounds = gdf.geometry.bounds.rename(
        columns={"minx": "bbox_minx", "miny": "bbox_miny", "maxx": "bbox_maxx", "maxy": "bbox_maxy"}
    )
    if inflate_degenerate:
        same_x = bounds["bbox_minx"] >= bounds["bbox_maxx"]
        same_y = bounds["bbox_miny"] >= bounds["bbox_maxy"]
        bounds.loc[same_x, "bbox_minx"] -= SNAP_BBOX_EPSILON
        bounds.loc[same_x, "bbox_maxx"] += SNAP_BBOX_EPSILON
        bounds.loc[same_y, "bbox_miny"] -= SNAP_BBOX_EPSILON
        bounds.loc[same_y, "bbox_maxy"] += SNAP_BBOX_EPSILON
    return bounds


def stage_1_inspect_source(merit_basins_root: Path, rasters_root: Path, pfaf: int) -> SourceData:
    """Load MERIT-Basins vectors and resolve source raster paths."""
    basins_dir = _basins_dir(merit_basins_root.expanduser())
    cat_path = _single_match(basins_dir, f"cat_pfaf_{pfaf:02d}_*.shp")
    riv_path = _single_match(basins_dir, f"riv_pfaf_{pfaf:02d}_*.shp")
    flow_dir = rasters_root.expanduser() / "flow_dir_basins" / f"flowdir{pfaf:02d}.tif"
    flow_acc = rasters_root.expanduser() / "accum_basins" / f"accum{pfaf:02d}.tif"
    if not flow_dir.exists() or not flow_acc.exists():
        raise AdapterError(f"missing source rasters for pfaf-{pfaf:02d}: {flow_dir}, {flow_acc}")

    catchments = gpd.read_file(cat_path, engine="pyogrio").set_crs(CRS, allow_override=True)
    rivers = gpd.read_file(riv_path, engine="pyogrio").set_crs(CRS, allow_override=True)
    for frame, name, required in (
        (catchments, "catchments", {"COMID", "unitarea"}),
        (rivers, "rivers", {"COMID", "NextDownID", "uparea"}),
    ):
        missing = sorted(required - set(frame.columns))
        if missing:
            raise AdapterError(f"{name} missing required columns: {missing}")
    catchments["_pfaf"] = pfaf
    rivers["_pfaf"] = pfaf
    return SourceData(
        pfaf=pfaf,
        pfaf_codes=(pfaf,),
        catchments=catchments,
        rivers=rivers,
        flow_dir_path=flow_dir,
        flow_acc_path=flow_acc,
        raster_paths={pfaf: (flow_dir, flow_acc)},
    )


def stage_1_inspect_sources(merit_basins_root: Path, rasters_root: Path, pfaf_codes: tuple[int, ...]) -> SourceData:
    """Load and concatenate MERIT-Basins inputs for multiple Pfaf-L2 basins."""
    sources = [
        stage_1_inspect_source(merit_basins_root, rasters_root, pfaf)
        for pfaf in pfaf_codes
    ]
    catchments = gpd.GeoDataFrame(
        pd_concat([source.catchments for source in sources]),
        geometry="geometry",
        crs=CRS,
    )
    rivers = gpd.GeoDataFrame(
        pd_concat([source.rivers for source in sources]),
        geometry="geometry",
        crs=CRS,
    )
    raster_paths = {
        pfaf: paths
        for source in sources
        for pfaf, paths in source.raster_paths.items()
    }
    return SourceData(
        pfaf=None,
        pfaf_codes=pfaf_codes,
        catchments=catchments,
        rivers=rivers,
        flow_dir_path=sources[0].flow_dir_path,
        flow_acc_path=sources[0].flow_acc_path,
        raster_paths=raster_paths,
    )


def stage_2_assign_ids(source: SourceData) -> gpd.GeoDataFrame:
    """Assign HFX unit IDs directly from MERIT COMID."""
    catchments = source.catchments.copy()
    catchments["id"] = catchments["COMID"].astype("int64")
    if (catchments["id"] <= 0).any():
        raise AdapterError("COMID contains non-positive values")
    if catchments["id"].duplicated().any():
        dup = catchments.loc[catchments["id"].duplicated(), "id"].head(10).tolist()
        raise AdapterError(f"duplicate COMID values in pfaf-{source.pfaf:02d}: {dup}")
    return catchments


def stage_3_reproject(catchments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Assert EPSG:4326 source coordinates."""
    if catchments.crs is None:
        return catchments.set_crs(CRS, allow_override=True)
    if catchments.crs.to_epsg() != 4326 and str(catchments.crs) != CRS:
        return catchments.to_crs(CRS)
    return catchments


def stage_4_make_valid(catchments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Repair catchment polygons."""
    repaired = catchments.copy()
    invalid_before = int((~repaired.geometry.is_valid).sum())
    repaired["geometry"] = repaired.geometry.map(_coerce_to_polygonal)
    invalid_after = int((~repaired.geometry.is_valid).sum())
    if invalid_after:
        raise AdapterError(f"{invalid_after} invalid geometries remain after make_valid")
    logger.info("make_valid invalid_before=%d invalid_after=%d", invalid_before, invalid_after)
    return repaired


def stage_5_hilbert_sort(catchments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Sort catchments by Hilbert index and COMID."""
    sorted_gdf = catchments.copy()
    sorted_gdf["_hilbert"] = sorted_gdf.geometry.centroid.hilbert_distance(total_bounds=sorted_gdf.total_bounds)
    sorted_gdf = sorted_gdf.sort_values(["_hilbert", "id"], kind="mergesort").drop(columns=["_hilbert"])
    return sorted_gdf.reset_index(drop=True)


def _outlets_for_units(
    catchments: gpd.GeoDataFrame,
    rivers: gpd.GeoDataFrame,
    ctx: BuildContext,
) -> tuple[np.ndarray, np.ndarray]:
    river_geom = dict(zip(rivers["COMID"].astype("int64"), rivers.geometry, strict=False))
    outlet_lon: list[float] = []
    outlet_lat: list[float] = []
    snap_count = 0
    max_drift = 0.0
    for row in catchments.itertuples(index=False):
        unit_id = int(row.id)
        endpoint = _downstream_endpoint(river_geom[unit_id])
        point = Point(endpoint)
        geom = row.geometry
        distance = float(geom.distance(point))
        if distance > OUTLET_SNAP_TOLERANCE_DEGREES:
            snapped = nearest_points(geom.boundary, point)[0]
            endpoint = (float(snapped.x), float(snapped.y))
            snap_count += 1
            max_drift = max(max_drift, distance)
        outlet_lon.append(float(endpoint[0]))
        outlet_lat.append(float(endpoint[1]))
    ctx.outlet_snap_count = snap_count
    ctx.max_outlet_drift_degrees = max_drift
    return np.asarray(outlet_lon, dtype="float64"), np.asarray(outlet_lat, dtype="float64")


def _write_table(path: Path, schema: pa.Schema, columns: list[pa.Array], row_count: int) -> None:
    row_groups = balanced_row_group_bounds(row_count)
    with pq.ParquetWriter(path, schema=schema, compression="snappy", write_statistics=True) as writer:
        table = pa.Table.from_arrays(columns, schema=schema)
        for start, stop in row_groups:
            writer.write_table(table.slice(start, stop - start))


def stage_6_write_catchments(source: SourceData, catchments: gpd.GeoDataFrame, ctx: BuildContext) -> None:
    """Write catchments.parquet."""
    rivers = source.rivers
    uparea = dict(zip(rivers["COMID"].astype("int64"), rivers["uparea"].astype("float64"), strict=False))
    ids = catchments["id"].astype("int64").to_numpy()
    up_area = np.asarray([uparea[int(unit_id)] for unit_id in ids], dtype="float32")
    outlet_lon, outlet_lat = _outlets_for_units(catchments, rivers, ctx)
    bounds = _bbox_frame(catchments)

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
            pa.field("source_id", pa.string(), nullable=True),
            pa.field("level_label", pa.string(), nullable=True),
        ]
    ).with_metadata(build_geo_metadata(["Polygon", "MultiPolygon"]))

    row_count = len(catchments)
    parent_ids = pa.array([None] * row_count, type=pa.int64())
    columns = [
        pa.array(ids, type=pa.int64()),
        pa.array(np.zeros(row_count, dtype="int16"), type=pa.int16()),
        parent_ids,
        pa.array(catchments["unitarea"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(up_area, type=pa.float32()),
        pa.array(outlet_lon, type=pa.float64()),
        pa.array(outlet_lat, type=pa.float64()),
        pa.array(bounds["bbox_minx"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(bounds["bbox_miny"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(bounds["bbox_maxx"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(bounds["bbox_maxy"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(catchments.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
        pa.array([f"merit:{int(unit_id)}" for unit_id in ids], type=pa.string()),
        pa.array(["merit-basins"] * row_count, type=pa.string()),
    ]
    _write_table(ctx.out_dir / "catchments.parquet", schema, columns, row_count)
    assert_geoparquet_valid(ctx.out_dir / "catchments.parquet")
    ctx.ids = [int(value) for value in ids.tolist()]
    ctx.bbox = (
        float(bounds["bbox_minx"].astype("float32").min()) - 1e-6,
        float(bounds["bbox_miny"].astype("float32").min()) - 1e-6,
        float(bounds["bbox_maxx"].astype("float32").max()) + 1e-6,
        float(bounds["bbox_maxy"].astype("float32").max()) + 1e-6,
    )
    ctx.per_basin_counts = {
        int(pfaf): int(count)
        for pfaf, count in catchments.groupby("_pfaf", sort=True).size().items()
    }
    ctx.basin_bboxes = {}
    for pfaf, group in catchments.groupby("_pfaf", sort=True):
        group_bounds = _bbox_frame(group)
        ctx.basin_bboxes[int(pfaf)] = (
            float(group_bounds["bbox_minx"].astype("float32").min()) - 1e-6,
            float(group_bounds["bbox_miny"].astype("float32").min()) - 1e-6,
            float(group_bounds["bbox_maxx"].astype("float32").max()) + 1e-6,
            float(group_bounds["bbox_maxy"].astype("float32").max()) + 1e-6,
        )


def stage_7_write_graph(source: SourceData, catchments: gpd.GeoDataFrame, ctx: BuildContext) -> None:
    """Write graph.parquet."""
    id_set = set(ctx.ids)
    upstream: dict[int, list[int]] = {unit_id: [] for unit_id in ctx.ids}
    for comid, next_down in zip(source.rivers["COMID"], source.rivers["NextDownID"], strict=True):
        cid = int(comid)
        nxt = int(next_down)
        if nxt <= 0 or nxt == cid:
            continue
        if nxt in id_set and cid in id_set:
            upstream[nxt].append(cid)

    bounds = _bbox_frame(catchments)
    bbox_by_id = {
        int(unit_id): tuple(float(v) for v in bbox)
        for unit_id, bbox in zip(
            catchments["id"],
            bounds[["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"]].to_numpy(),
            strict=True,
        )
    }
    rows = [(unit_id, sorted(upstream[unit_id]), bbox_by_id[unit_id]) for unit_id in ctx.ids]
    list_type = pa.list_(pa.field("item", pa.int64(), nullable=True))
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("upstream_ids", list_type, nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )
    row_count = len(rows)
    columns = [
        pa.array([row[0] for row in rows], type=pa.int64()),
        pa.array(np.zeros(row_count, dtype="int16"), type=pa.int16()),
        pa.array([row[1] for row in rows], type=list_type),
        pa.array([row[2][0] for row in rows], type=pa.float32()),
        pa.array([row[2][1] for row in rows], type=pa.float32()),
        pa.array([row[2][2] for row in rows], type=pa.float32()),
        pa.array([row[2][3] for row in rows], type=pa.float32()),
    ]
    _write_table(ctx.out_dir / "graph.parquet", schema, columns, row_count)


def _stem_roles(rivers: gpd.GeoDataFrame) -> dict[int, str]:
    roles: dict[int, str] = {int(comid): "mainstem" for comid in rivers["COMID"]}
    children: dict[int, list[tuple[int, float]]] = {}
    for comid, next_down, uparea in zip(rivers["COMID"], rivers["NextDownID"], rivers["uparea"], strict=True):
        if np.isnan(float(uparea)):
            roles[int(comid)] = "unknown"
            continue
        nxt = int(next_down)
        if nxt <= 0:
            continue
        children.setdefault(nxt, []).append((int(comid), float(uparea)))
    for kids in children.values():
        if len(kids) <= 1:
            continue
        winner = max(kids, key=lambda item: (item[1], item[0]))[0]
        for kid, _area in kids:
            if kid != winner and roles.get(kid) != "unknown":
                roles[kid] = "tributary"
    return roles


def stage_8_write_snap(source: SourceData, ctx: BuildContext) -> None:
    """Write aux/snap_stems.parquet."""
    snap_dir = ctx.out_dir / "aux"
    ensure_dir(snap_dir)
    rivers = source.rivers.copy()
    rivers["COMID"] = rivers["COMID"].astype("int64")
    rivers = rivers[rivers["COMID"].isin(ctx.ids)].copy()
    roles = _stem_roles(rivers)
    ctx.stem_role_counts = {
        role: int(sum(1 for value in roles.values() if value == role))
        for role in ("mainstem", "tributary", "unknown")
    }
    bounds = _bbox_frame(rivers, inflate_degenerate=True)
    rivers["_hilbert"] = rivers.geometry.centroid.hilbert_distance(total_bounds=ctx.bbox)
    rivers = rivers.sort_values(["_hilbert", "COMID"], kind="mergesort").reset_index(drop=True)
    bounds = _bbox_frame(rivers, inflate_degenerate=True).reset_index(drop=True)
    row_count = len(rivers)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox_minx", pa.float32(), nullable=True),
            pa.field("bbox_miny", pa.float32(), nullable=True),
            pa.field("bbox_maxx", pa.float32(), nullable=True),
            pa.field("bbox_maxy", pa.float32(), nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(build_geo_metadata(["LineString"]))
    columns = [
        pa.array(np.arange(1, row_count + 1, dtype="int64"), type=pa.int64()),
        pa.array(rivers["COMID"].astype("int64").to_numpy(), type=pa.int64()),
        pa.array(rivers["uparea"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array([roles[int(comid)] for comid in rivers["COMID"]], type=pa.string()),
        pa.array(bounds["bbox_minx"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(bounds["bbox_miny"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(bounds["bbox_maxx"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(bounds["bbox_maxy"].astype("float32").to_numpy(), type=pa.float32()),
        pa.array(rivers.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
    ]
    out_path = snap_dir / "snap_stems.parquet"
    _write_table(out_path, schema, columns, row_count)
    assert_geoparquet_valid(out_path)


def _window_for_bbox(src: rasterio.io.DatasetReader, bbox: tuple[float, float, float, float], pad_pixels: int = 10) -> tuple[Window, rasterio.Affine]:
    raw = from_bounds(*bbox, transform=src.transform)
    col_off = max(0, int(math.floor(raw.col_off)) - pad_pixels)
    row_off = max(0, int(math.floor(raw.row_off)) - pad_pixels)
    col_end = min(src.width, int(math.ceil(raw.col_off + raw.width)) + pad_pixels)
    row_end = min(src.height, int(math.ceil(raw.row_off + raw.height)) + pad_pixels)
    window = Window(col_off=col_off, row_off=row_off, width=max(1, col_end - col_off), height=max(1, row_end - row_off))
    return window, rasterio.windows.transform(window, src.transform)


def _write_cog(dst_path: Path, data: np.ndarray, profile: dict, predictor: int) -> None:
    ensure_dir(dst_path.parent)
    cog_profile = cog_profiles.get("deflate")
    cog_profile.update(blockxsize=512, blockysize=512, predictor=predictor, BIGTIFF="YES")
    with tempfile.TemporaryDirectory(prefix="merit-cog-") as tmp_dir:
        tmp_path = Path(tmp_dir) / "source.tif"
        with rasterio.open(tmp_path, "w", **profile) as tmp:
            tmp.write(data, 1)
        with rasterio.open(tmp_path) as tmp:
            cog_translate(tmp, str(dst_path), dst_kwargs=cog_profile, nodata=profile.get("nodata"), dtype=profile["dtype"], in_memory=False, quiet=True)
    valid, errors, warnings = cog_validate(str(dst_path))
    if not valid:
        raise AdapterError(f"COG validation failed for {dst_path}: errors={errors} warnings={warnings}")


def stage_8b_write_d8(source: SourceData, ctx: BuildContext) -> None:
    """Transcode D8 rasters into aux/d8/pfaf_NN for every selected basin."""
    if not ctx.basin_bboxes:
        raise AdapterError("ctx.basin_bboxes missing before raster transcode")
    ctx.d8_entries_written = []
    for pfaf in source.pfaf_codes:
        flow_dir_path, flow_acc_path = source.raster_paths[pfaf]
        bbox = ctx.basin_bboxes[pfaf]
        out_dir = ctx.out_dir / "aux" / "d8" / f"pfaf_{pfaf:02d}"
        with rasterio.open(flow_dir_path) as src:
            window, transform = _window_for_bbox(src, bbox)
            data = src.read(1, window=window)
            if data.dtype != np.uint8:
                data = data.astype("uint8")
            valid_mask = np.isin(data, [0, 1, 2, 4, 8, 16, 32, 64, 128, 255])
            data[~valid_mask] = FLOW_DIR_NODATA_OUT
            data[data == MERIT_FLOWDIR_UNDEFINED_AS_UINT8] = FLOW_DIR_NODATA_OUT
            out = data
            profile = src.profile.copy()
            profile.update(driver="GTiff", dtype="uint8", count=1, width=out.shape[1], height=out.shape[0], transform=transform, nodata=FLOW_DIR_NODATA_OUT, crs=src.crs)
            _write_cog(out_dir / "flow_dir.tif", out, profile, predictor=2)
        with rasterio.open(flow_acc_path) as src:
            window, transform = _window_for_bbox(src, bbox)
            data = src.read(1, window=window)
            out = data.astype("float32", copy=True)
            del data
            out[out == 0.0] = FLOW_ACC_NODATA_OUT
            profile = src.profile.copy()
            profile.update(driver="GTiff", dtype="float32", count=1, width=out.shape[1], height=out.shape[0], transform=transform, nodata=FLOW_ACC_NODATA_OUT, crs=src.crs)
            _write_cog(out_dir / "flow_acc.tif", out, profile, predictor=3)
        ctx.d8_entries_written.append(f"pfaf-{pfaf:02d}")


def stage_9_write_manifest(ctx: BuildContext) -> None:
    """Write manifest.json and README.md."""
    if ctx.bbox is None:
        raise AdapterError("ctx.bbox missing before manifest")
    bbox = PLANETARY_BBOX if ctx.planetary else [float(value) for value in ctx.bbox]
    pfaf_codes = ctx.pfaf_codes or (ctx.pfaf,)
    region = ",".join(f"pfaf-{pfaf:02d}" for pfaf in pfaf_codes)
    d8_entries = [
        {
            "schema": "hfx.aux.d8_raster.v1",
            "artifacts": {
                "flow_dir": f"aux/d8/pfaf_{pfaf:02d}/flow_dir.tif",
                "flow_acc": f"aux/d8/pfaf_{pfaf:02d}/flow_acc.tif",
            },
            "metadata": {"flow_dir_encoding": FLOW_DIR_ENCODING, "name": f"pfaf-{pfaf:02d}"},
        }
        for pfaf in pfaf_codes
    ]
    manifest = {
        "format_version": FORMAT_VERSION,
        "fabric_name": FABRIC_NAME,
        "fabric_version": FABRIC_VERSION,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "bbox": bbox,
        "unit_count": len(ctx.ids),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "adapter_version": ADAPTER_VERSION,
        "auxiliary": [
            {
                "schema": "hfx.aux.snap.v1",
                "artifacts": {"snap": "aux/snap_stems.parquet"},
                "metadata": {
                    "name": "stems",
                    "description": "MERIT-Basins reach centerlines for Pfaf-L2 basin snapping. weight = uparea (km^2). stem_role derived by largest-uparea descent at each confluence.",
                    "references_levels": [0],
                    "weight_semantics": "drainage_area_km2",
                },
            },
            *d8_entries,
        ],
    }
    if not ctx.planetary:
        manifest["region"] = region
    (ctx.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if ctx.planetary:
        built_at = datetime.now(timezone.utc).isoformat()
        readme = f"""# MERIT-Basins HFX v0.2.1 (global)

This dataset is a global HFX v0.2.1 compilation of the MERIT-
Basins vector hydrography (Lin et al. 2019) and MERIT Hydro
D8 flow-direction / flow-accumulation rasters (Yamazaki et
al. 2019), built by the MERIT v2 adapter
(adapters/merit-v2/build_adapter.py).

## Coverage

This dataset covers 60 of the 63 MERIT-Basins Pfaf-L2 basins.
The following are deliberately excluded:

- pfaf-87, pfaf-88 (Antarctic): no MERIT Hydro raster
  coverage at source (mghydro returns 404 for
  flowdir87/88.tif). MERIT-Basins itself has no Antarctic
  rivers.
- pfaf-35 (New Zealand / Pacific antimeridian): the catchment
  polygons wrap past 180\u00b0E and the per-basin D8 raster is
  clipped at the antimeridian. Excluded to preserve the
  invariant that every catchment has a paired D8 raster aux.

The manifest declares planetary bbox [-180, -90, 180, 90] for
catalog-discoverability; actual data coverage is the 60
included basins listed above.

## D8 Raster Layout

60 hfx.aux.d8_raster.v1 auxiliary entries, one per included
Pfaf basin, with COGs at aux/d8/pfaf_<NN>/flow_dir.tif and
aux/d8/pfaf_<NN>/flow_acc.tif. Each raster preserves native
MERIT Hydro grid geometry (cell-centered on 3-arcsec
resolution; cell edges may extend half a pixel outside
integer degree boundaries).

## Snap

One hfx.aux.snap.v1 entry ("stems") derived from MERIT-Basins
reach centerlines. weight = uparea (km\u00b2). stem_role assigned
by largest-uparea descent at each confluence.

## Provenance

- Source: MERIT-Basins v0.7 / v1.0_bugfix1 (Lin et al. 2019).
  Downloaded from the Google Drive share. Licensed CC BY-NC-SA
  4.0.
- Source: MERIT Hydro D8 rasters basin-merged rehost by M.
  Heberger at mghydro.com, derived from Yamazaki et al. 2019.
  Licensed dual CC BY-NC 4.0 / ODbL 1.0.
- Adapter version: see manifest.adapter_version.
- HFX spec version: 0.2.1.
- Built: {built_at}
"""
    else:
        readme = f"""# MERIT-Basins HFX v0.2.1 {region}

Tier smoke dataset for MERIT v2 adapter validation.

## Coverage

This partial-fabric dataset covers MERIT-Basins Pfaf-L2 basin(s) {region}. The manifest region is `{region}` and bbox is the source catchment union extent.

## D8 Raster Bounds

The D8 rasters preserve native MERIT Hydro grid geometry during transcode. HFX d8_raster.v1 requires EPSG:4326 COGs with declared dtype, nodata, tiling, and matching CRS; it does not impose a strict longitude/latitude domain clamp on GeoTIFF bounds.

The canonical D8 artifact paths are under `aux/d8/pfaf_<NN>/`.
"""
    (ctx.out_dir / "README.md").write_text(readme, encoding="utf-8")


def read_d8_spec_note() -> str:
    """Confirm d8_raster.v1 has no strict raster-bound domain constraint."""
    spec_path = Path(__file__).parents[2] / "spec" / "aux" / "d8_raster" / "v1.md"
    text = spec_path.read_text(encoding="utf-8")
    if "[-180" in text or "domain" in text.lower():
        raise AdapterError("d8_raster.v1 mentions an unexpected strict domain constraint; escalate before raster transcode")
    return "spec/aux/d8_raster/v1.md requires EPSG:4326 COGs and CRS consistency; it does not impose strict GeoTIFF bounds clamping."


def validate_dataset(dataset: Path, report_dir: Path) -> None:
    """Run strict validator in text and JSON modes."""
    ensure_dir(report_dir)
    commands = {
        "text": ["cargo", "run", "-p", "hfx-validator", "--", str(dataset), "--format", "text", "--strict", "--sample-pct", "100"],
        "json": ["cargo", "run", "-p", "hfx-validator", "--", str(dataset), "--format", "json", "--strict", "--sample-pct", "100"],
    }
    env = dict(os.environ)
    env.setdefault("RUST_LOG", "hfx_validator::reader::raster=debug")
    for kind, command in commands.items():
        result = subprocess.run(command, cwd=Path(__file__).parents[2], env=env, capture_output=True, text=True, check=False)
        (report_dir / f"validator-report.{kind}").write_text(result.stdout, encoding="utf-8")
        if result.stderr:
            (report_dir / f"validator-report.{kind}.stderr").write_text(result.stderr, encoding="utf-8")
        if result.returncode != 0:
            raise AdapterError(f"validator {kind} failed with code {result.returncode}; see {report_dir}")


def write_telemetry(ctx: BuildContext) -> None:
    """Write build telemetry for gate reporting."""
    telemetry = {
        "pfaf": ctx.pfaf,
        "timings": ctx.timings,
        "outlet_snap_count": ctx.outlet_snap_count,
        "max_outlet_drift_degrees": ctx.max_outlet_drift_degrees,
        "stem_role_counts": ctx.stem_role_counts,
        "spec_d8_bounds_note": ctx.spec_d8_bounds_note,
        "bbox": list(ctx.bbox or ()),
        "basin_bboxes": {str(key): list(value) for key, value in ctx.basin_bboxes.items()},
        "per_basin_counts": {str(key): value for key, value in ctx.per_basin_counts.items()},
        "hilbert_monotonic": ctx.hilbert_monotonic,
        "d8_entries_written": ctx.d8_entries_written,
        "unit_count": len(ctx.ids),
    }
    (ctx.out_dir / "build-telemetry.json").write_text(json.dumps(telemetry, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_pfaf_codes(raw: str | None, single: int | None, all_basins: bool = False) -> tuple[int, ...]:
    """Parse CLI Pfaf code arguments."""
    if all_basins:
        if raw or single is not None:
            raise AdapterError("--all-basins is mutually exclusive with --pfaf and --pfaf-codes")
        return ALL_INCLUDED_PFAF_CODES
    if raw and single is not None:
        raise AdapterError("--pfaf and --pfaf-codes are mutually exclusive")
    if raw:
        return tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if single is not None:
        return (int(single),)
    raise AdapterError("provide --pfaf or --pfaf-codes")


def output_name_for(pfaf_codes: tuple[int, ...]) -> str:
    """Return the canonical output directory name for the selected basin set.

    Planetary mode bypasses this helper and writes directly to the requested
    output path.
    """
    if len(pfaf_codes) == 1:
        return f"merit-hfx-pfaf{pfaf_codes[0]:02d}"
    if pfaf_codes == (27, 42, 91):
        return "merit-hfx-3basin"
    suffix = "-".join(f"{pfaf:02d}" for pfaf in pfaf_codes)
    return f"merit-hfx-pfaf-{suffix}"


def assert_hilbert_monotonic(catchments: gpd.GeoDataFrame) -> bool:
    """Confirm the final catchments are globally Hilbert-sorted."""
    hilbert = catchments.geometry.centroid.hilbert_distance(total_bounds=catchments.total_bounds)
    values = hilbert.to_numpy()
    if len(values) < 2:
        return True
    return bool(np.all(values[:-1] <= values[1:]))


def build_dataset(args: argparse.Namespace) -> None:
    """Build a single- or multi-basin HFX v0.2.1 dataset."""
    planetary = bool(getattr(args, "planetary", False))
    pfaf_codes = parse_pfaf_codes(
        getattr(args, "pfaf_codes", None),
        getattr(args, "pfaf", None),
        bool(getattr(args, "all_basins", False)),
    )
    dataset = args.out.expanduser() if planetary else args.out.expanduser() / output_name_for(pfaf_codes)
    if planetary and dataset.exists() and any(dataset.iterdir()) and not args.force:
        raise AdapterError(f"planetary output directory exists and is not empty: {dataset}; pass --force to replace it")
    if dataset.exists() and args.force:
        shutil.rmtree(dataset)
    ensure_dir(dataset)
    ctx = BuildContext(pfaf=pfaf_codes[0], out_dir=dataset, tmp_dir=dataset / "tmp", pfaf_codes=pfaf_codes, planetary=planetary)
    ctx.spec_d8_bounds_note = read_d8_spec_note()
    source = run_stage(ctx, "stage_1_inspect_source", stage_1_inspect_sources, args.merit_basins, args.rasters, pfaf_codes)
    catchments = run_stage(ctx, "stage_2_assign_ids", stage_2_assign_ids, source)
    catchments = run_stage(ctx, "stage_3_reproject", stage_3_reproject, catchments)
    catchments = run_stage(ctx, "stage_4_make_valid", stage_4_make_valid, catchments)
    catchments = run_stage(ctx, "stage_5_hilbert_sort", stage_5_hilbert_sort, catchments)
    ctx.hilbert_monotonic = assert_hilbert_monotonic(catchments)
    if not ctx.hilbert_monotonic:
        raise AdapterError("catchments are not globally Hilbert-monotonic after stage_5")
    run_stage(ctx, "stage_6_write_catchments", stage_6_write_catchments, source, catchments, ctx)
    run_stage(ctx, "stage_7_write_graph", stage_7_write_graph, source, catchments, ctx)
    run_stage(ctx, "stage_8_write_snap", stage_8_write_snap, source, ctx)
    run_stage(ctx, "stage_8b_write_d8", stage_8b_write_d8, source, ctx)
    run_stage(ctx, "stage_9_write_manifest", stage_9_write_manifest, ctx)
    run_stage(ctx, "phase_4_validate", validate_dataset, dataset, dataset / "validation")
    write_telemetry(ctx)
    logger.info("built %s", dataset)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""
    parser = argparse.ArgumentParser(description="Build MERIT v2 HFX v0.2.1 datasets.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="build one Pfaf-L2 basin")
    build.add_argument("--merit-basins", type=Path, default=DEFAULT_MERIT_BASINS_ROOT)
    build.add_argument("--rasters", type=Path, default=DEFAULT_RASTERS_ROOT)
    build.add_argument("--pfaf", type=int)
    build.add_argument("--pfaf-codes", help="comma-separated Pfaf-L2 codes, e.g. 27,42,91")
    build.add_argument(
        "--planetary",
        action="store_true",
        help="emit a planetary-mode dataset (literal planet bbox, no region, coverage README). Use with --all-basins for the full 60-basin global set.",
    )
    build.add_argument(
        "--all-basins",
        action="store_true",
        help="build all 60 included basins (VALID_PFAF_CODES - {35}). Mutually exclusive with --pfaf/--pfaf-codes.",
    )
    build.add_argument("--out", type=Path, default=DEFAULT_OUT)
    build.add_argument("--force", action="store_true")
    build.add_argument("--log-level", default="INFO")
    validate = subparsers.add_parser("validate", help="validate an existing dataset")
    validate.add_argument("dataset", type=Path)
    validate.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.log_level)
    if args.command == "build":
        build_dataset(args)
        return 0
    if args.command == "validate":
        validate_dataset(args.dataset.expanduser(), args.dataset.expanduser() / "validation")
        return 0
    raise AdapterError(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
