#!/usr/bin/env python3
"""HydroBASINS HFX adapter command-line shell."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import subprocess
import warnings
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from geoparquet_io.core.validate import validate_geoparquet
from shapely import make_valid
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


FABRIC_NAME = "hydrobasins"
ADAPTER_VERSION = "0.1.0"
FORMAT_VERSION = "0.3.0"
TOPOLOGY = "tree"
HAS_UP_AREA = True
CRS = "EPSG:4326"
PLANETARY_BBOX = [-180.0, -90.0, 180.0, 90.0]
FABRIC_VERSION = "v1c"  # Assumed HydroBASINS standard-product version, pending confirmation per the vision open question.
HAS_RASTERS = False
HAS_SNAP = False
ROW_GROUP_MIN = 4096
ROW_GROUP_MAX = 8192
SNAP_BBOX_EPSILON = 1e-4
BBOX_LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")
STANDARD_REGION_CODES = ("af", "ar", "as", "au", "eu", "gr", "na", "sa", "si")
LOGGER = logging.getLogger(__name__)


class AdapterError(RuntimeError):
    """Report a HydroBASINS source-contract violation."""


def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    """Build GeoParquet 1.1 metadata with a `bbox` covering."""
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
                "covering": {
                    "bbox": {name: ["bbox", name] for name in BBOX_LEAF_NAMES}
                },
            }
        },
    }
    return {b"geo": json.dumps(geo).encode("utf-8")}


def bbox_struct_type() -> pa.DataType:
    """Return the GeoParquet covering `bbox` type with non-nullable leaves."""
    return pa.struct(
        [pa.field(name, pa.float32(), nullable=False) for name in BBOX_LEAF_NAMES]
    )


def build_bbox_struct(minx, miny, maxx, maxy) -> pa.StructArray:
    """Build a bbox struct whose float32 leaves carry row-group statistics."""
    return pa.StructArray.from_arrays(
        [
            pa.array(minx, type=pa.float32()),
            pa.array(miny, type=pa.float32()),
            pa.array(maxx, type=pa.float32()),
            pa.array(maxy, type=pa.float32()),
        ],
        fields=[
            pa.field(name, pa.float32(), nullable=False)
            for name in BBOX_LEAF_NAMES
        ],
    )


def assert_geoparquet_valid(path: Path) -> None:
    """Raise when a Parquet file fails GeoParquet 1.1 validation."""
    result = validate_geoparquet(str(path), target_version="1.1")
    if result.is_valid:
        return
    failures = [check for check in result.checks if check.status.value == "failed"]
    details = "; ".join(f"{check.name}: {check.message}" for check in failures)
    raise AdapterError(f"GeoParquet validation failed for {path}: {details}")


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


def _write_table(
    path: Path,
    schema: pa.Schema,
    columns: list[pa.Array],
    row_count: int,
) -> None:
    """Write an Arrow table with balanced Parquet row groups."""
    table = pa.Table.from_arrays(columns, schema=schema)
    with pq.ParquetWriter(
        path,
        schema=schema,
        compression="snappy",
        write_statistics=True,
    ) as writer:
        for start, stop in balanced_row_group_bounds(row_count):
            writer.write_table(table.slice(start, stop - start))


def _source_path(region: str, basins_dir: Path) -> Path:
    filename = f"hybas_{region}_lev12_v1.shp"
    matches = sorted(basins_dir.glob(filename))
    if len(matches) != 1:
        raise AdapterError(
            f"expected exactly one {filename} layer under {basins_dir}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _pour_points_source_path(pour_points_dir: Path) -> Path:
    filename = "hybas_pour_lev12_v1.shp"
    matches = sorted(pour_points_dir.rglob(filename))
    if len(matches) != 1:
        raise AdapterError(
            f"expected exactly one {filename} layer under {pour_points_dir}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _rivers_source_path(rivers_dir: Path) -> Path:
    if not rivers_dir.is_dir():
        raise AdapterError(
            f"HydroRIVERS source {rivers_dir} must be an existing readable directory"
        )
    try:
        matches = sorted(rivers_dir.rglob("*.shp"))
    except OSError as error:
        raise AdapterError(
            f"HydroRIVERS source {rivers_dir} must be a readable directory: {error}"
        ) from error
    if len(matches) != 1:
        raise AdapterError(
            f"expected exactly one HydroRIVERS layer under {rivers_dir}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _coerce_to_polygonal(geometry: BaseGeometry | None) -> BaseGeometry:
    if geometry is None or geometry.is_empty:
        raise AdapterError("geometry is null or empty")
    if geometry.is_valid and isinstance(geometry, (Polygon, MultiPolygon)):
        return geometry

    repaired = make_valid(geometry)
    if repaired.is_empty:
        raise AdapterError("geometry repair produced an empty geometry")
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
            raise AdapterError("geometry repair produced no polygonal parts")
        return polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
    raise AdapterError(
        f"unsupported repaired geometry type: {type(repaired).__name__}"
    )


def _normalize_ids(
    values: pd.Series,
    *,
    reject_duplicates: bool = True,
    source_name: str = "HYBAS_ID",
) -> pd.Series:
    normalized: list[int] = []
    maximum = 2**63 - 1
    for value in values:
        try:
            decimal = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise AdapterError(
                f"{source_name} contains a non-numeric value: {value!r}"
            ) from None
        if not decimal.is_finite() or decimal != decimal.to_integral_value():
            raise AdapterError(
                f"{source_name} contains a non-integral value: {value!r}"
            )
        identifier = int(decimal)
        if identifier <= 0:
            raise AdapterError(
                f"{source_name} values must be strictly positive; "
                "0 is the terminal-sink sentinel"
            )
        if identifier > maximum:
            raise AdapterError(
                f"{source_name} is outside the int64 range: {value!r}"
            )
        normalized.append(identifier)

    result = pd.Series(normalized, index=values.index, dtype="int64")
    if reject_duplicates and result.duplicated().any():
        duplicates = result.loc[result.duplicated(keep=False)].unique().tolist()
        raise AdapterError(f"duplicate {source_name} values: {duplicates}")
    return result


def _normalize_area(values: pd.Series, source_name: str) -> pd.Series:
    try:
        return pd.to_numeric(values, errors="raise").astype("float64")
    except (TypeError, ValueError, OverflowError) as error:
        raise AdapterError(f"{source_name} cannot be converted to float64: {error}") from error


def _normalize_topology_integers(
    values: pd.Series,
    source_name: str,
    *,
    allow_negative: bool,
) -> pd.Series:
    """Normalize a required topology field to non-null int64 values."""
    normalized: list[int] = []
    minimum = -(2**63)
    maximum = 2**63 - 1
    for value in values:
        try:
            decimal = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise AdapterError(
                f"{source_name} contains a non-numeric value: {value!r}"
            ) from None
        if not decimal.is_finite() or decimal != decimal.to_integral_value():
            raise AdapterError(
                f"{source_name} contains a non-integral value: {value!r}"
            )
        integer = int(decimal)
        if not allow_negative and integer < 0:
            raise AdapterError(
                f"{source_name} values must be non-negative; "
                "0 is the terminal-sink sentinel"
            )
        if integer < minimum or integer > maximum:
            raise AdapterError(f"{source_name} is outside the int64 range: {value!r}")
        normalized.append(integer)
    return pd.Series(normalized, index=values.index, dtype="int64")


def _hilbert_sort(units: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Sort units stably by centroid Hilbert distance and ID."""
    sorted_units = units.copy()
    sorted_units["_hilbert"] = sorted_units.geometry.centroid.hilbert_distance(
        total_bounds=sorted_units.total_bounds
    )
    sorted_units = sorted_units.sort_values(
        ["_hilbert", "id"], kind="mergesort"
    )
    return sorted_units.drop(columns=["_hilbert"]).reset_index(drop=True)


def load_region_units(region: str, basins_dir: Path) -> gpd.GeoDataFrame:
    """Load and normalize one HydroBASINS Pfaf-12 regional polygon layer."""
    source_path = _source_path(region, basins_dir)
    try:
        units = gpd.read_file(source_path, engine="pyogrio")
    except Exception as error:
        raise AdapterError(f"failed to read HydroBASINS layer {source_path}: {error}") from error

    required = {"HYBAS_ID", "SUB_AREA", "UP_AREA", "NEXT_DOWN", "ENDO"}
    missing = sorted(required - set(units.columns))
    if missing:
        raise AdapterError(f"HydroBASINS layer missing required columns: {missing}")
    if units.empty:
        raise AdapterError("HydroBASINS layer contains no units")
    if units.crs is None:
        raise AdapterError("HydroBASINS layer has no declared CRS")
    if units.crs.to_epsg() != 4326:
        try:
            units = units.to_crs("EPSG:4326")
        except Exception as error:
            raise AdapterError(f"failed to transform HydroBASINS layer to EPSG:4326: {error}") from error
    if units.crs is None or units.crs.to_epsg() != 4326:
        raise AdapterError("normalized HydroBASINS layer does not resolve to EPSG:4326")

    units = units.copy()
    units["geometry"] = units.geometry.map(_coerce_to_polygonal)
    if units.geometry.isna().any() or units.geometry.is_empty.any():
        raise AdapterError("null or empty geometries remain after repair")
    if (~units.geometry.is_valid).any():
        raise AdapterError("invalid geometries remain after repair")
    if not units.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all():
        raise AdapterError("non-polygonal geometries remain after repair")

    units["id"] = _normalize_ids(units["HYBAS_ID"])
    units["level"] = pd.Series(0, index=units.index, dtype="int64")
    units["parent_id"] = pd.Series(pd.NA, index=units.index, dtype="Int64")
    units["area_km2"] = _normalize_area(units["SUB_AREA"], "SUB_AREA")
    units["up_area_km2"] = _normalize_area(units["UP_AREA"], "UP_AREA")
    units["NEXT_DOWN"] = _normalize_topology_integers(
        units["NEXT_DOWN"],
        "NEXT_DOWN",
        allow_negative=False,
    )
    units["ENDO"] = _normalize_topology_integers(
        units["ENDO"],
        "ENDO",
        allow_negative=True,
    )

    return _hilbert_sort(units)


def load_pour_points(pour_points_dir: Path) -> gpd.GeoDataFrame:
    """Load and normalize the HydroBASINS Pfaf-12 ancillary pour points."""
    source_path = _pour_points_source_path(pour_points_dir)
    try:
        pour_points = gpd.read_file(source_path, engine="pyogrio")
    except Exception as error:
        raise AdapterError(
            f"failed to read HydroBASINS pour-points layer {source_path}: {error}"
        ) from error

    if "HYBAS_ID" in pour_points.columns:
        join_key = "HYBAS_ID"
    else:
        candidates = [
            column
            for column in pour_points.columns
            if column.casefold() == "HYBAS_ID".casefold()
        ]
        if not candidates:
            raise AdapterError(
                "HydroBASINS pour-points layer missing join key HYBAS_ID; "
                f"available columns: {list(pour_points.columns)}"
            )
        if len(candidates) > 1:
            raise AdapterError(
                "ambiguous case-insensitive HYBAS_ID columns in HydroBASINS "
                f"pour-points layer: {candidates}"
            )
        join_key = candidates[0]

    if pour_points.crs is None:
        raise AdapterError("HydroBASINS pour-points layer has no declared CRS")
    if pour_points.crs.to_epsg() != 4326:
        try:
            pour_points = pour_points.to_crs("EPSG:4326")
        except Exception as error:
            raise AdapterError(
                "failed to transform HydroBASINS pour-points layer to "
                f"EPSG:4326: {error}"
            ) from error
    if pour_points.crs is None or pour_points.crs.to_epsg() != 4326:
        raise AdapterError(
            "normalized HydroBASINS pour-points layer does not resolve to EPSG:4326"
        )

    if pour_points.geometry.isna().any():
        raise AdapterError("HydroBASINS pour-points geometry is null")
    if pour_points.geometry.is_empty.any():
        raise AdapterError("HydroBASINS pour-points geometry is empty")
    if not pour_points.geometry.geom_type.eq("Point").all():
        invalid_types = sorted(set(pour_points.geometry.geom_type) - {"Point"})
        raise AdapterError(
            "HydroBASINS pour-points geometry must contain only Point values; "
            f"found {invalid_types}"
        )

    normalized = pour_points.copy()
    normalized["id"] = _normalize_ids(
        normalized[join_key], reject_duplicates=False
    )
    normalized["outlet_lon"] = normalized.geometry.x.astype("float64")
    normalized["outlet_lat"] = normalized.geometry.y.astype("float64")
    return normalized


def _resolve_rivers_fields(rivers: pd.DataFrame) -> pd.DataFrame:
    """Resolve required HydroRIVERS attributes to canonical names."""
    required = ("HYRIV_ID", "HYBAS_L12", "UPLAND_SKM", "NEXT_DOWN")
    available = [str(column) for column in rivers.columns]
    renames: dict[object, str] = {}
    for canonical in required:
        candidates = [
            column
            for column in rivers.columns
            if str(column).casefold() == canonical.casefold()
        ]
        if not candidates:
            raise AdapterError(
                f"HydroRIVERS layer missing required field {canonical}; "
                f"available fields: {available}"
            )
        if len(candidates) > 1:
            raise AdapterError(
                f"ambiguous HydroRIVERS field {canonical}: "
                f"candidates {[str(candidate) for candidate in candidates]}"
            )
        if candidates[0] != canonical:
            renames[candidates[0]] = canonical
    return rivers.rename(columns=renames)


def _normalize_rivers_layer(rivers: pd.DataFrame) -> gpd.GeoDataFrame:
    """Normalize one loaded HydroRIVERS layer at the source boundary."""
    geometry_columns = [
        str(column)
        for column in rivers.columns
        if isinstance(rivers[column].dtype, gpd.array.GeometryDtype)
    ]
    if not isinstance(rivers, gpd.GeoDataFrame) or not geometry_columns:
        raise AdapterError("HydroRIVERS layer has no active geometry column")
    try:
        active_geometry = rivers.geometry.name
    except AttributeError as error:
        raise AdapterError("HydroRIVERS layer has no active geometry column") from error
    if len(geometry_columns) != 1:
        raise AdapterError(
            "ambiguous HydroRIVERS geometry columns: "
            f"{geometry_columns}"
        )
    if active_geometry != geometry_columns[0]:
        raise AdapterError(
            "HydroRIVERS active geometry does not match its geometry column: "
            f"active={active_geometry!r}, available={geometry_columns}"
        )

    rivers = gpd.GeoDataFrame(
        _resolve_rivers_fields(rivers),
        geometry=active_geometry,
        crs=rivers.crs,
    )
    if rivers.empty:
        raise AdapterError("HydroRIVERS layer contains no reaches")
    if rivers.crs is None:
        raise AdapterError("HydroRIVERS layer has no declared CRS")
    if rivers.geometry.isna().any():
        raise AdapterError("HydroRIVERS layer contains null geometries")
    if rivers.geometry.is_empty.any():
        raise AdapterError("HydroRIVERS layer contains empty geometries")
    if not rivers.geometry.geom_type.eq("LineString").all():
        invalid_types = sorted(set(rivers.geometry.geom_type) - {"LineString"})
        raise AdapterError(
            "HydroRIVERS layer must contain only LineString geometries; "
            f"found {invalid_types}"
        )
    if rivers.crs.to_epsg() != 4326:
        try:
            rivers = rivers.to_crs(CRS)
        except Exception as error:
            raise AdapterError(
                f"failed to transform HydroRIVERS layer to EPSG:4326: {error}"
            ) from error
    if rivers.crs is None or rivers.crs.to_epsg() != 4326:
        raise AdapterError(
            "normalized HydroRIVERS layer does not resolve to EPSG:4326"
        )

    rivers = rivers.copy()
    rivers["HYRIV_ID"] = _normalize_ids(
        rivers["HYRIV_ID"],
        reject_duplicates=True,
        source_name="HYRIV_ID",
    )
    rivers["HYBAS_L12"] = _normalize_ids(
        rivers["HYBAS_L12"],
        reject_duplicates=False,
        source_name="HYBAS_L12",
    )
    try:
        weights = pd.to_numeric(rivers["UPLAND_SKM"], errors="raise").astype(
            "float64"
        )
    except (TypeError, ValueError, OverflowError) as error:
        raise AdapterError(
            f"UPLAND_SKM cannot be converted to float64: {error}"
        ) from error
    float32_weights = weights.to_numpy(dtype="float32")
    if not np.isfinite(weights.to_numpy()).all() or not np.isfinite(
        float32_weights
    ).all():
        raise AdapterError("UPLAND_SKM values must be finite float32 values")
    if (weights < 0).any():
        raise AdapterError("UPLAND_SKM values must be non-negative")
    rivers["UPLAND_SKM"] = pd.Series(
        float32_weights,
        index=rivers.index,
        dtype="float32",
    )
    rivers["NEXT_DOWN"] = _normalize_topology_integers(
        rivers["NEXT_DOWN"],
        "NEXT_DOWN",
        allow_negative=False,
    )
    rivers["_source_order"] = np.arange(len(rivers), dtype="int64")
    return rivers


def load_rivers(rivers_dir: Path) -> gpd.GeoDataFrame:
    """Load and normalize one recursively discovered HydroRIVERS layer."""
    source_path = _rivers_source_path(rivers_dir)
    try:
        rivers = gpd.read_file(source_path, engine="pyogrio")
    except Exception as error:
        raise AdapterError(
            f"failed to read HydroRIVERS layer {source_path}: {error}"
        ) from error
    return _normalize_rivers_layer(rivers)


def _stem_roles(rivers: gpd.GeoDataFrame) -> dict[int, str]:
    """Derive HydroRIVERS stem roles from confluence topology."""
    roles = {int(reach_id): "mainstem" for reach_id in rivers["HYRIV_ID"]}
    children: dict[int, list[tuple[int, float]]] = {}
    for reach_id, next_down, upland_skm in zip(
        rivers["HYRIV_ID"],
        rivers["NEXT_DOWN"],
        rivers["UPLAND_SKM"],
        strict=True,
    ):
        if np.isnan(float(upland_skm)):
            roles[int(reach_id)] = "unknown"
            continue
        downstream_id = int(next_down)
        if downstream_id <= 0:
            continue
        children.setdefault(downstream_id, []).append(
            (int(reach_id), float(upland_skm))
        )
    for contributors in children.values():
        if len(contributors) <= 1:
            continue
        winner = max(contributors, key=lambda item: (item[1], item[0]))[0]
        for reach_id, _area in contributors:
            if reach_id != winner and roles.get(reach_id) != "unknown":
                roles[reach_id] = "tributary"
    return roles


def assign_outlets(
    units: gpd.GeoDataFrame,
    pour_points: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Assign one deterministic, finite pour-point outlet to every unit."""
    points_by_id = {
        int(identifier): group
        for identifier, group in pour_points.groupby("id", sort=False)
    }
    missing = sorted(
        int(identifier)
        for identifier in units["id"]
        if int(identifier) not in points_by_id
    )
    if missing:
        raise AdapterError(f"units missing HydroBASINS pour points: {missing}")

    selected: list[tuple[float, float]] = []
    for unit in units.itertuples(index=False):
        identifier = int(unit.id)
        centroid = unit.geometry.centroid
        candidates = points_by_id[identifier]
        ranked = sorted(
            (
                (
                    centroid.distance(point.geometry),
                    float(point.outlet_lon),
                    float(point.outlet_lat),
                )
                for point in candidates.itertuples(index=False)
            ),
            key=lambda candidate: candidate,
        )
        _, outlet_lon, outlet_lat = ranked[0]
        if (
            not math.isfinite(outlet_lon)
            or not math.isfinite(outlet_lat)
            or not -180.0 <= outlet_lon <= 180.0
            or not -90.0 <= outlet_lat <= 90.0
        ):
            raise AdapterError(
                f"unit {identifier} has invalid outlet coordinate "
                f"({outlet_lon}, {outlet_lat})"
            )
        selected.append((outlet_lon, outlet_lat))

    assigned = units.copy()
    assigned["outlet_lon"] = pd.Series(
        (coordinates[0] for coordinates in selected),
        index=assigned.index,
        dtype="float64",
    )
    assigned["outlet_lat"] = pd.Series(
        (coordinates[1] for coordinates in selected),
        index=assigned.index,
        dtype="float64",
    )
    return assigned


def guard_antimeridian(
    units: gpd.GeoDataFrame,
    *,
    strict_build: bool,
) -> None:
    """Warn about or reject unit geometries whose raw longitude extent exceeds 180 degrees."""
    bounds = units.geometry.bounds
    candidate_ids = sorted(
        int(identifier)
        for identifier in units.loc[
            (bounds["maxx"] - bounds["minx"]) > 180.0,
            "id",
        ]
    )
    if not candidate_ids:
        return

    message = (
        "antimeridian-wrap candidates detected: "
        f"count={len(candidate_ids)}, HYBAS_IDs={candidate_ids}"
    )
    if strict_build:
        raise AdapterError(message)
    warnings.warn(message, UserWarning, stacklevel=2)


def write_catchments(units: gpd.GeoDataFrame, out_dir: Path) -> Path:
    """Write normalized units as an HFX catchments GeoParquet slice."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "catchments.parquet"
    row_count = len(units)
    bounds = units.reset_index(drop=True).geometry.bounds

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox", bbox_struct_type(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(build_geo_metadata(["Polygon", "MultiPolygon"]))

    columns = [
        pa.array(units["id"].to_numpy(), type=pa.int64()),
        pa.array(units["level"].to_numpy(dtype="int16"), type=pa.int16()),
        pa.nulls(row_count, type=pa.int64()),
        pa.array(units["area_km2"].to_numpy(dtype="float32"), type=pa.float32()),
        pa.array(
            units["up_area_km2"].to_numpy(dtype="float32"),
            type=pa.float32(),
        ),
        pa.array(units["outlet_lon"].to_numpy(dtype="float64"), type=pa.float64()),
        pa.array(units["outlet_lat"].to_numpy(dtype="float64"), type=pa.float64()),
        build_bbox_struct(
            bounds["minx"].to_numpy(dtype="float32"),
            bounds["miny"].to_numpy(dtype="float32"),
            bounds["maxx"].to_numpy(dtype="float32"),
            bounds["maxy"].to_numpy(dtype="float32"),
        ),
        pa.array(units.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
    ]
    _write_table(path, schema, columns, row_count)
    assert_geoparquet_valid(path)
    return path


def _assert_acyclic(outgoing: dict[int, int]) -> None:
    """Reject cycles in a deterministic scalar downstream relation."""
    visited: set[int] = set()
    for start in sorted(outgoing):
        if start in visited:
            continue
        path: list[int] = []
        positions: dict[int, int] = {}
        current = start
        while current in outgoing and current not in visited:
            if current in positions:
                cycle = path[positions[current] :] + [current]
                raise AdapterError(
                    "cut HydroBASINS graph must be acyclic; cycle detected: "
                    + " -> ".join(str(identifier) for identifier in cycle)
                )
            positions[current] = len(path)
            path.append(current)
            current = outgoing[current]
        visited.update(path)


def write_graph(units: gpd.GeoDataFrame, out_dir: Path) -> Path:
    """Write the cut, acyclic HydroBASINS tree as HFX graph Parquet."""
    unit_ids = [int(identifier) for identifier in units["id"]]
    id_set = set(unit_ids)
    upstream: dict[int, list[int]] = {identifier: [] for identifier in unit_ids}
    outgoing: dict[int, int] = {}

    for unit in units.itertuples(index=False):
        source_id = int(unit.id)
        downstream_id = int(unit.NEXT_DOWN)
        endo = int(unit.ENDO)
        if downstream_id == 0:
            continue
        if endo == 2:
            continue
        if downstream_id < 0:
            raise AdapterError(
                f"unit {source_id} has negative NEXT_DOWN value {downstream_id}"
            )
        if downstream_id == source_id:
            raise AdapterError(f"unit {source_id} has a topology self-link cycle")
        if downstream_id not in id_set:
            raise AdapterError(
                f"unit {source_id} has downstream ID {downstream_id}, "
                "which is absent from this dataset"
            )
        outgoing[source_id] = downstream_id
        upstream[downstream_id].append(source_id)

    _assert_acyclic(outgoing)
    for identifiers in upstream.values():
        identifiers.sort()

    bounds = units.reset_index(drop=True).geometry.bounds
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
    row_count = len(units)
    columns = [
        pa.array(units["id"].to_numpy(dtype="int64"), type=pa.int64()),
        pa.array(units["level"].to_numpy(dtype="int16"), type=pa.int16()),
        pa.array([upstream[identifier] for identifier in unit_ids], type=list_type),
        pa.array(bounds["minx"].to_numpy(dtype="float32"), type=pa.float32()),
        pa.array(bounds["miny"].to_numpy(dtype="float32"), type=pa.float32()),
        pa.array(bounds["maxx"].to_numpy(dtype="float32"), type=pa.float32()),
        pa.array(bounds["maxy"].to_numpy(dtype="float32"), type=pa.float32()),
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "graph.parquet"
    _write_table(path, schema, columns, row_count)
    return path


def write_snap_stems(
    rivers: gpd.GeoDataFrame,
    units: gpd.GeoDataFrame,
    out_dir: Path,
) -> Path:
    """Write normalized HydroRIVERS reaches as an HFX snap auxiliary."""
    ordered = rivers.copy()
    ordered["_hilbert"] = ordered.geometry.centroid.hilbert_distance(
        total_bounds=units.geometry.total_bounds
    )
    ordered = ordered.sort_values(
        ["_hilbert", "_source_order"], kind="mergesort"
    ).reset_index(drop=True)
    bounds = ordered.geometry.bounds
    minx = bounds["minx"].to_numpy(dtype="float32")
    miny = bounds["miny"].to_numpy(dtype="float32")
    maxx = bounds["maxx"].to_numpy(dtype="float32")
    maxy = bounds["maxy"].to_numpy(dtype="float32")
    degenerate_x = minx >= maxx
    degenerate_y = miny >= maxy
    minx[degenerate_x] -= SNAP_BBOX_EPSILON
    maxx[degenerate_x] += SNAP_BBOX_EPSILON
    miny[degenerate_y] -= SNAP_BBOX_EPSILON
    maxy[degenerate_y] += SNAP_BBOX_EPSILON

    row_count = len(ordered)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox", bbox_struct_type(), nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(build_geo_metadata(["LineString"]))
    columns = [
        pa.array(np.arange(1, row_count + 1, dtype="int64"), type=pa.int64()),
        pa.array(ordered["HYBAS_L12"].to_numpy(dtype="int64"), type=pa.int64()),
        pa.array(ordered["UPLAND_SKM"].to_numpy(dtype="float32"), type=pa.float32()),
        pa.array(ordered["stem_role"].tolist(), type=pa.string()),
        build_bbox_struct(minx, miny, maxx, maxy),
        pa.array(ordered.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
    ]
    aux_dir = out_dir / "aux"
    aux_dir.mkdir(parents=True, exist_ok=True)
    path = aux_dir / "snap_stems.parquet"
    _write_table(path, schema, columns, row_count)
    assert_geoparquet_valid(path)
    return path


def write_manifest(
    units: gpd.GeoDataFrame,
    out_dir: Path,
    region_label: str,
    planetary: bool,
    snap_path: Path | None = None,
) -> Path:
    """Write the HFX dataset manifest with regional or planetary semantics."""
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "format_version": FORMAT_VERSION,
        "fabric_name": FABRIC_NAME,
        "fabric_version": FABRIC_VERSION,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "unit_count": len(units),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "adapter_version": ADAPTER_VERSION,
    }
    if planetary:
        manifest["bbox"] = list(PLANETARY_BBOX)
    else:
        manifest["region"] = region_label
        manifest["bbox"] = [float(value) for value in units.geometry.total_bounds]
    if snap_path is not None:
        manifest["auxiliary"] = [
            {
                "schema": "hfx.aux.snap.v2",
                "artifacts": {"snap": str(snap_path.relative_to(out_dir))},
                "metadata": {
                    "name": "stems",
                    "description": "Unclipped HydroRIVERS reach centerlines for HydroBASINS Pfaf-12 snapping. HydroRIVERS and HydroBASINS are HydroSHEDS products covered by the HydroSHEDS License Agreement. weight = UPLAND_SKM (km^2). stem_role = mainstem/tributary derived from NEXT_DOWN confluences.",
                    "references_levels": [0],
                    "weight_semantics": "drainage_area_km2",
                },
            }
        ]

    path = out_dir / "manifest.json"
    path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def resolve_build_regions(args: argparse.Namespace) -> list[str]:
    """Resolve the selected build regions in their authored deterministic order."""
    if args.region is not None:
        return [args.region]
    if args.regions is not None:
        regions = [
            code
            for code in re.split(r"[\s,]+", " ".join(args.regions))
            if code
        ]
        if not regions:
            raise AdapterError("--regions must contain at least one region code")
        duplicates = sorted(
            {code for code in regions if regions.count(code) > 1}
        )
        if duplicates:
            raise AdapterError(f"duplicate region codes: {duplicates}")
        return regions

    regions = [
        code
        for code in STANDARD_REGION_CODES
        if (args.basins / f"hybas_{code}_lev12_v1.shp").is_file()
    ]
    if not regions:
        raise AdapterError(
            f"no regions found under {args.basins} for --all-regions"
        )
    return regions


def build_dataset(args: argparse.Namespace) -> None:
    """Load, normalize, merge, and write the selected regional polygon layers."""
    if args.rivers is not None and args.region is None:
        raise AdapterError("HydroRIVERS currently requires --region")
    regions = resolve_build_regions(args)
    if args.rivers is not None and len(regions) != 1:
        raise AdapterError("HydroRIVERS currently requires --region")
    assigned_frames: list[gpd.GeoDataFrame] = []
    for region in regions:
        units = load_region_units(region, args.basins)
        pour_points_dir = (
            args.pour_points if args.region is not None else args.pour_points / region
        )
        pour_points = load_pour_points(pour_points_dir)
        assigned_frames.append(assign_outlets(units, pour_points))

    units = gpd.GeoDataFrame(
        pd.concat(assigned_frames, ignore_index=True),
        geometry="geometry",
        crs=CRS,
    )
    duplicates = sorted(
        int(identifier)
        for identifier in units.loc[
            units["id"].duplicated(keep=False), "id"
        ].unique()
    )
    if duplicates:
        raise AdapterError(f"duplicate HYBAS_ID values: {duplicates}")

    units = _hilbert_sort(units)
    guard_antimeridian(units, strict_build=args.strict_build)
    snap_rivers: gpd.GeoDataFrame | None = None
    if args.rivers is not None:
        rivers = load_rivers(args.rivers)
        roles = _stem_roles(rivers)
        rivers["stem_role"] = rivers["HYRIV_ID"].map(roles)
        unit_ids = set(int(identifier) for identifier in units["id"])
        snap_rivers = rivers.loc[rivers["HYBAS_L12"].isin(unit_ids)].copy()
        dropped = len(rivers) - len(snap_rivers)
        LOGGER.info(
            "dropped %d HydroRIVERS reaches with HYBAS_L12 absent from the unit set",
            dropped,
        )
    write_catchments(units, args.out)
    write_graph(units, args.out)
    snap_path = None
    if snap_rivers is not None:
        snap_path = write_snap_stems(snap_rivers, units, args.out)
    write_manifest(
        units,
        args.out,
        region_label=",".join(regions),
        planetary=args.planetary,
        snap_path=snap_path,
    )


def extract_dataset(args: argparse.Namespace) -> None:
    """Inspect one regional polygon layer and its ancillary pour points."""
    basins_path = _source_path(args.region, args.basins)
    pour_points_path = _pour_points_source_path(args.pour_points)

    try:
        basins = gpd.read_file(basins_path, engine="pyogrio")
    except Exception as error:
        raise AdapterError(
            f"failed to read regional HydroBASINS polygon layer {basins_path}: {error}"
        ) from error

    try:
        pour_points = gpd.read_file(pour_points_path, engine="pyogrio")
    except Exception as error:
        raise AdapterError(
            f"failed to read HydroBASINS pour-points layer {pour_points_path}: {error}"
        ) from error

    print(f"basins path: {basins_path}")
    print(f"basins features: {len(basins)}")
    print(f"basins CRS: {basins.crs}")
    for column in ("HYBAS_ID", "SUB_AREA", "UP_AREA", "NEXT_DOWN", "ENDO"):
        status = "present" if column in basins.columns else "absent"
        print(f"basins column {column}: {status}")

    if "HYBAS_ID" in pour_points.columns:
        join_key_status = "HYBAS_ID"
    else:
        candidates = [
            column
            for column in pour_points.columns
            if column.casefold() == "HYBAS_ID".casefold()
        ]
        if len(candidates) == 1:
            join_key_status = candidates[0]
        elif candidates:
            join_key_status = f"ambiguous ({', '.join(candidates)})"
        else:
            join_key_status = "missing"

    print(f"pour points path: {pour_points_path}")
    print(f"pour points features: {len(pour_points)}")
    print(f"pour points CRS: {pour_points.crs}")
    print(f"pour points join key: {join_key_status}")


def validate_dataset(dataset: Path, report_dir: Path) -> None:
    """Run strict text and JSON validation and persist the reports."""
    report_dir.mkdir(parents=True, exist_ok=True)
    commands = {
        "text": [
            "cargo", "run", "-p", "hfx-cli", "--", str(dataset),
            "--format", "text", "--strict", "--sample-pct", "100",
        ],
        "json": [
            "cargo", "run", "-p", "hfx-cli", "--", str(dataset),
            "--format", "json", "--strict", "--sample-pct", "100",
        ],
    }
    for report_format, command in commands.items():
        result = subprocess.run(
            command,
            cwd=Path(__file__).parents[2],
            env=dict(os.environ),
            capture_output=True,
            text=True,
            check=False,
        )
        report_path = report_dir / f"validator-report.{report_format}"
        report_path.write_text(result.stdout, encoding="utf-8")
        if result.stderr:
            stderr_path = report_dir / f"validator-report.{report_format}.stderr"
            stderr_path.write_text(result.stderr, encoding="utf-8")
        if result.returncode != 0:
            raise AdapterError(
                f"{report_format} validator failed with return code "
                f"{result.returncode}; see reports in {report_dir}"
            )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the HydroBASINS adapter command-line parser."""
    parser = argparse.ArgumentParser(
        description="Build standard without-lakes HydroBASINS Pfaf-12 HFX datasets."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="build a Pfaf-12 dataset")
    region_selector = build.add_mutually_exclusive_group(required=True)
    region_selector.add_argument("--region", help="HydroBASINS region code")
    region_selector.add_argument(
        "--regions",
        nargs="+",
        help="HydroBASINS region codes separated by commas or whitespace",
    )
    region_selector.add_argument(
        "--all-regions",
        action="store_true",
        help="build all present standard HydroBASINS regions",
    )
    build.add_argument(
        "--basins",
        type=Path,
        required=True,
        help="directory containing the regional Pfaf-12 polygon layer",
    )
    build.add_argument(
        "--pour-points",
        type=Path,
        required=True,
        help="directory containing hybas_pour_lev12_v1.shp",
    )
    build.add_argument(
        "--rivers",
        type=Path,
        help=(
            "directory containing exactly one recursively discoverable "
            "HydroRIVERS layer for a single-region build"
        ),
    )
    build.add_argument("--out", type=Path, required=True, help="output directory")
    build.add_argument(
        "--planetary",
        action="store_true",
        help="omit the manifest region and use the exact planetary bbox",
    )
    build.add_argument(
        "--strict-build",
        action="store_true",
        default=False,
        help="make antimeridian-wrap candidates build errors",
    )
    extract = subparsers.add_parser(
        "extract", help="inspect one region's source inputs"
    )
    extract.add_argument("--region", required=True, help="HydroBASINS region code")
    extract.add_argument(
        "--basins",
        type=Path,
        required=True,
        help="directory containing the regional Pfaf-12 polygon layer",
    )
    extract.add_argument(
        "--pour-points",
        type=Path,
        required=True,
        help="directory containing hybas_pour_lev12_v1.shp",
    )
    validate = subparsers.add_parser(
        "validate", help="run strict validation and persist reports"
    )
    validate.add_argument("dataset", type=Path, help="HFX dataset directory")
    validate.add_argument(
        "--report-dir",
        type=Path,
        help="report directory (default: <dataset>/validation)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Dispatch the selected adapter command."""
    args = build_arg_parser().parse_args(argv)
    if args.command == "build":
        build_dataset(args)
        return 0
    if args.command == "extract":
        extract_dataset(args)
        return 0
    if args.command == "validate":
        dataset = args.dataset.expanduser()
        report_dir = (
            args.report_dir.expanduser()
            if args.report_dir is not None
            else dataset / "validation"
        )
        validate_dataset(dataset, report_dir)
        return 0
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
