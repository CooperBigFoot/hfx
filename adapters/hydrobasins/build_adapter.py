#!/usr/bin/env python3
"""HydroBASINS HFX adapter command-line shell."""

from __future__ import annotations

import argparse
import json
import math
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
HAS_RASTERS = False
HAS_SNAP = False
ROW_GROUP_MIN = 4096
ROW_GROUP_MAX = 8192
BBOX_LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")


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


def _normalize_ids(values: pd.Series) -> pd.Series:
    normalized: list[int] = []
    maximum = 2**63 - 1
    for value in values:
        try:
            decimal = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise AdapterError(f"HYBAS_ID contains a non-numeric value: {value!r}") from None
        if not decimal.is_finite() or decimal != decimal.to_integral_value():
            raise AdapterError(f"HYBAS_ID contains a non-integral value: {value!r}")
        identifier = int(decimal)
        if identifier <= 0:
            raise AdapterError(
                "HYBAS_ID values must be strictly positive; 0 is the terminal-sink sentinel"
            )
        if identifier > maximum:
            raise AdapterError(f"HYBAS_ID is outside the int64 range: {value!r}")
        normalized.append(identifier)

    result = pd.Series(normalized, index=values.index, dtype="int64")
    if result.duplicated().any():
        duplicates = result.loc[result.duplicated(keep=False)].unique().tolist()
        raise AdapterError(f"duplicate HYBAS_ID values: {duplicates}")
    return result


def _normalize_area(values: pd.Series, source_name: str) -> pd.Series:
    try:
        return pd.to_numeric(values, errors="raise").astype("float64")
    except (TypeError, ValueError, OverflowError) as error:
        raise AdapterError(f"{source_name} cannot be converted to float64: {error}") from error


def load_region_units(region: str, basins_dir: Path) -> gpd.GeoDataFrame:
    """Load and normalize one HydroBASINS Pfaf-12 regional polygon layer."""
    source_path = _source_path(region, basins_dir)
    try:
        units = gpd.read_file(source_path, engine="pyogrio")
    except Exception as error:
        raise AdapterError(f"failed to read HydroBASINS layer {source_path}: {error}") from error

    required = {"HYBAS_ID", "SUB_AREA", "UP_AREA"}
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

    units["_hilbert"] = units.geometry.centroid.hilbert_distance(
        total_bounds=units.total_bounds
    )
    units = units.sort_values(["_hilbert", "id"], kind="mergesort")
    return units.drop(columns=["_hilbert"]).reset_index(drop=True)


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

    # These non-null NaNs are temporary M1 schema placeholders that intentionally
    # fail full HFX semantics; M2 replaces them with HydroBASINS pour-point values.
    outlet_stubs = np.full(row_count, np.nan, dtype="float64")
    columns = [
        pa.array(units["id"].to_numpy(), type=pa.int64()),
        pa.array(units["level"].to_numpy(dtype="int16"), type=pa.int16()),
        pa.nulls(row_count, type=pa.int64()),
        pa.array(units["area_km2"].to_numpy(dtype="float32"), type=pa.float32()),
        pa.array(
            units["up_area_km2"].to_numpy(dtype="float32"),
            type=pa.float32(),
        ),
        pa.array(outlet_stubs, type=pa.float64()),
        pa.array(outlet_stubs, type=pa.float64()),
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


def build_dataset(args: argparse.Namespace) -> None:
    """Load, normalize, and write the requested regional polygon layer."""
    units = load_region_units(args.region, args.basins)
    write_catchments(units, args.out)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the HydroBASINS adapter command-line parser."""
    parser = argparse.ArgumentParser(
        description="Build standard without-lakes HydroBASINS Pfaf-12 HFX datasets."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="build one regional Pfaf-12 dataset")
    build.add_argument("--region", required=True, help="HydroBASINS region code")
    build.add_argument(
        "--basins",
        type=Path,
        required=True,
        help="directory containing the regional Pfaf-12 polygon layer",
    )
    build.add_argument("--out", type=Path, required=True, help="output directory")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Dispatch the selected adapter command."""
    args = build_arg_parser().parse_args(argv)
    if args.command == "build":
        build_dataset(args)
        return 0
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
