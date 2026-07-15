#!/usr/bin/env python3
"""HydroBASINS HFX adapter command-line shell."""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
from pathlib import Path

import geopandas as gpd
import pandas as pd
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


class AdapterError(RuntimeError):
    """Report a HydroBASINS source-contract violation."""


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


def build_dataset(args: argparse.Namespace) -> None:
    """Load and normalize the requested regional polygon layer in memory."""
    load_region_units(args.region, args.basins)


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
