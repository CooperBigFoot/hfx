#!/usr/bin/env python3
"""Build an NGA TDX-Hydro processing basin as an HFX dataset."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import shutil
import subprocess
import tempfile
from collections import Counter, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from numbers import Integral, Real
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from geoparquet_io.core.validate import validate_geoparquet
from pyproj import Geod
from shapely import get_coordinates, set_coordinates
from shapely.geometry import LineString


CROSSWALK_PATH = Path(__file__).parent / "data" / "tdx_header_numbers.json"
FABRIC_NAME = "tdx_hydro"
ADAPTER_VERSION = "0.1.0"
FORMAT_VERSION = "0.3.0"
TOPOLOGY = "tree"
HAS_UP_AREA = True
ROW_GROUP_MIN = 4096
ROW_GROUP_MAX = 8192
BBOX_LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")
GLOBAL_LINKNO_STRIDE = 10_000_000
TDX_LINKNO_SENTINEL = -1
LOGGER = logging.getLogger("tdx-hydro")
CRS = "EPSG:4326"
# NGA describes TDX-Hydro as a nominal 12 m hydrography suite derived from
# TanDEM-X input:
# https://earth-info.nga.mil/index.php?action=geosciences&dir=geosci
# The DLR TanDEM-X DEM Product Specification, table 1, maps the standard global
# 12 m-at-the-equator DEM to 0.4 arc-second spacing:
# https://tandemx-science.dlr.de/pdfs/TD-GS-PS-0021_DEM-Product-Specification_v3.2.pdf
# TDX-Hydro derives from 12 m TanDEM-X DEM data; DLR maps that source resolution
# to 0.4 arc-second spacing; one source cell is therefore 0.4 / 3600 degrees.
TDX_SOURCE_CELL_ARCSECONDS = 0.4
COORDINATE_DOMAIN_TOLERANCE_DEGREES = TDX_SOURCE_CELL_ARCSECONDS / 3600.0
DSCONTAREA_RELATIVE_TOLERANCE = 0.05
DEFAULT_ENDPOINT_TOLERANCE = 0.001


@dataclass(frozen=True)
class LayerClampDiagnostics:
    altered_vertex_count: int
    altered_native_ids: tuple[int, ...]


@dataclass(frozen=True)
class DSContAreaDiagnostics:
    source_unit: str
    checked_polygon_bearing_link_count: int
    geodesic_upstream_area_sum_m2: float
    dscontarea_sum_raw: float
    m2_relative_error: float
    km2_relative_error: float
    selected_relative_error: float


@dataclass(frozen=True)
class IngestionDiagnostics:
    basins_clamp: LayerClampDiagnostics
    streamnet_clamp: LayerClampDiagnostics
    dscontarea: DSContAreaDiagnostics


@dataclass(frozen=True)
class TdxSourceData:
    basins: gpd.GeoDataFrame
    streamnet: gpd.GeoDataFrame
    diagnostics: IngestionDiagnostics


@dataclass(frozen=True)
class StreamnetUnit:
    linkno: int
    id: int
    level: int
    parent_id: None
    downstream_linkno: int
    downstream_id: int
    contracted_link_count: int
    outlet_lon: float
    outlet_lat: float


@dataclass(frozen=True)
class StreamnetDiagnostics:
    polygon_bearing_link_count: int
    polygonless_dropped_reach_count: int
    root_count: int
    contracted_edge_count: int
    contracted_root_count: int
    contracted_link_traversal_count: int
    endpoint_coincidence_proven_link_count: int
    predecessor_orientation_proven_root_count: int
    trusted_orientation_isolated_root_count: int
    trusted_orientation_isolated_root_native_linknos: tuple[int, ...]
    trusted_orientation_polygon_bearing_isolated_root_count: int
    trusted_orientation_polygon_bearing_isolated_root_native_linknos: tuple[int, ...]
    orientation_tolerance: float


@dataclass(frozen=True)
class StreamnetModel:
    units: tuple[StreamnetUnit, ...]
    edges: tuple[tuple[int, int], ...]
    roots: tuple[int, ...]
    diagnostics: StreamnetDiagnostics


@dataclass(frozen=True)
class CoreBuildDiagnostics:
    ingestion: IngestionDiagnostics
    streamnet: StreamnetDiagnostics


@dataclass(frozen=True)
class CoreBuildResult:
    catchments_path: Path
    graph_path: Path
    manifest_path: Path
    diagnostics: CoreBuildDiagnostics


def _load_single_geopackage(path: Path, layer_name: str) -> gpd.GeoDataFrame:
    expanded_path = path.expanduser()
    if (
        expanded_path.suffix.lower() != ".gpkg"
        or not expanded_path.is_file()
    ):
        raise ValueError(
            f"{layer_name} path must be an existing regular .gpkg file: {expanded_path}"
        )
    try:
        discovered = pyogrio.list_layers(expanded_path)
    except Exception as exc:
        raise ValueError(f"failed to list {layer_name} layers in {expanded_path}: {exc}") from exc
    layer_names = [str(name) for name in discovered[:, 0].tolist()]
    if len(layer_names) != 1:
        raise ValueError(
            f"{layer_name} GeoPackage {expanded_path} must contain exactly one vector "
            f"layer; discovered layer names: {layer_names}"
        )
    try:
        frame = pyogrio.read_dataframe(expanded_path, layer=layer_names[0])
    except Exception as exc:
        raise ValueError(f"failed to read {layer_name} layer from {expanded_path}: {exc}") from exc
    if not isinstance(frame, gpd.GeoDataFrame) or frame.empty:
        raise ValueError(f"{layer_name} must be a non-empty GeoDataFrame")
    geometry_columns = [
        column for column in frame.columns
        if isinstance(frame[column].dtype, gpd.array.GeometryDtype)
    ]
    if len(geometry_columns) != 1 or frame.geometry.name != geometry_columns[0]:
        raise ValueError(f"{layer_name} must have exactly one active geometry column")
    if frame.crs is None:
        raise ValueError(f"{layer_name} must declare a CRS")
    normalized = frame.copy(deep=True)
    if normalized.crs.to_epsg() != 4326:
        try:
            normalized = normalized.to_crs(CRS)
        except Exception as exc:
            raise ValueError(f"failed to transform {layer_name} to {CRS}: {exc}") from exc
    if normalized.crs is None or normalized.crs.to_epsg() != 4326:
        raise ValueError(f"{layer_name} CRS does not resolve to {CRS}")
    return normalized


def _require_columns(
    table: gpd.GeoDataFrame,
    table_name: str,
    required: set[str],
) -> None:
    missing = sorted(required - set(table.columns))
    if missing:
        raise ValueError(f"{table_name} is missing required columns: {missing}")


def _validate_layer_geometry(
    table: gpd.GeoDataFrame,
    layer_name: str,
    allowed_types: set[str],
) -> None:
    expected = " or ".join(sorted(allowed_types))
    for geometry in table.geometry:
        if geometry is None or geometry.is_empty:
            raise ValueError(f"{layer_name} geometry must be non-null and non-empty")
        if not geometry.is_valid:
            raise ValueError(f"{layer_name} geometry must be valid")
        if geometry.has_z:
            raise ValueError(f"{layer_name} geometry must be two-dimensional")
        if geometry.geom_type not in allowed_types:
            raise ValueError(
                f"{layer_name} geometry must have exact type {expected}; got {geometry.geom_type}"
            )


def _normalize_topology_column(
    table: gpd.GeoDataFrame,
    table_name: str,
    column: str,
) -> None:
    table[column] = pd.Series(
        _normalize_column(table, table_name, column),
        index=table.index,
        dtype="int64",
    )


def _validate_duplicate_ids(
    basin_linknos: list[int],
    stream_linknos: list[int],
    downstream_linknos: list[int],
) -> None:
    basin_counts = Counter(basin_linknos)
    duplicate_units = sorted(linkno for linkno, count in basin_counts.items() if count > 1)
    if duplicate_units:
        raise ValueError(f"duplicate unit identity for streamID {duplicate_units[0]}")

    link_counts = Counter(stream_linknos)
    targets_by_linkno: dict[int, list[int]] = {}
    for linkno, downstream_linkno in zip(stream_linknos, downstream_linknos, strict=True):
        targets_by_linkno.setdefault(linkno, []).append(downstream_linkno)
    for linkno in sorted(linkno for linkno, count in link_counts.items() if count > 1):
        if len(set(targets_by_linkno[linkno])) > 1:
            raise ValueError(
                f"bifurcation: duplicate LINKNO {linkno} has multiple DSLINKNO targets"
            )
        raise ValueError(f"duplicate LINKNO {linkno} in streamnet")


def _normalize_dscontarea(streamnet: gpd.GeoDataFrame) -> None:
    values = streamnet["DSContArea"].tolist()
    for value in values:
        if isinstance(value, bool) or pd.isna(value):
            raise ValueError(
                f"streamnet.DSContArea must contain finite positive values; got {value!r}"
            )
    try:
        numeric = pd.to_numeric(streamnet["DSContArea"], errors="raise")
    except (TypeError, ValueError) as exc:
        offending = next(
            (
                value
                for value in values
                if _cannot_convert_to_number(value)
            ),
            values[0],
        )
        raise ValueError(
            f"streamnet.DSContArea must contain finite positive values; got {offending!r}"
        ) from exc
    normalized = numeric.astype("float64")
    for original, value in zip(values, normalized.tolist(), strict=True):
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(
                f"streamnet.DSContArea must contain finite positive values; got {original!r}"
            )
    streamnet["DSContArea"] = normalized


def _cannot_convert_to_number(value: object) -> bool:
    try:
        pd.to_numeric(pd.Series([value]), errors="raise")
    except (TypeError, ValueError):
        return True
    return False


def _clamp_coordinate_domain(
    table: gpd.GeoDataFrame,
    layer_name: str,
    native_id_field: str,
) -> LayerClampDiagnostics:
    scanned: list[np.ndarray] = []
    for geometry, native_id in zip(
        table.geometry, table[native_id_field].tolist(), strict=True
    ):
        coordinates = get_coordinates(geometry)
        scanned.append(coordinates)
        for coordinate in coordinates:
            longitude = float(coordinate[0])
            latitude = float(coordinate[1])
            if not math.isfinite(longitude) or not math.isfinite(latitude):
                raise ValueError(
                    f"non-finite coordinate: layer={layer_name}, "
                    f"{native_id_field}={native_id}, coordinate=({longitude!r}, {latitude!r})"
                )
            longitude_excess = max(-180.0 - longitude, longitude - 180.0, 0.0)
            latitude_excess = max(-90.0 - latitude, latitude - 90.0, 0.0)
            if (
                longitude_excess > COORDINATE_DOMAIN_TOLERANCE_DEGREES
                or latitude_excess > COORDINATE_DOMAIN_TOLERANCE_DEGREES
            ):
                raise ValueError(
                    "coordinate-domain overshoot exceeds tolerance: "
                    f"layer={layer_name}, {native_id_field}={native_id}, "
                    f"coordinate=({longitude!r}, {latitude!r}), "
                    f"longitude_excess={longitude_excess!r}, "
                    f"latitude_excess={latitude_excess!r}, "
                    f"tolerance={COORDINATE_DOMAIN_TOLERANCE_DEGREES!r}"
                )

    normalized_geometries = []
    altered_vertex_count = 0
    altered_native_ids: set[int] = set()
    for geometry, coordinates, native_id in zip(
        table.geometry, scanned, table[native_id_field].tolist(), strict=True
    ):
        normalized = coordinates.copy()
        normalized[:, 0] = np.clip(normalized[:, 0], -180.0, 180.0)
        normalized[:, 1] = np.clip(normalized[:, 1], -90.0, 90.0)
        altered_rows = np.any(normalized[:, :2] != coordinates[:, :2], axis=1)
        count = int(np.count_nonzero(altered_rows))
        altered_vertex_count += count
        if count:
            altered_native_ids.add(int(native_id))
        normalized_geometries.append(set_coordinates(geometry, normalized))
    table[table.geometry.name] = gpd.GeoSeries(
        normalized_geometries, index=table.index, crs=table.crs
    )
    diagnostics = LayerClampDiagnostics(
        altered_vertex_count,
        tuple(sorted(altered_native_ids)),
    )
    if altered_vertex_count:
        LOGGER.warning(
            "diagnostic=%s_clamp.altered_vertex_count count=%d native_ids=%s",
            layer_name,
            altered_vertex_count,
            diagnostics.altered_native_ids,
        )
    return diagnostics


def _infer_dscontarea_unit(
    basins: gpd.GeoDataFrame,
    streamnet: gpd.GeoDataFrame,
    relation: dict[int, int],
) -> DSContAreaDiagnostics:
    geod = Geod(ellps="WGS84")
    own_area_by_linkno = {linkno: 0.0 for linkno in relation}
    for linkno, geometry in zip(
        basins["streamID"].tolist(), basins.geometry, strict=True
    ):
        area = abs(float(geod.geometry_area_perimeter(geometry)[0]))
        if not math.isfinite(area) or area <= 0.0:
            raise ValueError(
                f"basins geometry for streamID {linkno} has non-positive geodesic area"
            )
        own_area_by_linkno[linkno] = area

    predecessors = {linkno: [] for linkno in relation}
    for linkno, downstream_linkno in relation.items():
        if downstream_linkno != TDX_LINKNO_SENTINEL:
            predecessors[downstream_linkno].append(linkno)
    remaining = {linkno: len(values) for linkno, values in predecessors.items()}
    ready = deque(linkno for linkno, count in remaining.items() if count == 0)
    upstream_area: dict[int, float] = {}
    while ready:
        linkno = ready.popleft()
        upstream_area[linkno] = math.fsum(
            [own_area_by_linkno[linkno]]
            + [upstream_area[value] for value in predecessors[linkno]]
        )
        downstream_linkno = relation[linkno]
        if downstream_linkno != TDX_LINKNO_SENTINEL:
            remaining[downstream_linkno] -= 1
            if remaining[downstream_linkno] == 0:
                ready.append(downstream_linkno)

    raw_by_linkno = dict(
        zip(streamnet["LINKNO"].tolist(), streamnet["DSContArea"].tolist(), strict=True)
    )
    polygon_links = basins["streamID"].tolist()
    expected_samples = [upstream_area[linkno] for linkno in polygon_links]
    raw_samples = [raw_by_linkno[linkno] for linkno in polygon_links]
    expected_sum = math.fsum(expected_samples)
    raw_sum = math.fsum(raw_samples)
    m2_relative_error = math.fsum(
        abs(raw - expected) for raw, expected in zip(raw_samples, expected_samples, strict=True)
    ) / expected_sum
    km2_relative_error = math.fsum(
        abs(raw * 1_000_000 - expected)
        for raw, expected in zip(raw_samples, expected_samples, strict=True)
    ) / expected_sum
    if m2_relative_error == km2_relative_error:
        raise ValueError(
            "DSContArea unit candidates are numerically tied: "
            f"m2_relative_error={m2_relative_error!r}, "
            f"km2_relative_error={km2_relative_error!r}"
        )
    source_unit = "m2" if m2_relative_error < km2_relative_error else "km2"
    selected_relative_error = min(m2_relative_error, km2_relative_error)
    if selected_relative_error > DSCONTAREA_RELATIVE_TOLERANCE:
        raise ValueError(
            "DSContArea empirical unit verification failed: "
            f"m2_relative_error={m2_relative_error!r}, "
            f"km2_relative_error={km2_relative_error!r}, "
            f"tolerance={DSCONTAREA_RELATIVE_TOLERANCE!r}"
        )
    streamnet["DSContArea_km2"] = (
        streamnet["DSContArea"] / 1_000_000
        if source_unit == "m2"
        else streamnet["DSContArea"].copy()
    ).astype("float64")
    diagnostics = DSContAreaDiagnostics(
        source_unit=source_unit,
        checked_polygon_bearing_link_count=len(polygon_links),
        geodesic_upstream_area_sum_m2=expected_sum,
        dscontarea_sum_raw=raw_sum,
        m2_relative_error=m2_relative_error,
        km2_relative_error=km2_relative_error,
        selected_relative_error=selected_relative_error,
    )
    LOGGER.info(
        "dscontarea source_unit=%s checked_polygon_bearing_link_count=%d "
        "geodesic_upstream_area_sum_m2=%s dscontarea_sum_raw=%s "
        "m2_relative_error=%s km2_relative_error=%s selected_relative_error=%s",
        diagnostics.source_unit,
        diagnostics.checked_polygon_bearing_link_count,
        diagnostics.geodesic_upstream_area_sum_m2,
        diagnostics.dscontarea_sum_raw,
        diagnostics.m2_relative_error,
        diagnostics.km2_relative_error,
        diagnostics.selected_relative_error,
    )
    return diagnostics


def load_tdx_geopackages(
    basins_path: Path,
    streamnet_path: Path,
) -> TdxSourceData:
    """Load and normalize one TDX-Hydro basin and streamnet GeoPackage pair."""
    basins = _load_single_geopackage(basins_path, "basins")
    streamnet = _load_single_geopackage(streamnet_path, "streamnet")
    _require_columns(basins, "basins", {"streamID", basins.geometry.name})
    _require_columns(
        streamnet,
        "streamnet",
        {"LINKNO", "DSLINKNO", "DSContArea", streamnet.geometry.name},
    )
    _validate_layer_geometry(basins, "basins", {"Polygon", "MultiPolygon"})
    _validate_layer_geometry(streamnet, "streamnet", {"LineString"})
    _normalize_topology_column(basins, "basins", "streamID")
    _normalize_topology_column(streamnet, "streamnet", "LINKNO")
    _normalize_topology_column(streamnet, "streamnet", "DSLINKNO")
    basin_linknos = basins["streamID"].tolist()
    stream_linknos = streamnet["LINKNO"].tolist()
    downstream_linknos = streamnet["DSLINKNO"].tolist()
    for linkno in basin_linknos:
        if linkno < 0:
            raise ValueError(f"basins.streamID must be non-negative; got {linkno}")
    for linkno in stream_linknos:
        if linkno < 0:
            raise ValueError(f"streamnet.LINKNO must be non-negative; got {linkno}")
    for downstream_linkno in downstream_linknos:
        if downstream_linkno < TDX_LINKNO_SENTINEL:
            raise ValueError(
                "streamnet.DSLINKNO must be non-negative or -1; "
                f"got {downstream_linkno}"
            )
    _validate_duplicate_ids(basin_linknos, stream_linknos, downstream_linknos)
    _normalize_dscontarea(streamnet)
    relation = dict(zip(stream_linknos, downstream_linknos, strict=True))
    _validate_streamnet_relation(relation)
    missing_units = sorted(set(basin_linknos) - set(stream_linknos))
    if missing_units:
        raise ValueError(
            "basins.streamID does not join to streamnet.LINKNO: "
            f"{missing_units[0]}"
        )
    basins_clamp = _clamp_coordinate_domain(basins, "basins", "streamID")
    streamnet_clamp = _clamp_coordinate_domain(streamnet, "streamnet", "LINKNO")
    _validate_layer_geometry(basins, "basins", {"Polygon", "MultiPolygon"})
    _validate_layer_geometry(streamnet, "streamnet", {"LineString"})
    dscontarea = _infer_dscontarea_unit(basins, streamnet, relation)
    return TdxSourceData(
        basins=basins,
        streamnet=streamnet,
        diagnostics=IngestionDiagnostics(basins_clamp, streamnet_clamp, dscontarea),
    )


def load_header_crosswalk(path: Path = CROSSWALK_PATH) -> dict[str, int]:
    """Load processing-basin header numbers from the vendored crosswalk."""
    with path.open(encoding="utf-8") as source:
        raw = json.load(source)

    if not isinstance(raw, dict):
        raise ValueError("TDX-Hydro header crosswalk must be a JSON object")

    try:
        crosswalk = {
            processing_basin_id: int(header_number)
            for processing_basin_id, header_number in raw.items()
        }
    except (TypeError, ValueError) as exc:
        raise ValueError("TDX-Hydro header numbers must be integers") from exc

    if any(not isinstance(key, str) or not key.isdigit() for key in crosswalk):
        raise ValueError("TDX-Hydro processing-basin IDs must be digit strings")
    if any(header_number <= 0 for header_number in crosswalk.values()):
        raise ValueError("TDX-Hydro header numbers must be positive")
    if len(set(crosswalk.values())) != len(crosswalk):
        raise ValueError("TDX-Hydro header numbers must be unique")

    return crosswalk


def global_linkno(linkno: int, header_number: int) -> int:
    """Return a Global LINKNO while preserving the native root sentinel."""
    if linkno == TDX_LINKNO_SENTINEL:
        return TDX_LINKNO_SENTINEL
    if linkno < 0:
        raise ValueError("native LINKNO must be non-negative or -1")
    if header_number <= 0:
        raise ValueError("header number must be positive")
    return linkno + header_number * GLOBAL_LINKNO_STRIDE


def _require_column(table: pd.DataFrame, table_name: str, column: str) -> None:
    if column not in table.columns:
        raise ValueError(f"{table_name} is missing required column {column}")


def _topology_integer(value: object, table_name: str, column: str) -> int:
    if isinstance(value, bool) or pd.isna(value):
        raise ValueError(
            f"{table_name}.{column} must contain integer topology values; got {value!r}"
        )
    if isinstance(value, Integral):
        return int(value)
    if isinstance(value, Real):
        numeric_value = float(value)
        if math.isfinite(numeric_value) and numeric_value.is_integer():
            return int(numeric_value)
    raise ValueError(
        f"{table_name}.{column} must contain integer topology values; got {value!r}"
    )


def _normalize_column(
    table: pd.DataFrame,
    table_name: str,
    column: str,
) -> list[int]:
    return [
        _topology_integer(value, table_name, column)
        for value in table[column].tolist()
    ]


def _validate_streamnet_relation(relation: dict[int, int]) -> None:
    for linkno, downstream_linkno in relation.items():
        if downstream_linkno == linkno:
            raise ValueError(f"streamnet self-link at native LINKNO {linkno}")
        if (
            downstream_linkno != TDX_LINKNO_SENTINEL
            and downstream_linkno not in relation
        ):
            raise ValueError(
                "streamnet missing downstream LINKNO "
                f"{downstream_linkno} referenced by native LINKNO {linkno}"
            )

    finished: set[int] = set()
    for start in relation:
        if start in finished:
            continue

        path: list[int] = []
        positions: dict[int, int] = {}
        current = start
        while current != TDX_LINKNO_SENTINEL and current not in finished:
            if current in positions:
                cycle = path[positions[current] :] + [current]
                cycle_text = " -> ".join(str(linkno) for linkno in cycle)
                raise ValueError(f"streamnet cycle detected: {cycle_text}")
            positions[current] = len(path)
            path.append(current)
            current = relation[current]
        finished.update(path)


def _resolve_downstream(
    linkno: int,
    relation: dict[int, int],
    polygon_bearing_links: set[int],
) -> tuple[int, int]:
    downstream_linkno = relation[linkno]
    contracted_link_count = 0
    while (
        downstream_linkno != TDX_LINKNO_SENTINEL
        and downstream_linkno not in polygon_bearing_links
    ):
        contracted_link_count += 1
        downstream_linkno = relation[downstream_linkno]
    return downstream_linkno, contracted_link_count


def _positive_finite_tolerance(endpoint_tolerance: object) -> float:
    if isinstance(endpoint_tolerance, bool) or not isinstance(endpoint_tolerance, Real):
        raise ValueError("endpoint_tolerance must be a positive finite number")
    tolerance = float(endpoint_tolerance)
    if not math.isfinite(tolerance) or tolerance <= 0.0:
        raise ValueError("endpoint_tolerance must be a positive finite number")
    return tolerance


def _streamnet_endpoints(
    stream_linknos: list[int],
    geometries: list[object],
) -> dict[int, tuple[tuple[float, float], tuple[float, float]]]:
    endpoints_by_linkno: dict[
        int, tuple[tuple[float, float], tuple[float, float]]
    ] = {}
    for linkno, geometry in zip(stream_linknos, geometries, strict=True):
        if not isinstance(geometry, LineString) or geometry.is_empty:
            raise ValueError(
                f"streamnet geometry for native LINKNO {linkno} must be a non-empty LineString"
            )

        coordinates = list(geometry.coords)
        if len(coordinates) < 2:
            raise ValueError(
                f"streamnet geometry for native LINKNO {linkno} must have at least two coordinates"
            )
        start = coordinates[0]
        end = coordinates[-1]
        if (
            len(start) != 2
            or len(end) != 2
            or not all(math.isfinite(float(value)) for value in (*start, *end))
        ):
            raise ValueError(
                f"streamnet geometry for native LINKNO {linkno} must have finite two-dimensional endpoints"
            )

        start_xy = (float(start[0]), float(start[1]))
        end_xy = (float(end[0]), float(end[1]))
        if start_xy == end_xy:
            raise ValueError(
                f"streamnet geometry for native LINKNO {linkno} has degenerate endpoints"
            )
        endpoints_by_linkno[linkno] = (start_xy, end_xy)

    return endpoints_by_linkno


@dataclass(frozen=True)
class _OrientationResolution:
    downstream_endpoints: dict[int, tuple[float, float]]
    endpoint_coincidence_proven_link_count: int
    predecessor_orientation_proven_root_count: int
    trusted_orientation_isolated_root_native_linknos: tuple[int, ...]


def _resolve_native_orientation(
    relation: dict[int, int],
    endpoints_by_linkno: dict[
        int, tuple[tuple[float, float], tuple[float, float]]
    ],
    endpoint_tolerance: float,
) -> _OrientationResolution:
    downstream_endpoints: dict[int, tuple[float, float]] = {}
    matched_successor_endpoints: dict[int, list[int]] = {}

    for linkno, downstream_linkno in relation.items():
        if downstream_linkno == TDX_LINKNO_SENTINEL:
            continue

        current_endpoints = endpoints_by_linkno[linkno]
        successor_endpoints = endpoints_by_linkno[downstream_linkno]
        matches = [
            (current_index, successor_index)
            for current_index, current_endpoint in enumerate(current_endpoints)
            for successor_index, successor_endpoint in enumerate(successor_endpoints)
            if math.dist(current_endpoint, successor_endpoint) <= endpoint_tolerance
        ]
        if not matches:
            raise ValueError(
                "orientation proof for native LINKNO "
                f"{linkno} and downstream LINKNO {downstream_linkno} is non-coincident"
            )
        if len(matches) > 1:
            raise ValueError(
                "orientation proof for native LINKNO "
                f"{linkno} and downstream LINKNO {downstream_linkno} is ambiguous"
            )

        current_index, successor_index = matches[0]
        downstream_endpoints[linkno] = current_endpoints[current_index]
        matched_successor_endpoints.setdefault(downstream_linkno, []).append(
            successor_index
        )

    predecessor_proven_roots = 0
    trusted_isolated_roots: list[int] = []
    for root_linkno, downstream_linkno in relation.items():
        if downstream_linkno != TDX_LINKNO_SENTINEL:
            continue

        predecessor_matches = matched_successor_endpoints.get(root_linkno, [])
        if not predecessor_matches:
            # TDX/TauDEM native-vertex-order TRUST ASSUMPTION: a genuinely
            # isolated root has no topology from which orientation can be
            # proven, so preserve source order and use its final vertex.
            downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][1]
            trusted_isolated_roots.append(root_linkno)
            continue
        upstream_endpoint_indexes = set(predecessor_matches)
        if len(upstream_endpoint_indexes) > 1:
            raise ValueError(
                f"orientation proof for root LINKNO {root_linkno} has conflicting predecessor matches"
            )

        upstream_endpoint_index = upstream_endpoint_indexes.pop()
        downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][
            1 - upstream_endpoint_index
        ]
        predecessor_proven_roots += 1

    return _OrientationResolution(
        downstream_endpoints=downstream_endpoints,
        endpoint_coincidence_proven_link_count=sum(
            downstream_linkno != TDX_LINKNO_SENTINEL
            for downstream_linkno in relation.values()
        ),
        predecessor_orientation_proven_root_count=predecessor_proven_roots,
        trusted_orientation_isolated_root_native_linknos=tuple(
            sorted(trusted_isolated_roots)
        ),
    )


def build_streamnet_model(
    basins: pd.DataFrame,
    streamnet: pd.DataFrame,
    header_number: int,
    *,
    endpoint_tolerance: float,
) -> StreamnetModel:
    """Build a deterministic contracted topology model from TDX-Hydro tables."""
    tolerance = _positive_finite_tolerance(endpoint_tolerance)
    _require_column(basins, "basins", "streamID")
    _require_column(streamnet, "streamnet", "LINKNO")
    _require_column(streamnet, "streamnet", "DSLINKNO")

    basin_linknos = _normalize_column(basins, "basins", "streamID")
    if not basin_linknos:
        raise ValueError("basins contains no drainage units")
    for linkno in basin_linknos:
        if linkno < 0:
            raise ValueError(f"basins.streamID must be non-negative; got {linkno}")
    basin_counts = Counter(basin_linknos)
    duplicate_units = sorted(
        linkno for linkno, count in basin_counts.items() if count > 1
    )
    if duplicate_units:
        raise ValueError(f"duplicate unit identity for streamID {duplicate_units[0]}")

    stream_linknos = _normalize_column(streamnet, "streamnet", "LINKNO")
    downstream_linknos = _normalize_column(streamnet, "streamnet", "DSLINKNO")
    for linkno in stream_linknos:
        if linkno < 0:
            raise ValueError(f"streamnet.LINKNO must be non-negative; got {linkno}")
    for downstream_linkno in downstream_linknos:
        if downstream_linkno < TDX_LINKNO_SENTINEL:
            raise ValueError(
                "streamnet.DSLINKNO must be non-negative or -1; "
                f"got {downstream_linkno}"
            )

    link_counts = Counter(stream_linknos)
    rows_by_linkno: dict[int, list[int]] = {}
    for linkno, downstream_linkno in zip(stream_linknos, downstream_linknos, strict=True):
        rows_by_linkno.setdefault(linkno, []).append(downstream_linkno)
    for linkno, targets in rows_by_linkno.items():
        if link_counts[linkno] > 1:
            if len(set(targets)) > 1:
                raise ValueError(
                    f"bifurcation: duplicate LINKNO {linkno} has multiple DSLINKNO targets"
                )
            raise ValueError(f"duplicate LINKNO {linkno} in streamnet")

    relation = {
        linkno: downstream_linknos[index]
        for index, linkno in enumerate(stream_linknos)
    }
    _validate_streamnet_relation(relation)

    missing_units = sorted(set(basin_linknos) - relation.keys())
    if missing_units:
        raise ValueError(
            "basins.streamID does not join to streamnet.LINKNO: "
            f"{missing_units[0]}"
        )

    _require_column(streamnet, "streamnet", "geometry")
    endpoints_by_linkno = _streamnet_endpoints(
        stream_linknos, streamnet["geometry"].tolist()
    )
    orientation = _resolve_native_orientation(
        relation, endpoints_by_linkno, tolerance
    )
    downstream_endpoints = orientation.downstream_endpoints

    polygon_bearing_links = set(basin_linknos)
    units: list[StreamnetUnit] = []
    for linkno in sorted(polygon_bearing_links):
        downstream_linkno, contracted_link_count = _resolve_downstream(
            linkno, relation, polygon_bearing_links
        )
        units.append(
            StreamnetUnit(
                linkno=linkno,
                id=global_linkno(linkno, header_number),
                level=0,
                parent_id=None,
                downstream_linkno=downstream_linkno,
                downstream_id=global_linkno(downstream_linkno, header_number),
                contracted_link_count=contracted_link_count,
                outlet_lon=downstream_endpoints[linkno][0],
                outlet_lat=downstream_endpoints[linkno][1],
            )
        )

    unit_tuple = tuple(units)
    edges = tuple(
        sorted(
            (unit.id, unit.downstream_id)
            for unit in unit_tuple
            if unit.downstream_linkno != TDX_LINKNO_SENTINEL
        )
    )
    roots = tuple(
        sorted(
            unit.id
            for unit in unit_tuple
            if unit.downstream_linkno == TDX_LINKNO_SENTINEL
        )
    )
    trusted_isolated_ids = orientation.trusted_orientation_isolated_root_native_linknos
    trusted_polygon_bearing_ids = tuple(
        linkno for linkno in trusted_isolated_ids if linkno in polygon_bearing_links
    )
    diagnostics = StreamnetDiagnostics(
        polygon_bearing_link_count=len(unit_tuple),
        polygonless_dropped_reach_count=len(relation) - len(polygon_bearing_links),
        root_count=len(roots),
        contracted_edge_count=sum(
            unit.downstream_linkno != TDX_LINKNO_SENTINEL
            and unit.contracted_link_count > 0
            for unit in unit_tuple
        ),
        contracted_root_count=sum(
            unit.downstream_linkno == TDX_LINKNO_SENTINEL
            and unit.contracted_link_count > 0
            for unit in unit_tuple
        ),
        contracted_link_traversal_count=sum(
            unit.contracted_link_count for unit in unit_tuple
        ),
        endpoint_coincidence_proven_link_count=(
            orientation.endpoint_coincidence_proven_link_count
        ),
        predecessor_orientation_proven_root_count=(
            orientation.predecessor_orientation_proven_root_count
        ),
        trusted_orientation_isolated_root_count=len(trusted_isolated_ids),
        trusted_orientation_isolated_root_native_linknos=trusted_isolated_ids,
        trusted_orientation_polygon_bearing_isolated_root_count=len(
            trusted_polygon_bearing_ids
        ),
        trusted_orientation_polygon_bearing_isolated_root_native_linknos=(
            trusted_polygon_bearing_ids
        ),
        orientation_tolerance=tolerance,
    )
    LOGGER.info(
        "streamnet_model polygon_bearing_links=%d roots=%d contracted_edges=%d "
        "contracted_roots=%d contracted_link_traversals=%d "
        "endpoint_coincidence_proven_links=%d predecessor_orientation_proven_roots=%d "
        "trusted_orientation_isolated_roots=%d "
        "trusted_orientation_isolated_root_native_linknos=%s "
        "trusted_orientation_polygon_bearing_isolated_roots=%d "
        "trusted_orientation_polygon_bearing_isolated_root_native_linknos=%s "
        "orientation_tolerance=%s polygonless_dropped_reach_count=%d",
        diagnostics.polygon_bearing_link_count,
        diagnostics.root_count,
        diagnostics.contracted_edge_count,
        diagnostics.contracted_root_count,
        diagnostics.contracted_link_traversal_count,
        diagnostics.endpoint_coincidence_proven_link_count,
        diagnostics.predecessor_orientation_proven_root_count,
        diagnostics.trusted_orientation_isolated_root_count,
        diagnostics.trusted_orientation_isolated_root_native_linknos,
        diagnostics.trusted_orientation_polygon_bearing_isolated_root_count,
        diagnostics.trusted_orientation_polygon_bearing_isolated_root_native_linknos,
        diagnostics.orientation_tolerance,
        diagnostics.polygonless_dropped_reach_count,
    )
    return StreamnetModel(
        units=unit_tuple,
        edges=edges,
        roots=roots,
        diagnostics=diagnostics,
    )


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


def balanced_row_group_bounds(
    total_rows: int,
    min_size: int = ROW_GROUP_MIN,
    max_size: int = ROW_GROUP_MAX,
) -> list[tuple[int, int]]:
    """Split rows into balanced Parquet row groups."""
    if total_rows <= 0:
        return []

    min_groups = math.ceil(total_rows / max_size)
    max_groups = max(1, total_rows // min_size)
    group_count = max_groups
    while group_count >= min_groups:
        base = total_rows // group_count
        remainder = total_rows % group_count
        if (
            min_size <= base <= max_size
            and base + (1 if remainder else 0) <= max_size
        ):
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
    table = pa.Table.from_arrays(columns, schema=schema)
    with pq.ParquetWriter(
        path,
        schema=schema,
        compression="snappy",
        write_statistics=True,
    ) as writer:
        for start, stop in balanced_row_group_bounds(row_count):
            writer.write_table(table.slice(start, stop - start))


def _assert_geoparquet_result(path: Path, result: object) -> None:
    if result.is_valid:
        return
    failures = [check for check in result.checks if check.status.value == "failed"]
    details = "; ".join(
        f"{check.name}: {check.message}" for check in failures
    )
    raise ValueError(f"GeoParquet validation failed for {path}: {details}")


def assert_geoparquet_valid(path: Path) -> None:
    """Raise when a Parquet file fails GeoParquet 1.1 validation."""
    result = validate_geoparquet(str(path), target_version="1.1")
    _assert_geoparquet_result(path, result)


def _validate_core_model(
    source: TdxSourceData,
    streamnet_model: StreamnetModel,
) -> dict[int, StreamnetUnit]:
    basin_linknos = [int(value) for value in source.basins["streamID"].tolist()]
    if len(basin_linknos) != len(set(basin_linknos)):
        raise ValueError("source.basins.streamID must be unique")

    units_by_native = {unit.linkno: unit for unit in streamnet_model.units}
    if len(units_by_native) != len(streamnet_model.units):
        raise ValueError("streamnet_model unit native linkno must be unique")
    if set(basin_linknos) != set(units_by_native):
        raise ValueError(
            "source.basins.streamID must equal streamnet_model unit linkno set"
        )

    units_by_id: dict[int, StreamnetUnit] = {}
    for unit in streamnet_model.units:
        if unit.id <= 0:
            raise ValueError("streamnet_model unit id must be positive")
        if unit.id in units_by_id:
            raise ValueError("streamnet_model unit id must be unique")
        if unit.level != 0:
            raise ValueError("streamnet_model unit level must equal 0")
        if unit.parent_id is not None:
            raise ValueError("streamnet_model unit parent_id must be None")
        units_by_id[unit.id] = unit

    _validate_edge_relation(units_by_id, streamnet_model.edges)
    return units_by_id


def _validate_edge_relation(
    units_by_id: dict[int, StreamnetUnit],
    edges: tuple[tuple[int, int], ...],
) -> None:
    downstream_by_upstream: dict[int, int] = {}
    for upstream_id, downstream_id in edges:
        if upstream_id not in units_by_id or downstream_id not in units_by_id:
            raise ValueError("streamnet_model edge endpoint must reference a unit")
        if upstream_id in downstream_by_upstream:
            raise ValueError(
                "streamnet_model source unit may have at most one downstream edge"
            )
        downstream_by_upstream[upstream_id] = downstream_id

    for start in units_by_id:
        seen: set[int] = set()
        current = start
        while current in downstream_by_upstream:
            if current in seen:
                raise ValueError("streamnet_model edge relation must be acyclic")
            seen.add(current)
            current = downstream_by_upstream[current]


def _prepare_core_units(
    source: TdxSourceData,
    streamnet_model: StreamnetModel,
) -> gpd.GeoDataFrame:
    _validate_core_model(source, streamnet_model)
    units_by_native = {unit.linkno: unit for unit in streamnet_model.units}
    stream_rows: dict[int, float] = {}
    for linkno, up_area in zip(
        source.streamnet["LINKNO"],
        source.streamnet["DSContArea_km2"],
        strict=True,
    ):
        native_id = int(linkno)
        if native_id in stream_rows:
            raise ValueError("source.streamnet.LINKNO must be unique")
        value = float(up_area)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError("source.streamnet.DSContArea_km2 must be positive and finite")
        stream_rows[native_id] = value
    missing_stream_rows = sorted(set(units_by_native) - set(stream_rows))
    if missing_stream_rows:
        raise ValueError(
            "streamnet_model unit linkno must join to source.streamnet.LINKNO: "
            f"{missing_stream_rows[0]}"
        )

    geod = Geod(ellps="WGS84")
    records: list[dict[str, object]] = []
    geometries = []
    for native_id, geometry in zip(
        source.basins["streamID"], source.basins.geometry, strict=True
    ):
        unit = units_by_native[int(native_id)]
        area_km2 = abs(geod.geometry_area_perimeter(geometry)[0]) / 1_000_000
        if not math.isfinite(area_km2) or area_km2 <= 0.0:
            raise ValueError("catchment geodesic area must be positive and finite")
        area_f32 = np.float32(area_km2)
        if not np.isfinite(area_f32) or area_f32 <= 0:
            raise ValueError("catchment float32 area must be positive and finite")
        up_area = stream_rows[unit.linkno]
        up_area_f32 = np.float32(up_area)
        if not np.isfinite(up_area_f32) or up_area_f32 <= 0:
            raise ValueError("catchment float32 upstream area must be positive and finite")
        records.append(
            {
                "native_id": unit.linkno,
                "id": unit.id,
                "level": unit.level,
                "parent_id": unit.parent_id,
                "area_km2": area_f32,
                "up_area_km2": up_area_f32,
                "outlet_lon": unit.outlet_lon,
                "outlet_lat": unit.outlet_lat,
            }
        )
        geometries.append(geometry)

    ordered = gpd.GeoDataFrame(records, geometry=geometries, crs=CRS)
    ordered["_hilbert"] = ordered.geometry.centroid.hilbert_distance(
        total_bounds=ordered.geometry.total_bounds
    )
    ordered = ordered.sort_values(["_hilbert", "id"], kind="mergesort")
    ordered = ordered.drop(columns=["_hilbert"]).reset_index(drop=True)
    return ordered


def _float32_bounds(units: gpd.GeoDataFrame) -> np.ndarray:
    bounds = units.geometry.bounds.to_numpy(dtype="float64")
    return bounds.astype("float32")


def _write_catchments(path: Path, units: gpd.GeoDataFrame) -> np.ndarray:
    bounds = _float32_bounds(units)
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
        pa.array(units["id"], type=pa.int64()),
        pa.array(units["level"], type=pa.int16()),
        pa.array(units["parent_id"], type=pa.int64()),
        pa.array(units["area_km2"], type=pa.float32()),
        pa.array(units["up_area_km2"], type=pa.float32()),
        pa.array(units["outlet_lon"], type=pa.float64()),
        pa.array(units["outlet_lat"], type=pa.float64()),
        build_bbox_struct(bounds[:, 0], bounds[:, 1], bounds[:, 2], bounds[:, 3]),
        pa.array([geometry.wkb for geometry in units.geometry], type=pa.binary()),
    ]
    _write_table(path, schema, columns, len(units))
    assert_geoparquet_valid(path)
    return bounds


def _write_graph(
    path: Path,
    units: gpd.GeoDataFrame,
    bounds: np.ndarray,
    streamnet_model: StreamnetModel,
) -> None:
    units_by_id = {unit.id: unit for unit in streamnet_model.units}
    _validate_edge_relation(units_by_id, streamnet_model.edges)
    upstream_by_id = {int(unit_id): [] for unit_id in units["id"]}
    for upstream_id, downstream_id in streamnet_model.edges:
        upstream_by_id[downstream_id].append(upstream_id)
    for upstream_ids in upstream_by_id.values():
        upstream_ids.sort()

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("item", pa.int64(), nullable=True)),
                nullable=False,
            ),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )
    columns = [
        pa.array(units["id"], type=pa.int64()),
        pa.array(units["level"], type=pa.int16()),
        pa.array(
            [upstream_by_id[int(unit_id)] for unit_id in units["id"]],
            type=pa.list_(pa.int64()),
        ),
        pa.array(bounds[:, 0], type=pa.float32()),
        pa.array(bounds[:, 1], type=pa.float32()),
        pa.array(bounds[:, 2], type=pa.float32()),
        pa.array(bounds[:, 3], type=pa.float32()),
    ]
    _write_table(path, schema, columns, len(units))


def compile_core_hfx(
    source: TdxSourceData,
    streamnet_model: StreamnetModel,
    out_dir: Path,
    *,
    processing_basin_id: str,
    fabric_version: str,
    created_at: datetime,
) -> CoreBuildResult:
    """Compile normalized TDX-Hydro inputs into HFX v0.3.0 core artifacts."""
    if not processing_basin_id or not processing_basin_id.isdigit():
        raise ValueError("processing_basin_id must be a non-empty digit string")
    if not fabric_version or not fabric_version.strip():
        raise ValueError("fabric_version must be a non-empty, non-whitespace string")
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")

    units = _prepare_core_units(source, streamnet_model)
    out_dir.mkdir(parents=True, exist_ok=True)
    catchments_path = out_dir / "catchments.parquet"
    graph_path = out_dir / "graph.parquet"
    manifest_path = out_dir / "manifest.json"
    bounds = _write_catchments(catchments_path, units)
    _write_graph(graph_path, units, bounds, streamnet_model)
    manifest = {
        "format_version": FORMAT_VERSION,
        "fabric_name": FABRIC_NAME,
        "fabric_version": fabric_version,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "region": processing_basin_id,
        "bbox": [float(np.float32(value)) for value in units.geometry.total_bounds],
        "unit_count": len(units),
        "created_at": created_at.astimezone(timezone.utc).isoformat(),
        "adapter_version": ADAPTER_VERSION,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CoreBuildResult(
        catchments_path=catchments_path,
        graph_path=graph_path,
        manifest_path=manifest_path,
        diagnostics=CoreBuildDiagnostics(
            ingestion=source.diagnostics,
            streamnet=streamnet_model.diagnostics,
        ),
    )


def build_diagnostics_report(
    result: CoreBuildResult,
    *,
    processing_basin_id: str,
    fabric_version: str,
    created_at: datetime,
    dataset_root: Path,
) -> dict[str, object]:
    """Serialize build identity and preserved compiler diagnostics."""
    return {
        "build_identity": {
            "processing_basin_id": processing_basin_id,
            "fabric_name": FABRIC_NAME,
            "fabric_version": fabric_version,
            "created_at": created_at.astimezone(timezone.utc).isoformat(),
            "adapter_version": ADAPTER_VERSION,
            "dataset_root": str(dataset_root.resolve(strict=False)),
        },
        "diagnostics": asdict(result.diagnostics),
    }


def _warn_nonzero_build_diagnostics(diagnostics: CoreBuildDiagnostics) -> None:
    streamnet = diagnostics.streamnet
    for field_name in (
        "contracted_edge_count",
        "contracted_root_count",
        "contracted_link_traversal_count",
        "polygonless_dropped_reach_count",
    ):
        count = getattr(streamnet, field_name)
        if count:
            LOGGER.warning("diagnostic=%s count=%d", field_name, count)
    for field_name, native_ids_field in (
        (
            "trusted_orientation_isolated_root_count",
            "trusted_orientation_isolated_root_native_linknos",
        ),
        (
            "trusted_orientation_polygon_bearing_isolated_root_count",
            "trusted_orientation_polygon_bearing_isolated_root_native_linknos",
        ),
    ):
        count = getattr(streamnet, field_name)
        if count:
            LOGGER.warning(
                "diagnostic=%s count=%d native_ids=%s",
                field_name,
                count,
                getattr(streamnet, native_ids_field),
            )


def build_dataset(
    basins_path: Path,
    streamnet_path: Path,
    output_root: Path,
    report_path: Path,
    *,
    processing_basin_id: str,
    fabric_version: str,
    endpoint_tolerance: float = DEFAULT_ENDPOINT_TOLERANCE,
    created_at: datetime,
) -> CoreBuildResult:
    """Build and atomically publish one TDX-Hydro HFX dataset and report."""
    basins_path = basins_path.expanduser()
    streamnet_path = streamnet_path.expanduser()
    output_root = output_root.expanduser().resolve(strict=False)
    report_path = report_path.expanduser().resolve(strict=False)

    if report_path == output_root or report_path.is_relative_to(output_root):
        raise ValueError("report path must be outside dataset root")
    output_existed_empty = False
    if output_root.exists():
        if not output_root.is_dir():
            raise ValueError("output dataset root exists and is not a directory")
        if any(output_root.iterdir()):
            raise ValueError("output dataset root exists and is not empty")
        output_existed_empty = True
    if report_path.exists():
        raise ValueError("report path already exists")

    output_root.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    staging_root: Path | None = None
    report_temporary: Path | None = None
    published_dataset = False
    published_report = False
    removed_existing_output = False
    try:
        staging_root = Path(
            tempfile.mkdtemp(
                prefix=f".{output_root.name}.tmp-", dir=output_root.parent
            )
        )
        staging_dataset_root = staging_root / "dataset"
        source = load_tdx_geopackages(basins_path, streamnet_path)
        header_number = load_header_crosswalk()[processing_basin_id]
        model = build_streamnet_model(
            source.basins,
            source.streamnet,
            header_number,
            endpoint_tolerance=endpoint_tolerance,
        )
        staging_result = compile_core_hfx(
            source,
            model,
            staging_dataset_root,
            processing_basin_id=processing_basin_id,
            fabric_version=fabric_version,
            created_at=created_at,
        )
        result = CoreBuildResult(
            catchments_path=output_root / "catchments.parquet",
            graph_path=output_root / "graph.parquet",
            manifest_path=output_root / "manifest.json",
            diagnostics=staging_result.diagnostics,
        )
        report = build_diagnostics_report(
            result,
            processing_basin_id=processing_basin_id,
            fabric_version=fabric_version,
            created_at=created_at,
            dataset_root=output_root,
        )
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            prefix=f".{report_path.name}.tmp-",
            dir=report_path.parent,
            delete=False,
        ) as report_file:
            report_temporary = Path(report_file.name)
            report_file.write(json.dumps(report, indent=2, sort_keys=True) + "\n")

        _warn_nonzero_build_diagnostics(result.diagnostics)
        if output_existed_empty:
            output_root.rmdir()
            removed_existing_output = True
        staging_dataset_root.replace(output_root)
        published_dataset = True
        os.replace(report_temporary, report_path)
        published_report = True
        return result
    except Exception:
        if published_report:
            report_path.unlink(missing_ok=True)
        if published_dataset:
            shutil.rmtree(output_root, ignore_errors=True)
        if removed_existing_output and not output_root.exists():
            output_root.mkdir(parents=True)
        raise
    finally:
        if report_temporary is not None:
            report_temporary.unlink(missing_ok=True)
        if staging_root is not None:
            shutil.rmtree(staging_root, ignore_errors=True)


def validate_dataset(
    dataset: Path,
    *,
    hfx_binary: str | Path = "hfx",
) -> None:
    """Run strict HFX validation and inspect both Parquet files."""
    completed = subprocess.run(
        [
            str(hfx_binary),
            str(dataset),
            "--strict",
            "--sample-pct",
            "100",
            "--format",
            "text",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"HFX validation failed with return code {completed.returncode}: "
            f"stderr={completed.stderr!r} stdout={completed.stdout!r}"
        )

    catchments_path = dataset / "catchments.parquet"
    catchments_result = validate_geoparquet(
        str(catchments_path), target_version="1.1"
    )
    _assert_geoparquet_result(catchments_path, catchments_result)
    graph_path = dataset / "graph.parquet"
    graph_result = validate_geoparquet(str(graph_path), target_version="1.1")
    graph_failures = [
        check for check in graph_result.checks if check.status.value == "failed"
    ]
    if graph_result.is_valid or [check.name for check in graph_failures] != [
        "version_match"
    ]:
        details = "; ".join(
            f"{check.name}: {check.message}" for check in graph_failures
        )
        raise ValueError(
            f"unexpected graph GeoParquet classification for {graph_path}: {details}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the TDX-Hydro adapter command-line parser."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build_parser = subparsers.add_parser("build")
    build_parser.add_argument("--basins", required=True, type=Path)
    build_parser.add_argument("--streamnet", required=True, type=Path)
    build_parser.add_argument("--out", required=True, type=Path)
    build_parser.add_argument("--report", required=True, type=Path)
    build_parser.add_argument("--processing-basin-id", required=True)
    build_parser.add_argument("--fabric-version", required=True)
    build_parser.add_argument(
        "--endpoint-tolerance",
        type=float,
        default=DEFAULT_ENDPOINT_TOLERANCE,
    )
    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("dataset", type=Path)
    validate_parser.add_argument("--hfx-binary", default="hfx")
    return parser


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main(argv: list[str] | None = None) -> int:
    """Dispatch the selected adapter command."""
    arguments = build_arg_parser().parse_args(argv)
    if arguments.command == "build":
        created_at = _utc_now()
        build_dataset(
            arguments.basins,
            arguments.streamnet,
            arguments.out,
            arguments.report,
            processing_basin_id=arguments.processing_basin_id,
            fabric_version=arguments.fabric_version,
            endpoint_tolerance=arguments.endpoint_tolerance,
            created_at=created_at,
        )
    else:
        validate_dataset(arguments.dataset, hfx_binary=arguments.hfx_binary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
