#!/usr/bin/env python3
"""Build an NGA TDX-Hydro processing basin as an HFX dataset."""

from __future__ import annotations

import json
import logging
import math
from collections import Counter, deque
from dataclasses import dataclass
from numbers import Integral, Real
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
from pyproj import Geod
from shapely import get_coordinates, set_coordinates
from shapely.geometry import LineString


CROSSWALK_PATH = Path(__file__).parent / "data" / "tdx_header_numbers.json"
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
            "clamped coordinate-domain overshoot: layer=%s, altered_vertices=%d, native_ids=%s",
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
        "orientation_tolerance=%s",
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
    )
    return StreamnetModel(
        units=unit_tuple,
        edges=edges,
        roots=roots,
        diagnostics=diagnostics,
    )
