#!/usr/bin/env python3
"""Build an NGA TDX-Hydro processing basin as an HFX dataset."""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import logging
import math
import os
import shutil
import subprocess
import tempfile
from collections import Counter, deque
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from numbers import Integral, Real
from pathlib import Path
from typing import Iterator, Sequence

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
MERGE_INPUT_BATCH_SIZE = 1024
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
DSCONTAREA_UNIT_DECISIVENESS_MIN_RATIO = 1_000.0
DSCONTAREA_FABRIC_DIVERGENCE_SANITY_CEILING = 1.0
DEFAULT_ENDPOINT_TOLERANCE = 0.001
SNAP_BBOX_EPSILON = 1e-4


@dataclass(frozen=True)
class LayerClampDiagnostics:
    altered_vertex_count: int
    altered_native_ids: tuple[int, ...]


@dataclass(frozen=True)
class CoreMergeMetrics:
    input_count: int
    total_input_rows: int
    emitted_rows: int
    input_batch_size: int
    row_group_min: int
    row_group_max: int
    peak_input_buffer_row_pairs: int
    peak_output_buffer_row_pairs: int
    peak_heap_entries: int
    peak_buffered_row_pairs: int
    buffer_row_pair_ceiling: int


@dataclass(frozen=True)
class CoreMergeResult:
    catchments_path: Path
    graph_path: Path
    metrics: CoreMergeMetrics


@dataclass(frozen=True)
class SnapMergeMetrics:
    input_count: int
    total_input_rows: int
    emitted_rows: int
    input_batch_size: int
    row_group_min: int
    row_group_max: int
    peak_retained_input_rows: int
    peak_output_rows: int
    peak_heap_entries: int
    peak_buffered_rows: int
    buffer_row_ceiling: int


@dataclass(frozen=True)
class AssemblyResult:
    catchments_path: Path
    graph_path: Path
    snap_path: Path
    manifest_path: Path
    notice_path: Path
    citation_path: Path
    core_metrics: CoreMergeMetrics
    snap_metrics: SnapMergeMetrics


SNAP_AUXILIARY_DECLARATION = {
    "schema": "hfx.aux.snap.v2",
    "artifacts": {"snap": "aux/snap_stems.parquet"},
    "metadata": {
        "name": "stems",
        "description": (
            "Native TDX-Hydro LineString reaches for polygon-bearing level 0 "
            "drainage units."
        ),
        "references_levels": [0],
        "weight_semantics": (
            "Drainage-area weight equals inclusive DSContArea in km2; higher "
            "values indicate stronger drainage dominance."
        ),
    },
}


@dataclass(frozen=True)
class DSContAreaDiagnostics:
    source_unit: str
    checked_polygon_bearing_link_count: int
    geodesic_upstream_area_sum_m2: float
    dscontarea_sum_raw: float
    m2_relative_error: float
    km2_relative_error: float
    selected_relative_error: float
    signed_aggregate_relative_divergence: float
    absolute_aggregate_relative_divergence: float
    max_absolute_relative_divergence: float


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
    degenerate_reach_count: int
    degenerate_reach_native_linknos: tuple[int, ...]
    degenerate_polygon_bearing_reach_count: int
    degenerate_polygon_bearing_reach_native_linknos: tuple[int, ...]
    degenerate_polygonless_reach_count: int
    degenerate_polygonless_reach_native_linknos: tuple[int, ...]
    short_successor_resolved_reach_count: int
    short_successor_resolved_reach_native_linknos: tuple[int, ...]
    reach_side_near_degenerate_resolved_reach_count: int
    reach_side_near_degenerate_resolved_reach_native_linknos: tuple[int, ...]
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
    snap_path: Path
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


def _is_tdx_degenerate_reach(geometry: object) -> bool:
    if (
        not isinstance(geometry, LineString)
        or geometry.is_empty
        or geometry.has_z
    ):
        return False
    coordinates = list(geometry.coords)
    if len(coordinates) != 2 or any(len(coordinate) != 2 for coordinate in coordinates):
        return False
    converted = tuple(
        (float(coordinate[0]), float(coordinate[1])) for coordinate in coordinates
    )
    return all(math.isfinite(value) for coordinate in converted for value in coordinate) and (
        converted[0] == converted[1]
    )


def _validate_layer_geometry(
    table: gpd.GeoDataFrame,
    layer_name: str,
    allowed_types: set[str],
    *,
    allow_tdx_degenerate_reaches: bool = False,
) -> None:
    expected = " or ".join(sorted(allowed_types))
    for geometry in table.geometry:
        if geometry is None or geometry.is_empty:
            raise ValueError(f"{layer_name} geometry must be non-null and non-empty")
        if not geometry.is_valid and not (
            allow_tdx_degenerate_reaches and _is_tdx_degenerate_reach(geometry)
        ):
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
    losing_relative_error = max(m2_relative_error, km2_relative_error)
    unit_decisiveness_ratio = (
        math.inf
        if selected_relative_error == 0.0
        else losing_relative_error / selected_relative_error
    )
    if unit_decisiveness_ratio < DSCONTAREA_UNIT_DECISIVENESS_MIN_RATIO:
        raise ValueError(
            "DSContArea unit candidates are not decisive: "
            f"m2_relative_error={m2_relative_error!r}, "
            f"km2_relative_error={km2_relative_error!r}, "
            f"unit_decisiveness_ratio={unit_decisiveness_ratio!r}, "
            "minimum_ratio="
            f"{DSCONTAREA_UNIT_DECISIVENESS_MIN_RATIO!r}"
        )

    converted_samples_m2 = (
        raw_samples
        if source_unit == "m2"
        else [raw * 1_000_000 for raw in raw_samples]
    )
    signed_aggregate_relative_divergence = math.fsum(
        converted - expected
        for converted, expected in zip(
            converted_samples_m2, expected_samples, strict=True
        )
    ) / expected_sum
    absolute_aggregate_relative_divergence = math.fsum(
        abs(converted - expected)
        for converted, expected in zip(
            converted_samples_m2, expected_samples, strict=True
        )
    ) / expected_sum
    max_absolute_relative_divergence = max(
        abs(converted - expected) / expected
        for converted, expected in zip(
            converted_samples_m2, expected_samples, strict=True
        )
    )
    if (
        selected_relative_error
        > DSCONTAREA_FABRIC_DIVERGENCE_SANITY_CEILING
    ):
        raise ValueError(
            "DSContArea fabric divergence sanity check failed: "
            f"source_unit={source_unit!r}, "
            f"selected_relative_error={selected_relative_error!r}, "
            "sanity_ceiling="
            f"{DSCONTAREA_FABRIC_DIVERGENCE_SANITY_CEILING!r}"
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
        signed_aggregate_relative_divergence=(
            signed_aggregate_relative_divergence
        ),
        absolute_aggregate_relative_divergence=(
            absolute_aggregate_relative_divergence
        ),
        max_absolute_relative_divergence=max_absolute_relative_divergence,
    )
    LOGGER.info(
        "dscontarea source_unit=%s checked_polygon_bearing_link_count=%d "
        "geodesic_upstream_area_sum_m2=%s dscontarea_sum_raw=%s "
        "m2_relative_error=%s km2_relative_error=%s "
        "selected_relative_error=%s unit_decisiveness_ratio=%s "
        "signed_aggregate_relative_divergence=%s "
        "absolute_aggregate_relative_divergence=%s "
        "max_absolute_relative_divergence=%s",
        diagnostics.source_unit,
        diagnostics.checked_polygon_bearing_link_count,
        diagnostics.geodesic_upstream_area_sum_m2,
        diagnostics.dscontarea_sum_raw,
        diagnostics.m2_relative_error,
        diagnostics.km2_relative_error,
        diagnostics.selected_relative_error,
        unit_decisiveness_ratio,
        diagnostics.signed_aggregate_relative_divergence,
        diagnostics.absolute_aggregate_relative_divergence,
        diagnostics.max_absolute_relative_divergence,
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
    _validate_layer_geometry(
        streamnet,
        "streamnet",
        {"LineString"},
        allow_tdx_degenerate_reaches=True,
    )
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
    degenerate_linknos_before_clamp = tuple(
        sorted(
            linkno
            for linkno, geometry in zip(
                stream_linknos, streamnet.geometry, strict=True
            )
            if _is_tdx_degenerate_reach(geometry)
        )
    )
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
    _validate_layer_geometry(
        streamnet,
        "streamnet",
        {"LineString"},
        allow_tdx_degenerate_reaches=True,
    )
    degenerate_linknos_after_clamp = tuple(
        sorted(
            linkno
            for linkno, geometry in zip(
                stream_linknos, streamnet.geometry, strict=True
            )
            if _is_tdx_degenerate_reach(geometry)
        )
    )
    if degenerate_linknos_before_clamp != degenerate_linknos_after_clamp:
        raise ValueError(
            "streamnet degenerate reach classification changed during coordinate "
            "normalization"
        )
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
) -> tuple[
    dict[int, tuple[tuple[float, float], tuple[float, float]]],
    frozenset[int],
]:
    endpoints_by_linkno: dict[
        int, tuple[tuple[float, float], tuple[float, float]]
    ] = {}
    degenerate_linknos: set[int] = set()
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
            if not _is_tdx_degenerate_reach(geometry):
                raise ValueError(
                    "streamnet geometry for native LINKNO "
                    f"{linkno} has unsupported degenerate geometry"
                )
            degenerate_linknos.add(linkno)
        endpoints_by_linkno[linkno] = (start_xy, end_xy)

    return endpoints_by_linkno, frozenset(degenerate_linknos)


@dataclass(frozen=True)
class _OrientationResolution:
    downstream_endpoints: dict[int, tuple[float, float]]
    endpoint_coincidence_proven_link_count: int
    predecessor_orientation_proven_root_count: int
    trusted_orientation_isolated_root_native_linknos: tuple[int, ...]
    short_successor_resolved_reach_native_linknos: tuple[int, ...]
    reach_side_near_degenerate_resolved_reach_native_linknos: tuple[int, ...]


def _resolve_native_orientation(
    relation: dict[int, int],
    endpoints_by_linkno: dict[
        int, tuple[tuple[float, float], tuple[float, float]]
    ],
    degenerate_linknos: frozenset[int],
    endpoint_tolerance: float,
) -> _OrientationResolution:
    downstream_endpoints: dict[int, tuple[float, float]] = {}
    matched_successor_endpoints: dict[int, list[int]] = {}
    indeterminate_successor_endpoint_predecessors: dict[int, list[int]] = {}
    short_successor_resolved_linknos: set[int] = set()
    reach_side_near_degenerate_resolved_linknos: set[int] = set()
    endpoint_coincidence_proven_links = 0

    for linkno, downstream_linkno in relation.items():
        if downstream_linkno == TDX_LINKNO_SENTINEL:
            continue

        current_endpoints = endpoints_by_linkno[linkno]
        successor_endpoints = endpoints_by_linkno[downstream_linkno]
        current_candidates = (
            ((0, current_endpoints[0]),)
            if linkno in degenerate_linknos
            else tuple(enumerate(current_endpoints))
        )
        successor_candidates = (
            ((0, successor_endpoints[0]),)
            if downstream_linkno in degenerate_linknos
            else tuple(enumerate(successor_endpoints))
        )
        matches = [
            (current_index, successor_index)
            for current_index, current_endpoint in current_candidates
            for successor_index, successor_endpoint in successor_candidates
            if math.dist(current_endpoint, successor_endpoint) <= endpoint_tolerance
        ]
        if not matches:
            raise ValueError(
                "orientation proof for native LINKNO "
                f"{linkno} and downstream LINKNO {downstream_linkno} is non-coincident"
            )
        matched_current_indexes = sorted(
            {current_index for current_index, _ in matches}
        )
        if len(matched_current_indexes) == 1:
            current_index = matched_current_indexes[0]
            if linkno not in degenerate_linknos:
                endpoint_coincidence_proven_links += 1
        else:
            current_endpoint_separation = math.dist(
                current_endpoints[0], current_endpoints[1]
            )
            near_degenerate_limit = 2.0 * endpoint_tolerance
            if current_endpoint_separation > near_degenerate_limit:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{linkno} and downstream LINKNO {downstream_linkno} is reach-side "
                    "ambiguous: both current endpoints coincide within tolerance but "
                    f"endpoint separation {current_endpoint_separation} exceeds "
                    f"near-degenerate limit {near_degenerate_limit}"
                )
            current_index = 1
            reach_side_near_degenerate_resolved_linknos.add(linkno)

        downstream_endpoints[linkno] = current_endpoints[current_index]
        matched_successor_indexes = sorted(
            {
                successor_index
                for matched_current_index, successor_index in matches
                if matched_current_index == current_index
            }
        )
        if len(matched_successor_indexes) == 1:
            matched_successor_endpoints.setdefault(downstream_linkno, []).append(
                matched_successor_indexes[0]
            )
        else:
            indeterminate_successor_endpoint_predecessors.setdefault(
                downstream_linkno, []
            ).append(linkno)
            if (
                linkno not in degenerate_linknos
                and downstream_linkno not in degenerate_linknos
                and len(matched_current_indexes) == 1
            ):
                short_successor_resolved_linknos.add(linkno)

    predecessor_proven_roots = 0
    trusted_isolated_roots: list[int] = []
    for root_linkno, downstream_linkno in relation.items():
        if downstream_linkno != TDX_LINKNO_SENTINEL:
            continue

        if root_linkno in degenerate_linknos:
            downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][0]
            continue

        predecessor_matches = matched_successor_endpoints.get(root_linkno, [])
        indeterminate_predecessors = (
            indeterminate_successor_endpoint_predecessors.get(root_linkno, [])
        )
        upstream_endpoint_indexes = set(predecessor_matches)
        if len(upstream_endpoint_indexes) > 1:
            raise ValueError(
                f"orientation proof for root LINKNO {root_linkno} has conflicting predecessor matches"
            )
        if len(upstream_endpoint_indexes) == 1:
            upstream_endpoint_index = upstream_endpoint_indexes.pop()
            downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][
                1 - upstream_endpoint_index
            ]
            predecessor_proven_roots += 1
            continue
        if indeterminate_predecessors:
            root_endpoint_separation = math.dist(
                endpoints_by_linkno[root_linkno][0],
                endpoints_by_linkno[root_linkno][1],
            )
            if root_endpoint_separation > 2.0 * endpoint_tolerance:
                raise ValueError(
                    "orientation proof for root LINKNO "
                    f"{root_linkno} is reach-side ambiguous: predecessors "
                    f"{tuple(sorted(indeterminate_predecessors))} match both root endpoints "
                    f"but endpoint separation {root_endpoint_separation} exceeds "
                    f"near-degenerate limit {2.0 * endpoint_tolerance}"
                )
            downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][1]
            reach_side_near_degenerate_resolved_linknos.add(root_linkno)
            continue
        if not predecessor_matches:
            # TDX/TauDEM native-vertex-order TRUST ASSUMPTION: a genuinely
            # isolated root has no topology from which orientation can be
            # proven, so preserve source order and use its final vertex.
            downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][1]
            trusted_isolated_roots.append(root_linkno)
            continue

    return _OrientationResolution(
        downstream_endpoints=downstream_endpoints,
        endpoint_coincidence_proven_link_count=endpoint_coincidence_proven_links,
        predecessor_orientation_proven_root_count=predecessor_proven_roots,
        trusted_orientation_isolated_root_native_linknos=tuple(
            sorted(trusted_isolated_roots)
        ),
        short_successor_resolved_reach_native_linknos=tuple(
            sorted(short_successor_resolved_linknos)
        ),
        reach_side_near_degenerate_resolved_reach_native_linknos=tuple(
            sorted(reach_side_near_degenerate_resolved_linknos)
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
    endpoints_by_linkno, degenerate_linknos = _streamnet_endpoints(
        stream_linknos, streamnet["geometry"].tolist()
    )
    orientation = _resolve_native_orientation(
        relation, endpoints_by_linkno, degenerate_linknos, tolerance
    )
    downstream_endpoints = orientation.downstream_endpoints

    polygon_bearing_links = set(basin_linknos)
    degenerate_ids = tuple(sorted(degenerate_linknos))
    degenerate_polygon_bearing_ids = tuple(
        linkno for linkno in degenerate_ids if linkno in polygon_bearing_links
    )
    degenerate_polygonless_ids = tuple(
        linkno for linkno in degenerate_ids if linkno not in polygon_bearing_links
    )
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
    short_successor_resolved_ids = (
        orientation.short_successor_resolved_reach_native_linknos
    )
    reach_side_near_degenerate_resolved_ids = (
        orientation.reach_side_near_degenerate_resolved_reach_native_linknos
    )
    diagnostics = StreamnetDiagnostics(
        polygon_bearing_link_count=len(unit_tuple),
        polygonless_dropped_reach_count=len(relation) - len(polygon_bearing_links),
        degenerate_reach_count=len(degenerate_ids),
        degenerate_reach_native_linknos=degenerate_ids,
        degenerate_polygon_bearing_reach_count=len(degenerate_polygon_bearing_ids),
        degenerate_polygon_bearing_reach_native_linknos=(
            degenerate_polygon_bearing_ids
        ),
        degenerate_polygonless_reach_count=len(degenerate_polygonless_ids),
        degenerate_polygonless_reach_native_linknos=degenerate_polygonless_ids,
        short_successor_resolved_reach_count=len(short_successor_resolved_ids),
        short_successor_resolved_reach_native_linknos=(
            short_successor_resolved_ids
        ),
        reach_side_near_degenerate_resolved_reach_count=len(
            reach_side_near_degenerate_resolved_ids
        ),
        reach_side_near_degenerate_resolved_reach_native_linknos=(
            reach_side_near_degenerate_resolved_ids
        ),
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
        "streamnet_model polygon_bearing_links=%d degenerate_reaches=%d "
        "degenerate_reach_native_linknos=%s degenerate_polygon_bearing_reaches=%d "
        "degenerate_polygon_bearing_reach_native_linknos=%s "
        "degenerate_polygonless_reaches=%d "
        "degenerate_polygonless_reach_native_linknos=%s "
        "short_successor_resolved_reaches=%d "
        "short_successor_resolved_reach_native_linknos=%s "
        "reach_side_near_degenerate_resolved_reaches=%d "
        "reach_side_near_degenerate_resolved_reach_native_linknos=%s "
        "roots=%d contracted_edges=%d "
        "contracted_roots=%d contracted_link_traversals=%d "
        "endpoint_coincidence_proven_links=%d predecessor_orientation_proven_roots=%d "
        "trusted_orientation_isolated_roots=%d "
        "trusted_orientation_isolated_root_native_linknos=%s "
        "trusted_orientation_polygon_bearing_isolated_roots=%d "
        "trusted_orientation_polygon_bearing_isolated_root_native_linknos=%s "
        "orientation_tolerance=%s polygonless_dropped_reach_count=%d",
        diagnostics.polygon_bearing_link_count,
        diagnostics.degenerate_reach_count,
        diagnostics.degenerate_reach_native_linknos,
        diagnostics.degenerate_polygon_bearing_reach_count,
        diagnostics.degenerate_polygon_bearing_reach_native_linknos,
        diagnostics.degenerate_polygonless_reach_count,
        diagnostics.degenerate_polygonless_reach_native_linknos,
        diagnostics.short_successor_resolved_reach_count,
        diagnostics.short_successor_resolved_reach_native_linknos,
        diagnostics.reach_side_near_degenerate_resolved_reach_count,
        diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
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


def _merge_catchment_schema() -> pa.Schema:
    return pa.schema(
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


def _merge_graph_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("element", pa.int64(), nullable=True)),
                nullable=False,
            ),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )


def _snap_merge_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox", bbox_struct_type(), nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(build_geo_metadata(["LineString"]))


def _balanced_row_group_targets(
    total_rows: int,
    min_size: int,
    max_size: int,
) -> Iterator[int]:
    if total_rows <= 0:
        return

    min_groups = math.ceil(total_rows / max_size)
    group_count = max(1, total_rows // min_size)
    while group_count >= min_groups:
        base = total_rows // group_count
        remainder = total_rows % group_count
        if (
            min_size <= base <= max_size
            and base + (1 if remainder else 0) <= max_size
        ):
            for index in range(group_count):
                yield base + (1 if index < remainder else 0)
            return
        group_count -= 1

    yield total_rows


@dataclass
class _PairedMergeCursor:
    root: Path
    catchment_batches: Iterator[pa.RecordBatch]
    graph_batches: Iterator[pa.RecordBatch]
    catchment_batch: pa.RecordBatch | None = None
    graph_batch: pa.RecordBatch | None = None
    hilbert_keys: tuple[int, ...] = ()
    row_index: int = 0
    rows_seen: int = 0
    previous_input_key: tuple[int, int] | None = None
    exhausted: bool = False

    @property
    def retained_row_pairs(self) -> int:
        if self.catchment_batch is None:
            return 0
        return self.catchment_batch.num_rows

    @property
    def current_key(self) -> tuple[int, int]:
        if self.catchment_batch is None:
            raise RuntimeError("exhausted merge cursor has no current key")
        unit_id = int(self.catchment_batch.column("id")[self.row_index].as_py())
        return self.hilbert_keys[self.row_index], unit_id

    def current_rows(self) -> tuple[dict[str, object], dict[str, object]]:
        if self.catchment_batch is None or self.graph_batch is None:
            raise RuntimeError("exhausted merge cursor has no current row")
        return (
            self.catchment_batch.slice(self.row_index, 1).to_pylist()[0],
            self.graph_batch.slice(self.row_index, 1).to_pylist()[0],
        )

    def start(self) -> None:
        self._load_next_batch()

    def advance(self) -> None:
        if self.catchment_batch is None:
            raise RuntimeError("cannot advance an exhausted merge cursor")
        if self.row_index + 1 < self.catchment_batch.num_rows:
            self.row_index += 1
            return
        self._load_next_batch()

    def _load_next_batch(self) -> None:
        self.catchment_batch = None
        self.graph_batch = None
        self.hilbert_keys = ()
        catchment_batch = next(self.catchment_batches, None)
        graph_batch = next(self.graph_batches, None)
        if (catchment_batch is None) != (graph_batch is None):
            raise ValueError(
                f"{self.root}: catchment and graph batch iterators ended "
                "at different positions"
            )
        if catchment_batch is None:
            self.exhausted = True
            return
        if catchment_batch.num_rows != graph_batch.num_rows:
            raise ValueError(
                f"{self.root}: paired batch sizes differ "
                f"({catchment_batch.num_rows} != {graph_batch.num_rows})"
            )

        ids = catchment_batch.column("id").to_pylist()
        levels = catchment_batch.column("level").to_pylist()
        graph_ids = graph_batch.column("id").to_pylist()
        graph_levels = graph_batch.column("level").to_pylist()
        catchment_bboxes = catchment_batch.column("bbox").to_pylist()
        graph_bbox_columns = [
            graph_batch.column(name).to_pylist()
            for name in ("bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy")
        ]
        geometries = gpd.GeoSeries.from_wkb(
            catchment_batch.column("geometry").to_pylist(),
            crs=CRS,
        )
        hilbert_keys = tuple(
            int(value)
            for value in geometries.centroid.hilbert_distance(
                total_bounds=[-180, -90, 180, 90]
            )
        )

        previous_key = self.previous_input_key
        for offset, (hilbert, unit_id, level) in enumerate(
            zip(hilbert_keys, ids, levels, strict=True)
        ):
            absolute_row = self.rows_seen + offset
            if graph_ids[offset] != unit_id:
                raise ValueError(
                    f"{self.root}: catchment/graph id mismatch at row "
                    f"{absolute_row}: {unit_id} != {graph_ids[offset]}"
                )
            if graph_levels[offset] != level:
                raise ValueError(
                    f"{self.root}: catchment/graph level mismatch at row "
                    f"{absolute_row}: {level} != {graph_levels[offset]}"
                )
            bbox = catchment_bboxes[offset]
            graph_bbox = tuple(
                values[offset] for values in graph_bbox_columns
            )
            catchment_bbox = tuple(
                bbox[name] for name in BBOX_LEAF_NAMES
            )
            if graph_bbox != catchment_bbox:
                raise ValueError(
                    f"{self.root}: catchment/graph bbox mismatch at row "
                    f"{absolute_row}: {catchment_bbox} != {graph_bbox}"
                )
            current_key = hilbert, int(unit_id)
            if previous_key is not None and current_key < previous_key:
                raise ValueError(
                    f"{self.root}: non-monotonic input; previous key "
                    f"{previous_key}, current key {current_key}"
                )
            previous_key = current_key

        self.catchment_batch = catchment_batch
        self.graph_batch = graph_batch
        self.hilbert_keys = hilbert_keys
        self.row_index = 0
        self.rows_seen += catchment_batch.num_rows
        self.previous_input_key = previous_key


@dataclass
class _MergePeakTracker:
    peak_input: int = 0
    peak_output: int = 0
    peak_heap: int = 0
    peak_buffered: int = 0

    def observe(
        self,
        cursors: Sequence[_PairedMergeCursor],
        output_rows: int,
        heap_entries: int,
    ) -> None:
        input_rows = sum(cursor.retained_row_pairs for cursor in cursors)
        self.peak_input = max(self.peak_input, input_rows)
        self.peak_output = max(self.peak_output, output_rows)
        self.peak_heap = max(self.peak_heap, heap_entries)
        self.peak_buffered = max(
            self.peak_buffered, input_rows + output_rows
        )


def merge_catchments_and_graph(
    input_dataset_roots: Sequence[Path],
    output_root: Path,
    *,
    input_batch_size: int = MERGE_INPUT_BATCH_SIZE,
    row_group_min: int = ROW_GROUP_MIN,
    row_group_max: int = ROW_GROUP_MAX,
) -> CoreMergeResult:
    """Merge sorted catchment and graph runs with bounded row-pair buffers."""
    if not input_dataset_roots:
        raise ValueError("at least one input dataset root is required")
    if input_batch_size <= 0:
        raise ValueError("input_batch_size must be positive")
    if row_group_min <= 0:
        raise ValueError("row_group_min must be positive")
    if row_group_max < row_group_min:
        raise ValueError("row_group_max must be at least row_group_min")

    roots = tuple(Path(root).resolve() for root in input_dataset_roots)
    if len(roots) != len(set(roots)):
        raise ValueError("input dataset roots must be unique after resolution")
    resolved_output = Path(output_root).resolve()
    for root in roots:
        if root == resolved_output:
            raise ValueError(f"{root}: input dataset root aliases output root")

    expected_catchment_schema = _merge_catchment_schema()
    expected_graph_schema = _merge_graph_schema()
    total_input_rows = 0
    cursors: list[_PairedMergeCursor] = []
    peaks = _MergePeakTracker()

    with ExitStack() as stack:
        for root in roots:
            catchments_path = root / "catchments.parquet"
            graph_path = root / "graph.parquet"
            if not catchments_path.is_file():
                raise ValueError(
                    f"{root}: required regular catchments.parquet is missing"
                )
            if not graph_path.is_file():
                raise ValueError(
                    f"{root}: required regular graph.parquet is missing"
                )
            catchment_file = pq.ParquetFile(catchments_path)
            graph_file = pq.ParquetFile(graph_path)
            stack.callback(catchment_file.close)
            stack.callback(graph_file.close)
            if not catchment_file.schema_arrow.equals(
                expected_catchment_schema, check_metadata=True
            ):
                raise ValueError(f"{root}: incompatible catchment schema")
            if not graph_file.schema_arrow.equals(
                expected_graph_schema, check_metadata=True
            ):
                raise ValueError(f"{root}: incompatible graph schema")
            catchment_rows = catchment_file.metadata.num_rows
            graph_rows = graph_file.metadata.num_rows
            if catchment_rows != graph_rows:
                raise ValueError(
                    f"{root}: catchment and graph row counts differ "
                    f"({catchment_rows} != {graph_rows})"
                )
            if catchment_rows == 0:
                raise ValueError(f"{root}: input core files must be nonempty")
            total_input_rows += catchment_rows
            cursor = _PairedMergeCursor(
                root=root,
                catchment_batches=catchment_file.iter_batches(
                    batch_size=input_batch_size
                ),
                graph_batches=graph_file.iter_batches(
                    batch_size=input_batch_size
                ),
            )
            cursor.start()
            cursors.append(cursor)
            peaks.observe(cursors, 0, 0)

        output_root = Path(output_root)
        output_root.mkdir(parents=True, exist_ok=True)
        catchments_path = output_root / "catchments.parquet"
        graph_path = output_root / "graph.parquet"
        heap: list[tuple[int, int, int, _PairedMergeCursor]] = []
        for ordinal, cursor in enumerate(cursors):
            hilbert, unit_id = cursor.current_key
            heapq.heappush(heap, (hilbert, unit_id, ordinal, cursor))
        peaks.observe(cursors, 0, len(heap))

        target_sizes = iter(
            _balanced_row_group_targets(
                total_input_rows, row_group_min, row_group_max
            )
        )
        target_size = next(target_sizes)
        catchment_output: list[dict[str, object]] = []
        graph_output: list[dict[str, object]] = []
        emitted_rows = 0
        previous_output_key: tuple[int, int] | None = None
        with pq.ParquetWriter(
            catchments_path,
            schema=expected_catchment_schema,
            compression="snappy",
            write_statistics=True,
        ) as catchment_writer, pq.ParquetWriter(
            graph_path,
            schema=expected_graph_schema,
            compression="snappy",
            write_statistics=True,
        ) as graph_writer:
            while heap:
                hilbert, unit_id, ordinal, cursor = heapq.heappop(heap)
                current_key = hilbert, unit_id
                if previous_output_key is not None:
                    if current_key == previous_output_key:
                        raise ValueError(
                            "incompatible duplicate merge key "
                            f"{current_key} from {cursor.root}"
                        )
                    if current_key < previous_output_key:
                        raise ValueError(
                            "non-monotonic merged output; previous key "
                            f"{previous_output_key}, current key {current_key}"
                        )
                catchment_row, graph_row = cursor.current_rows()
                catchment_output.append(catchment_row)
                graph_output.append(graph_row)
                emitted_rows += 1
                previous_output_key = current_key
                cursor.advance()
                if not cursor.exhausted:
                    next_hilbert, next_unit_id = cursor.current_key
                    heapq.heappush(
                        heap,
                        (next_hilbert, next_unit_id, ordinal, cursor),
                    )
                peaks.observe(
                    cursors, len(catchment_output), len(heap)
                )

                if len(catchment_output) == target_size:
                    catchment_writer.write_table(
                        pa.Table.from_pylist(
                            catchment_output,
                            schema=expected_catchment_schema,
                        )
                    )
                    graph_writer.write_table(
                        pa.Table.from_pylist(
                            graph_output,
                            schema=expected_graph_schema,
                        )
                    )
                    catchment_output.clear()
                    graph_output.clear()
                    peaks.observe(cursors, 0, len(heap))
                    target_size = next(target_sizes, 0)

        if catchment_output or graph_output or target_size != 0:
            raise AssertionError("merge output row-group targets were not exhausted")

    assert_geoparquet_valid(catchments_path)
    buffer_ceiling = len(roots) * input_batch_size + row_group_max
    metrics = CoreMergeMetrics(
        input_count=len(roots),
        total_input_rows=total_input_rows,
        emitted_rows=emitted_rows,
        input_batch_size=input_batch_size,
        row_group_min=row_group_min,
        row_group_max=row_group_max,
        peak_input_buffer_row_pairs=peaks.peak_input,
        peak_output_buffer_row_pairs=peaks.peak_output,
        peak_heap_entries=peaks.peak_heap,
        peak_buffered_row_pairs=peaks.peak_buffered,
        buffer_row_pair_ceiling=buffer_ceiling,
    )
    assert metrics.peak_input_buffer_row_pairs <= len(roots) * input_batch_size
    assert metrics.peak_output_buffer_row_pairs <= row_group_max
    assert metrics.peak_heap_entries <= len(roots)
    assert metrics.peak_buffered_row_pairs <= buffer_ceiling
    assert metrics.emitted_rows == metrics.total_input_rows
    return CoreMergeResult(
        catchments_path=catchments_path,
        graph_path=graph_path,
        metrics=metrics,
    )


@dataclass
class _SnapMergeCursor:
    root: Path
    batches: Iterator[pa.RecordBatch]
    batch: pa.RecordBatch | None = None
    hilbert_keys: tuple[int, ...] = ()
    row_index: int = 0
    rows_seen: int = 0
    previous_input_key: tuple[int, int] | None = None
    exhausted: bool = False

    @property
    def retained_rows(self) -> int:
        return 0 if self.batch is None else self.batch.num_rows

    @property
    def current_key(self) -> tuple[int, int]:
        if self.batch is None:
            raise RuntimeError("exhausted snap cursor has no current key")
        unit_id = int(self.batch.column("unit_id")[self.row_index].as_py())
        return self.hilbert_keys[self.row_index], unit_id

    def current_row(self) -> dict[str, object]:
        if self.batch is None:
            raise RuntimeError("exhausted snap cursor has no current row")
        return self.batch.slice(self.row_index, 1).to_pylist()[0]

    def start(self) -> None:
        self._load_next_batch()

    def advance(self) -> None:
        if self.batch is None:
            raise RuntimeError("cannot advance an exhausted snap cursor")
        if self.row_index + 1 < self.batch.num_rows:
            self.row_index += 1
        else:
            self._load_next_batch()

    def _load_next_batch(self) -> None:
        self.batch = None
        self.hilbert_keys = ()
        batch = next(self.batches, None)
        if batch is None:
            self.exhausted = True
            return
        unit_ids = batch.column("unit_id").to_pylist()
        weights = batch.column("weight").to_pylist()
        stem_roles = batch.column("stem_role").to_pylist()
        bboxes = batch.column("bbox").to_pylist()
        geometries_wkb = batch.column("geometry").to_pylist()
        geometries = gpd.GeoSeries.from_wkb(geometries_wkb, crs=CRS)
        hilbert_keys = tuple(
            int(value)
            for value in geometries.centroid.hilbert_distance(
                total_bounds=[-180, -90, 180, 90]
            )
        )
        previous_key = self.previous_input_key
        for offset, (
            hilbert,
            unit_id,
            weight,
            stem_role,
            bbox,
            wkb,
            geometry,
        ) in enumerate(
            zip(
                hilbert_keys,
                unit_ids,
                weights,
                stem_roles,
                bboxes,
                geometries_wkb,
                geometries,
                strict=True,
            )
        ):
            absolute_row = self.rows_seen + offset
            if unit_id is None:
                raise ValueError(f"{self.root}: null snap unit_id at row {absolute_row}")
            if (
                weight is None
                or not math.isfinite(float(weight))
                or float(weight) < 0
            ):
                raise ValueError(f"{self.root}: invalid snap weight at row {absolute_row}")
            if stem_role not in {
                None,
                "mainstem",
                "tributary",
                "distributary",
                "unknown",
            }:
                raise ValueError(
                    f"{self.root}: invalid snap stem_role at row {absolute_row}"
                )
            if wkb is None:
                raise ValueError(f"{self.root}: null snap geometry at row {absolute_row}")
            if geometry.is_empty or geometry.geom_type != "LineString":
                raise ValueError(f"{self.root}: invalid snap geometry at row {absolute_row}")
            if bbox is not None:
                values = [float(bbox[name]) for name in BBOX_LEAF_NAMES]
                if (
                    not all(math.isfinite(value) for value in values)
                    or values[0] > values[2]
                    or values[1] > values[3]
                ):
                    raise ValueError(f"{self.root}: invalid snap bbox at row {absolute_row}")
            current_key = hilbert, int(unit_id)
            if previous_key is not None and current_key < previous_key:
                raise ValueError(
                    f"{self.root}: non-monotonic snap input; previous key "
                    f"{previous_key}, current key {current_key}"
                )
            previous_key = current_key
        self.batch = batch
        self.hilbert_keys = hilbert_keys
        self.row_index = 0
        self.rows_seen += batch.num_rows
        self.previous_input_key = previous_key


def _validate_snap_references(root: Path, batch_size: int) -> None:
    catchment_ids: set[int] = set()
    catchment_file = pq.ParquetFile(root / "catchments.parquet")
    try:
        for batch in catchment_file.iter_batches(
            batch_size=batch_size, columns=["id"]
        ):
            for value in batch.column("id").to_pylist():
                unit_id = int(value)
                if unit_id in catchment_ids:
                    raise ValueError(f"{root}: duplicate catchment id {unit_id}")
                catchment_ids.add(unit_id)
    finally:
        catchment_file.close()
    seen: set[int] = set()
    snap_file = pq.ParquetFile(root / "aux" / "snap_stems.parquet")
    try:
        for batch in snap_file.iter_batches(
            batch_size=batch_size, columns=["unit_id"]
        ):
            for value in batch.column("unit_id").to_pylist():
                unit_id = int(value)
                if unit_id not in catchment_ids:
                    raise ValueError(f"{root}: dangling snap unit_id {unit_id}")
                if unit_id in seen:
                    raise ValueError(f"{root}: duplicate snap unit_id {unit_id}")
                seen.add(unit_id)
    finally:
        snap_file.close()
    if seen != catchment_ids:
        missing = sorted(catchment_ids - seen)
        raise ValueError(f"{root}: snap references do not cover catchments {missing[:5]}")


def _merge_snap_stems(
    roots: Sequence[Path],
    output_root: Path,
    *,
    input_batch_size: int,
    row_group_min: int,
    row_group_max: int,
) -> tuple[Path, SnapMergeMetrics]:
    schema = _snap_merge_schema()
    cursors: list[_SnapMergeCursor] = []
    total_rows = 0
    peak_input = peak_output = peak_heap = peak_buffered = 0

    def observe(output_rows: int, heap_entries: int) -> None:
        nonlocal peak_input, peak_output, peak_heap, peak_buffered
        input_rows = sum(cursor.retained_rows for cursor in cursors)
        peak_input = max(peak_input, input_rows)
        peak_output = max(peak_output, output_rows)
        peak_heap = max(peak_heap, heap_entries)
        peak_buffered = max(peak_buffered, input_rows + output_rows)

    with ExitStack() as stack:
        for root in roots:
            path = root / "aux" / "snap_stems.parquet"
            if not path.is_file():
                raise ValueError(f"{root}: required regular aux/snap_stems.parquet is missing")
            parquet_file = pq.ParquetFile(path)
            stack.callback(parquet_file.close)
            if not parquet_file.schema_arrow.equals(schema, check_metadata=True):
                raise ValueError(f"{root}: incompatible snap schema")
            rows = parquet_file.metadata.num_rows
            if rows == 0:
                raise ValueError(f"{root}: input snap file must be nonempty")
            total_rows += rows
            cursor = _SnapMergeCursor(
                root=root,
                batches=parquet_file.iter_batches(batch_size=input_batch_size),
            )
            cursor.start()
            cursors.append(cursor)
            observe(0, 0)

        aux = output_root / "aux"
        aux.mkdir(parents=True, exist_ok=True)
        output_path = aux / "snap_stems.parquet"
        heap: list[tuple[int, int, int, _SnapMergeCursor]] = []
        for ordinal, cursor in enumerate(cursors):
            hilbert, unit_id = cursor.current_key
            heapq.heappush(heap, (hilbert, unit_id, ordinal, cursor))
        observe(0, len(heap))
        targets = iter(
            _balanced_row_group_targets(total_rows, row_group_min, row_group_max)
        )
        target = next(targets)
        output: list[dict[str, object]] = []
        emitted = 0
        previous_key: tuple[int, int] | None = None
        with pq.ParquetWriter(
            output_path, schema=schema, compression="snappy", write_statistics=True
        ) as writer:
            while heap:
                hilbert, unit_id, ordinal, cursor = heapq.heappop(heap)
                key = hilbert, unit_id
                if previous_key is not None and key <= previous_key:
                    kind = "duplicate" if key == previous_key else "non-monotonic"
                    raise ValueError(f"{kind} snap merge key {key} from {cursor.root}")
                row = cursor.current_row()
                emitted += 1
                row["id"] = emitted
                output.append(row)
                previous_key = key
                cursor.advance()
                if not cursor.exhausted:
                    next_hilbert, next_id = cursor.current_key
                    heapq.heappush(heap, (next_hilbert, next_id, ordinal, cursor))
                observe(len(output), len(heap))
                if len(output) == target:
                    writer.write_table(pa.Table.from_pylist(output, schema=schema))
                    output.clear()
                    observe(0, len(heap))
                    target = next(targets, 0)
        if output or target != 0:
            raise AssertionError("snap row-group targets were not exhausted")

    assert_geoparquet_valid(output_path)
    ceiling = len(roots) * input_batch_size + row_group_max
    metrics = SnapMergeMetrics(
        input_count=len(roots),
        total_input_rows=total_rows,
        emitted_rows=emitted,
        input_batch_size=input_batch_size,
        row_group_min=row_group_min,
        row_group_max=row_group_max,
        peak_retained_input_rows=peak_input,
        peak_output_rows=peak_output,
        peak_heap_entries=peak_heap,
        peak_buffered_rows=peak_buffered,
        buffer_row_ceiling=ceiling,
    )
    assert peak_input <= len(roots) * input_batch_size
    assert peak_output <= row_group_max
    assert peak_heap <= len(roots)
    assert peak_buffered <= ceiling
    assert emitted == total_rows
    return output_path, metrics


def _checked_assembly_manifests(
    roots: Sequence[Path],
) -> tuple[str, int, list[str], list[list[float]]]:
    crosswalk = load_header_crosswalk()
    fabric_version: str | None = None
    total_units = 0
    regions: list[str] = []
    bboxes: list[list[float]] = []
    identity = {
        "format_version": FORMAT_VERSION,
        "fabric_name": FABRIC_NAME,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "adapter_version": ADAPTER_VERSION,
    }
    for root in roots:
        path = root / "manifest.json"
        if not path.is_file():
            raise ValueError(f"{root}: required regular manifest.json is missing")
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ValueError(f"{root}: invalid manifest.json") from error
        if not isinstance(manifest, dict):
            raise ValueError(f"{root}: manifest must be one JSON object")
        for key, expected in identity.items():
            actual = manifest.get(key)
            if actual != expected or (
                isinstance(expected, bool) and actual is not expected
            ):
                raise ValueError(f"{root}: incompatible manifest {key}")
        version = manifest.get("fabric_version")
        if not isinstance(version, str) or not version:
            raise ValueError(f"{root}: invalid fabric_version")
        if fabric_version is None:
            fabric_version = version
        elif version != fabric_version:
            raise ValueError(f"{root}: incompatible fabric_version")
        region = manifest.get("region")
        if not isinstance(region, str) or not region.isdigit() or region not in crosswalk:
            raise ValueError(f"{root}: unknown manifest region")
        if region in regions:
            raise ValueError(f"{root}: duplicate manifest region {region}")
        regions.append(region)
        count = manifest.get("unit_count")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ValueError(f"{root}: invalid manifest unit_count")
        counts = [
            pq.ParquetFile(root / relative).metadata.num_rows
            for relative in (
                "catchments.parquet",
                "graph.parquet",
                "aux/snap_stems.parquet",
            )
        ]
        if any(value != count for value in counts):
            raise ValueError(f"{root}: manifest and artifact row counts differ")
        total_units += count
        bbox = manifest.get("bbox")
        if not isinstance(bbox, list) or len(bbox) != 4:
            raise ValueError(f"{root}: invalid manifest bbox")
        checked_bbox: list[float] = []
        for value in bbox:
            if (
                isinstance(value, bool)
                or not isinstance(value, Real)
                or not math.isfinite(float(value))
                or float(np.float32(value)) != float(value)
            ):
                raise ValueError(f"{root}: invalid manifest bbox")
            checked_bbox.append(float(np.float32(value)))
        if (
            checked_bbox[0] < -180
            or checked_bbox[1] < -90
            or checked_bbox[2] > 180
            or checked_bbox[3] > 90
            or checked_bbox[0] > checked_bbox[2]
            or checked_bbox[1] > checked_bbox[3]
        ):
            raise ValueError(f"{root}: invalid manifest bbox")
        bboxes.append(checked_bbox)
        if manifest.get("auxiliary") != [SNAP_AUXILIARY_DECLARATION]:
            raise ValueError(f"{root}: incompatible auxiliary declaration")
    if fabric_version is None:
        raise AssertionError("validated manifests did not provide fabric_version")
    return fabric_version, total_units, regions, bboxes


def _atomic_bytes(destination: Path, content: bytes) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=destination.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
        os.replace(temporary, destination)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    if destination.read_bytes() != content:
        raise AssertionError(f"{destination}: staged bytes changed")


def assemble_hfx(
    input_dataset_roots: Sequence[Path],
    output_root: Path,
    *,
    created_at: datetime,
    input_batch_size: int = MERGE_INPUT_BATCH_SIZE,
    row_group_min: int = ROW_GROUP_MIN,
    row_group_max: int = ROW_GROUP_MAX,
) -> AssemblyResult:
    """Assemble checked basin datasets into one bounded-memory HFX dataset."""
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    if input_batch_size <= 0:
        raise ValueError("input_batch_size must be positive")
    if row_group_min <= 0:
        raise ValueError("row_group_min must be positive")
    if row_group_max < row_group_min:
        raise ValueError("row_group_max must be at least row_group_min")
    roots = tuple(Path(root).resolve() for root in input_dataset_roots)
    if not roots:
        raise ValueError("at least one input dataset root is required")
    if len(roots) != len(set(roots)):
        raise ValueError("input dataset roots must be unique after resolution")
    fabric_version, unit_count, regions, bboxes = _checked_assembly_manifests(roots)
    for root in roots:
        _validate_snap_references(root, input_batch_size)

    output_root = Path(output_root)
    core = merge_catchments_and_graph(
        roots,
        output_root,
        input_batch_size=input_batch_size,
        row_group_min=row_group_min,
        row_group_max=row_group_max,
    )
    snap_path, snap_metrics = _merge_snap_stems(
        roots,
        output_root,
        input_batch_size=input_batch_size,
        row_group_min=row_group_min,
        row_group_max=row_group_max,
    )
    source_root = Path(__file__).resolve().parent
    attribution: dict[str, bytes] = {}
    for name in ("NOTICE", "CITATION.txt"):
        content = (source_root / name).read_bytes()
        text = content.decode("utf-8")
        for phrase in ("TDX-Hydro", "National Geospatial-Intelligence Agency"):
            if phrase not in text:
                raise ValueError(f"{name}: missing required phrase {phrase}")
        attribution[name] = content
    notice_path = output_root / "NOTICE"
    citation_path = output_root / "CITATION.txt"
    _atomic_bytes(notice_path, attribution["NOTICE"])
    _atomic_bytes(citation_path, attribution["CITATION.txt"])

    region_set = set(regions)
    if region_set == set(load_header_crosswalk()):
        coverage_bbox: list[float | int] = [-180, -90, 180, 90]
        region_name = None
    else:
        joined = ",".join(sorted(regions))
        digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]
        region_name = f"tdx-hydro-partial-{digest}"
        coverage_bbox = [
            float(np.float32(min(bbox[0] for bbox in bboxes))),
            float(np.float32(min(bbox[1] for bbox in bboxes))),
            float(np.float32(max(bbox[2] for bbox in bboxes))),
            float(np.float32(max(bbox[3] for bbox in bboxes))),
        ]
    manifest: dict[str, object] = {
        "adapter_version": ADAPTER_VERSION,
        "auxiliary": [SNAP_AUXILIARY_DECLARATION],
        "bbox": coverage_bbox,
        "created_at": created_at.astimezone(timezone.utc).isoformat(),
        "crs": CRS,
        "fabric_name": FABRIC_NAME,
        "fabric_version": fabric_version,
        "format_version": FORMAT_VERSION,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "unit_count": unit_count,
    }
    if region_name is not None:
        manifest["region"] = region_name
    manifest_path = output_root / "manifest.json"
    _atomic_bytes(
        manifest_path,
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return AssemblyResult(
        catchments_path=core.catchments_path,
        graph_path=core.graph_path,
        snap_path=snap_path,
        manifest_path=manifest_path,
        notice_path=notice_path,
        citation_path=citation_path,
        core_metrics=core.metrics,
        snap_metrics=snap_metrics,
    )


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
        total_bounds=[-180, -90, 180, 90]
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


def _prepare_snap_stems(
    source: TdxSourceData,
    streamnet_model: StreamnetModel,
    units: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    units_by_native = {unit.linkno: unit for unit in streamnet_model.units}
    if len(units_by_native) != len(streamnet_model.units):
        raise ValueError("streamnet_model unit native linkno must be unique")

    selected_rows: dict[int, tuple[object, float]] = {}
    for linkno, weight, geometry in zip(
        source.streamnet["LINKNO"],
        source.streamnet["DSContArea_km2"],
        source.streamnet.geometry,
        strict=True,
    ):
        native_id = int(linkno)
        if native_id not in units_by_native:
            continue
        if native_id in selected_rows:
            raise ValueError("selected source.streamnet.LINKNO must be unique")
        weight_value = float(weight)
        if not math.isfinite(weight_value) or weight_value <= 0.0:
            raise ValueError(
                "selected source.streamnet.DSContArea_km2 must be positive and finite"
            )
        weight_f32 = np.float32(weight_value)
        if not np.isfinite(weight_f32) or weight_f32 < 0:
            raise ValueError(
                "selected snap float32 weight must be finite and non-negative"
            )
        selected_rows[native_id] = (geometry, float(weight_f32))

    missing_linknos = sorted(set(units_by_native) - set(selected_rows))
    if missing_linknos:
        raise ValueError(
            "streamnet_model unit linkno must join exactly once to "
            f"source.streamnet.LINKNO: {missing_linknos[0]}"
        )
    if len(selected_rows) != len(units_by_native):
        raise ValueError("selected source.streamnet rows must match model units exactly")

    records: list[dict[str, object]] = []
    geometries = []
    for native_id, (geometry, weight) in selected_rows.items():
        records.append(
            {
                "unit_id": units_by_native[native_id].id,
                "weight": np.float32(weight),
                "stem_role": None,
            }
        )
        geometries.append(geometry)

    ordered = gpd.GeoDataFrame(records, geometry=geometries, crs=CRS)
    ordered["_hilbert"] = ordered.geometry.centroid.hilbert_distance(
        total_bounds=[-180, -90, 180, 90]
    )
    ordered = ordered.sort_values(["_hilbert", "unit_id"], kind="mergesort")
    return ordered.drop(columns=["_hilbert"]).reset_index(drop=True)


def _write_snap_stems(path: Path, stems: gpd.GeoDataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    bounds = stems.geometry.bounds.to_numpy(dtype="float64").astype("float32")
    x_degenerate = bounds[:, 0] == bounds[:, 2]
    y_degenerate = bounds[:, 1] == bounds[:, 3]
    bounds[x_degenerate, 0] -= np.float32(SNAP_BBOX_EPSILON)
    bounds[x_degenerate, 2] += np.float32(SNAP_BBOX_EPSILON)
    bounds[y_degenerate, 1] -= np.float32(SNAP_BBOX_EPSILON)
    bounds[y_degenerate, 3] += np.float32(SNAP_BBOX_EPSILON)
    if not np.isfinite(bounds).all() or not (
        (bounds[:, 0] < bounds[:, 2]).all()
        and (bounds[:, 1] < bounds[:, 3]).all()
    ):
        raise ValueError("snap float32 bbox values must be finite and ordered")

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
    row_count = len(stems)
    columns = [
        pa.array(np.arange(1, row_count + 1, dtype="int64"), type=pa.int64()),
        pa.array(stems["unit_id"], type=pa.int64()),
        pa.array(stems["weight"], type=pa.float32()),
        pa.array(stems["stem_role"], type=pa.string()),
        build_bbox_struct(bounds[:, 0], bounds[:, 1], bounds[:, 2], bounds[:, 3]),
        pa.array([geometry.wkb for geometry in stems.geometry], type=pa.binary()),
    ]
    _write_table(path, schema, columns, row_count)
    assert_geoparquet_valid(path)


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
    snap_path = out_dir / "aux" / "snap_stems.parquet"
    manifest_path = out_dir / "manifest.json"
    bounds = _write_catchments(catchments_path, units)
    _write_graph(graph_path, units, bounds, streamnet_model)
    stems = _prepare_snap_stems(source, streamnet_model, units)
    _write_snap_stems(snap_path, stems)
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
        "auxiliary": [
            {
                "schema": "hfx.aux.snap.v2",
                "artifacts": {"snap": "aux/snap_stems.parquet"},
                "metadata": {
                    "name": "stems",
                    "description": "Native TDX-Hydro LineString reaches for polygon-bearing level 0 drainage units.",
                    "references_levels": [0],
                    "weight_semantics": "Drainage-area weight equals inclusive DSContArea in km2; higher values indicate stronger drainage dominance.",
                },
            }
        ],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CoreBuildResult(
        catchments_path=catchments_path,
        graph_path=graph_path,
        snap_path=snap_path,
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
            "degenerate_reach_count",
            "degenerate_reach_native_linknos",
        ),
        (
            "degenerate_polygon_bearing_reach_count",
            "degenerate_polygon_bearing_reach_native_linknos",
        ),
        (
            "degenerate_polygonless_reach_count",
            "degenerate_polygonless_reach_native_linknos",
        ),
        (
            "short_successor_resolved_reach_count",
            "short_successor_resolved_reach_native_linknos",
        ),
        (
            "reach_side_near_degenerate_resolved_reach_count",
            "reach_side_near_degenerate_resolved_reach_native_linknos",
        ),
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
            snap_path=output_root / "aux" / "snap_stems.parquet",
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
    """Run strict HFX validation and inspect every dataset layer."""
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
    snap_path = dataset / "aux" / "snap_stems.parquet"
    snap_result = validate_geoparquet(str(snap_path), target_version="1.1")
    _assert_geoparquet_result(snap_path, snap_result)


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
