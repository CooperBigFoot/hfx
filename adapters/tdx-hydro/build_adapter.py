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
import sys
import tempfile
import threading
from collections import Counter, deque
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from numbers import Integral, Real
from pathlib import Path
from typing import Callable, Iterator, Mapping, Sequence

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import psutil
from geoparquet_io.core.validate import validate_geoparquet
from pyproj import Geod
from shapely import from_wkb, get_coordinates, set_coordinates
from shapely.geometry import LineString, MultiPolygon, Polygon


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
NON_ROOT_REACH_SIDE_AMBIGUITY_TOLERANCE_MULTIPLIER = 3.0
SNAP_BBOX_EPSILON = 1e-4
COMPILE_BATCH_SIZE = 4_096
COMPILE_MERGE_FAN_IN = 32
COMPILE_MEMORY_TARGET_BYTES = 25_769_803_776
COMPILE_MEMORY_SAMPLE_INTERVAL_MS = 50
_COMPILE_SCRATCH_EVENT_OBSERVER: (
    Callable[[str, dict[str, int], int], None] | None
) = None

ABSENT_PROCESSING_BASIN_IDS = (
    "1020018110",
    "2020003440",
    "2020065840",
    "2020071190",
    "4020050470",
    "5020049720",
    "6020000010",
)
GEOMETRY_ADJUDICATION_IDS = ("1020018110", "2020003440", "2020071190")
TRANSFER_ADJUDICATION_IDS = (
    "2020065840",
    "4020050470",
    "5020049720",
    "6020000010",
)
ADJUDICATION_CONTROL_ID = "7020000010"
ADJUDICATED_ADAPTER_GIT_REVISION = "bca87d8adb0651d130bde9c7dfcf3947427cfa24"
SQLITE3_IDENTITY_HEX = "53514c69746520666f726d6174203300"
HISTORICAL_TRANSFER_FAILURE_REASON = "transfer interrupted; retry from byte zero"
DUPLICATE_STREAM_ID = 9
AMBIGUOUS_NON_ROOT_LINKNO = 148956
CONFLICTING_ROOT_LINKNO = 1104039


class BasinVerdict(Enum):
    """The exhaustive classifications for absent TDX-Hydro processing basins."""

    SOURCE_DEFECT = "source defect"
    ADAPTER_STRICTNESS = "adapter strictness"
    TRANSFER_FAILURE = "transfer failure"


class AdjudicationEvidenceKind(Enum):
    """The authoritative evidence family used for a basin verdict."""

    ACQUIRED_SOURCE_GEOMETRY = "acquired source geometry"
    HISTORICAL_TRANSFER_WITH_RESOLUTION = (
        "historical transfer record with later acquisition resolution"
    )


@dataclass(frozen=True)
class AcquiredProduct:
    processing_basin_id: str
    product: str
    path: Path
    layer_name: str
    byte_count: int
    sha256: str
    attempts: int


@dataclass(frozen=True)
class HistoricalExhaustion:
    processing_basin_id: str
    failed_product: str
    attempts: int
    failure_reason: str


@dataclass(frozen=True)
class AdjudicationVerdict:
    processing_basin_id: str
    verdict: BasinVerdict
    evidence_kind: AdjudicationEvidenceKind
    evidence: dict[str, object]


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


@dataclass(frozen=True, slots=True)
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
    memory: CompileMemoryDiagnostics | None = None


@dataclass(frozen=True)
class CompilePhaseDiagnostics:
    start_rss_bytes: int | None
    end_rss_bytes: int | None
    peak_rss_bytes: int | None
    allocation_delta_bytes: int | None
    max_intra_phase_increase_bytes: int | None
    sample_count: int | None


@dataclass(frozen=True)
class CompileMemoryDiagnostics:
    target_bytes: int
    measurement_available: bool
    unavailable_reason: str | None
    observed_peak_rss_bytes: int | None
    high_water_rss_bytes: int | None
    sample_interval_ms: int
    measurement_method: str
    peak_scratch_bytes: int | None
    scratch_high_water_bytes: int | None
    scratch_measurement_available: bool
    scratch_unavailable_reason: str | None
    basins_rows: int
    streamnet_rows: int
    basins_geometry_count: int
    streamnet_geometry_count: int
    basins_coordinate_count: int
    streamnet_coordinate_count: int
    basins_input_bytes: int
    streamnet_input_bytes: int
    selected_dtypes: dict[str, str]
    phases: dict[str, CompilePhaseDiagnostics]


class _CompileMemoryRecorder:
    """Retain only phase maxima while sampling process RSS and private scratch."""

    def __init__(self, scratch_root: Path) -> None:
        self._scratch_root = scratch_root
        self._process: psutil.Process | None = None
        self._unavailable_reason: str | None = None
        self._scratch_unavailable_reason: str | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._active_phase: str | None = None
        self._phases: dict[str, dict[str, int]] = {}
        self._observed_peak = 0
        self._peak_scratch = 0
        self._scratch_high_water = 0
        self._counters = {
            "basins_rows": 0,
            "streamnet_rows": 0,
            "basins_geometry_count": 0,
            "streamnet_geometry_count": 0,
            "basins_coordinate_count": 0,
            "streamnet_coordinate_count": 0,
            "basins_input_bytes": 0,
            "streamnet_input_bytes": 0,
        }

    def start(self) -> None:
        try:
            self._process = psutil.Process(os.getpid())
            self._rss()
        except Exception as exc:
            self._unavailable_reason = f"failed to initialize process RSS sampling: {exc}"
            return
        self._thread = threading.Thread(
            target=self._sample_loop,
            name="tdx-compile-memory",
            daemon=True,
        )
        self._thread.start()

    def _rss(self) -> int:
        if self._process is None:
            raise RuntimeError("process RSS sampler is not initialized")
        return int(self._process.memory_info().rss)

    def _scratch_bytes(self) -> int:
        dataset_root = self._scratch_root / "dataset"
        total = 0
        for path in self._scratch_root.rglob("*"):
            if path.is_relative_to(dataset_root):
                continue
            try:
                if path.is_file():
                    total += path.stat().st_size
            except FileNotFoundError:
                # Run inputs are deliberately unlinked as soon as their final
                # row is consumed, so a sampling walk may race that transition.
                continue
        return total

    def _sample(self) -> None:
        if self._unavailable_reason is None:
            try:
                rss = self._rss()
            except Exception as exc:
                self._unavailable_reason = f"failed to sample process RSS: {exc}"
            else:
                with self._lock:
                    self._observed_peak = max(self._observed_peak, rss)
                    if self._active_phase is not None:
                        phase = self._phases[self._active_phase]
                        phase["peak"] = max(phase["peak"], rss)
                        phase["samples"] += 1
        if self._scratch_unavailable_reason is None:
            try:
                scratch = self._scratch_bytes()
            except Exception as exc:
                self._scratch_unavailable_reason = (
                    f"failed to sample private scratch bytes: {exc}"
                )
            else:
                self._peak_scratch = max(self._peak_scratch, scratch)
                self._scratch_high_water = max(self._scratch_high_water, scratch)

    def _sample_loop(self) -> None:
        while not self._stop_event.wait(COMPILE_MEMORY_SAMPLE_INTERVAL_MS / 1_000):
            self._sample()

    def scratch_event(self, label: str = "scratch-transition") -> None:
        """Update the event ledger after a private-file lifecycle transition."""
        if self._scratch_unavailable_reason is not None:
            return
        try:
            scratch = self._scratch_bytes()
        except Exception as exc:
            self._scratch_unavailable_reason = (
                f"failed to account private scratch bytes: {exc}"
            )
            return
        self._scratch_high_water = max(self._scratch_high_water, scratch)
        if _COMPILE_SCRATCH_EVENT_OBSERVER is not None:
            dataset_root = self._scratch_root / "dataset"
            private_files = {
                str(path.relative_to(self._scratch_root)): path.stat().st_size
                for path in self._scratch_root.rglob("*")
                if path.is_file() and not path.is_relative_to(dataset_root)
            }
            final_bytes = (
                sum(
                    path.stat().st_size
                    for path in dataset_root.rglob("*")
                    if path.is_file()
                )
                if dataset_root.exists()
                else 0
            )
            _COMPILE_SCRATCH_EVENT_OBSERVER(label, private_files, final_bytes)

    def record_counts(self, **counters: int) -> None:
        for name, value in counters.items():
            if name not in self._counters:
                raise ValueError(f"unknown compile counter: {name}")
            self._counters[name] = int(value)

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        if name in self._phases:
            raise ValueError(f"compile memory phase repeated: {name}")
        start = None
        if self._unavailable_reason is None:
            try:
                start = self._rss()
            except Exception as exc:
                self._unavailable_reason = f"failed to sample phase RSS: {exc}"
        with self._lock:
            self._phases[name] = {
                "start": 0 if start is None else start,
                "end": 0 if start is None else start,
                "peak": 0 if start is None else start,
                "samples": 0 if start is None else 1,
            }
            self._active_phase = name
        try:
            yield
        finally:
            end = None
            if self._unavailable_reason is None:
                try:
                    end = self._rss()
                except Exception as exc:
                    self._unavailable_reason = f"failed to sample phase RSS: {exc}"
            with self._lock:
                phase = self._phases[name]
                if end is not None:
                    phase["end"] = end
                    phase["peak"] = max(phase["peak"], end)
                    phase["samples"] += 1
                    self._observed_peak = max(self._observed_peak, end)
                self._active_phase = None

    def stop(self) -> CompileMemoryDiagnostics:
        self._sample()
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
        high_water = None
        if self._unavailable_reason is None:
            try:
                high_water = self._read_high_water()
            except Exception as exc:
                self._unavailable_reason = (
                    f"failed to read process high-water RSS: {exc}"
                )
        available = self._unavailable_reason is None
        phases: dict[str, CompilePhaseDiagnostics] = {}
        for name, values in self._phases.items():
            if not available:
                phases[name] = CompilePhaseDiagnostics(
                    None, None, None, None, None, None
                )
                continue
            start = values["start"]
            end = values["end"]
            peak = values["peak"]
            phases[name] = CompilePhaseDiagnostics(
                start,
                end,
                peak,
                end - start,
                max(0, peak - start),
                values["samples"],
            )
        return CompileMemoryDiagnostics(
            target_bytes=COMPILE_MEMORY_TARGET_BYTES,
            measurement_available=available,
            unavailable_reason=self._unavailable_reason,
            observed_peak_rss_bytes=self._observed_peak if available else None,
            high_water_rss_bytes=high_water if available else None,
            sample_interval_ms=COMPILE_MEMORY_SAMPLE_INTERVAL_MS,
            measurement_method="psutil-rss-plus-os-high-water",
            peak_scratch_bytes=(
                self._peak_scratch
                if self._scratch_unavailable_reason is None
                else None
            ),
            scratch_high_water_bytes=(
                self._scratch_high_water
                if self._scratch_unavailable_reason is None
                else None
            ),
            scratch_measurement_available=self._scratch_unavailable_reason is None,
            scratch_unavailable_reason=self._scratch_unavailable_reason,
            selected_dtypes={
                "native_id": "int64",
                "downstream_native_id": "int64",
                "global_id": "int64",
                "dscontarea": "float64",
                "hilbert": "uint32",
            },
            phases=phases,
            **self._counters,
        )

    def _read_high_water(self) -> int:
        status = Path("/proc/self/status")
        if status.is_file():
            for line in status.read_text().splitlines():
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1_024
        import resource

        raw = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return raw if sys.platform == "darwin" else raw * 1_024


@contextmanager
def _record_phase(
    recorder: _CompileMemoryRecorder | None, name: str
) -> Iterator[None]:
    if recorder is None:
        yield
    else:
        with recorder.phase(name):
            yield


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
    altered_vertex_count = 0
    altered_native_ids: set[int] = set()
    native_ids = table[native_id_field].to_numpy(dtype="int64", copy=False)
    for start in range(0, len(table), COMPILE_BATCH_SIZE):
        stop = min(start + COMPILE_BATCH_SIZE, len(table))
        normalized, count, changed = _clamp_coordinate_batch(
            table.geometry.iloc[start:stop],
            native_ids[start:stop],
            layer_name,
            native_id_field,
        )
        altered_vertex_count += count
        altered_native_ids.update(changed)
        table.iloc[
            start:stop, table.columns.get_loc(table.geometry.name)
        ] = gpd.GeoSeries(normalized, crs=table.crs).array
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


def _clamp_coordinate_batch(
    geometries: Sequence[object],
    native_ids: Sequence[int],
    layer_name: str,
    native_id_field: str,
) -> tuple[list[object], int, set[int]]:
    normalized: list[object] = []
    altered_vertex_count = 0
    altered_native_ids: set[int] = set()
    for geometry, native_id in zip(geometries, native_ids, strict=True):
        replacement, count = _clamp_geometry_coordinate_domain(
            geometry, layer_name, native_id_field, int(native_id)
        )
        normalized.append(replacement)
        altered_vertex_count += count
        if count:
            altered_native_ids.add(int(native_id))
    return normalized, altered_vertex_count, altered_native_ids


def _clamp_geometry_coordinate_domain(
    geometry: object,
    layer_name: str,
    native_id_field: str,
    native_id: int,
) -> tuple[object, int]:
    coordinates = get_coordinates(geometry)
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
    normalized = coordinates.copy()
    normalized[:, 0] = np.clip(normalized[:, 0], -180.0, 180.0)
    normalized[:, 1] = np.clip(normalized[:, 1], -90.0, 90.0)
    count = int(np.count_nonzero(np.any(normalized[:, :2] != coordinates[:, :2], axis=1)))
    return set_coordinates(geometry, normalized), count


def _infer_dscontarea_unit(
    basins: gpd.GeoDataFrame,
    streamnet: gpd.GeoDataFrame,
    relation: Mapping[int, int],
) -> DSContAreaDiagnostics:
    geod = Geod(ellps="WGS84")
    native_ids = np.fromiter(relation, dtype="int64", count=len(relation))
    native_order = np.argsort(native_ids, kind="stable")
    sorted_native_ids = native_ids[native_order]
    own_areas = np.zeros(len(native_ids), dtype="float64")
    basin_linknos = basins["streamID"].to_numpy(dtype="int64", copy=False)
    for linkno, geometry in zip(
        basin_linknos, basins.geometry, strict=True
    ):
        area = abs(float(geod.geometry_area_perimeter(geometry)[0]))
        if not math.isfinite(area) or area <= 0.0:
            raise ValueError(
                f"basins geometry for streamID {linkno} has non-positive geodesic area"
            )
        position = int(np.searchsorted(sorted_native_ids, linkno))
        own_areas[int(native_order[position])] = area

    downstream_native_ids = np.fromiter(
        (downstream for _, downstream in relation.items()),
        dtype="int64",
        count=len(relation),
    )
    downstream_rows = np.full(len(native_ids), -1, dtype="int64")
    connected = downstream_native_ids != TDX_LINKNO_SENTINEL
    connected_positions = np.searchsorted(
        sorted_native_ids, downstream_native_ids[connected]
    )
    downstream_rows[connected] = native_order[connected_positions]
    predecessor_counts = np.bincount(
        downstream_rows[connected], minlength=len(native_ids)
    ).astype("int64")
    upstream_rows = np.flatnonzero(connected).astype("int64")
    predecessor_order = np.argsort(
        downstream_rows[connected], kind="stable"
    )
    sorted_upstream_rows = upstream_rows[predecessor_order]
    offsets = np.empty(len(native_ids) + 1, dtype="int64")
    offsets[0] = 0
    np.cumsum(predecessor_counts, out=offsets[1:])
    remaining = predecessor_counts.copy()
    ready = deque(int(row) for row in np.flatnonzero(remaining == 0))
    upstream_area = np.empty(len(native_ids), dtype="float64")
    while ready:
        row = ready.popleft()
        predecessors = sorted_upstream_rows[offsets[row] : offsets[row + 1]]
        upstream_area[row] = math.fsum(
            (own_areas[row], *(upstream_area[predecessors].tolist()))
        )
        downstream_row = int(downstream_rows[row])
        if downstream_row >= 0:
            remaining[downstream_row] -= 1
            if remaining[downstream_row] == 0:
                ready.append(downstream_row)

    basin_positions = np.searchsorted(sorted_native_ids, basin_linknos)
    basin_rows = native_order[basin_positions]
    expected_samples = upstream_area[basin_rows]
    stream_ids = streamnet["LINKNO"].to_numpy(dtype="int64", copy=False)
    stream_order = np.argsort(stream_ids, kind="stable")
    stream_positions = np.searchsorted(stream_ids[stream_order], basin_linknos)
    raw_samples = streamnet["DSContArea"].to_numpy(
        dtype="float64", copy=False
    )[stream_order[stream_positions]]
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
        else raw_samples * 1_000_000
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
        checked_polygon_bearing_link_count=len(basin_linknos),
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
    *,
    _memory_recorder: _CompileMemoryRecorder | None = None,
) -> TdxSourceData:
    """Load and normalize one TDX-Hydro basin and streamnet GeoPackage pair."""
    with _record_phase(_memory_recorder, "basins_load"):
        basins = _load_single_geopackage(basins_path, "basins")
    with _record_phase(_memory_recorder, "streamnet_load"):
        streamnet = _load_single_geopackage(streamnet_path, "streamnet")
    with _record_phase(_memory_recorder, "source_validate"):
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
        relation = _CompactRelation(stream_linknos, downstream_linknos)
        _validate_streamnet_relation(relation)
        missing_units = sorted(set(basin_linknos) - set(stream_linknos))
        if missing_units:
            raise ValueError(
                "basins.streamID does not join to streamnet.LINKNO: "
                f"{missing_units[0]}"
            )
    with _record_phase(_memory_recorder, "basins_clamp"):
        basins_clamp = _clamp_coordinate_domain(basins, "basins", "streamID")
    with _record_phase(_memory_recorder, "streamnet_clamp"):
        streamnet_clamp = _clamp_coordinate_domain(streamnet, "streamnet", "LINKNO")
    with _record_phase(_memory_recorder, "source_post_clamp_validate"):
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
    with _record_phase(_memory_recorder, "dscontarea_infer"):
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


def _validate_streamnet_relation(relation: Mapping[int, int]) -> None:
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
    relation: Mapping[int, int],
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
) -> tuple[_StreamnetEndpoints, frozenset[int]]:
    values = np.empty((len(stream_linknos), 2, 2), dtype="float64")
    degenerate_linknos: set[int] = set()
    for row, (linkno, geometry) in enumerate(
        zip(stream_linknos, geometries, strict=True)
    ):
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
        values[row, 0] = start_xy
        values[row, 1] = end_xy

    native_ids = np.asarray(stream_linknos, dtype="int64")
    order = np.argsort(native_ids, kind="stable")
    return (
        _StreamnetEndpoints(native_ids[order], order.astype("int64"), values),
        frozenset(degenerate_linknos),
    )


@dataclass(frozen=True, slots=True)
class _StreamnetEndpoints:
    sorted_native_ids: np.ndarray
    source_rows: np.ndarray
    values: np.ndarray

    def row(self, linkno: int) -> int:
        position = int(np.searchsorted(self.sorted_native_ids, linkno))
        if (
            position == len(self.sorted_native_ids)
            or int(self.sorted_native_ids[position]) != linkno
        ):
            raise KeyError(linkno)
        return int(self.source_rows[position])

    def get(self, linkno: int) -> np.ndarray:
        return self.values[self.row(linkno)]


class _CompactRelation(Mapping[int, int]):
    """Store native topology in fixed-width columns with binary-search lookup."""

    def __init__(
        self, native_ids: Sequence[int], downstream_native_ids: Sequence[int]
    ) -> None:
        self.native_ids = np.asarray(native_ids, dtype="int64")
        self.downstream_native_ids = np.asarray(
            downstream_native_ids, dtype="int64"
        )
        self.order = np.argsort(self.native_ids, kind="stable")
        self.sorted_native_ids = self.native_ids[self.order]

    def __len__(self) -> int:
        return len(self.native_ids)

    def __iter__(self) -> Iterator[int]:
        return (int(value) for value in self.native_ids)

    def __getitem__(self, native_id: int) -> int:
        position = int(np.searchsorted(self.sorted_native_ids, native_id))
        if (
            position == len(self.sorted_native_ids)
            or int(self.sorted_native_ids[position]) != native_id
        ):
            raise KeyError(native_id)
        return int(self.downstream_native_ids[int(self.order[position])])

    def items(self) -> Iterator[tuple[int, int]]:
        return (
            (int(native_id), int(downstream_native_id))
            for native_id, downstream_native_id in zip(
                self.native_ids, self.downstream_native_ids, strict=True
            )
        )


@dataclass(frozen=True)
class _OrientationResolution:
    downstream_endpoints: np.ndarray
    endpoint_coincidence_proven_link_count: int
    predecessor_orientation_proven_root_count: int
    trusted_orientation_isolated_root_native_linknos: tuple[int, ...]
    short_successor_resolved_reach_native_linknos: tuple[int, ...]
    reach_side_near_degenerate_resolved_reach_native_linknos: tuple[int, ...]


def _resolve_native_orientation(
    relation: Mapping[int, int],
    endpoints_by_linkno: _StreamnetEndpoints,
    degenerate_linknos: frozenset[int],
    endpoint_tolerance: float,
) -> _OrientationResolution:
    downstream_endpoints = np.full(
        (len(endpoints_by_linkno.values), 2), np.nan, dtype="float64"
    )
    matched_successor_endpoints: dict[int, list[int]] = {}
    indeterminate_successor_endpoint_predecessors: dict[int, list[int]] = {}
    short_successor_resolved_linknos: set[int] = set()
    reach_side_near_degenerate_resolved_linknos: set[int] = set()
    endpoint_coincidence_proven_links = 0

    for linkno, downstream_linkno in relation.items():
        if downstream_linkno == TDX_LINKNO_SENTINEL:
            continue

        current_endpoints = endpoints_by_linkno.get(linkno)
        successor_endpoints = endpoints_by_linkno.get(downstream_linkno)
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

        downstream_endpoints[
            endpoints_by_linkno.row(linkno)
        ] = current_endpoints[current_index]
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
            downstream_endpoints[
                endpoints_by_linkno.row(root_linkno)
            ] = endpoints_by_linkno.get(root_linkno)[0]
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
            downstream_endpoints[
                endpoints_by_linkno.row(root_linkno)
            ] = endpoints_by_linkno.get(root_linkno)[1 - upstream_endpoint_index]
            predecessor_proven_roots += 1
            continue
        if indeterminate_predecessors:
            root_endpoint_separation = math.dist(
                endpoints_by_linkno.get(root_linkno)[0],
                endpoints_by_linkno.get(root_linkno)[1],
            )
            if root_endpoint_separation > 2.0 * endpoint_tolerance:
                raise ValueError(
                    "orientation proof for root LINKNO "
                    f"{root_linkno} is reach-side ambiguous: predecessors "
                    f"{tuple(sorted(indeterminate_predecessors))} match both root endpoints "
                    f"but endpoint separation {root_endpoint_separation} exceeds "
                    f"near-degenerate limit {2.0 * endpoint_tolerance}"
                )
            downstream_endpoints[
                endpoints_by_linkno.row(root_linkno)
            ] = endpoints_by_linkno.get(root_linkno)[1]
            reach_side_near_degenerate_resolved_linknos.add(root_linkno)
            continue
        if not predecessor_matches:
            # TDX/TauDEM native-vertex-order TRUST ASSUMPTION: a genuinely
            # isolated root has no topology from which orientation can be
            # proven, so preserve source order and use its final vertex.
            downstream_endpoints[
                endpoints_by_linkno.row(root_linkno)
            ] = endpoints_by_linkno.get(root_linkno)[1]
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

    relation = _CompactRelation(stream_linknos, downstream_linknos)
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
        endpoint = downstream_endpoints[endpoints_by_linkno.row(linkno)]
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
                outlet_lon=float(endpoint[0]),
                outlet_lat=float(endpoint[1]),
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
        for offset, (wkb, geometry) in enumerate(
            zip(geometries_wkb, geometries, strict=True)
        ):
            absolute_row = self.rows_seen + offset
            if wkb is None:
                raise ValueError(
                    f"{self.root}: null snap geometry at row {absolute_row}"
                )
            if geometry.is_empty or geometry.geom_type != "LineString":
                raise ValueError(
                    f"{self.root}: invalid snap geometry at row {absolute_row}"
                )
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
        ) in enumerate(
            zip(
                hilbert_keys,
                unit_ids,
                weights,
                stem_roles,
                bboxes,
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
    *,
    partial_input_root: Path | None = None,
    partial_basin_roster: Sequence[str] | None = None,
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
        required_paths = {
            "catchments.parquet": root / "catchments.parquet",
            "graph.parquet": root / "graph.parquet",
            "aux/snap_stems.parquet": root / "aux" / "snap_stems.parquet",
        }
        for relative, required_path in required_paths.items():
            if not required_path.is_file():
                raise ValueError(f"{root}: required regular {relative} is missing")
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
        if root == partial_input_root:
            assert partial_basin_roster is not None
            joined = ",".join(sorted(partial_basin_roster))
            digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]
            expected_region = f"tdx-hydro-partial-{digest}"
            if not isinstance(region, str) or region != expected_region:
                raise ValueError(
                    f"{root}: partial manifest region {region!r} does not match "
                    f"roster label {expected_region}"
                )
        else:
            if (
                not isinstance(region, str)
                or not region.isdigit()
                or region not in crosswalk
            ):
                raise ValueError(f"{root}: unknown manifest region")
            if partial_basin_roster is not None and region in partial_basin_roster:
                raise ValueError(
                    f"{root}: manifest region {region} overlaps partial basin roster"
                )
            if region in regions:
                raise ValueError(f"{root}: duplicate manifest region {region}")
            regions.append(region)
        count = manifest.get("unit_count")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ValueError(f"{root}: invalid manifest unit_count")
        snap_file = pq.ParquetFile(required_paths["aux/snap_stems.parquet"])
        try:
            if not snap_file.schema_arrow.equals(
                _snap_merge_schema(), check_metadata=True
            ):
                raise ValueError(f"{root}: incompatible snap schema")
            if snap_file.metadata.num_rows == 0:
                raise ValueError(f"{root}: input snap file must be nonempty")
        finally:
            snap_file.close()
        counts = [
            pq.ParquetFile(required_path).metadata.num_rows
            for required_path in required_paths.values()
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
    if partial_basin_roster is not None:
        regions.extend(partial_basin_roster)
    return fabric_version, total_units, sorted(regions), bboxes


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


def _assemble_hfx_into(
    roots: Sequence[Path],
    output_root: Path,
    *,
    created_at: datetime,
    input_batch_size: int,
    row_group_min: int,
    row_group_max: int,
    fabric_version: str,
    unit_count: int,
    regions: Sequence[str],
    bboxes: Sequence[Sequence[float]],
) -> AssemblyResult:
    """Write a fully checked assembly into an already-created staging root."""
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


def assemble_hfx(
    input_dataset_roots: Sequence[Path],
    output_root: Path,
    *,
    created_at: datetime,
    input_batch_size: int = MERGE_INPUT_BATCH_SIZE,
    row_group_min: int = ROW_GROUP_MIN,
    row_group_max: int = ROW_GROUP_MAX,
    partial_input_root: Path | None = None,
    partial_basin_roster: Sequence[str] | None = None,
) -> AssemblyResult:
    """Assemble and atomically publish checked basin datasets."""
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    if input_batch_size <= 0:
        raise ValueError("input_batch_size must be positive")
    if row_group_min <= 0:
        raise ValueError("row_group_min must be positive")
    if row_group_max < row_group_min:
        raise ValueError("row_group_max must be at least row_group_min")
    if (partial_input_root is None) != (partial_basin_roster is None):
        raise ValueError(
            "--partial-input and --partial-roster must be supplied together"
        )
    crosswalk = load_header_crosswalk()
    checked_partial_roster: tuple[str, ...] | None = None
    if partial_basin_roster is not None:
        if not partial_basin_roster:
            raise ValueError("partial basin roster must be nonempty")
        checked_entries: list[str] = []
        seen: set[str] = set()
        for index, basin_id in enumerate(partial_basin_roster):
            if not isinstance(basin_id, str) or basin_id not in crosswalk:
                raise ValueError(
                    "partial basin roster entry at index "
                    f"{index} is not an authoritative basin ID"
                )
            if basin_id in seen:
                raise ValueError(f"duplicate partial basin roster entry {basin_id}")
            checked_entries.append(basin_id)
            seen.add(basin_id)
        checked_partial_roster = tuple(checked_entries)
    ordinary_roots = tuple(
        Path(root).expanduser().resolve() for root in input_dataset_roots
    )
    if not ordinary_roots:
        raise ValueError("at least one input dataset root is required")
    resolved_partial_root = (
        Path(partial_input_root).expanduser().resolve()
        if partial_input_root is not None
        else None
    )
    roots = (
        (resolved_partial_root, *ordinary_roots)
        if resolved_partial_root is not None
        else ordinary_roots
    )
    if len(roots) != len(set(roots)):
        raise ValueError("input dataset roots must be unique after resolution")
    output_root = Path(output_root).expanduser().resolve(strict=False)
    if output_root in roots:
        raise ValueError("output dataset root must not alias an input dataset root")

    output_existed_empty = False
    if output_root.exists():
        if not output_root.is_dir():
            raise ValueError("output dataset root exists and is not a directory")
        if any(output_root.iterdir()):
            raise ValueError("output dataset root exists and is not empty")
        output_existed_empty = True
    if not output_root.parent.is_dir():
        raise ValueError(
            f"{output_root.parent}: output parent must be an existing directory"
        )

    fabric_version, unit_count, regions, bboxes = _checked_assembly_manifests(
        roots,
        partial_input_root=resolved_partial_root,
        partial_basin_roster=checked_partial_roster,
    )
    for root in roots:
        _validate_snap_references(root, input_batch_size)

    staging_root: Path | None = None
    published = False
    removed_existing_output = False
    try:
        staging_root = Path(
            tempfile.mkdtemp(
                prefix=f".{output_root.name}.tmp-", dir=output_root.parent
            )
        )
        staging_dataset_root = staging_root / "dataset"
        staging_result = _assemble_hfx_into(
            roots,
            staging_dataset_root,
            created_at=created_at,
            input_batch_size=input_batch_size,
            row_group_min=row_group_min,
            row_group_max=row_group_max,
            fabric_version=fabric_version,
            unit_count=unit_count,
            regions=regions,
            bboxes=bboxes,
        )
        if output_existed_empty:
            output_root.rmdir()
            removed_existing_output = True
        staging_dataset_root.replace(output_root)
        published = True
        return AssemblyResult(
            catchments_path=output_root / "catchments.parquet",
            graph_path=output_root / "graph.parquet",
            snap_path=output_root / "aux" / "snap_stems.parquet",
            manifest_path=output_root / "manifest.json",
            notice_path=output_root / "NOTICE",
            citation_path=output_root / "CITATION.txt",
            core_metrics=staging_result.core_metrics,
            snap_metrics=staging_result.snap_metrics,
        )
    except Exception:
        if published:
            shutil.rmtree(output_root, ignore_errors=True)
        if removed_existing_output and not output_root.exists():
            output_root.mkdir()
        raise
    finally:
        if staging_root is not None:
            shutil.rmtree(staging_root, ignore_errors=True)


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


def geographic_bbox_float32_coverings(exact_bounds: object) -> np.ndarray:
    """Round geographic min/min/max/max bounds outward to float32 coverings."""
    exact = np.asarray(exact_bounds, dtype="float64")
    if exact.ndim != 2 or exact.shape[1] != 4:
        raise ValueError("geographic bbox bounds must have shape (N, 4)")
    covering = exact.astype("float32")
    widened = covering.astype("float64")
    lower_inward = widened[:, :2] > exact[:, :2]
    upper_inward = widened[:, 2:] < exact[:, 2:]
    covering[:, :2] = np.where(
        lower_inward,
        np.nextafter(
            covering[:, :2],
            np.float32(-np.inf),
            dtype=np.float32,
        ),
        covering[:, :2],
    )
    covering[:, 2:] = np.where(
        upper_inward,
        np.nextafter(
            covering[:, 2:],
            np.float32(np.inf),
            dtype=np.float32,
        ),
        covering[:, 2:],
    )
    return covering


def _basin_raw_spool_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("native_id", pa.int64(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )


def _stream_raw_spool_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("native_id", pa.int64(), nullable=False),
            pa.field("downstream_native_id", pa.int64(), nullable=False),
            pa.field("dscontarea_raw", pa.float64(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )


def _basin_spool_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("native_id", pa.int64(), nullable=False),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("exact_minx", pa.float64(), nullable=False),
            pa.field("exact_miny", pa.float64(), nullable=False),
            pa.field("exact_maxx", pa.float64(), nullable=False),
            pa.field("exact_maxy", pa.float64(), nullable=False),
            pa.field("hilbert", pa.uint32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )


def _stream_spool_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("native_id", pa.int64(), nullable=False),
            pa.field("downstream_native_id", pa.int64(), nullable=False),
            pa.field("dscontarea_raw", pa.float64(), nullable=False),
            pa.field("start_lon", pa.float64(), nullable=False),
            pa.field("start_lat", pa.float64(), nullable=False),
            pa.field("end_lon", pa.float64(), nullable=False),
            pa.field("end_lat", pa.float64(), nullable=False),
            pa.field("is_degenerate", pa.bool_(), nullable=False),
            pa.field("exact_minx", pa.float64(), nullable=False),
            pa.field("exact_miny", pa.float64(), nullable=False),
            pa.field("exact_maxx", pa.float64(), nullable=False),
            pa.field("exact_maxy", pa.float64(), nullable=False),
            pa.field("hilbert", pa.uint32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )


@dataclass(frozen=True)
class _NormalizedSpools:
    basin_path: Path
    stream_path: Path
    basin_crs: object
    stream_crs: object
    diagnostics: IngestionDiagnostics


@dataclass(frozen=True)
class _CompactTopology:
    native_ids: np.ndarray
    global_ids: np.ndarray
    downstream_native_ids: np.ndarray
    downstream_global_ids: np.ndarray
    contracted_counts: np.ndarray
    outlet_lons: np.ndarray
    outlet_lats: np.ndarray
    up_area_km2: np.ndarray
    upstream_offsets: np.ndarray
    upstream_global_ids: np.ndarray
    diagnostics: StreamnetDiagnostics

    @property
    def nbytes(self) -> int:
        return sum(
            int(values.nbytes)
            for values in (
                self.native_ids,
                self.global_ids,
                self.downstream_native_ids,
                self.downstream_global_ids,
                self.contracted_counts,
                self.outlet_lons,
                self.outlet_lats,
                self.up_area_km2,
                self.upstream_offsets,
                self.upstream_global_ids,
            )
        )

    def rows_for(self, native_ids: np.ndarray) -> np.ndarray:
        positions = np.searchsorted(self.native_ids, native_ids)
        if (
            np.any(positions == len(self.native_ids))
            or np.any(self.native_ids[np.minimum(positions, len(self.native_ids) - 1)] != native_ids)
        ):
            raise ValueError("native ID does not join to compact topology")
        return positions.astype("int64", copy=False)


@dataclass(frozen=True)
class _StreamingSource:
    spools: _NormalizedSpools
    basin_native_ids: np.ndarray
    stream_native_ids: np.ndarray
    downstream_native_ids: np.ndarray
    endpoints: np.ndarray
    degenerate: np.ndarray
    up_area_km2: np.ndarray


def _validate_compile_tuning(batch_size: int, merge_fan_in: int) -> None:
    if isinstance(batch_size, bool) or not isinstance(batch_size, Integral) or batch_size <= 0:
        raise ValueError("compile batch size must be a positive integer")
    if (
        isinstance(merge_fan_in, bool)
        or not isinstance(merge_fan_in, Integral)
        or merge_fan_in < 2
    ):
        raise ValueError("compile merge fan-in must be an integer of at least 2")


def _single_layer_metadata(path: Path, layer_name: str) -> tuple[str, dict[str, object]]:
    expanded = path.expanduser()
    if expanded.suffix.lower() != ".gpkg" or not expanded.is_file():
        raise ValueError(
            f"{layer_name} path must be an existing regular .gpkg file: {expanded}"
        )
    try:
        discovered = pyogrio.list_layers(expanded)
    except Exception as exc:
        raise ValueError(f"failed to list {layer_name} layers in {expanded}: {exc}") from exc
    names = [str(name) for name in discovered[:, 0].tolist()]
    if len(names) != 1:
        raise ValueError(
            f"{layer_name} GeoPackage {expanded} must contain exactly one vector "
            f"layer; discovered layer names: {names}"
        )
    try:
        info = pyogrio.read_info(expanded, layer=names[0])
    except Exception as exc:
        raise ValueError(f"failed to inspect {layer_name} layer in {expanded}: {exc}") from exc
    return names[0], info


def _raw_wkb_values(
    batch: pa.RecordBatch, geometry_name: str, layer_name: str
) -> pa.Array:
    geometry = batch.column(batch.schema.get_field_index(geometry_name))
    if geometry.null_count:
        raise ValueError(f"{layer_name} geometry must be non-null and non-empty")
    return geometry


def _write_raw_layer_spool(
    source_path: Path,
    destination: Path,
    layer_name: str,
    fields: tuple[str, ...],
    schema: pa.Schema,
    *,
    batch_size: int,
    recorder: _CompileMemoryRecorder | None,
) -> tuple[object, int]:
    layer, info = _single_layer_metadata(source_path, layer_name)
    available = set(str(name) for name in info["fields"])
    missing = sorted(set(fields) - available)
    if missing:
        raise ValueError(f"{layer_name} is missing required columns: {missing}")
    crs = info.get("crs")
    if crs is None:
        raise ValueError(f"{layer_name} must declare a CRS")
    row_count = 0
    try:
        with pyogrio.open_arrow(
            source_path,
            layer=layer,
            columns=list(fields),
            batch_size=batch_size,
            use_pyarrow=True,
        ) as (metadata, reader), pq.ParquetWriter(
            destination,
            schema=schema,
            compression="snappy",
            write_statistics=True,
        ) as writer:
            geometry_name = str(metadata.get("geometry_name") or "wkb_geometry")
            for batch in reader:
                if batch.num_rows > batch_size:
                    raise ValueError("source Arrow batch exceeds compile batch size")
                geometry = _raw_wkb_values(batch, geometry_name, layer_name)
                if layer_name == "basins":
                    native = [
                        _topology_integer(value.as_py(), "basins", "streamID")
                        for value in batch.column(batch.schema.get_field_index("streamID"))
                    ]
                    table = pa.Table.from_arrays(
                        [
                            pa.array(native, type=pa.int64()),
                            pa.array(geometry, type=pa.binary()),
                        ],
                        schema=schema,
                    )
                else:
                    native = [
                        _topology_integer(value.as_py(), "streamnet", "LINKNO")
                        for value in batch.column(batch.schema.get_field_index("LINKNO"))
                    ]
                    downstream = [
                        _topology_integer(value.as_py(), "streamnet", "DSLINKNO")
                        for value in batch.column(batch.schema.get_field_index("DSLINKNO"))
                    ]
                    raw_values = [
                        value.as_py()
                        for value in batch.column(batch.schema.get_field_index("DSContArea"))
                    ]
                    converted: list[float] = []
                    for original in raw_values:
                        if original is None:
                            original = float("nan")
                        if isinstance(original, bool):
                            raise ValueError(
                                "streamnet.DSContArea must contain finite positive "
                                f"values; got {original!r}"
                            )
                        try:
                            value = float(original)
                        except (TypeError, ValueError) as exc:
                            raise ValueError(
                                "streamnet.DSContArea must contain finite positive "
                                f"values; got {original!r}"
                            ) from exc
                        if not math.isfinite(value) or value <= 0.0:
                            raise ValueError(
                                "streamnet.DSContArea must contain finite positive "
                                f"values; got {original!r}"
                            )
                        converted.append(value)
                    table = pa.Table.from_arrays(
                        [
                            pa.array(native, type=pa.int64()),
                            pa.array(downstream, type=pa.int64()),
                            pa.array(converted, type=pa.float64()),
                            pa.array(geometry, type=pa.binary()),
                        ],
                        schema=schema,
                    )
                writer.write_table(table)
                row_count += batch.num_rows
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"failed to read {layer_name} layer from {source_path}: {exc}") from exc
    if row_count == 0:
        raise ValueError(f"{layer_name} must be a non-empty GeoDataFrame")
    if recorder is not None:
        recorder.scratch_event(f"{layer_name}-raw-closed")
    return crs, row_count


def _geometry_series_from_wkb(values: pa.Array, crs: object) -> gpd.GeoSeries:
    geometries = from_wkb(values.to_numpy(zero_copy_only=False), on_invalid="ignore")
    series = gpd.GeoSeries(geometries, crs=crs)
    if series.crs is None:
        raise ValueError("source geometry batch must declare a CRS")
    if series.crs.to_epsg() != 4326:
        try:
            series = series.to_crs(CRS)
        except Exception as exc:
            raise ValueError(f"failed to transform source batch to {CRS}: {exc}") from exc
    return series


def _scan_raw_geometry_spool(
    path: Path,
    crs: object,
    layer_name: str,
    allowed_types: set[str],
    *,
    allow_degenerate: bool = False,
) -> tuple[int, ...]:
    degenerate: list[int] = []
    columns = ["native_id", "geometry"]
    for batch in pq.ParquetFile(path).iter_batches(
        batch_size=COMPILE_BATCH_SIZE, columns=columns
    ):
        native_ids = batch.column(0).to_numpy(zero_copy_only=False)
        series = _geometry_series_from_wkb(batch.column(1), crs)
        frame = gpd.GeoDataFrame(
            {"native_id": native_ids}, geometry=series, crs=CRS
        )
        _validate_layer_geometry(
            frame,
            layer_name,
            allowed_types,
            allow_tdx_degenerate_reaches=allow_degenerate,
        )
        if allow_degenerate:
            degenerate.extend(
                int(native_id)
                for native_id, geometry in zip(native_ids, series, strict=True)
                if _is_tdx_degenerate_reach(geometry)
            )
    return tuple(sorted(degenerate))


def _hilbert_for_series(series: gpd.GeoSeries) -> np.ndarray:
    values = series.centroid.hilbert_distance(
        total_bounds=[-180, -90, 180, 90]
    ).to_numpy(dtype="uint32", copy=False)
    return values


def _normalize_basin_spool(
    raw_path: Path,
    normalized_path: Path,
    crs: object,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> tuple[LayerClampDiagnostics, int, int, np.ndarray, np.ndarray]:
    schema = _basin_spool_schema()
    altered = 0
    altered_ids: set[int] = set()
    geometry_count = 0
    coordinate_count = 0
    all_native: list[np.ndarray] = []
    all_own_area_m2: list[np.ndarray] = []
    geod = Geod(ellps="WGS84")
    with pq.ParquetWriter(
        normalized_path, schema=schema, compression="snappy", write_statistics=True
    ) as writer:
        for batch in pq.ParquetFile(raw_path).iter_batches(
            batch_size=COMPILE_BATCH_SIZE
        ):
            native = batch.column(batch.schema.get_field_index("native_id")).to_numpy(
                zero_copy_only=False
            ).astype("int64", copy=False)
            series = _geometry_series_from_wkb(
                batch.column(batch.schema.get_field_index("geometry")), crs
            )
            normalized, count, changed = _clamp_coordinate_batch(
                series, native, "basins", "streamID"
            )
            series = gpd.GeoSeries(normalized, crs=CRS)
            altered += count
            altered_ids.update(changed)
            geometry_count += len(series)
            coordinate_count += sum(len(get_coordinates(geometry)) for geometry in series)
            own_area_m2 = np.asarray(
                [
                    abs(float(geod.geometry_area_perimeter(geometry)[0]))
                    for geometry in series
                ],
                dtype="float64",
            )
            if np.any(~np.isfinite(own_area_m2)) or np.any(own_area_m2 <= 0):
                bad = int(native[np.flatnonzero((~np.isfinite(own_area_m2)) | (own_area_m2 <= 0))[0]])
                raise ValueError(
                    f"basins geometry for streamID {bad} has non-positive geodesic area"
                )
            area_km2 = (own_area_m2 / 1_000_000).astype("float32")
            if np.any(~np.isfinite(area_km2)) or np.any(area_km2 <= 0):
                raise ValueError("catchment float32 area must be positive and finite")
            bounds = series.bounds.to_numpy(dtype="float64")
            hilbert = _hilbert_for_series(series)
            writer.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(native, type=pa.int64()),
                        pa.array(area_km2, type=pa.float32()),
                        pa.array(bounds[:, 0], type=pa.float64()),
                        pa.array(bounds[:, 1], type=pa.float64()),
                        pa.array(bounds[:, 2], type=pa.float64()),
                        pa.array(bounds[:, 3], type=pa.float64()),
                        pa.array(hilbert, type=pa.uint32()),
                        pa.array([geometry.wkb for geometry in series], type=pa.binary()),
                    ],
                    schema=schema,
                )
            )
            all_native.append(native.copy())
            all_own_area_m2.append(own_area_m2)
    if recorder is not None:
        recorder.scratch_event("basin-normalized-closed")
    return (
        LayerClampDiagnostics(altered, tuple(sorted(altered_ids))),
        geometry_count,
        coordinate_count,
        np.concatenate(all_native),
        np.concatenate(all_own_area_m2),
    )


def _normalize_stream_spool(
    raw_path: Path,
    normalized_path: Path,
    crs: object,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> tuple[LayerClampDiagnostics, int, int]:
    schema = _stream_spool_schema()
    altered = 0
    altered_ids: set[int] = set()
    geometry_count = 0
    coordinate_count = 0
    with pq.ParquetWriter(
        normalized_path, schema=schema, compression="snappy", write_statistics=True
    ) as writer:
        for batch in pq.ParquetFile(raw_path).iter_batches(
            batch_size=COMPILE_BATCH_SIZE
        ):
            names = batch.schema.names
            native = batch.column(names.index("native_id")).to_numpy(
                zero_copy_only=False
            ).astype("int64", copy=False)
            downstream = batch.column(names.index("downstream_native_id")).to_numpy(
                zero_copy_only=False
            ).astype("int64", copy=False)
            raw = batch.column(names.index("dscontarea_raw")).to_numpy(
                zero_copy_only=False
            ).astype("float64", copy=False)
            series = _geometry_series_from_wkb(batch.column(names.index("geometry")), crs)
            normalized, count, changed = _clamp_coordinate_batch(
                series, native, "streamnet", "LINKNO"
            )
            series = gpd.GeoSeries(normalized, crs=CRS)
            altered += count
            altered_ids.update(changed)
            geometry_count += len(series)
            coordinate_count += sum(len(get_coordinates(geometry)) for geometry in series)
            endpoints = np.empty((len(series), 4), dtype="float64")
            degenerate = np.zeros(len(series), dtype=bool)
            for row, (native_id, geometry) in enumerate(zip(native, series, strict=True)):
                coordinates = list(geometry.coords)
                if len(coordinates) < 2:
                    raise ValueError(
                        f"streamnet geometry for native LINKNO {native_id} must have at least two coordinates"
                    )
                endpoints[row] = (
                    float(coordinates[0][0]),
                    float(coordinates[0][1]),
                    float(coordinates[-1][0]),
                    float(coordinates[-1][1]),
                )
                degenerate[row] = _is_tdx_degenerate_reach(geometry)
                if endpoints[row, 0] == endpoints[row, 2] and endpoints[row, 1] == endpoints[row, 3] and not degenerate[row]:
                    raise ValueError(
                        "streamnet geometry for native LINKNO "
                        f"{native_id} has unsupported degenerate geometry"
                    )
            bounds = series.bounds.to_numpy(dtype="float64")
            hilbert = _hilbert_for_series(series)
            writer.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(native, type=pa.int64()),
                        pa.array(downstream, type=pa.int64()),
                        pa.array(raw, type=pa.float64()),
                        pa.array(endpoints[:, 0], type=pa.float64()),
                        pa.array(endpoints[:, 1], type=pa.float64()),
                        pa.array(endpoints[:, 2], type=pa.float64()),
                        pa.array(endpoints[:, 3], type=pa.float64()),
                        pa.array(degenerate, type=pa.bool_()),
                        pa.array(bounds[:, 0], type=pa.float64()),
                        pa.array(bounds[:, 1], type=pa.float64()),
                        pa.array(bounds[:, 2], type=pa.float64()),
                        pa.array(bounds[:, 3], type=pa.float64()),
                        pa.array(hilbert, type=pa.uint32()),
                        pa.array([geometry.wkb for geometry in series], type=pa.binary()),
                    ],
                    schema=schema,
                )
            )
    if recorder is not None:
        recorder.scratch_event("stream-normalized-closed")
    return (
        LayerClampDiagnostics(altered, tuple(sorted(altered_ids))),
        geometry_count,
        coordinate_count,
    )


def _spool_numpy(path: Path, columns: Sequence[str]) -> tuple[np.ndarray, ...]:
    table = pq.read_table(path, columns=list(columns))
    return tuple(
        table.column(name).combine_chunks().to_numpy(zero_copy_only=False)
        for name in columns
    )


def _sorted_unique_ids(values: np.ndarray, label: str) -> np.ndarray:
    order = np.argsort(values, kind="stable")
    sorted_values = values[order]
    duplicates = np.flatnonzero(sorted_values[1:] == sorted_values[:-1])
    if len(duplicates):
        duplicate = int(sorted_values[int(duplicates[0])])
        if label == "basins":
            raise ValueError(f"duplicate unit identity for streamID {duplicate}")
        raise ValueError(f"duplicate LINKNO {duplicate} in streamnet")
    return sorted_values


def _compact_downstream_rows(
    native_ids: np.ndarray, downstream_native_ids: np.ndarray
) -> np.ndarray:
    rows = np.full(len(native_ids), -1, dtype="int64")
    connected = downstream_native_ids != TDX_LINKNO_SENTINEL
    positions = np.searchsorted(native_ids, downstream_native_ids[connected])
    missing = (positions == len(native_ids)) | (
        native_ids[np.minimum(positions, len(native_ids) - 1)]
        != downstream_native_ids[connected]
    )
    if np.any(missing):
        missing_id = int(downstream_native_ids[connected][np.flatnonzero(missing)[0]])
        raise ValueError(
            f"streamnet missing downstream LINKNO {missing_id} referenced by native LINKNO "
            f"{int(native_ids[np.flatnonzero(connected)[np.flatnonzero(missing)[0]]])}"
        )
    rows[connected] = positions
    self_links = np.flatnonzero(rows == np.arange(len(rows)))
    if len(self_links):
        raise ValueError(
            f"streamnet self-link at native LINKNO {int(native_ids[int(self_links[0])])}"
        )
    return rows


def _topological_order(
    native_ids: np.ndarray, downstream_rows: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    connected_rows = np.flatnonzero(downstream_rows >= 0).astype("int64")
    predecessor_counts = np.bincount(
        downstream_rows[connected_rows], minlength=len(native_ids)
    ).astype("int64")
    order = np.argsort(downstream_rows[connected_rows], kind="stable")
    upstream_rows = connected_rows[order]
    offsets = np.empty(len(native_ids) + 1, dtype="int64")
    offsets[0] = 0
    np.cumsum(predecessor_counts, out=offsets[1:])
    remaining = predecessor_counts.copy()
    queue = np.empty(len(native_ids), dtype="int64")
    ready = np.flatnonzero(remaining == 0)
    queue[: len(ready)] = ready
    head = 0
    tail = len(ready)
    topology = np.empty(len(native_ids), dtype="int64")
    count = 0
    while head < tail:
        row = int(queue[head])
        head += 1
        topology[count] = row
        count += 1
        downstream = int(downstream_rows[row])
        if downstream >= 0:
            remaining[downstream] -= 1
            if remaining[downstream] == 0:
                queue[tail] = downstream
                tail += 1
    if count != len(native_ids):
        cycle_row = int(np.flatnonzero(remaining > 0)[0])
        cycle = [int(native_ids[cycle_row])]
        current = int(downstream_rows[cycle_row])
        while current != cycle_row and current >= 0 and len(cycle) <= len(native_ids):
            cycle.append(int(native_ids[current]))
            current = int(downstream_rows[current])
        cycle.append(int(native_ids[cycle_row]))
        raise ValueError(
            "streamnet cycle detected: " + " -> ".join(str(value) for value in cycle)
        )
    return topology, offsets, upstream_rows


def _infer_dscontarea_from_columns(
    basin_native_ids: np.ndarray,
    own_area_m2: np.ndarray,
    stream_native_ids: np.ndarray,
    downstream_rows: np.ndarray,
    dscontarea_raw: np.ndarray,
    topology_order: np.ndarray,
    predecessor_offsets: np.ndarray,
    predecessor_rows: np.ndarray,
) -> tuple[DSContAreaDiagnostics, np.ndarray]:
    upstream_area = np.empty(len(stream_native_ids), dtype="float64")
    basin_positions = np.searchsorted(stream_native_ids, basin_native_ids)
    own_by_stream = np.zeros(len(stream_native_ids), dtype="float64")
    own_by_stream[basin_positions] = own_area_m2
    for row_value in topology_order:
        row = int(row_value)
        predecessors = predecessor_rows[
            predecessor_offsets[row] : predecessor_offsets[row + 1]
        ]
        upstream_area[row] = math.fsum(
            (own_by_stream[row], *(upstream_area[predecessors].tolist()))
        )
    expected_samples = upstream_area[basin_positions]
    raw_samples = dscontarea_raw[basin_positions]
    expected_sum = math.fsum(expected_samples)
    raw_sum = math.fsum(raw_samples)
    m2_relative_error = math.fsum(
        abs(raw - expected)
        for raw, expected in zip(raw_samples, expected_samples, strict=True)
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
    ratio = (
        math.inf
        if selected_relative_error == 0.0
        else losing_relative_error / selected_relative_error
    )
    if ratio < DSCONTAREA_UNIT_DECISIVENESS_MIN_RATIO:
        raise ValueError(
            "DSContArea unit candidates are not decisive: "
            f"m2_relative_error={m2_relative_error!r}, "
            f"km2_relative_error={km2_relative_error!r}, "
            f"unit_decisiveness_ratio={ratio!r}, "
            f"minimum_ratio={DSCONTAREA_UNIT_DECISIVENESS_MIN_RATIO!r}"
        )
    converted = raw_samples if source_unit == "m2" else raw_samples * 1_000_000
    signed = math.fsum(
        value - expected
        for value, expected in zip(converted, expected_samples, strict=True)
    ) / expected_sum
    absolute = math.fsum(
        abs(value - expected)
        for value, expected in zip(converted, expected_samples, strict=True)
    ) / expected_sum
    maximum = max(
        abs(value - expected) / expected
        for value, expected in zip(converted, expected_samples, strict=True)
    )
    if selected_relative_error > DSCONTAREA_FABRIC_DIVERGENCE_SANITY_CEILING:
        raise ValueError(
            "DSContArea fabric divergence sanity check failed: "
            f"source_unit={source_unit!r}, "
            f"selected_relative_error={selected_relative_error!r}, "
            f"sanity_ceiling={DSCONTAREA_FABRIC_DIVERGENCE_SANITY_CEILING!r}"
        )
    diagnostics = DSContAreaDiagnostics(
        source_unit=source_unit,
        checked_polygon_bearing_link_count=len(basin_native_ids),
        geodesic_upstream_area_sum_m2=expected_sum,
        dscontarea_sum_raw=raw_sum,
        m2_relative_error=m2_relative_error,
        km2_relative_error=km2_relative_error,
        selected_relative_error=selected_relative_error,
        signed_aggregate_relative_divergence=signed,
        absolute_aggregate_relative_divergence=absolute,
        max_absolute_relative_divergence=maximum,
    )
    up_area_km2 = (
        dscontarea_raw / 1_000_000
        if source_unit == "m2"
        else dscontarea_raw.copy()
    )
    return diagnostics, up_area_km2.astype("float64", copy=False)


def _build_compact_topology(
    basin_native_ids: np.ndarray,
    stream_native_ids: np.ndarray,
    downstream_native_ids: np.ndarray,
    endpoints: np.ndarray,
    degenerate: np.ndarray,
    up_area_km2: np.ndarray,
    header_number: int,
    endpoint_tolerance: float,
) -> _CompactTopology:
    tolerance = _positive_finite_tolerance(endpoint_tolerance)
    non_root_reach_side_ambiguity_limit = (
        NON_ROOT_REACH_SIDE_AMBIGUITY_TOLERANCE_MULTIPLIER * tolerance
    )
    polygon_positions = np.searchsorted(stream_native_ids, basin_native_ids)
    if (
        np.any(polygon_positions == len(stream_native_ids))
        or np.any(
            stream_native_ids[
                np.minimum(polygon_positions, len(stream_native_ids) - 1)
            ]
            != basin_native_ids
        )
    ):
        missing = int(
            basin_native_ids[
                np.flatnonzero(
                    (polygon_positions == len(stream_native_ids))
                    | (
                        stream_native_ids[
                            np.minimum(polygon_positions, len(stream_native_ids) - 1)
                        ]
                        != basin_native_ids
                    )
                )[0]
            ]
        )
        raise ValueError(
            f"basins.streamID does not join to streamnet.LINKNO: {missing}"
        )
    polygon_mask = np.zeros(len(stream_native_ids), dtype=bool)
    polygon_mask[polygon_positions] = True
    downstream_rows = _compact_downstream_rows(
        stream_native_ids, downstream_native_ids
    )
    topology_order, predecessor_offsets, predecessor_rows = _topological_order(
        stream_native_ids, downstream_rows
    )

    downstream_endpoints = np.full((len(stream_native_ids), 2), np.nan)
    successor_match = np.full(len(stream_native_ids), -1, dtype="int8")
    successor_conflict = np.zeros(len(stream_native_ids), dtype=bool)
    indeterminate = np.zeros(len(stream_native_ids), dtype=bool)
    short_resolved = np.zeros(len(stream_native_ids), dtype=bool)
    near_resolved = np.zeros(len(stream_native_ids), dtype=bool)
    endpoint_proven = 0
    connected_matches: list[list[tuple[int, int]] | None] = [
        None
    ] * len(stream_native_ids)
    connected_current_indexes: list[list[int] | None] = [
        None
    ] * len(stream_native_ids)
    for row in np.flatnonzero(downstream_rows >= 0):
        successor = int(downstream_rows[row])
        current_candidates = (0,) if degenerate[row] else (0, 1)
        successor_candidates = (0,) if degenerate[successor] else (0, 1)
        matches = [
            (current_index, successor_index)
            for current_index in current_candidates
            for successor_index in successor_candidates
            if math.dist(
                endpoints[row, current_index], endpoints[successor, successor_index]
            )
            <= tolerance
        ]
        if not matches:
            raise ValueError(
                "orientation proof for native LINKNO "
                f"{int(stream_native_ids[row])} and downstream LINKNO "
                f"{int(stream_native_ids[successor])} is non-coincident"
            )
        current_indexes = sorted({current for current, _ in matches})
        if len(current_indexes) > 1:
            separation = math.dist(endpoints[row, 0], endpoints[row, 1])
            if separation > non_root_reach_side_ambiguity_limit:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{int(stream_native_ids[row])} and downstream LINKNO "
                    f"{int(stream_native_ids[successor])} is reach-side ambiguous: "
                    "both current endpoints coincide within tolerance but endpoint "
                    f"separation {separation} exceeds near-degenerate limit {non_root_reach_side_ambiguity_limit}"
                )
        connected_matches[row] = matches
        connected_current_indexes[row] = current_indexes

    for row_value in topology_order[::-1]:
        row = int(row_value)
        successor = int(downstream_rows[row])
        if successor < 0:
            continue
        matches = connected_matches[row]
        current_indexes = connected_current_indexes[row]
        if matches is None or current_indexes is None:
            raise RuntimeError(
                "internal compact-topology match state is missing for native LINKNO "
                f"{int(stream_native_ids[row])} and downstream LINKNO "
                f"{int(stream_native_ids[successor])}"
            )
        if len(current_indexes) == 1:
            current_index = current_indexes[0]
            if not degenerate[row]:
                endpoint_proven += 1
        else:
            current_id = int(stream_native_ids[row])
            successor_id = int(stream_native_ids[successor])
            current_area = float(up_area_km2[row])
            successor_area = float(up_area_km2[successor])
            if current_area == successor_area:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} cannot use "
                    "source vertex order: DSContArea evidence is tied at "
                    f"{current_area!r} km2"
                )
            if current_area > successor_area:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} contradicts "
                    "source vertex order: upstream DSContArea "
                    f"{current_area!r} km2 exceeds downstream DSContArea "
                    f"{successor_area!r} km2"
                )
            if degenerate[successor]:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} cannot use "
                    "source vertex order: downstream-nondecreasing DSContArea "
                    f"{current_area!r} km2 -> {successor_area!r} km2 does not "
                    "distinguish the successor endpoint side"
                )
            if downstream_rows[successor] < 0:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} cannot use "
                    "source vertex order: downstream-nondecreasing DSContArea "
                    f"{current_area!r} km2 -> {successor_area!r} km2 cannot determine "
                    f"the upstream endpoint of root successor LINKNO {successor_id}"
                )
            if np.isnan(downstream_endpoints[successor]).any():
                raise RuntimeError(
                    "internal compact-topology orientation is missing for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id}"
                )
            successor_downstream_index = next(
                (
                    index
                    for index in (0, 1)
                    if np.array_equal(
                        endpoints[successor, index],
                        downstream_endpoints[successor],
                    )
                ),
                -1,
            )
            if successor_downstream_index < 0:
                raise RuntimeError(
                    "internal compact-topology orientation is invalid for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id}"
                )
            successor_upstream_index = 1 - successor_downstream_index
            source_successor_indexes = sorted(
                {
                    successor_index
                    for matched_current, successor_index in matches
                    if matched_current == 1
                }
            )
            if source_successor_indexes == [successor_downstream_index]:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} contradicts "
                    "source vertex order: downstream-nondecreasing DSContArea "
                    f"{current_area!r} km2 -> {successor_area!r} km2 identifies "
                    f"successor endpoint {successor_upstream_index} as upstream, but "
                    "source endpoint 1 matches successor downstream endpoint "
                    f"{successor_downstream_index}"
                )
            if source_successor_indexes != [successor_upstream_index]:
                raise ValueError(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} cannot use "
                    "source vertex order: downstream-nondecreasing DSContArea "
                    f"{current_area!r} km2 -> {successor_area!r} km2 does not "
                    "distinguish the successor endpoint side"
                )
            current_index = 1
            near_resolved[row] = True
        downstream_endpoints[row] = endpoints[row, current_index]
        successor_indexes = sorted(
            {
                successor_index
                for matched_current, successor_index in matches
                if matched_current == current_index
            }
        )
        if len(successor_indexes) == 1:
            matched = successor_indexes[0]
            if successor_match[successor] < 0:
                successor_match[successor] = matched
            elif successor_match[successor] != matched:
                successor_conflict[successor] = True
        else:
            indeterminate[successor] = True
            if (
                not degenerate[row]
                and not degenerate[successor]
                and len(current_indexes) == 1
            ):
                short_resolved[row] = True

    predecessor_proven_roots = 0
    trusted_roots: list[int] = []
    for row in np.flatnonzero(downstream_rows < 0):
        native_id = int(stream_native_ids[row])
        if degenerate[row]:
            downstream_endpoints[row] = endpoints[row, 0]
        elif successor_conflict[row]:
            raise ValueError(
                f"orientation proof for root LINKNO {native_id} has conflicting predecessor matches"
            )
        elif successor_match[row] >= 0:
            downstream_endpoints[row] = endpoints[row, 1 - successor_match[row]]
            predecessor_proven_roots += 1
        elif indeterminate[row]:
            separation = math.dist(endpoints[row, 0], endpoints[row, 1])
            if separation > 2.0 * tolerance:
                predecessor_ids = tuple(
                    sorted(
                        int(stream_native_ids[value])
                        for value in predecessor_rows[
                            predecessor_offsets[row] : predecessor_offsets[row + 1]
                        ]
                    )
                )
                raise ValueError(
                    "orientation proof for root LINKNO "
                    f"{native_id} is reach-side ambiguous: predecessors "
                    f"{predecessor_ids} match both root endpoints but endpoint "
                    f"separation {separation} exceeds near-degenerate limit {2.0 * tolerance}"
                )
            root_area = float(up_area_km2[row])
            predecessors = sorted(
                (
                    int(stream_native_ids[predecessor]),
                    int(predecessor),
                )
                for predecessor in predecessor_rows[
                    predecessor_offsets[row] : predecessor_offsets[row + 1]
                ]
            )
            for predecessor_id, predecessor in predecessors:
                predecessor_area = float(up_area_km2[predecessor])
                if predecessor_area == root_area:
                    raise ValueError(
                        f"orientation proof for root LINKNO {native_id} cannot use "
                        "source vertex order: DSContArea evidence is tied with "
                        f"predecessor LINKNO {predecessor_id} at {root_area!r} km2"
                    )
                if predecessor_area > root_area:
                    raise ValueError(
                        f"orientation proof for root LINKNO {native_id} contradicts "
                        "source vertex order: predecessor LINKNO "
                        f"{predecessor_id} DSContArea {predecessor_area!r} km2 "
                        f"exceeds root DSContArea {root_area!r} km2"
                    )
            downstream_endpoints[row] = endpoints[row, 1]
            near_resolved[row] = True
        else:
            downstream_endpoints[row] = endpoints[row, 1]
            trusted_roots.append(native_id)

    next_polygon = np.full(len(stream_native_ids), -1, dtype="int64")
    distance_to_polygon_or_root = np.zeros(len(stream_native_ids), dtype="int64")
    for row_value in topology_order[::-1]:
        row = int(row_value)
        if polygon_mask[row]:
            next_polygon[row] = row
            distance_to_polygon_or_root[row] = 0
        else:
            downstream = int(downstream_rows[row])
            if downstream >= 0:
                next_polygon[row] = next_polygon[downstream]
                distance_to_polygon_or_root[row] = (
                    distance_to_polygon_or_root[downstream] + 1
                )
    unit_native_ids = stream_native_ids[polygon_positions]
    unit_order = np.argsort(unit_native_ids, kind="stable")
    unit_native_ids = unit_native_ids[unit_order]
    unit_stream_rows = polygon_positions[unit_order]
    resolved_rows = np.full(len(unit_native_ids), -1, dtype="int64")
    contracted = np.zeros(len(unit_native_ids), dtype="int64")
    for unit_row, stream_row in enumerate(unit_stream_rows):
        downstream = int(downstream_rows[stream_row])
        if downstream >= 0:
            resolved_rows[unit_row] = next_polygon[downstream]
            contracted[unit_row] = (
                0
                if polygon_mask[downstream]
                else distance_to_polygon_or_root[downstream]
            )
    resolved_native = np.full(len(unit_native_ids), TDX_LINKNO_SENTINEL, dtype="int64")
    connected_units = resolved_rows >= 0
    resolved_native[connected_units] = stream_native_ids[
        resolved_rows[connected_units]
    ]
    global_ids = unit_native_ids + header_number * GLOBAL_LINKNO_STRIDE
    downstream_global = np.full(len(unit_native_ids), TDX_LINKNO_SENTINEL, dtype="int64")
    downstream_global[connected_units] = (
        resolved_native[connected_units] + header_number * GLOBAL_LINKNO_STRIDE
    )
    outlet_lons = downstream_endpoints[unit_stream_rows, 0].copy()
    outlet_lats = downstream_endpoints[unit_stream_rows, 1].copy()
    unit_up_area = up_area_km2[unit_stream_rows].copy()

    connected_unit_rows = np.flatnonzero(connected_units)
    downstream_unit_rows = np.searchsorted(
        unit_native_ids, resolved_native[connected_units]
    )
    edge_order = np.lexsort(
        (global_ids[connected_unit_rows], downstream_global[connected_units])
    )
    sorted_upstream = global_ids[connected_unit_rows][edge_order]
    sorted_downstream_rows = downstream_unit_rows[edge_order]
    upstream_counts = np.bincount(
        sorted_downstream_rows, minlength=len(unit_native_ids)
    )
    upstream_offsets = np.empty(len(unit_native_ids) + 1, dtype="int64")
    upstream_offsets[0] = 0
    np.cumsum(upstream_counts, out=upstream_offsets[1:])

    degenerate_ids = tuple(int(value) for value in stream_native_ids[degenerate])
    degenerate_polygon = tuple(
        int(value) for value in stream_native_ids[degenerate & polygon_mask]
    )
    degenerate_polygonless = tuple(
        int(value) for value in stream_native_ids[degenerate & ~polygon_mask]
    )
    short_ids = tuple(int(value) for value in stream_native_ids[short_resolved])
    near_ids = tuple(int(value) for value in stream_native_ids[near_resolved])
    trusted_ids = tuple(sorted(trusted_roots))
    trusted_polygon_ids = tuple(
        value
        for value in trusted_ids
        if polygon_mask[int(np.searchsorted(stream_native_ids, value))]
    )
    diagnostics = StreamnetDiagnostics(
        polygon_bearing_link_count=len(unit_native_ids),
        polygonless_dropped_reach_count=len(stream_native_ids) - len(unit_native_ids),
        degenerate_reach_count=len(degenerate_ids),
        degenerate_reach_native_linknos=degenerate_ids,
        degenerate_polygon_bearing_reach_count=len(degenerate_polygon),
        degenerate_polygon_bearing_reach_native_linknos=degenerate_polygon,
        degenerate_polygonless_reach_count=len(degenerate_polygonless),
        degenerate_polygonless_reach_native_linknos=degenerate_polygonless,
        short_successor_resolved_reach_count=len(short_ids),
        short_successor_resolved_reach_native_linknos=short_ids,
        reach_side_near_degenerate_resolved_reach_count=len(near_ids),
        reach_side_near_degenerate_resolved_reach_native_linknos=near_ids,
        root_count=int(np.count_nonzero(~connected_units)),
        contracted_edge_count=int(np.count_nonzero(connected_units & (contracted > 0))),
        contracted_root_count=int(np.count_nonzero(~connected_units & (contracted > 0))),
        contracted_link_traversal_count=int(contracted.sum()),
        endpoint_coincidence_proven_link_count=endpoint_proven,
        predecessor_orientation_proven_root_count=predecessor_proven_roots,
        trusted_orientation_isolated_root_count=len(trusted_ids),
        trusted_orientation_isolated_root_native_linknos=trusted_ids,
        trusted_orientation_polygon_bearing_isolated_root_count=len(
            trusted_polygon_ids
        ),
        trusted_orientation_polygon_bearing_isolated_root_native_linknos=(
            trusted_polygon_ids
        ),
        orientation_tolerance=tolerance,
    )
    return _CompactTopology(
        native_ids=unit_native_ids,
        global_ids=global_ids,
        downstream_native_ids=resolved_native,
        downstream_global_ids=downstream_global,
        contracted_counts=contracted,
        outlet_lons=outlet_lons,
        outlet_lats=outlet_lats,
        up_area_km2=unit_up_area,
        upstream_offsets=upstream_offsets,
        upstream_global_ids=sorted_upstream,
        diagnostics=diagnostics,
    )


def _ingest_source_spools(
    basins_path: Path,
    streamnet_path: Path,
    scratch_root: Path,
    recorder: _CompileMemoryRecorder,
) -> _StreamingSource:
    _validate_compile_tuning(COMPILE_BATCH_SIZE, COMPILE_MERGE_FAN_IN)
    basin_raw = scratch_root / "basins.raw.parquet"
    stream_raw = scratch_root / "streamnet.raw.parquet"
    basin_spool = scratch_root / "basins.normalized.parquet"
    stream_spool = scratch_root / "streamnet.normalized.parquet"
    with recorder.phase("basins_load"):
        basin_crs, basin_rows = _write_raw_layer_spool(
            basins_path,
            basin_raw,
            "basins",
            ("streamID",),
            _basin_raw_spool_schema(),
            batch_size=COMPILE_BATCH_SIZE,
            recorder=recorder,
        )
    with recorder.phase("streamnet_load"):
        stream_crs, stream_rows = _write_raw_layer_spool(
            streamnet_path,
            stream_raw,
            "streamnet",
            ("LINKNO", "DSLINKNO", "DSContArea"),
            _stream_raw_spool_schema(),
            batch_size=COMPILE_BATCH_SIZE,
            recorder=recorder,
        )
    with recorder.phase("source_validate"):
        degenerate_before = _scan_raw_geometry_spool(
            stream_raw,
            stream_crs,
            "streamnet",
            {"LineString"},
            allow_degenerate=True,
        )
        _scan_raw_geometry_spool(
            basin_raw, basin_crs, "basins", {"Polygon", "MultiPolygon"}
        )
        (basin_native_input,) = _spool_numpy(basin_raw, ("native_id",))
        stream_native_input, downstream_input = _spool_numpy(
            stream_raw, ("native_id", "downstream_native_id")
        )
        basin_native_input = basin_native_input.astype("int64", copy=False)
        stream_native_input = stream_native_input.astype("int64", copy=False)
        downstream_input = downstream_input.astype("int64", copy=False)
        if np.any(basin_native_input < 0):
            raise ValueError(
                "basins.streamID must be non-negative; got "
                f"{int(basin_native_input[np.flatnonzero(basin_native_input < 0)[0]])}"
            )
        if np.any(stream_native_input < 0):
            raise ValueError(
                "streamnet.LINKNO must be non-negative; got "
                f"{int(stream_native_input[np.flatnonzero(stream_native_input < 0)[0]])}"
            )
        if np.any(downstream_input < TDX_LINKNO_SENTINEL):
            raise ValueError(
                "streamnet.DSLINKNO must be non-negative or -1; got "
                f"{int(downstream_input[np.flatnonzero(downstream_input < TDX_LINKNO_SENTINEL)[0]])}"
            )
        basin_sorted = _sorted_unique_ids(basin_native_input, "basins")
        stream_order = np.argsort(stream_native_input, kind="stable")
        stream_sorted = stream_native_input[stream_order]
        downstream_sorted = downstream_input[stream_order]
        duplicates = np.flatnonzero(stream_sorted[1:] == stream_sorted[:-1])
        if len(duplicates):
            duplicate = int(stream_sorted[int(duplicates[0])])
            targets = downstream_sorted[stream_sorted == duplicate]
            if len(np.unique(targets)) > 1:
                raise ValueError(
                    f"bifurcation: duplicate LINKNO {duplicate} has multiple DSLINKNO targets"
                )
            raise ValueError(f"duplicate LINKNO {duplicate} in streamnet")
        missing_positions = np.searchsorted(stream_sorted, basin_sorted)
        missing = (missing_positions == len(stream_sorted)) | (
            stream_sorted[np.minimum(missing_positions, len(stream_sorted) - 1)]
            != basin_sorted
        )
        if np.any(missing):
            raise ValueError(
                "basins.streamID does not join to streamnet.LINKNO: "
                f"{int(basin_sorted[np.flatnonzero(missing)[0]])}"
            )
        _compact_downstream_rows(stream_sorted, downstream_sorted)
        _topological_order(
            stream_sorted,
            _compact_downstream_rows(stream_sorted, downstream_sorted),
        )
    with recorder.phase("basins_clamp"):
        (
            basins_clamp,
            basin_geometry_count,
            basin_coordinate_count,
            basin_native_normalized,
            own_area_m2,
        ) = _normalize_basin_spool(
            basin_raw, basin_spool, basin_crs, recorder=recorder
        )
        basin_raw.unlink()
        recorder.scratch_event("basin-raw-unlinked")
    with recorder.phase("streamnet_clamp"):
        streamnet_clamp, stream_geometry_count, stream_coordinate_count = (
            _normalize_stream_spool(
                stream_raw, stream_spool, stream_crs, recorder=recorder
            )
        )
        stream_raw.unlink()
        recorder.scratch_event("stream-raw-unlinked")
    with recorder.phase("source_post_clamp_validate"):
        _scan_raw_geometry_spool(
            basin_spool, CRS, "basins", {"Polygon", "MultiPolygon"}
        )
        degenerate_after = _scan_raw_geometry_spool(
            stream_spool,
            CRS,
            "streamnet",
            {"LineString"},
            allow_degenerate=True,
        )
        if degenerate_before != degenerate_after:
            raise ValueError(
                "streamnet degenerate reach classification changed during coordinate "
                "normalization"
            )
    with recorder.phase("dscontarea_infer"):
        stream_columns = (
            "native_id",
            "downstream_native_id",
            "dscontarea_raw",
            "start_lon",
            "start_lat",
            "end_lon",
            "end_lat",
            "is_degenerate",
        )
        stream_values = _spool_numpy(stream_spool, stream_columns)
        stream_order = np.argsort(stream_values[0], kind="stable")
        stream_native = stream_values[0][stream_order].astype("int64", copy=False)
        downstream_native = stream_values[1][stream_order].astype(
            "int64", copy=False
        )
        dscontarea_raw = stream_values[2][stream_order].astype(
            "float64", copy=False
        )
        endpoints = np.column_stack(
            [
                stream_values[3][stream_order],
                stream_values[4][stream_order],
                stream_values[5][stream_order],
                stream_values[6][stream_order],
            ]
        ).reshape(-1, 2, 2)
        degenerate = stream_values[7][stream_order].astype(bool, copy=False)
        basin_order = np.argsort(basin_native_normalized, kind="stable")
        basin_native = basin_native_normalized[basin_order].astype(
            "int64", copy=False
        )
        own_area_m2 = own_area_m2[basin_order]
        downstream_rows = _compact_downstream_rows(
            stream_native, downstream_native
        )
        topology_order, predecessor_offsets, predecessor_rows = _topological_order(
            stream_native, downstream_rows
        )
        dscontarea, up_area_km2 = _infer_dscontarea_from_columns(
            basin_native,
            own_area_m2,
            stream_native,
            downstream_rows,
            dscontarea_raw,
            topology_order,
            predecessor_offsets,
            predecessor_rows,
        )
    recorder.record_counts(
        basins_rows=basin_rows,
        streamnet_rows=stream_rows,
        basins_geometry_count=basin_geometry_count,
        streamnet_geometry_count=stream_geometry_count,
        basins_coordinate_count=basin_coordinate_count,
        streamnet_coordinate_count=stream_coordinate_count,
        basins_input_bytes=basins_path.stat().st_size,
        streamnet_input_bytes=streamnet_path.stat().st_size,
    )
    return _StreamingSource(
        spools=_NormalizedSpools(
            basin_path=basin_spool,
            stream_path=stream_spool,
            basin_crs=CRS,
            stream_crs=CRS,
            diagnostics=IngestionDiagnostics(
                basins_clamp=basins_clamp,
                streamnet_clamp=streamnet_clamp,
                dscontarea=dscontarea,
            ),
        ),
        basin_native_ids=basin_native,
        stream_native_ids=stream_native,
        downstream_native_ids=downstream_native,
        endpoints=endpoints,
        degenerate=degenerate,
        up_area_km2=up_area_km2,
    )


def _compile_graph_schema() -> pa.Schema:
    return pa.schema(
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


def _catchment_run_schema() -> pa.Schema:
    return _merge_catchment_schema().append(
        pa.field("hilbert", pa.uint32(), nullable=False)
    )


def _snap_run_schema() -> pa.Schema:
    return _snap_merge_schema().append(
        pa.field("hilbert", pa.uint32(), nullable=False)
    )


def _table_with_schema_rows(rows: list[dict[str, object]], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=schema)


@dataclass
class _CompileRunCursor:
    path: Path
    source: pa.NativeFile
    batches: Iterator[pa.RecordBatch]
    recorder: _CompileMemoryRecorder | None
    batch: pa.RecordBatch | None = None
    row: int = 0
    exhausted: bool = False

    def _advance_batch(self) -> bool:
        try:
            self.batch = next(self.batches)
        except StopIteration:
            self.batch = None
            self.exhausted = True
            self.source.close()
            self.path.unlink()
            if self.recorder is not None:
                self.recorder.scratch_event(f"run-unlinked:{self.path.name}")
            return False
        self.row = 0
        return True

    def current(self) -> dict[str, object]:
        if self.batch is None and not self._advance_batch():
            raise StopIteration
        assert self.batch is not None
        return {
            name: self.batch.column(index)[self.row].as_py()
            for index, name in enumerate(self.batch.schema.names)
        }

    def advance(self) -> bool:
        assert self.batch is not None
        self.row += 1
        if self.row < self.batch.num_rows:
            return True
        return self._advance_batch()


def _merged_run_rows(
    paths: Sequence[Path],
    key_fields: tuple[str, str],
    *,
    recorder: _CompileMemoryRecorder | None,
) -> Iterator[dict[str, object]]:
    with ExitStack() as stack:
        cursors: list[_CompileRunCursor] = []
        heap: list[tuple[int, int, int]] = []
        for index, path in enumerate(paths):
            source = stack.enter_context(pa.memory_map(str(path), "r"))
            parquet = pq.ParquetFile(source)
            cursor = _CompileRunCursor(
                path=path,
                source=source,
                batches=iter(
                    parquet.iter_batches(batch_size=COMPILE_BATCH_SIZE)
                ),
                recorder=recorder,
            )
            cursors.append(cursor)
            row = cursor.current()
            heapq.heappush(
                heap,
                (int(row[key_fields[0]]), int(row[key_fields[1]]), index),
            )
        while heap:
            _, _, index = heapq.heappop(heap)
            cursor = cursors[index]
            row = cursor.current()
            yield row
            if cursor.advance():
                next_row = cursor.current()
                heapq.heappush(
                    heap,
                    (
                        int(next_row[key_fields[0]]),
                        int(next_row[key_fields[1]]),
                        index,
                    ),
                )


def _merge_run_group(
    paths: Sequence[Path],
    destination: Path,
    schema: pa.Schema,
    key_fields: tuple[str, str],
    *,
    recorder: _CompileMemoryRecorder | None,
) -> None:
    buffer: list[dict[str, object]] = []
    with pq.ParquetWriter(
        destination, schema=schema, compression="snappy", write_statistics=True
    ) as writer:
        for row in _merged_run_rows(paths, key_fields, recorder=recorder):
            buffer.append(row)
            if len(buffer) == COMPILE_BATCH_SIZE:
                writer.write_table(_table_with_schema_rows(buffer, schema))
                buffer.clear()
                if recorder is not None:
                    recorder.scratch_event("merge-output-batch-written")
        if buffer:
            writer.write_table(_table_with_schema_rows(buffer, schema))
    if recorder is not None:
        consumed = ",".join(path.name for path in paths)
        recorder.scratch_event(
            f"merge-run-closed:{destination.name}:consumed:{consumed}"
        )


def _reduce_run_fan_in(
    paths: list[Path],
    scratch_root: Path,
    stem: str,
    schema: pa.Schema,
    key_fields: tuple[str, str],
    merge_fan_in: int,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> list[Path]:
    generation = 1
    current = paths
    while len(current) > merge_fan_in:
        replacement: list[Path] = []
        for group_index, start in enumerate(range(0, len(current), merge_fan_in)):
            group = current[start : start + merge_fan_in]
            destination = (
                scratch_root
                / f"{stem}.merge-{generation:03d}-{group_index:06d}.parquet"
            )
            _merge_run_group(
                group,
                destination,
                schema,
                key_fields,
                recorder=recorder,
            )
            replacement.append(destination)
        current = replacement
        generation += 1
    return current


def _create_catchment_runs(
    basin_spool: Path,
    scratch_root: Path,
    topology: _CompactTopology,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> list[Path]:
    schema = _catchment_run_schema()
    paths: list[Path] = []
    parquet = pq.ParquetFile(basin_spool)
    for run_number, batch in enumerate(
        parquet.iter_batches(batch_size=COMPILE_BATCH_SIZE)
    ):
        names = batch.schema.names
        native = batch.column(names.index("native_id")).to_numpy(
            zero_copy_only=False
        ).astype("int64", copy=False)
        unit_rows = topology.rows_for(native)
        hilbert = batch.column(names.index("hilbert")).to_numpy(
            zero_copy_only=False
        ).astype("uint32", copy=False)
        global_ids = topology.global_ids[unit_rows]
        order = np.lexsort((global_ids, hilbert))
        exact_bounds = np.column_stack(
            [
                batch.column(names.index(name)).to_numpy(zero_copy_only=False)
                for name in ("exact_minx", "exact_miny", "exact_maxx", "exact_maxy")
            ]
        )[order]
        bounds = geographic_bbox_float32_coverings(exact_bounds)
        count = len(order)
        table = pa.Table.from_arrays(
            [
                pa.array(global_ids[order], type=pa.int64()),
                pa.array(np.zeros(count, dtype="int16"), type=pa.int16()),
                pa.nulls(count, type=pa.int64()),
                pa.array(
                    batch.column(names.index("area_km2"))
                    .to_numpy(zero_copy_only=False)[order],
                    type=pa.float32(),
                ),
                pa.array(
                    topology.up_area_km2[unit_rows][order].astype("float32"),
                    type=pa.float32(),
                ),
                pa.array(topology.outlet_lons[unit_rows][order], type=pa.float64()),
                pa.array(topology.outlet_lats[unit_rows][order], type=pa.float64()),
                build_bbox_struct(
                    bounds[:, 0], bounds[:, 1], bounds[:, 2], bounds[:, 3]
                ),
                pa.array(
                    [
                        batch.column(names.index("geometry"))[int(index)].as_py()
                        for index in order
                    ],
                    type=pa.binary(),
                ),
                pa.array(hilbert[order], type=pa.uint32()),
            ],
            schema=schema,
        )
        path = scratch_root / f"catchment.run-{run_number:06d}.parquet"
        with pq.ParquetWriter(
            path, schema=schema, compression="snappy", write_statistics=True
        ) as writer:
            writer.write_table(table)
        paths.append(path)
        if recorder is not None:
            recorder.scratch_event(f"catchment-run-closed:{path.name}")
    return paths


def _create_snap_runs(
    stream_spool: Path,
    scratch_root: Path,
    topology: _CompactTopology,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> list[Path]:
    schema = _snap_run_schema()
    paths: list[Path] = []
    parquet = pq.ParquetFile(stream_spool)
    for run_number, batch in enumerate(
        parquet.iter_batches(batch_size=COMPILE_BATCH_SIZE)
    ):
        names = batch.schema.names
        native = batch.column(names.index("native_id")).to_numpy(
            zero_copy_only=False
        ).astype("int64", copy=False)
        positions = np.searchsorted(topology.native_ids, native)
        selected = (positions < len(topology.native_ids)) & (
            topology.native_ids[
                np.minimum(positions, len(topology.native_ids) - 1)
            ]
            == native
        )
        selected_rows = np.flatnonzero(selected)
        if not len(selected_rows):
            continue
        unit_rows = positions[selected]
        hilbert = batch.column(names.index("hilbert")).to_numpy(
            zero_copy_only=False
        ).astype("uint32", copy=False)[selected]
        unit_ids = topology.global_ids[unit_rows]
        order = np.lexsort((unit_ids, hilbert))
        exact_bounds = np.column_stack(
            [
                batch.column(names.index(name)).to_numpy(zero_copy_only=False)[selected]
                for name in ("exact_minx", "exact_miny", "exact_maxx", "exact_maxy")
            ]
        )[order]
        bounds = geographic_bbox_float32_coverings(exact_bounds)
        x_degenerate = exact_bounds[:, 0] == exact_bounds[:, 2]
        y_degenerate = exact_bounds[:, 1] == exact_bounds[:, 3]
        bounds[x_degenerate, 0] -= np.float32(SNAP_BBOX_EPSILON)
        bounds[x_degenerate, 2] += np.float32(SNAP_BBOX_EPSILON)
        bounds[y_degenerate, 1] -= np.float32(SNAP_BBOX_EPSILON)
        bounds[y_degenerate, 3] += np.float32(SNAP_BBOX_EPSILON)
        count = len(order)
        table = pa.Table.from_arrays(
            [
                pa.array(np.zeros(count, dtype="int64"), type=pa.int64()),
                pa.array(unit_ids[order], type=pa.int64()),
                pa.array(
                    topology.up_area_km2[unit_rows][order].astype("float32"),
                    type=pa.float32(),
                ),
                pa.nulls(count, type=pa.string()),
                build_bbox_struct(
                    bounds[:, 0], bounds[:, 1], bounds[:, 2], bounds[:, 3]
                ),
                pa.array(
                    [
                        batch.column(names.index("geometry"))[
                            int(selected_rows[int(index)])
                        ].as_py()
                        for index in order
                    ],
                    type=pa.binary(),
                ),
                pa.array(hilbert[order], type=pa.uint32()),
            ],
            schema=schema,
        )
        path = scratch_root / f"snap.run-{run_number:06d}.parquet"
        with pq.ParquetWriter(
            path, schema=schema, compression="snappy", write_statistics=True
        ) as writer:
            writer.write_table(table)
        paths.append(path)
        if recorder is not None:
            recorder.scratch_event(f"snap-run-closed:{path.name}")
    return paths


def _merge_write_catchments_and_graph(
    catchments_path: Path,
    graph_path: Path,
    run_paths: Sequence[Path],
    topology: _CompactTopology,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> np.ndarray:
    """Merge catchment runs and write catchments and graph in lockstep."""
    catchment_schema = _merge_catchment_schema()
    graph_schema = _compile_graph_schema()
    targets = [
        stop - start
        for start, stop in balanced_row_group_bounds(len(topology.native_ids))
    ]
    bounds_union = np.asarray(
        [np.inf, np.inf, -np.inf, -np.inf], dtype="float32"
    )
    rows: list[dict[str, object]] = []
    target_index = 0
    catchments_path.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(
        catchments_path,
        schema=catchment_schema,
        compression="snappy",
        write_statistics=True,
    ) as catchment_writer, pq.ParquetWriter(
        graph_path,
        schema=graph_schema,
        compression="snappy",
        write_statistics=True,
    ) as graph_writer:
        for row in _merged_run_rows(
            run_paths, ("hilbert", "id"), recorder=recorder
        ):
            if target_index == len(targets):
                raise ValueError(
                    "catchment merge row count does not match compact topology"
                )
            rows.append(row)
            if len(rows) != targets[target_index]:
                continue
            catchment_rows = [
                {name: value for name, value in item.items() if name != "hilbert"}
                for item in rows
            ]
            catchment_writer.write_table(
                _table_with_schema_rows(catchment_rows, catchment_schema)
            )
            ids = np.asarray([int(item["id"]) for item in rows], dtype="int64")
            unit_rows = np.searchsorted(topology.global_ids, ids)
            bbox = np.asarray(
                [
                    [
                        item["bbox"]["xmin"],
                        item["bbox"]["ymin"],
                        item["bbox"]["xmax"],
                        item["bbox"]["ymax"],
                    ]
                    for item in rows
                ],
                dtype="float32",
            )
            upstream = [
                topology.upstream_global_ids[
                    topology.upstream_offsets[unit_row] :
                    topology.upstream_offsets[unit_row + 1]
                ].tolist()
                for unit_row in unit_rows
            ]
            graph_writer.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(ids, type=pa.int64()),
                        pa.array(np.zeros(len(rows), dtype="int16"), type=pa.int16()),
                        pa.array(
                            upstream,
                            type=pa.list_(
                                pa.field("item", pa.int64(), nullable=True)
                            ),
                        ),
                        pa.array(bbox[:, 0], type=pa.float32()),
                        pa.array(bbox[:, 1], type=pa.float32()),
                        pa.array(bbox[:, 2], type=pa.float32()),
                        pa.array(bbox[:, 3], type=pa.float32()),
                    ],
                    schema=graph_schema,
                )
            )
            bounds_union[0] = min(bounds_union[0], bbox[:, 0].min())
            bounds_union[1] = min(bounds_union[1], bbox[:, 1].min())
            bounds_union[2] = max(bounds_union[2], bbox[:, 2].max())
            bounds_union[3] = max(bounds_union[3], bbox[:, 3].max())
            rows.clear()
            target_index += 1
    if rows or target_index != len(targets):
        raise ValueError("catchment merge row count does not match compact topology")
    assert_geoparquet_valid(catchments_path)
    return bounds_union


def _merge_write_snap_stems(
    path: Path,
    run_paths: Sequence[Path],
    total_rows: int,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> None:
    """Merge snap runs and assign sequential snap IDs in file order."""
    schema = _snap_merge_schema()
    targets = [
        stop - start for start, stop in balanced_row_group_bounds(total_rows)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    target_index = 0
    next_id = 1
    with pq.ParquetWriter(
        path, schema=schema, compression="snappy", write_statistics=True
    ) as writer:
        for row in _merged_run_rows(
            run_paths, ("hilbert", "unit_id"), recorder=recorder
        ):
            row["id"] = next_id
            next_id += 1
            rows.append({name: value for name, value in row.items() if name != "hilbert"})
            if len(rows) == targets[target_index]:
                writer.write_table(_table_with_schema_rows(rows, schema))
                rows.clear()
                target_index += 1
    if rows or next_id - 1 != total_rows or target_index != len(targets):
        raise ValueError("snap merge row count does not match compact topology")
    assert_geoparquet_valid(path)


def _manifest_for_compiled_spools(
    processing_basin_id: str,
    fabric_version: str,
    created_at: datetime,
    bounds: np.ndarray,
    unit_count: int,
) -> dict[str, object]:
    return {
        "format_version": FORMAT_VERSION,
        "fabric_name": FABRIC_NAME,
        "fabric_version": fabric_version,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "region": processing_basin_id,
        "bbox": [float(value) for value in bounds],
        "unit_count": unit_count,
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


def _compile_spooled_hfx(
    spools: _NormalizedSpools,
    topology: _CompactTopology,
    out_dir: Path,
    scratch_root: Path,
    *,
    processing_basin_id: str,
    fabric_version: str,
    created_at: datetime,
    merge_fan_in: int = COMPILE_MERGE_FAN_IN,
    _memory_recorder: _CompileMemoryRecorder | None = None,
) -> CoreBuildResult:
    """Compile normalized spools with bounded sorted runs and k-way merges."""
    _validate_compile_tuning(COMPILE_BATCH_SIZE, merge_fan_in)
    if not processing_basin_id or not processing_basin_id.isdigit():
        raise ValueError("processing_basin_id must be a non-empty digit string")
    if not fabric_version or not fabric_version.strip():
        raise ValueError("fabric_version must be a non-empty, non-whitespace string")
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("created_at must be timezone-aware")
    out_dir.mkdir(parents=True, exist_ok=True)
    catchments_path = out_dir / "catchments.parquet"
    graph_path = out_dir / "graph.parquet"
    snap_path = out_dir / "aux" / "snap_stems.parquet"
    manifest_path = out_dir / "manifest.json"

    with _record_phase(_memory_recorder, "catchment_run_creation"):
        catchment_runs = _create_catchment_runs(
            spools.basin_path,
            scratch_root,
            topology,
            recorder=_memory_recorder,
        )
    with _record_phase(_memory_recorder, "catchment_graph_merge_write"):
        spools.basin_path.unlink()
        if _memory_recorder is not None:
            _memory_recorder.scratch_event("basin-normalized-unlinked")
        catchment_runs = _reduce_run_fan_in(
            catchment_runs,
            scratch_root,
            "catchment",
            _catchment_run_schema(),
            ("hilbert", "id"),
            merge_fan_in,
            recorder=_memory_recorder,
        )
        bounds = _merge_write_catchments_and_graph(
            catchments_path,
            graph_path,
            catchment_runs,
            topology,
            recorder=_memory_recorder,
        )
    with _record_phase(_memory_recorder, "snap_run_creation"):
        snap_runs = _create_snap_runs(
            spools.stream_path,
            scratch_root,
            topology,
            recorder=_memory_recorder,
        )
    with _record_phase(_memory_recorder, "snap_merge_write"):
        spools.stream_path.unlink()
        if _memory_recorder is not None:
            _memory_recorder.scratch_event("stream-normalized-unlinked")
        snap_runs = _reduce_run_fan_in(
            snap_runs,
            scratch_root,
            "snap",
            _snap_run_schema(),
            ("hilbert", "unit_id"),
            merge_fan_in,
            recorder=_memory_recorder,
        )
        _merge_write_snap_stems(
            snap_path,
            snap_runs,
            len(topology.native_ids),
            recorder=_memory_recorder,
        )
    manifest = _manifest_for_compiled_spools(
        processing_basin_id,
        fabric_version,
        created_at,
        bounds,
        len(topology.native_ids),
    )
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CoreBuildResult(
        catchments_path=catchments_path,
        graph_path=graph_path,
        snap_path=snap_path,
        manifest_path=manifest_path,
        diagnostics=CoreBuildDiagnostics(
            ingestion=spools.diagnostics,
            streamnet=topology.diagnostics,
        ),
    )


def _compact_topology_from_model(
    source: TdxSourceData, model: StreamnetModel
) -> _CompactTopology:
    _validate_core_model(source, model)
    units = sorted(model.units, key=lambda unit: unit.linkno)
    native_ids = np.asarray([unit.linkno for unit in units], dtype="int64")
    global_ids = np.asarray([unit.id for unit in units], dtype="int64")
    downstream_native_ids = np.asarray(
        [unit.downstream_linkno for unit in units], dtype="int64"
    )
    downstream_global_ids = np.asarray(
        [unit.downstream_id for unit in units], dtype="int64"
    )
    contracted = np.asarray(
        [unit.contracted_link_count for unit in units], dtype="int64"
    )
    outlet_lons = np.asarray([unit.outlet_lon for unit in units], dtype="float64")
    outlet_lats = np.asarray([unit.outlet_lat for unit in units], dtype="float64")
    stream_native = source.streamnet["LINKNO"].to_numpy(dtype="int64", copy=False)
    stream_order = np.argsort(stream_native, kind="stable")
    sorted_stream_native = stream_native[stream_order]
    positions = np.searchsorted(sorted_stream_native, native_ids)
    up_area = source.streamnet["DSContArea_km2"].to_numpy(
        dtype="float64", copy=False
    )[stream_order[positions]]
    edges = np.asarray(model.edges, dtype="int64")
    if len(edges):
        downstream_rows = np.searchsorted(global_ids, edges[:, 1])
        edge_order = np.lexsort((edges[:, 0], edges[:, 1]))
        upstream_ids = edges[:, 0][edge_order]
        counts = np.bincount(
            downstream_rows[edge_order], minlength=len(global_ids)
        )
    else:
        upstream_ids = np.empty(0, dtype="int64")
        counts = np.zeros(len(global_ids), dtype="int64")
    offsets = np.empty(len(global_ids) + 1, dtype="int64")
    offsets[0] = 0
    np.cumsum(counts, out=offsets[1:])
    return _CompactTopology(
        native_ids=native_ids,
        global_ids=global_ids,
        downstream_native_ids=downstream_native_ids,
        downstream_global_ids=downstream_global_ids,
        contracted_counts=contracted,
        outlet_lons=outlet_lons,
        outlet_lats=outlet_lats,
        up_area_km2=up_area,
        upstream_offsets=offsets,
        upstream_global_ids=upstream_ids,
        diagnostics=model.diagnostics,
    )


def _write_materialized_normalized_spools(
    source: TdxSourceData,
    scratch_root: Path,
    *,
    recorder: _CompileMemoryRecorder | None,
) -> _NormalizedSpools:
    basin_path = scratch_root / "basins.normalized.parquet"
    stream_path = scratch_root / "streamnet.normalized.parquet"
    geod = Geod(ellps="WGS84")
    with pq.ParquetWriter(
        basin_path,
        schema=_basin_spool_schema(),
        compression="snappy",
        write_statistics=True,
    ) as writer:
        for start in range(0, len(source.basins), COMPILE_BATCH_SIZE):
            frame = source.basins.iloc[start : start + COMPILE_BATCH_SIZE]
            series = frame.geometry
            own_area = np.asarray(
                [
                    abs(float(geod.geometry_area_perimeter(geometry)[0]))
                    for geometry in series
                ],
                dtype="float64",
            )
            bounds = series.bounds.to_numpy(dtype="float64")
            writer.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(frame["streamID"], type=pa.int64()),
                        pa.array((own_area / 1_000_000).astype("float32"), type=pa.float32()),
                        pa.array(bounds[:, 0], type=pa.float64()),
                        pa.array(bounds[:, 1], type=pa.float64()),
                        pa.array(bounds[:, 2], type=pa.float64()),
                        pa.array(bounds[:, 3], type=pa.float64()),
                        pa.array(_hilbert_for_series(series), type=pa.uint32()),
                        pa.array([geometry.wkb for geometry in series], type=pa.binary()),
                    ],
                    schema=_basin_spool_schema(),
                )
            )
    if recorder is not None:
        recorder.scratch_event("facade-basin-normalized-closed")
    with pq.ParquetWriter(
        stream_path,
        schema=_stream_spool_schema(),
        compression="snappy",
        write_statistics=True,
    ) as writer:
        for start in range(0, len(source.streamnet), COMPILE_BATCH_SIZE):
            frame = source.streamnet.iloc[start : start + COMPILE_BATCH_SIZE]
            series = frame.geometry
            endpoints = np.asarray(
                [
                    (
                        float(geometry.coords[0][0]),
                        float(geometry.coords[0][1]),
                        float(geometry.coords[-1][0]),
                        float(geometry.coords[-1][1]),
                    )
                    for geometry in series
                ],
                dtype="float64",
            )
            bounds = series.bounds.to_numpy(dtype="float64")
            raw = (
                frame["DSContArea"].to_numpy(dtype="float64", copy=False)
                if "DSContArea" in frame
                else frame["DSContArea_km2"].to_numpy(dtype="float64", copy=False)
            )
            writer.write_table(
                pa.Table.from_arrays(
                    [
                        pa.array(frame["LINKNO"], type=pa.int64()),
                        pa.array(frame["DSLINKNO"], type=pa.int64()),
                        pa.array(raw, type=pa.float64()),
                        pa.array(endpoints[:, 0], type=pa.float64()),
                        pa.array(endpoints[:, 1], type=pa.float64()),
                        pa.array(endpoints[:, 2], type=pa.float64()),
                        pa.array(endpoints[:, 3], type=pa.float64()),
                        pa.array(
                            [_is_tdx_degenerate_reach(geometry) for geometry in series],
                            type=pa.bool_(),
                        ),
                        pa.array(bounds[:, 0], type=pa.float64()),
                        pa.array(bounds[:, 1], type=pa.float64()),
                        pa.array(bounds[:, 2], type=pa.float64()),
                        pa.array(bounds[:, 3], type=pa.float64()),
                        pa.array(_hilbert_for_series(series), type=pa.uint32()),
                        pa.array([geometry.wkb for geometry in series], type=pa.binary()),
                    ],
                    schema=_stream_spool_schema(),
                )
            )
    if recorder is not None:
        recorder.scratch_event("facade-stream-normalized-closed")
    return _NormalizedSpools(
        basin_path=basin_path,
        stream_path=stream_path,
        basin_crs=CRS,
        stream_crs=CRS,
        diagnostics=source.diagnostics,
    )


def compile_core_hfx(
    source: TdxSourceData,
    streamnet_model: StreamnetModel,
    out_dir: Path,
    *,
    processing_basin_id: str,
    fabric_version: str,
    created_at: datetime,
    _memory_recorder: _CompileMemoryRecorder | None = None,
    _merge_fan_in: int = COMPILE_MERGE_FAN_IN,
) -> CoreBuildResult:
    """Compile through the same private compiler used by the build command."""
    _validate_compile_tuning(COMPILE_BATCH_SIZE, _merge_fan_in)
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{out_dir.name}.compile-scratch-",
        dir=out_dir.parent,
    ) as temporary:
        scratch_root = Path(temporary)
        spools = _write_materialized_normalized_spools(
            source, scratch_root, recorder=_memory_recorder
        )
        topology = _compact_topology_from_model(source, streamnet_model)
        return _compile_spooled_hfx(
            spools,
            topology,
            out_dir,
            scratch_root,
            processing_basin_id=processing_basin_id,
            fabric_version=fabric_version,
            created_at=created_at,
            merge_fan_in=_merge_fan_in,
            _memory_recorder=_memory_recorder,
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
    recorder: _CompileMemoryRecorder | None = None
    recorder_stopped = False
    try:
        staging_root = Path(
            tempfile.mkdtemp(
                prefix=f".{output_root.name}.tmp-", dir=output_root.parent
            )
        )
        staging_dataset_root = staging_root / "dataset"
        recorder = _CompileMemoryRecorder(staging_root)
        recorder.start()
        source = _ingest_source_spools(
            basins_path,
            streamnet_path,
            staging_root,
            recorder,
        )
        with recorder.phase("topology"):
            header_number = load_header_crosswalk()[processing_basin_id]
            topology = _build_compact_topology(
                source.basin_native_ids,
                source.stream_native_ids,
                source.downstream_native_ids,
                source.endpoints,
                source.degenerate,
                source.up_area_km2,
                header_number,
                endpoint_tolerance,
            )
            LOGGER.info(
                "compact_topology rows=%d bytes=%d representation=numpy-fixed-width",
                len(topology.native_ids),
                topology.nbytes,
            )
        staging_result = _compile_spooled_hfx(
            source.spools,
            topology,
            staging_dataset_root,
            staging_root,
            processing_basin_id=processing_basin_id,
            fabric_version=fabric_version,
            created_at=created_at,
            merge_fan_in=COMPILE_MERGE_FAN_IN,
            _memory_recorder=recorder,
        )
        memory = recorder.stop()
        recorder_stopped = True
        result = CoreBuildResult(
            catchments_path=output_root / "catchments.parquet",
            graph_path=output_root / "graph.parquet",
            snap_path=output_root / "aux" / "snap_stems.parquet",
            manifest_path=output_root / "manifest.json",
            diagnostics=staging_result.diagnostics,
        )
        result = CoreBuildResult(
            catchments_path=result.catchments_path,
            graph_path=result.graph_path,
            snap_path=result.snap_path,
            manifest_path=result.manifest_path,
            diagnostics=CoreBuildDiagnostics(
                ingestion=result.diagnostics.ingestion,
                streamnet=result.diagnostics.streamnet,
                memory=memory,
            ),
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
        if recorder is not None and not recorder_stopped:
            recorder.stop()
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


def _resolved_real_directory(path: Path, basin_id: str, label: str) -> Path:
    expanded = path.expanduser()
    if expanded.is_symlink() or not expanded.is_dir():
        raise ValueError(f"{basin_id} {label} directory is missing or unsafe: {expanded}")
    try:
        return expanded.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError(f"{basin_id} {label} directory is missing or unsafe: {expanded}") from error


def _fixed_directory(parent: Path, name: str, basin_id: str, label: str) -> Path:
    path = parent / name
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"{basin_id} {label} directory is missing or unsafe: {path}")
    return path


def _acquired_paths(root: Path, basin_id: str) -> tuple[Path, Path, Path]:
    root = _resolved_real_directory(root, basin_id, "acquired evidence root")
    salvage = _fixed_directory(root, "salvage", basin_id, "acquired salvage")
    downloads = _fixed_directory(salvage, "downloads", basin_id, "acquired downloads")
    state = _fixed_directory(salvage, "state", basin_id, "acquired state")
    states = _fixed_directory(state, "basins", basin_id, "acquired state basins")
    basin_state = _fixed_directory(states, basin_id, basin_id, "acquired processing basin state")
    return (
        downloads / f"{basin_id}-basins.gpkg",
        downloads / f"{basin_id}-streamnet.gpkg",
        basin_state / "current.json",
    )


def _historical_state_path(root: Path, basin_id: str) -> Path:
    root = _resolved_real_directory(root, basin_id, "historical evidence root")
    mirror = _fixed_directory(root, "mirror", basin_id, "historical mirror")
    state = _fixed_directory(mirror, "state", basin_id, "historical state")
    states = _fixed_directory(state, "basins", basin_id, "historical state basins")
    return _fixed_directory(states, basin_id, basin_id, "historical processing basin") / "current.json"


def _read_json_object(path: Path, basin_id: str, label: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{basin_id} {label} is missing: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{basin_id} {label} is malformed: {path}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{basin_id} {label} is malformed: {path}")
    return value


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _valid_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and value == value.lower() and all(c in "0123456789abcdef" for c in value)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _parse_acquired_product(root: Path, basin_id: str, product: str) -> AcquiredProduct:
    basins_path, streamnet_path, state_path = _acquired_paths(root, basin_id)
    path = basins_path if product == "basins" else streamnet_path
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{basin_id} acquired source data is missing for {product}: {path}")
    state = _read_json_object(state_path, basin_id, "acquired state")
    if state.get("processing_basin_id") != basin_id:
        raise ValueError(f"{basin_id} acquired state basin identity mismatch: {state_path}")
    acquisition, stages = state.get("acquisition"), state.get("stages")
    if state.get("schema_version") != 5 or not isinstance(acquisition, dict) or acquisition.get("product_attempt_ceiling") != 3 or not isinstance(stages, dict):
        raise ValueError(f"{basin_id} {product} acquired state mismatch: {state_path}")
    stage = stages.get(f"acquire_{product}")
    if not isinstance(stage, dict):
        raise ValueError(f"{basin_id} {product} acquired state mismatch: {state_path}")
    attempts, evidence = stage.get("attempts"), stage.get("evidence")
    if stage.get("status") != "succeeded" or not _is_int(attempts) or not 1 <= attempts <= 3 or stage.get("failure_reason") is not None or not isinstance(evidence, dict) or set(evidence) != {"bytes", "layer_name", "sha256", "sqlite_identity"}:
        raise ValueError(f"{basin_id} {product} acquired state mismatch: {state_path}")
    try:
        layers = [str(name) for name in pyogrio.list_layers(path)[:, 0].tolist()]
        with path.open("rb") as handle:
            header = handle.read(16).hex()
        byte_count, sha256 = path.stat().st_size, _file_sha256(path)
    except Exception as error:
        raise ValueError(f"{basin_id} {product} acquired source identity mismatch: {path}") from error
    if not _is_int(evidence.get("bytes")) or evidence["bytes"] != byte_count or not _valid_sha256(evidence.get("sha256")) or evidence["sha256"] != sha256 or evidence.get("sqlite_identity") != SQLITE3_IDENTITY_HEX or header != SQLITE3_IDENTITY_HEX or len(layers) != 1 or evidence.get("layer_name") != layers[0]:
        raise ValueError(f"{basin_id} {product} acquired source identity mismatch: {path}")
    return AcquiredProduct(basin_id, product, path, layers[0], byte_count, sha256, attempts)


def _read_adjudication_features(product: AcquiredProduct, columns: Sequence[str], where: str) -> gpd.GeoDataFrame:
    try:
        frame = pyogrio.read_dataframe(product.path, layer=product.layer_name, columns=list(columns), where=where)
    except Exception as error:
        raise ValueError(f"{product.processing_basin_id} {product.product} acquired source is malformed: {product.path}") from error
    if not isinstance(frame, gpd.GeoDataFrame) or frame.crs is None:
        raise ValueError(f"{product.processing_basin_id} {product.product} acquired source is malformed: {product.path}")
    if frame.crs.to_epsg() != 4326:
        try:
            frame = frame.to_crs(CRS)
        except Exception as error:
            raise ValueError(f"{product.processing_basin_id} {product.product} CRS does not resolve to {CRS}: {product.path}") from error
    if frame.crs is None or frame.crs.to_epsg() != 4326:
        raise ValueError(f"{product.processing_basin_id} {product.product} CRS does not resolve to {CRS}: {product.path}")
    return frame


def _polygon_coordinates(geometry: object, label: str) -> list[object]:
    if not isinstance(geometry, (Polygon, MultiPolygon)) or geometry.is_empty or geometry.has_z:
        raise ValueError(f"{label} must be a non-empty two-dimensional Polygon or MultiPolygon")
    if isinstance(geometry, Polygon):
        return [[[float(x), float(y)] for x, y in ring.coords] for ring in [geometry.exterior, *geometry.interiors]]
    return [_polygon_coordinates(polygon, label) for polygon in geometry.geoms]


def _coordinates_are_finite(geometry: Polygon | MultiPolygon) -> bool:
    return bool(np.isfinite(get_coordinates(geometry)).all())


def _geometry_is_valid(geometry: Polygon | MultiPolygon) -> bool:
    return bool(geometry.is_valid)


def _line_endpoints(geometry: object, label: str) -> list[list[float]]:
    if geometry is None or geometry.is_empty or geometry.has_z or not isinstance(geometry, LineString) or len(geometry.coords) < 2:
        raise ValueError(f"{label} must be a healthy two-dimensional LineString")
    return [[float(value) for value in geometry.coords[index]] for index in (0, -1)]


def _positive_float(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{label} must be a finite positive float")
    checked = float(value)
    if not math.isfinite(checked) or checked <= 0.0:
        raise ValueError(f"{label} must be a finite positive float")
    return checked


def _limit_classification(value: float, limit: float) -> str:
    return "inside" if value < limit else "on" if value == limit else "outside"


def _distance_records(first: list[list[float]], second: list[list[float]], first_name: str, second_name: str) -> list[dict[str, object]]:
    return [{f"{first_name}_endpoint_index": i, f"{second_name}_endpoint_index": j, "distance_degrees": math.dist(a, b)} for i, a in enumerate(first) for j, b in enumerate(second)]


def _derive_duplicate_verdict(
    spatially_equal: bool,
    coordinate_sequences_equal: bool,
) -> tuple[BasinVerdict, str]:
    if type(spatially_equal) is not bool or type(coordinate_sequences_equal) is not bool:
        raise ValueError("duplicate geometry measurements must be booleans")
    if not spatially_equal and coordinate_sequences_equal:
        raise ValueError(
            "duplicate geometry measurements are inconsistent: coordinate sequence equality requires spatial equality"
        )
    if (spatially_equal, coordinate_sequences_equal) == (True, True):
        return BasinVerdict.ADAPTER_STRICTNESS, "same_ground"
    if (spatially_equal, coordinate_sequences_equal) == (True, False):
        return BasinVerdict.ADAPTER_STRICTNESS, "same_ground"
    if (spatially_equal, coordinate_sequences_equal) == (False, False):
        return BasinVerdict.SOURCE_DEFECT, "different_ground"
    raise ValueError("duplicate geometry measurements do not determine a verdict")


def _adjudicate_duplicate(root: Path) -> AdjudicationVerdict:
    basin_id = "1020018110"
    product = _parse_acquired_product(root, basin_id, "basins")
    rows = _read_adjudication_features(product, ["streamID"], f"streamID = {DUPLICATE_STREAM_ID}")
    if len(rows) != 2:
        raise ValueError(f"{basin_id} required streamID {DUPLICATE_STREAM_ID} feature identity mismatch: expected 2, found {len(rows)}")
    geometries = rows.geometry.tolist()
    coordinates = [_polygon_coordinates(value, f"{basin_id} streamID {DUPLICATE_STREAM_ID} geometry") for value in geometries]
    coordinate_sequences_finite = [_coordinates_are_finite(value) for value in geometries]
    if not all(coordinate_sequences_finite):
        raise ValueError(f"{basin_id} streamID {DUPLICATE_STREAM_ID} geometry must have finite coordinates")
    geometries_valid = [_geometry_is_valid(value) for value in geometries]
    if not all(geometries_valid):
        raise ValueError(f"{basin_id} streamID {DUPLICATE_STREAM_ID} geometry must be valid")
    spatially_equal = bool(geometries[0].equals(geometries[1]))
    coordinate_sequences_equal = coordinates[0] == coordinates[1]
    verdict, selected_branch = _derive_duplicate_verdict(
        spatially_equal,
        coordinate_sequences_equal,
    )
    return AdjudicationVerdict(
        basin_id,
        verdict,
        AdjudicationEvidenceKind.ACQUIRED_SOURCE_GEOMETRY,
        {
            "streamID": DUPLICATE_STREAM_ID,
            "features": [{"coordinates": value} for value in coordinates],
            "coordinate_sequences_finite": coordinate_sequences_finite,
            "geometries_valid": geometries_valid,
            "spatially_equal": spatially_equal,
            "coordinate_sequences_equal": coordinate_sequences_equal,
            "source": {"bytes": product.byte_count, "layer_name": product.layer_name, "sha256": product.sha256},
            "derivation": {
                "rule_id": "duplicate-ground-equality-v1",
                "inputs": [
                    "coordinate_sequences_finite",
                    "geometries_valid",
                    "spatially_equal",
                    "coordinate_sequences_equal",
                ],
                "required_preconditions": {
                    "coordinate_sequences_finite": coordinate_sequences_finite,
                    "geometries_valid": geometries_valid,
                },
                "consistency_requirement": {
                    "coordinate_sequences_equal_implies": "spatially_equal",
                },
                "branches": [
                    {
                        "branch": "same_ground",
                        "spatially_equal": True,
                        "verdict": "adapter strictness",
                    },
                    {
                        "branch": "different_ground",
                        "spatially_equal": False,
                        "coordinate_sequences_equal": False,
                        "verdict": "source defect",
                    },
                ],
                "selected_branch": selected_branch,
            },
        },
    )


def _adjudicate_non_root(root: Path) -> AdjudicationVerdict:
    basin_id = "2020003440"
    product = _parse_acquired_product(root, basin_id, "streamnet")
    rows = _read_adjudication_features(product, ["LINKNO", "DSLINKNO", "DSContArea"], f"LINKNO = {AMBIGUOUS_NON_ROOT_LINKNO}")
    if len(rows) != 1:
        raise ValueError(f"{basin_id} required LINKNO {AMBIGUOUS_NON_ROOT_LINKNO} feature identity mismatch: expected 1, found {len(rows)}")
    current = rows.iloc[0]
    downstream = int(current["DSLINKNO"])
    successors = _read_adjudication_features(product, ["LINKNO", "DSLINKNO", "DSContArea"], f"LINKNO = {downstream}")
    if len(successors) != 1:
        raise ValueError(f"{basin_id} downstream LINKNO {downstream} feature identity mismatch: expected 1, found {len(successors)}")
    successor = successors.iloc[0]
    endpoints = _line_endpoints(current.geometry, f"{basin_id} current reach")
    successor_endpoints = _line_endpoints(successor.geometry, f"{basin_id} successor reach")
    separation = math.dist(*endpoints)
    distances = _distance_records(endpoints, successor_endpoints, "current", "successor")
    matches = [value for value in distances if value["distance_degrees"] <= DEFAULT_ENDPOINT_TOLERANCE]
    if {value["current_endpoint_index"] for value in matches} != {0, 1}:
        raise ValueError(f"{basin_id} required LINKNO {AMBIGUOUS_NON_ROOT_LINKNO} reach-side ambiguity identity mismatch: tolerance matches do not use both current endpoint indexes")
    if separation <= 0.002:
        raise ValueError(f"{basin_id} required LINKNO {AMBIGUOUS_NON_ROOT_LINKNO} reach-side ambiguity identity mismatch: separation is inside or on old limit 0.002")
    current_area = _positive_float(current["DSContArea"], f"{basin_id} current DSContArea")
    successor_area = _positive_float(successor["DSContArea"], f"{basin_id} successor DSContArea")
    verdict = BasinVerdict.ADAPTER_STRICTNESS if separation <= 0.003 and current_area < successor_area and any(value["current_endpoint_index"] == 1 for value in matches) else BasinVerdict.SOURCE_DEFECT
    return AdjudicationVerdict(
        basin_id, verdict, AdjudicationEvidenceKind.ACQUIRED_SOURCE_GEOMETRY,
        {
            "LINKNO": AMBIGUOUS_NON_ROOT_LINKNO, "DSLINKNO": downstream,
            "endpoints": endpoints, "successor_endpoints": successor_endpoints,
            "endpoint_distances": distances, "tolerance_matches": matches,
            "endpoint_separation": separation, "endpoint_tolerance": 0.001,
            "old_near_degenerate_limit": 0.002, "non_root_near_degenerate_limit": 0.003,
            "old_limit_classification": _limit_classification(separation, 0.002),
            "non_root_limit_classification": _limit_classification(separation, 0.003),
            "DSContArea": current_area, "successor_DSContArea": successor_area,
        },
    )


def _adjudicate_root(root_path: Path) -> AdjudicationVerdict:
    basin_id = "2020071190"
    product = _parse_acquired_product(root_path, basin_id, "streamnet")
    roots = _read_adjudication_features(product, ["LINKNO", "DSLINKNO", "DSContArea"], f"LINKNO = {CONFLICTING_ROOT_LINKNO}")
    if len(roots) != 1:
        raise ValueError(f"{basin_id} required root LINKNO {CONFLICTING_ROOT_LINKNO} feature identity mismatch: expected 1, found {len(roots)}")
    root = roots.iloc[0]
    if int(root["DSLINKNO"]) != -1:
        raise ValueError(f"{basin_id} required root LINKNO {CONFLICTING_ROOT_LINKNO} feature identity mismatch: DSLINKNO is not -1")
    predecessors = _read_adjudication_features(product, ["LINKNO", "DSLINKNO", "DSContArea"], f"DSLINKNO = {CONFLICTING_ROOT_LINKNO}")
    if len(predecessors) != 2 or predecessors["LINKNO"].nunique() != 2:
        raise ValueError(f"{basin_id} required root LINKNO {CONFLICTING_ROOT_LINKNO} conflict identity mismatch: expected exactly two unique predecessors")
    predecessors = predecessors.sort_values("LINKNO", kind="stable")
    root_endpoints = _line_endpoints(root.geometry, f"{basin_id} root")
    root_separation = math.dist(*root_endpoints)
    items: list[dict[str, object]] = []
    areas: list[float] = []
    definite = spanning = 0
    for _, predecessor in predecessors.iterrows():
        linkno = int(predecessor["LINKNO"])
        endpoints = _line_endpoints(predecessor.geometry, f"{basin_id} predecessor LINKNO {linkno}")
        separation = math.dist(*endpoints)
        distances = _distance_records(endpoints, root_endpoints, "predecessor", "root")
        candidates = [value for value in distances if value["distance_degrees"] <= 0.001]
        root_indexes = {value["root_endpoint_index"] for value in candidates}
        classification = "indeterminate"
        if len(root_indexes) == 1:
            definite += 1
            classification = "definite"
        elif root_indexes == {0, 1}:
            spanning += 1
            classification = "spanning"
        area = _positive_float(predecessor["DSContArea"], f"{basin_id} predecessor LINKNO {linkno} DSContArea")
        areas.append(area)
        item: dict[str, object] = {
            "LINKNO": linkno, "DSContArea": area, "endpoints": endpoints,
            "endpoint_separation": separation, "endpoint_distances_to_root": distances,
            "tolerance_candidates": candidates, "candidate_classification": classification,
        }
        if classification == "definite":
            item["definite_root_endpoint_index"] = next(iter(root_indexes))
        if classification == "spanning":
            item["endpoint_tolerance_classification"] = _limit_classification(separation, 0.001)
            item["non_root_limit_classification"] = _limit_classification(separation, 0.003)
        items.append(item)
    if definite == 0:
        raise ValueError(f"{basin_id} required root LINKNO {CONFLICTING_ROOT_LINKNO} conflict identity mismatch: expected at least one definite predecessor")
    if spanning == 0:
        raise ValueError(f"{basin_id} required root LINKNO {CONFLICTING_ROOT_LINKNO} conflict identity mismatch: expected at least one predecessor whose tolerance candidates span both root endpoints")
    root_area = _positive_float(root["DSContArea"], f"{basin_id} root DSContArea")
    verdict = BasinVerdict.ADAPTER_STRICTNESS if root_separation <= 0.002 and all(value < root_area for value in areas) else BasinVerdict.SOURCE_DEFECT
    return AdjudicationVerdict(
        basin_id, verdict, AdjudicationEvidenceKind.ACQUIRED_SOURCE_GEOMETRY,
        {
            "LINKNO": CONFLICTING_ROOT_LINKNO, "endpoints": root_endpoints,
            "endpoint_separation": root_separation, "endpoint_tolerance": 0.001,
            "root_near_degenerate_limit": 0.002,
            "root_limit_classification": _limit_classification(root_separation, 0.002),
            "DSContArea": root_area, "predecessors": items,
            "governing_pinned_adapter_refusal": "cannot determine the upstream endpoint of root successor LINKNO 1104039",
        },
    )


def _parse_historical_exhaustion(root: Path, basin_id: str) -> HistoricalExhaustion:
    path = _historical_state_path(root, basin_id)
    state = _read_json_object(path, basin_id, "historical campaign record")
    if state.get("processing_basin_id") != basin_id:
        raise ValueError(f"{basin_id} historical campaign basin identity mismatch: {path}")
    stages = state.get("stages")
    if set(state) != {"processing_basin_id", "retention", "schema_version", "stages"} or state.get("schema_version") != 4 or state.get("retention") != {"inputs_reclaimed": False, "policy": "reclaim-inputs-after-terminal"} or not isinstance(stages, dict) or set(stages) != {"acquire_basins", "acquire_streamnet", "compile"} or stages.get("compile") != {"attempts": 0, "diagnostic_report": None, "failure_reason": None, "status": "pending"}:
        raise ValueError(f"{basin_id} historical campaign record shape mismatch: {path}")
    failed: list[str] = []
    for product in ("basins", "streamnet"):
        stage = stages.get(f"acquire_{product}")
        if not isinstance(stage, dict) or set(stage) != {"attempts", "evidence", "failure_reason", "status"}:
            raise ValueError(f"{basin_id} historical acquisition exhaustion mismatch for {product}: {path}")
        if stage.get("status") == "failed":
            if stage.get("attempts") != 2 or stage.get("evidence") is not None or stage.get("failure_reason") != HISTORICAL_TRANSFER_FAILURE_REASON:
                raise ValueError(f"{basin_id} historical acquisition exhaustion mismatch for {product}: {path}")
            failed.append(product)
            continue
        evidence = stage.get("evidence")
        layer = "basins" if product == "basins" else f"TDX_streamnet_{basin_id}_01"
        if stage.get("status") != "succeeded" or not _is_int(stage.get("attempts")) or stage["attempts"] < 1 or stage.get("failure_reason") is not None or not isinstance(evidence, dict) or set(evidence) != {"bytes", "layer_name", "sha256", "sqlite_identity"} or not _is_int(evidence.get("bytes")) or evidence["bytes"] <= 0 or evidence.get("layer_name") != layer or not _valid_sha256(evidence.get("sha256")) or evidence.get("sqlite_identity") != SQLITE3_IDENTITY_HEX:
            raise ValueError(f"{basin_id} historical acquisition exhaustion mismatch for {product}: {path}")
    if len(failed) != 1:
        raise ValueError(f"{basin_id} historical acquisition exhaustion mismatch: exactly one product must have failed")
    return HistoricalExhaustion(basin_id, failed[0], 2, HISTORICAL_TRANSFER_FAILURE_REASON)


def _adjudicate_transfer(acquired_root: Path, historical_root: Path, basin_id: str) -> AdjudicationVerdict:
    exhaustion = _parse_historical_exhaustion(historical_root, basin_id)
    products = [_parse_acquired_product(acquired_root, basin_id, product) for product in ("basins", "streamnet")]
    return AdjudicationVerdict(
        basin_id, BasinVerdict.TRANSFER_FAILURE,
        AdjudicationEvidenceKind.HISTORICAL_TRANSFER_WITH_RESOLUTION,
        {
            "historical_failed_product": exhaustion.failed_product,
            "historical_attempts": exhaustion.attempts,
            "historical_failure_reason": exhaustion.failure_reason,
            "later_acquisition": [{"product": value.product, "bytes": value.byte_count, "sha256": value.sha256, "attempts": value.attempts} for value in products],
        },
    )


def _serialized_verdict(value: AdjudicationVerdict) -> dict[str, object]:
    return {"processing_basin_id": value.processing_basin_id, "verdict": value.verdict.value, "evidence_kind": value.evidence_kind.value, "evidence": value.evidence}


def _validate_adjudication_result(verdicts: Sequence[object]) -> None:
    if len(verdicts) != 7:
        raise ValueError("adjudication must contain exactly seven verdicts")
    values = [_serialized_verdict(value) if isinstance(value, AdjudicationVerdict) else value for value in verdicts]
    if not all(isinstance(value, dict) for value in values):
        raise ValueError("adjudication processing basin IDs must equal the seven absent IDs exactly once")
    ids = [value.get("processing_basin_id") for value in values]
    if tuple(ids) != ABSENT_PROCESSING_BASIN_IDS or len(set(ids)) != 7:
        raise ValueError("adjudication processing basin IDs must equal the seven absent IDs exactly once")
    allowed = {value.value for value in BasinVerdict}
    for value in values:
        basin_id = str(value["processing_basin_id"])
        if value.get("verdict") not in allowed or "unadjudicated" in value or "unadjudicated" in value.values():
            raise ValueError(f"adjudication verdict for {basin_id} is missing or invalid")
        expected = AdjudicationEvidenceKind.ACQUIRED_SOURCE_GEOMETRY.value if basin_id in GEOMETRY_ADJUDICATION_IDS else AdjudicationEvidenceKind.HISTORICAL_TRANSFER_WITH_RESOLUTION.value
        if value.get("evidence_kind") != expected:
            raise ValueError(f"adjudication evidence kind is invalid for {basin_id}")


def adjudicate_basins(acquired_evidence_root: Path, historical_evidence_root: Path) -> dict[str, object]:
    """Adjudicate seven absent processing basins from checked local evidence.

    Raises:
        ValueError: Required evidence is absent, unsafe, malformed, or inconsistent.
    """
    by_id = {
        "1020018110": _adjudicate_duplicate(acquired_evidence_root),
        "2020003440": _adjudicate_non_root(acquired_evidence_root),
        "2020071190": _adjudicate_root(acquired_evidence_root),
    }
    for basin_id in TRANSFER_ADJUDICATION_IDS:
        by_id[basin_id] = _adjudicate_transfer(acquired_evidence_root, historical_evidence_root, basin_id)
    verdicts = [by_id[basin_id] for basin_id in ABSENT_PROCESSING_BASIN_IDS]
    _validate_adjudication_result(verdicts)
    return {
        "adapter": {"adapter_version": ADAPTER_VERSION, "git_revision": ADJUDICATED_ADAPTER_GIT_REVISION},
        "endpoint_tolerance": DEFAULT_ENDPOINT_TOLERANCE,
        "schema_version": 1,
        "verdicts": [_serialized_verdict(value) for value in verdicts],
    }


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
    assemble_parser = subparsers.add_parser("assemble")
    assemble_parser.add_argument(
        "--input",
        dest="inputs",
        action="append",
        required=True,
        type=Path,
    )
    assemble_parser.add_argument("--partial-input", type=Path)
    assemble_parser.add_argument("--partial-roster", type=Path)
    assemble_parser.add_argument("--out", required=True, type=Path)
    adjudicate_parser = subparsers.add_parser("adjudicate")
    adjudicate_parser.add_argument(
        "--acquired-evidence-root", required=True, type=Path
    )
    adjudicate_parser.add_argument(
        "--historical-evidence-root", required=True, type=Path
    )
    return parser


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _load_partial_basin_roster(roster_path: Path) -> tuple[str, ...]:
    path = Path(roster_path).expanduser()
    try:
        path = path.resolve()
    except (OSError, RuntimeError) as error:
        raise ValueError(f"{path}: invalid partial basin roster") from error
    if not path.is_file():
        raise ValueError(f"{path}: invalid partial basin roster")
    try:
        roster = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{path}: invalid partial basin roster") from error
    if not isinstance(roster, list) or not roster:
        raise ValueError(
            f"{path}: partial basin roster must be one nonempty JSON array"
        )
    crosswalk = load_header_crosswalk()
    checked: list[str] = []
    seen: set[str] = set()
    for index, basin_id in enumerate(roster):
        if not isinstance(basin_id, str) or basin_id not in crosswalk:
            raise ValueError(
                f"{path}: partial basin roster entry at index {index} "
                "is not an authoritative basin ID"
            )
        if basin_id in seen:
            raise ValueError(f"{path}: duplicate partial basin roster entry {basin_id}")
        checked.append(basin_id)
        seen.add(basin_id)
    return tuple(checked)


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
    elif arguments.command == "assemble":
        if (arguments.partial_input is None) != (arguments.partial_roster is None):
            raise ValueError(
                "--partial-input and --partial-roster must be supplied together"
            )
        partial_basin_roster = (
            _load_partial_basin_roster(arguments.partial_roster)
            if arguments.partial_roster is not None
            else None
        )
        assemble_hfx(
            arguments.inputs,
            arguments.out,
            created_at=_utc_now(),
            partial_input_root=arguments.partial_input,
            partial_basin_roster=partial_basin_roster,
        )
    elif arguments.command == "adjudicate":
        result = adjudicate_basins(
            arguments.acquired_evidence_root,
            arguments.historical_evidence_root,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        validate_dataset(arguments.dataset, hfx_binary=arguments.hfx_binary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
