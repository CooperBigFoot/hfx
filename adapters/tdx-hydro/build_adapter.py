#!/usr/bin/env python3
"""Build an NGA TDX-Hydro processing basin as an HFX dataset."""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from numbers import Integral, Real
from pathlib import Path

import pandas as pd
from shapely.geometry import LineString


CROSSWALK_PATH = Path(__file__).parent / "data" / "tdx_header_numbers.json"
GLOBAL_LINKNO_STRIDE = 10_000_000
TDX_LINKNO_SENTINEL = -1
LOGGER = logging.getLogger("tdx-hydro")


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
    orientation_checked_link_count: int
    root_orientation_checked_count: int
    orientation_tolerance: float


@dataclass(frozen=True)
class StreamnetModel:
    units: tuple[StreamnetUnit, ...]
    edges: tuple[tuple[int, int], ...]
    roots: tuple[int, ...]
    diagnostics: StreamnetDiagnostics


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


def _prove_native_orientation(
    relation: dict[int, int],
    endpoints_by_linkno: dict[
        int, tuple[tuple[float, float], tuple[float, float]]
    ],
    endpoint_tolerance: float,
) -> dict[int, tuple[float, float]]:
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

    for root_linkno, downstream_linkno in relation.items():
        if downstream_linkno != TDX_LINKNO_SENTINEL:
            continue

        predecessor_matches = matched_successor_endpoints.get(root_linkno, [])
        if not predecessor_matches:
            raise ValueError(
                f"orientation proof for root LINKNO {root_linkno} is unprovable without a predecessor"
            )
        upstream_endpoint_indexes = set(predecessor_matches)
        if len(upstream_endpoint_indexes) > 1:
            raise ValueError(
                f"orientation proof for root LINKNO {root_linkno} has conflicting predecessor matches"
            )

        upstream_endpoint_index = upstream_endpoint_indexes.pop()
        downstream_endpoints[root_linkno] = endpoints_by_linkno[root_linkno][
            1 - upstream_endpoint_index
        ]

    return downstream_endpoints


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
    duplicate_units = sorted(
        linkno for linkno in set(basin_linknos) if basin_linknos.count(linkno) > 1
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

    rows_by_linkno: dict[int, list[int]] = {}
    for linkno, downstream_linkno in zip(stream_linknos, downstream_linknos, strict=True):
        rows_by_linkno.setdefault(linkno, []).append(downstream_linkno)
    for linkno, targets in rows_by_linkno.items():
        if len(targets) > 1:
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
    downstream_endpoints = _prove_native_orientation(
        relation, endpoints_by_linkno, tolerance
    )

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
        orientation_checked_link_count=len(relation),
        root_orientation_checked_count=sum(
            downstream_linkno == TDX_LINKNO_SENTINEL
            for downstream_linkno in relation.values()
        ),
        orientation_tolerance=tolerance,
    )
    LOGGER.info(
        "streamnet_model polygon_bearing_links=%d roots=%d contracted_edges=%d "
        "contracted_roots=%d contracted_link_traversals=%d "
        "orientation_checked_links=%d root_orientation_checks=%d "
        "orientation_tolerance=%s",
        diagnostics.polygon_bearing_link_count,
        diagnostics.root_count,
        diagnostics.contracted_edge_count,
        diagnostics.contracted_root_count,
        diagnostics.contracted_link_traversal_count,
        diagnostics.orientation_checked_link_count,
        diagnostics.root_orientation_checked_count,
        diagnostics.orientation_tolerance,
    )
    return StreamnetModel(
        units=unit_tuple,
        edges=edges,
        roots=roots,
        diagnostics=diagnostics,
    )
