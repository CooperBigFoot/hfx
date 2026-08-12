#!/usr/bin/env python3
"""measurement : ImmutableGeoPackages x EndpointTolerance -> PrePassDetermination."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from pyproj import Geod
from shapely import from_wkb, get_coordinates, set_coordinates


MIRRORED_REVISION = "1385b56bccd4758aea0d04882eee6edadcefe05b"
COMPARISON_REVISION = "d5cb9239f1228a0c709bebf23cf2edbb3444972a"
ENDPOINT_TOLERANCE = 0.001
NON_ROOT_MULTIPLIER = 3.0
NON_ROOT_LIMIT = 0.003
COORDINATE_DOMAIN_TOLERANCE = 0.00011111111111111112
DOWNSTREAM_SENTINEL = -1
CLASS_NAMES = (
    "NON_COINCIDENT",
    "SINGLE_ADMISSIBLE",
    "NEAR_DEGENERATE_ADMITTED",
    "REACH_SIDE_REFUSED",
)
OMISSION = (
    "The 1385b56:3871 source-order condition is deliberately omitted because it "
    "compares geometry against orientation assigned earlier in the reverse-topological "
    "traversal and recovered at 1385b56:3847-3862; enumerating it would measure the "
    "enumerator's continuation policy rather than the basin."
)


@dataclass(frozen=True)
class Source:
    """Pinned identity and public artifact metadata for one acquired source."""

    bytes: int
    sha256: str
    layer: str
    portable_path: str


@dataclass(frozen=True)
class BasinSpec:
    """Pinned inputs and population expectation for one processing basin."""

    total_reaches: int
    basins: Source
    streamnet: Source


SPECS = {
    "2020003440": BasinSpec(
        total_reaches=337_012,
        basins=Source(
            5_397_577_728,
            "a4fd60ff2623631906cc356fb83310a4071bc9a8bd7f3749cada97d2dac7fcba",
            "basins",
            "tdx-m5-seven-acquire-evidence/salvage/downloads/2020003440-basins.gpkg",
        ),
        streamnet=Source(
            1_688_866_816,
            "fa2676491f525fb769eff381ad165031c76ca6a8a73ee219573b336059a2d47e",
            "TDX_streamnet_2020003440_01",
            "tdx-m5-seven-acquire-evidence/salvage/downloads/2020003440-streamnet.gpkg",
        ),
    ),
    "2020071190": BasinSpec(
        total_reaches=664_189,
        basins=Source(
            13_388_906_496,
            "c646fd9ab70a655f038bcaa0f898e972675ab731027c48bac1e70aca18b3bf4f",
            "basins",
            "tdx-m5-seven-acquire-evidence/salvage/downloads/2020071190-basins.gpkg",
        ),
        streamnet=Source(
            3_697_729_536,
            "9aaa47aeae3ab8a9b9e564c1d0b7cda20401cc1958a671ae75cec63ef50bdc6c",
            "TDX_streamnet_2020071190_01",
            "tdx-m5-seven-acquire-evidence/salvage/downloads/2020071190-streamnet.gpkg",
        ),
    ),
}


class MeasurementError(RuntimeError):
    """Raised when immutable evidence or a measured invariant disagrees."""


class Progress:
    """Write infrequent progress reports to stderr."""

    def __init__(self) -> None:
        self.started = time.monotonic()
        self.last = self.started

    def report(self, message: str, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - self.last >= 30.0:
            print(f"[{now - self.started:7.1f}s] {message}", file=sys.stderr, flush=True)
            self.last = now


def _digest(path: Path, progress: Progress) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
            progress.report(f"hashing {path.name}")
    return digest.hexdigest()


def _metadata(path: Path) -> tuple[int, int, int, int, int, int, int]:
    stat = path.stat()
    return (
        stat.st_mode,
        stat.st_uid,
        stat.st_gid,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
    )


def _sidecars(paths: tuple[Path, Path]) -> list[str]:
    return sorted(
        str(path.with_name(path.name + suffix))
        for path in paths
        for suffix in ("-wal", "-shm", "-journal")
        if path.with_name(path.name + suffix).exists()
    )


def _verify_sources(
    paths: tuple[Path, Path], sources: tuple[Source, Source], progress: Progress
) -> tuple[tuple[int, int, int, int, int, int, int], ...]:
    sidecars = _sidecars(paths)
    if sidecars:
        raise MeasurementError(f"unexpected GeoPackage sidecars: {sidecars!r}")
    metadata = []
    for label, path, expected in zip(("basins", "streamnet"), paths, sources, strict=True):
        if not path.is_absolute() or not path.is_file():
            raise MeasurementError(f"{label} path is not an absolute regular file: {path}")
        observed = _metadata(path)
        if observed[4] != expected.bytes:
            raise MeasurementError(
                f"{label} bytes expected {expected.bytes}, measured {observed[4]}"
            )
        digest = _digest(path, progress)
        if digest != expected.sha256:
            raise MeasurementError(
                f"{label} sha256 expected {expected.sha256}, measured {digest}"
            )
        metadata.append(observed)
    progress.report("source identities, metadata, and sidecars verified", force=True)
    return tuple(metadata)


def _connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def _geometry_column(connection: sqlite3.Connection, layer: str) -> str:
    rows = connection.execute(
        "SELECT column_name, srs_id FROM gpkg_geometry_columns WHERE table_name = ?",
        (layer,),
    ).fetchall()
    if len(rows) != 1 or type(rows[0][0]) is not str or rows[0][1] != 4326:
        raise MeasurementError(f"layer {layer} does not declare exactly one EPSG 4326 geometry")
    authority = connection.execute(
        "SELECT organization, organization_coordsys_id FROM gpkg_spatial_ref_sys "
        "WHERE srs_id = 4326"
    ).fetchall()
    if authority != [("EPSG", 4326)]:
        raise MeasurementError(f"layer {layer} has unexpected EPSG 4326 authority {authority!r}")
    return rows[0][0]


def _wkb_payload(blob: object) -> bytes:
    if not isinstance(blob, bytes) or len(blob) < 8 or blob[:2] != b"GP" or blob[2] != 0:
        raise MeasurementError("geometry has an invalid GeoPackage binary header")
    envelope_code = (blob[3] >> 1) & 0b111
    envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code)
    if envelope_bytes is None or len(blob) <= 8 + envelope_bytes:
        raise MeasurementError("geometry has an invalid GPB envelope or no WKB payload")
    return blob[8 + envelope_bytes :]


def _clamp(geometry: Any, layer: str, native_id: int) -> tuple[Any, int]:
    coordinates = get_coordinates(geometry)
    if len(coordinates) == 0 or coordinates.ndim != 2 or coordinates.shape[1] != 2:
        raise MeasurementError(f"{layer} geometry {native_id} is empty or not two-dimensional")
    if not np.isfinite(coordinates).all():
        raise MeasurementError(f"{layer} geometry {native_id} has non-finite coordinates")
    longitude_excess = np.maximum.reduce(
        (-180.0 - coordinates[:, 0], coordinates[:, 0] - 180.0, np.zeros(len(coordinates)))
    )
    latitude_excess = np.maximum.reduce(
        (-90.0 - coordinates[:, 1], coordinates[:, 1] - 90.0, np.zeros(len(coordinates)))
    )
    if np.any(longitude_excess > COORDINATE_DOMAIN_TOLERANCE) or np.any(
        latitude_excess > COORDINATE_DOMAIN_TOLERANCE
    ):
        raise MeasurementError(f"{layer} geometry {native_id} exceeds coordinate tolerance")
    normalized = coordinates.copy()
    normalized[:, 0] = np.clip(normalized[:, 0], -180.0, 180.0)
    normalized[:, 1] = np.clip(normalized[:, 1], -90.0, 90.0)
    altered = int(np.count_nonzero(np.any(normalized != coordinates, axis=1)))
    return set_coordinates(geometry, normalized), altered


def _decode(blob: object, layer: str, native_id: int, allowed: tuple[str, ...]) -> tuple[Any, int]:
    try:
        geometry = from_wkb(_wkb_payload(blob))
    except MeasurementError:
        raise
    except Exception as error:
        raise MeasurementError(f"{layer} geometry {native_id} has invalid WKB: {error}") from error
    if geometry is None or geometry.is_empty or geometry.geom_type not in allowed:
        raise MeasurementError(
            f"{layer} geometry {native_id} must be a non-empty {'/'.join(allowed)}"
        )
    return _clamp(geometry, layer, native_id)


def _read_basins(
    connection: sqlite3.Connection, source: Source, progress: Progress
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    geometry_column = _geometry_column(connection, source.layer)
    count = int(connection.execute(f'SELECT count(*) FROM "{source.layer}"').fetchone()[0])
    native_ids = np.empty(count, dtype="int64")
    own_area = np.empty(count, dtype="float64")
    altered_count = 0
    altered_ids: list[int] = []
    geod = Geod(ellps="WGS84")
    query = f'SELECT streamID, "{geometry_column}" FROM "{source.layer}"'
    for row, (native_id, blob) in enumerate(connection.execute(query)):
        if type(native_id) is not int:
            raise MeasurementError(f"basins.streamID is not an integer: {native_id!r}")
        geometry, altered = _decode(blob, "basins", native_id, ("Polygon", "MultiPolygon"))
        area = abs(float(geod.geometry_area_perimeter(geometry)[0]))
        if not math.isfinite(area) or area <= 0.0:
            raise MeasurementError(f"basins geometry {native_id} has non-positive geodesic area")
        native_ids[row] = native_id
        own_area[row] = area
        altered_count += altered
        if altered:
            altered_ids.append(native_id)
        progress.report(f"decoded {row + 1}/{count} basin polygons")
    order = np.argsort(native_ids, kind="stable")
    native_ids = native_ids[order]
    own_area = own_area[order]
    if np.any(native_ids[1:] == native_ids[:-1]):
        raise MeasurementError(f"duplicate basins.streamID {int(native_ids[np.flatnonzero(native_ids[1:] == native_ids[:-1])[0]])}")
    progress.report(f"decoded and measured {count} basin polygons", force=True)
    return native_ids, own_area, {
        "altered_vertex_count": altered_count,
        "altered_native_ids": sorted(altered_ids),
    }


def _read_streamnet(
    connection: sqlite3.Connection, source: Source, expected_count: int, progress: Progress
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, dict[str, Any], int]:
    geometry_column = _geometry_column(connection, source.layer)
    count = int(connection.execute(f'SELECT count(*) FROM "{source.layer}"').fetchone()[0])
    if count != expected_count:
        raise MeasurementError(f"total reach count expected {expected_count}, measured {count}")
    native_ids = np.empty(count, dtype="int64")
    downstream_ids = np.empty(count, dtype="int64")
    areas = np.empty(count, dtype="float64")
    endpoints = np.empty((count, 2, 2), dtype="float64")
    degenerate = np.zeros(count, dtype=bool)
    altered_count = 0
    altered_ids: list[int] = []
    supported: list[int] = []
    unsupported: list[int] = []
    strict_below = 0
    query = (
        f'SELECT LINKNO, DSLINKNO, DSContArea, "{geometry_column}" '
        f'FROM "{source.layer}"'
    )
    for row, (native_id, downstream_id, raw_area, blob) in enumerate(connection.execute(query)):
        if type(native_id) is not int or type(downstream_id) is not int:
            raise MeasurementError(f"streamnet IDs are not integers: {native_id!r}, {downstream_id!r}")
        if isinstance(raw_area, bool) or not isinstance(raw_area, (int, float)):
            raise MeasurementError(f"DSContArea for {native_id} is not numeric")
        raw_area = float(raw_area)
        if not math.isfinite(raw_area) or raw_area <= 0.0:
            raise MeasurementError(f"DSContArea for {native_id} is not finite and positive")
        geometry, altered = _decode(blob, "streamnet", native_id, ("LineString",))
        coordinates = np.asarray(geometry.coords)
        if len(coordinates) < 2:
            raise MeasurementError(f"streamnet geometry {native_id} has fewer than two coordinates")
        start = (float(coordinates[0, 0]), float(coordinates[0, 1]))
        end = (float(coordinates[-1, 0]), float(coordinates[-1, 1]))
        separation = math.dist(start, end)
        exact = start == end and len(coordinates) == 2
        if start == end:
            (supported if exact else unsupported).append(native_id)
        strict_below += separation < ENDPOINT_TOLERANCE
        native_ids[row] = native_id
        downstream_ids[row] = downstream_id
        areas[row] = raw_area
        endpoints[row] = (start, end)
        degenerate[row] = exact
        altered_count += altered
        if altered:
            altered_ids.append(native_id)
        progress.report(f"decoded {row + 1}/{count} stream reaches")
    if unsupported:
        raise MeasurementError(
            "unsupported start-equals-end native LINKNO values: " + repr(sorted(unsupported))
        )
    order = np.argsort(native_ids, kind="stable")
    native_ids = native_ids[order]
    downstream_ids = downstream_ids[order]
    areas = areas[order]
    endpoints = endpoints[order]
    degenerate = degenerate[order]
    if np.any(native_ids[1:] == native_ids[:-1]):
        raise MeasurementError(f"duplicate streamnet.LINKNO {int(native_ids[np.flatnonzero(native_ids[1:] == native_ids[:-1])[0]])}")
    progress.report(f"decoded and stable-sorted {count} stream reaches", force=True)
    diagnostics = {
        "clamp": {
            "altered_vertex_count": altered_count,
            "altered_native_ids": sorted(altered_ids),
        },
        "start_equals_end": {
            "reach_count": len(supported) + len(unsupported),
            "supported_two_coordinate_count": len(supported),
            "supported_two_coordinate_native_linknos": sorted(supported),
            "unsupported_more_than_two_coordinate_count": len(unsupported),
            "unsupported_more_than_two_coordinate_native_linknos": sorted(unsupported),
        },
    }
    return native_ids, downstream_ids, areas, endpoints, degenerate, diagnostics, strict_below


def _topology(
    native_ids: np.ndarray, downstream_ids: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    if np.any((downstream_ids != DOWNSTREAM_SENTINEL) & (downstream_ids < 0)):
        raise MeasurementError("streamnet has a negative downstream ID other than -1")
    downstream_rows = np.full(len(native_ids), -1, dtype="int64")
    connected = downstream_ids != DOWNSTREAM_SENTINEL
    positions = np.searchsorted(native_ids, downstream_ids[connected])
    safe = np.minimum(positions, len(native_ids) - 1)
    missing = (positions == len(native_ids)) | (native_ids[safe] != downstream_ids[connected])
    if np.any(missing):
        raise MeasurementError(f"missing downstream LINKNO {int(downstream_ids[connected][np.flatnonzero(missing)[0]])}")
    downstream_rows[connected] = positions
    if np.any(downstream_rows == np.arange(len(native_ids))):
        raise MeasurementError("streamnet contains a self-link")
    connected_rows = np.flatnonzero(connected).astype("int64")
    counts = np.bincount(downstream_rows[connected_rows], minlength=len(native_ids)).astype("int64")
    upstream_rows = connected_rows[np.argsort(downstream_rows[connected_rows], kind="stable")]
    offsets = np.empty(len(native_ids) + 1, dtype="int64")
    offsets[0] = 0
    np.cumsum(counts, out=offsets[1:])
    remaining = counts.copy()
    queue = np.empty(len(native_ids), dtype="int64")
    ready = np.flatnonzero(remaining == 0)
    queue[: len(ready)] = ready
    head = 0
    tail = len(ready)
    order = np.empty(len(native_ids), dtype="int64")
    emitted = 0
    while head < tail:
        row = int(queue[head])
        head += 1
        order[emitted] = row
        emitted += 1
        successor = int(downstream_rows[row])
        if successor >= 0:
            remaining[successor] -= 1
            if remaining[successor] == 0:
                queue[tail] = successor
                tail += 1
    if emitted != len(native_ids):
        raise MeasurementError("streamnet topology is cyclic")
    return downstream_rows, order, offsets, upstream_rows


def classify_edges(
    native_ids: np.ndarray,
    downstream_rows: np.ndarray,
    endpoints: np.ndarray,
    degenerate: np.ndarray,
    tolerance: float = ENDPOINT_TOLERANCE,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    """Classify every connected edge without traversal or early refusal."""
    counts = {name: 0 for name in CLASS_NAMES}
    records: list[dict[str, Any]] = []
    for row_value in np.flatnonzero(downstream_rows >= 0):
        row = int(row_value)
        successor = int(downstream_rows[row])
        current_candidates = (0,) if degenerate[row] else (0, 1)
        successor_candidates = (0,) if degenerate[successor] else (0, 1)
        pairings = []
        current_indexes: set[int] = set()
        for current_index in current_candidates:
            for successor_index in successor_candidates:
                distance = math.dist(endpoints[row, current_index], endpoints[successor, successor_index])
                within = distance <= tolerance
                pairings.append(
                    {
                        "current_endpoint_index": current_index,
                        "successor_endpoint_index": successor_index,
                        "distance_degrees": distance,
                        "within_endpoint_tolerance": within,
                    }
                )
                if within:
                    current_indexes.add(current_index)
        current_separation = math.dist(endpoints[row, 0], endpoints[row, 1])
        if not current_indexes:
            classification = "NON_COINCIDENT"
        elif len(current_indexes) == 1:
            classification = "SINGLE_ADMISSIBLE"
        elif current_separation <= NON_ROOT_MULTIPLIER * tolerance:
            classification = "NEAR_DEGENERATE_ADMITTED"
        else:
            classification = "REACH_SIDE_REFUSED"
        counts[classification] += 1
        if classification in ("NEAR_DEGENERATE_ADMITTED", "REACH_SIDE_REFUSED"):
            if degenerate[row]:
                raise MeasurementError("a production-degenerate current reach entered a two-endpoint class")
            successor_separation = math.dist(endpoints[successor, 0], endpoints[successor, 1])
            records.append(
                {
                    "LINKNO": int(native_ids[row]),
                    "DSLINKNO": int(native_ids[successor]),
                    "class": classification,
                    "current_endpoint_separation_degrees": current_separation,
                    "successor_endpoint_separation_degrees": successor_separation,
                    "successor_is_exact_production_degenerate": bool(degenerate[successor]),
                    "successor_is_near_degenerate_under_non_root_limit": successor_separation <= NON_ROOT_MULTIPLIER * tolerance,
                    "candidate_pairing_distances": pairings,
                }
            )
    records.sort(key=lambda value: (value["LINKNO"], value["DSLINKNO"]))
    return counts, records


def _dscontarea(
    basin_ids: np.ndarray,
    own_area: np.ndarray,
    stream_ids: np.ndarray,
    downstream_rows: np.ndarray,
    raw_area: np.ndarray,
    topology_order: np.ndarray,
    predecessor_offsets: np.ndarray,
    predecessor_rows: np.ndarray,
) -> dict[str, Any]:
    positions = np.searchsorted(stream_ids, basin_ids)
    safe = np.minimum(positions, len(stream_ids) - 1)
    missing = (positions == len(stream_ids)) | (stream_ids[safe] != basin_ids)
    if np.any(missing):
        raise MeasurementError(f"basins.streamID does not join to LINKNO {int(basin_ids[np.flatnonzero(missing)[0]])}")
    own_by_stream = np.zeros(len(stream_ids), dtype="float64")
    own_by_stream[positions] = own_area
    upstream = np.empty(len(stream_ids), dtype="float64")
    for row_value in topology_order:
        row = int(row_value)
        predecessors = predecessor_rows[predecessor_offsets[row] : predecessor_offsets[row + 1]]
        upstream[row] = math.fsum((own_by_stream[row], *(upstream[predecessors].tolist())))
    expected = upstream[positions]
    raw = raw_area[positions]
    expected_sum = math.fsum(expected)
    raw_sum = math.fsum(raw)
    m2_error = math.fsum(abs(value - area) for value, area in zip(raw, expected, strict=True)) / expected_sum
    km2_error = math.fsum(abs(value * 1_000_000 - area) for value, area in zip(raw, expected, strict=True)) / expected_sum
    if m2_error == km2_error:
        raise MeasurementError("DSContArea unit candidates are tied")
    unit = "m2" if m2_error < km2_error else "km2"
    selected = min(m2_error, km2_error)
    ratio = math.inf if selected == 0.0 else max(m2_error, km2_error) / selected
    if ratio < 1000.0:
        raise MeasurementError(f"DSContArea unit decisiveness ratio is {ratio!r}, below 1000.0")
    converted = raw if unit == "m2" else raw * 1_000_000
    signed = math.fsum(value - area for value, area in zip(converted, expected, strict=True)) / expected_sum
    absolute = math.fsum(abs(value - area) for value, area in zip(converted, expected, strict=True)) / expected_sum
    maximum = max(abs(value - area) / area for value, area in zip(converted, expected, strict=True))
    if selected > 1.0:
        raise MeasurementError(f"DSContArea selected relative error {selected!r} exceeds 1.0")
    values = (expected_sum, raw_sum, m2_error, km2_error, selected, signed, absolute, maximum)
    if not all(math.isfinite(value) for value in values):
        raise MeasurementError("DSContArea diagnostics contain a non-finite value")
    return {
        "own_area_method": 'abs(float(Geod(ellps="WGS84").geometry_area_perimeter(post_clamp_geometry)[0])) stored as float64',
        "upstream_accumulation": "math.fsum in production topology order",
        "source_unit": unit,
        "checked_polygon_bearing_link_count": len(basin_ids),
        "geodesic_upstream_area_sum_m2": expected_sum,
        "dscontarea_sum_raw": raw_sum,
        "m2_relative_error": m2_error,
        "km2_relative_error": km2_error,
        "selected_relative_error": selected,
        "unit_decisiveness_ratio": "Infinity" if math.isinf(ratio) else ratio,
        "unit_decisiveness_min_ratio": 1000.0,
        "signed_aggregate_relative_divergence": signed,
        "absolute_aggregate_relative_divergence": absolute,
        "max_absolute_relative_divergence": maximum,
        "fabric_divergence_sanity_ceiling": 1.0,
        "divergence_is_production_comparable": True,
    }


def _verify_prepass(repo: Path) -> None:
    bodies = []
    for revision in (MIRRORED_REVISION, COMPARISON_REVISION):
        result = subprocess.run(
            ["git", "show", f"{revision}:adapters/tdx-hydro/build_adapter.py"],
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
        )
        bodies.append(b"\n".join(result.stdout.splitlines()[3709:3741]))
    if bodies[0] != bodies[1]:
        raise MeasurementError("pre-pass predicates at lines 3710-3741 differ between revisions")


def _reconciliation(basin_id: str, counts: dict[str, int], strict_below: int) -> dict[str, Any]:
    mismatches = []
    for name in ("NON_COINCIDENT", "REACH_SIDE_REFUSED"):
        if counts[name] != 0:
            mismatches.append(f"{name} expected 0, measured {counts[name]}")
    if basin_id == "2020003440":
        return {
            "prior_estimate": None,
            "measured_near_degenerate_admitted_count": counts["NEAR_DEGENERATE_ADMITTED"],
            "measured_non_coincident_count": counts["NON_COINCIDENT"],
            "measured_reach_side_refused_count": counts["REACH_SIDE_REFUSED"],
            "prohibited_comparisons": [4309, 4368],
            "status": "measured_without_sourced_prior",
            "inconsistency": "; ".join(mismatches) or None,
        }
    measured_prior = counts["NEAR_DEGENERATE_ADMITTED"] + counts["REACH_SIDE_REFUSED"]
    if measured_prior != 11_030:
        mismatches.append(f"two-current-endpoint count expected 11030, measured {measured_prior}")
    if strict_below != 11_445:
        mismatches.append(f"strict-below-tolerance context count expected 11445, measured {strict_below}")
    return {
        "prior_estimate": {
            "near_degenerate_admitted_plus_reach_side_refused_count": 11030,
            "endpoint_separation_strictly_below_tolerance_reach_count": 11445,
            "source": "milestone-7/step-2/plan.md:238-239, repeated at line 257",
        },
        "measured_near_degenerate_admitted_count": counts["NEAR_DEGENERATE_ADMITTED"],
        "measured_non_coincident_count": counts["NON_COINCIDENT"],
        "measured_reach_side_refused_count": counts["REACH_SIDE_REFUSED"],
        "status": "confirmed" if measured_prior == 11_030 else "corrected",
        "inconsistency": "; ".join(mismatches) or None,
    }


def measure(basin_id: str, basins_path: Path, streamnet_path: Path, tolerance: float) -> dict[str, Any]:
    """Measure one basin from immutable acquired-source GeoPackages.

    Raises:
        MeasurementError: If an input identity, geometry, topology, or invariant fails.
    """
    if basin_id not in SPECS:
        raise MeasurementError(f"unsupported processing basin ID {basin_id!r}")
    if tolerance != ENDPOINT_TOLERANCE:
        raise MeasurementError(f"endpoint tolerance must be exactly {ENDPOINT_TOLERANCE!r}")
    spec = SPECS[basin_id]
    paths = (basins_path, streamnet_path)
    sources = (spec.basins, spec.streamnet)
    progress = Progress()
    before = _verify_sources(paths, sources, progress)
    repo = Path(__file__).resolve().parents[2]
    _verify_prepass(repo)
    with _connect(basins_path) as connection:
        basin_ids, own_area, basin_clamp = _read_basins(connection, spec.basins, progress)
    with _connect(streamnet_path) as connection:
        stream = _read_streamnet(connection, spec.streamnet, spec.total_reaches, progress)
    stream_ids, downstream_ids, raw_area, endpoints, degenerate, stream_diagnostics, strict_below = stream
    downstream_rows, topology_order, predecessor_offsets, predecessor_rows = _topology(stream_ids, downstream_ids)
    counts, records = classify_edges(stream_ids, downstream_rows, endpoints, degenerate, tolerance)
    connected_count = int(np.count_nonzero(downstream_rows >= 0))
    class_sum = sum(counts.values())
    if class_sum != connected_count:
        raise MeasurementError(f"class sum {class_sum} differs from connected edge count {connected_count}")
    if len(records) != counts["NEAR_DEGENERATE_ADMITTED"] + counts["REACH_SIDE_REFUSED"]:
        raise MeasurementError("two-current-endpoint record count differs from class counts")
    dscontarea = _dscontarea(
        basin_ids,
        own_area,
        stream_ids,
        downstream_rows,
        raw_area,
        topology_order,
        predecessor_offsets,
        predecessor_rows,
    )
    after = _verify_sources(paths, sources, progress)
    if after != before:
        raise MeasurementError(f"source filesystem metadata changed: before={before!r}, after={after!r}")
    return {
        "schema_version": 1,
        "processing_basin_id": basin_id,
        "algorithm": {
            "mirrored_revision": MIRRORED_REVISION,
            "comparison_revision": COMPARISON_REVISION,
            "source_path": "adapters/tdx-hydro/build_adapter.py",
            "mirrored_lines": "3710-3741",
            "reused_instrument_path": "adapters/tdx-hydro/reconstruct_2020003440_topology.py",
            "reused_instrument_lines": "389-436",
            "prepass_predicates_unchanged": True,
            "imports_build_adapter": False,
        },
        "parameters": {
            "endpoint_tolerance": tolerance,
            "non_root_reach_side_ambiguity_tolerance_multiplier": NON_ROOT_MULTIPLIER,
            "non_root_reach_side_ambiguity_limit": NON_ROOT_MULTIPLIER * tolerance,
        },
        "source_inputs": [
            {
                "product": label,
                "path": source.portable_path,
                "layer_name": source.layer,
                "bytes": source.bytes,
                "sha256": source.sha256,
            }
            for label, source in zip(("basins", "streamnet"), sources, strict=True)
        ],
        "geometry_normalization": {
            "coordinate_domain_tolerance_degrees": COORDINATE_DOMAIN_TOLERANCE,
            "basins_clamp": basin_clamp,
            "streamnet_clamp": stream_diagnostics["clamp"],
            "start_equals_end": stream_diagnostics["start_equals_end"],
        },
        "population": {
            "total_reach_count": len(stream_ids),
            "connected_edge_count": connected_count,
            "endpoint_separation_strictly_below_tolerance_reach_count": strict_below,
            "class_counts": counts,
            "class_count_sum": class_sum,
            "class_count_sum_equals_connected_edge_count": True,
        },
        "two_current_endpoint_edges": records,
        "dscontarea_derivation": dscontarea,
        "scope": {
            "measurement_only": True,
            "orientation_derived_or_assigned": False,
            "reverse_topological_traversal_performed": False,
            "continued_past_production_refusal": False,
            "source_order_condition_3871_evaluated": False,
            "source_order_condition_3871_omission": OMISSION,
            "compiled_basin_output_retained": False,
        },
        "reconciliation": _reconciliation(basin_id, counts, strict_below),
    }


def serialize(value: dict[str, Any]) -> str:
    """Return the deterministic artifact representation."""
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--processing-basin-id", required=True, choices=sorted(SPECS))
    parser.add_argument("--basins", required=True, type=Path)
    parser.add_argument("--streamnet", required=True, type=Path)
    parser.add_argument("--endpoint-tolerance", required=True, type=float)
    return parser.parse_args()


def main() -> int:
    """Run the measurement CLI, emitting only a satisfying artifact to stdout."""
    arguments = _arguments()
    try:
        result = measure(
            arguments.processing_basin_id,
            arguments.basins,
            arguments.streamnet,
            arguments.endpoint_tolerance,
        )
    except (MeasurementError, OSError, sqlite3.Error, subprocess.CalledProcessError) as error:
        print(f"measurement failed: {error}", file=sys.stderr)
        return 1
    sys.stdout.write(serialize(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
