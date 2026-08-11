#!/usr/bin/env python3
"""reconstruct : ImmutableGeoPackages -> TopologyOrderingCrossCheck."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from shapely import from_wkb


STREAM_ROW_COUNT = 337_012
BASIN_ID_COUNT = 336_922
DOWNSTREAM_SENTINEL = -1
ENDPOINT_TOLERANCE = 0.001
AMBIGUITY_LIMIT = 0.003
EXPECTED_ALTERED_VERTEX_COUNT = 0
REFUSAL_LINKNO = 817_894
REFUSAL_DSLINKNO = 819_270
TARGET_LINKNO = 148_956
TARGET_DSLINKNO = 399_388
EXPECTED_SUCCESSOR_UPSTREAM_INDEX = 0
EXPECTED_SUCCESSOR_DOWNSTREAM_INDEX = 1
EXPECTED_CURRENT_AREA_KM2 = 262_708.625408
EXPECTED_SUCCESSOR_AREA_KM2 = 262_719.782912
ALGORITHM_SOURCE_REVISION = "d5cb9239f1228a0c709bebf23cf2edbb3444972a"
IMPLEMENTATION_PATH = "adapters/tdx-hydro/reconstruct_2020003440_topology.py"
POSITION_BASIS = (
    "one_based_connected_edge_position_in_reverse_production_topology_order"
)
LIMITATIONS = [
    "that the adapter was run",
    "the correctness or cause of the recorded refusal",
    "a pass or failure for LINKNO 148956 -> DSLINKNO 399388",
    "the published adapter strictness verdict in seven-basin-verdicts.json",
    "a verdict or classification for other basin-wide line-3825 matches",
]


@dataclass(frozen=True)
class InputIdentity:
    """The required identity and layer of one acquired GeoPackage."""

    bytes: int
    layer: str
    sha256: str


INPUT_IDENTITIES = {
    "basins": InputIdentity(
        bytes=5_397_577_728,
        layer="basins",
        sha256="a4fd60ff2623631906cc356fb83310a4071bc9a8bd7f3749cada97d2dac7fcba",
    ),
    "streamnet": InputIdentity(
        bytes=1_688_866_816,
        layer="TDX_streamnet_2020003440_01",
        sha256="fa2676491f525fb769eff381ad165031c76ca6a8a73ee219573b336059a2d47e",
    ),
}


class Disagreement(RuntimeError):
    """Raised when source reconstruction contradicts the recorded observation."""


class Progress:
    """Emit phase progress often enough to distinguish work from a stalled run."""

    def __init__(self) -> None:
        self.started = time.monotonic()
        self.last = self.started

    def phase(self, message: str, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - self.last >= 30.0:
            elapsed = now - self.started
            print(f"[{elapsed:7.1f}s] {message}", file=sys.stderr, flush=True)
            self.last = now


def _digest(path: Path, progress: Progress) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
            progress.phase(f"hashing {path.name}")
    return digest.hexdigest()


def _sidecars(paths: tuple[Path, Path]) -> list[str]:
    suffixes = ("-wal", "-shm", "-journal")
    return sorted(
        str(path.with_name(path.name + suffix))
        for path in paths
        for suffix in suffixes
        if path.with_name(path.name + suffix).exists()
    )


def _verify_inputs(
    basins_path: Path, streamnet_path: Path, progress: Progress
) -> None:
    paths = (basins_path, streamnet_path)
    sidecars = _sidecars(paths)
    if sidecars:
        raise Disagreement(f"unexpected GeoPackage sidecars: {sidecars!r}")
    for label, path in zip(("basins", "streamnet"), paths, strict=True):
        expected = INPUT_IDENTITIES[label]
        if not path.is_absolute():
            raise Disagreement(f"{label} path is not absolute: {path}")
        if not path.is_file():
            raise Disagreement(f"{label} input is not a regular file: {path}")
        actual_bytes = path.stat().st_size
        if actual_bytes != expected.bytes:
            raise Disagreement(
                f"{label} byte size differs: expected {expected.bytes}, got {actual_bytes}"
            )
        actual_digest = _digest(path, progress)
        if actual_digest != expected.sha256:
            raise Disagreement(
                f"{label} sha256 differs: expected {expected.sha256}, got {actual_digest}"
            )
    progress.phase("source identities and sidecar absence verified", force=True)


def _connect(path: Path) -> sqlite3.Connection:
    uri = f"file:{path}?mode=ro&immutable=1"
    return sqlite3.connect(uri, uri=True)


def _require_epsg_4326(connection: sqlite3.Connection, layer: str) -> str:
    rows = connection.execute(
        "SELECT column_name, srs_id FROM gpkg_geometry_columns WHERE table_name = ?",
        (layer,),
    ).fetchall()
    if len(rows) != 1:
        raise Disagreement(
            f"layer {layer} has {len(rows)} gpkg_geometry_columns declarations, expected 1"
        )
    column_name, srs_id = rows[0]
    if type(column_name) is not str or type(srs_id) is not int:
        raise Disagreement(f"layer {layer} has invalid geometry metadata {rows[0]!r}")
    srs_rows = connection.execute(
        "SELECT organization, organization_coordsys_id "
        "FROM gpkg_spatial_ref_sys WHERE srs_id = ?",
        (srs_id,),
    ).fetchall()
    if srs_id != 4326 or srs_rows != [("EPSG", 4326)]:
        raise Disagreement(
            f"layer {layer} does not declare exactly EPSG 4326: "
            f"srs_id={srs_id!r}, authority={srs_rows!r}"
        )
    return column_name


def _read_basin_ids(connection: sqlite3.Connection, progress: Progress) -> np.ndarray:
    geometry_column = _require_epsg_4326(connection, INPUT_IDENTITIES["basins"].layer)
    if geometry_column != "geom":
        raise Disagreement(
            f"basins geometry column differs: expected 'geom', got {geometry_column!r}"
        )
    values: list[int] = []
    for (value,) in connection.execute("SELECT streamID FROM basins"):
        if type(value) is not int:
            raise Disagreement(f"basins.streamID is not a non-null integer: {value!r}")
        values.append(value)
    if len(values) != BASIN_ID_COUNT:
        raise Disagreement(
            f"basins row count differs: expected {BASIN_ID_COUNT}, got {len(values)}"
        )
    basin_ids = np.asarray(values, dtype="int64")
    basin_ids.sort(kind="stable")
    if np.any(basin_ids[1:] == basin_ids[:-1]):
        duplicate = int(basin_ids[np.flatnonzero(basin_ids[1:] == basin_ids[:-1])[0]])
        raise Disagreement(f"duplicate basins.streamID {duplicate}")
    progress.phase("basin streamID values loaded and validated", force=True)
    return basin_ids


def _wkb_payload(blob: object) -> bytes:
    if not isinstance(blob, bytes):
        raise Disagreement(f"stream geometry is not a non-null blob: {type(blob).__name__}")
    if len(blob) < 8 or blob[:2] != b"GP":
        raise Disagreement("stream geometry has an invalid GeoPackage binary header")
    if blob[2] != 0:
        raise Disagreement(f"stream geometry has unsupported GPB version {blob[2]}")
    envelope_code = (blob[3] >> 1) & 0b111
    envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get(envelope_code)
    if envelope_bytes is None:
        raise Disagreement(
            f"stream geometry has invalid GPB envelope code {envelope_code}"
        )
    offset = 8 + envelope_bytes
    if len(blob) <= offset:
        raise Disagreement("stream geometry has no WKB payload")
    return blob[offset:]


def _read_streamnet(
    connection: sqlite3.Connection, progress: Progress
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    layer = INPUT_IDENTITIES["streamnet"].layer
    geometry_column = _require_epsg_4326(connection, layer)
    if geometry_column != "geom":
        raise Disagreement(
            f"streamnet geometry column differs: expected 'geom', got {geometry_column!r}"
        )
    native_ids = np.empty(STREAM_ROW_COUNT, dtype="int64")
    downstream_ids = np.empty(STREAM_ROW_COUNT, dtype="int64")
    areas = np.empty(STREAM_ROW_COUNT, dtype="float64")
    endpoints = np.empty((STREAM_ROW_COUNT, 2, 2), dtype="float64")
    degenerate = np.empty(STREAM_ROW_COUNT, dtype=bool)
    altered_vertex_count = 0
    query = (
        'SELECT LINKNO, DSLINKNO, DSContArea, geom FROM "'
        + layer
        + '"'
    )
    row_count = 0
    for linkno, dslinkno, area, blob in connection.execute(query):
        if row_count >= STREAM_ROW_COUNT:
            raise Disagreement(
                f"streamnet row count exceeds expected {STREAM_ROW_COUNT}"
            )
        if type(linkno) is not int or type(dslinkno) is not int:
            raise Disagreement(
                f"streamnet IDs are not non-null integers: {linkno!r}, {dslinkno!r}"
            )
        if isinstance(area, bool) or not isinstance(area, (int, float)):
            raise Disagreement(
                f"DSContArea for LINKNO {linkno} is not a non-null number: {area!r}"
            )
        area_float = float(area)
        if not math.isfinite(area_float) or area_float <= 0.0:
            raise Disagreement(
                f"DSContArea for LINKNO {linkno} is not finite and positive: {area!r}"
            )
        try:
            geometry = from_wkb(_wkb_payload(blob))
        except Disagreement:
            raise
        except Exception as error:
            raise Disagreement(
                f"stream geometry for LINKNO {linkno} has invalid WKB: {error}"
            ) from error
        if geometry is None or geometry.is_empty or geometry.geom_type != "LineString":
            raise Disagreement(
                f"stream geometry for LINKNO {linkno} is not a non-empty LineString"
            )
        coordinates = np.asarray(geometry.coords)
        if coordinates.ndim != 2 or coordinates.shape[1] != 2 or len(coordinates) < 2:
            raise Disagreement(
                f"stream geometry for LINKNO {linkno} is not a two-dimensional LineString"
            )
        if not np.isfinite(coordinates).all():
            raise Disagreement(
                f"stream geometry for LINKNO {linkno} has non-finite coordinates"
            )
        out_of_domain = (
            (coordinates[:, 0] < -180.0)
            | (coordinates[:, 0] > 180.0)
            | (coordinates[:, 1] < -90.0)
            | (coordinates[:, 1] > 90.0)
        )
        altered_vertex_count += int(np.count_nonzero(out_of_domain))
        native_ids[row_count] = linkno
        downstream_ids[row_count] = dslinkno
        areas[row_count] = area_float
        endpoints[row_count, 0] = coordinates[0]
        endpoints[row_count, 1] = coordinates[-1]
        degenerate[row_count] = len(coordinates) == 2 and np.array_equal(
            coordinates[0], coordinates[1]
        )
        row_count += 1
        progress.phase(f"decoded {row_count}/{STREAM_ROW_COUNT} stream reaches")
    if row_count != STREAM_ROW_COUNT:
        raise Disagreement(
            f"streamnet row count differs: expected {STREAM_ROW_COUNT}, got {row_count}"
        )
    if altered_vertex_count != EXPECTED_ALTERED_VERTEX_COUNT:
        raise Disagreement(
            "streamnet clamp-altered vertex count differs: "
            f"expected {EXPECTED_ALTERED_VERTEX_COUNT}, got {altered_vertex_count}"
        )
    order = np.argsort(native_ids, kind="stable")
    native_ids = native_ids[order]
    downstream_ids = downstream_ids[order]
    areas = areas[order]
    endpoints = endpoints[order]
    degenerate = degenerate[order]
    duplicates = np.flatnonzero(native_ids[1:] == native_ids[:-1])
    if len(duplicates):
        raise Disagreement(f"duplicate streamnet.LINKNO {int(native_ids[duplicates[0]])}")
    progress.phase("stream reaches decoded, validated, and stable-sorted", force=True)
    return native_ids, downstream_ids, areas, endpoints, degenerate


def _row_for_edge(
    native_ids: np.ndarray,
    downstream_ids: np.ndarray,
    linkno: int,
    dslinkno: int,
) -> int:
    row = int(np.searchsorted(native_ids, linkno))
    if row == len(native_ids) or int(native_ids[row]) != linkno:
        raise Disagreement(f"required LINKNO {linkno} is absent")
    actual_downstream = int(downstream_ids[row])
    if actual_downstream != dslinkno:
        raise Disagreement(
            f"required edge {linkno} -> {dslinkno} differs: got {linkno} -> {actual_downstream}"
        )
    return row


def _downstream_rows(
    native_ids: np.ndarray, downstream_ids: np.ndarray
) -> np.ndarray:
    rows = np.full(len(native_ids), -1, dtype="int64")
    connected = downstream_ids != DOWNSTREAM_SENTINEL
    connected_downstream = downstream_ids[connected]
    positions = np.searchsorted(native_ids, connected_downstream)
    safe_positions = np.minimum(positions, len(native_ids) - 1)
    missing = (positions == len(native_ids)) | (
        native_ids[safe_positions] != connected_downstream
    )
    if np.any(missing):
        missing_offset = int(np.flatnonzero(missing)[0])
        connected_row = int(np.flatnonzero(connected)[missing_offset])
        raise Disagreement(
            "streamnet missing downstream LINKNO "
            f"{int(connected_downstream[missing_offset])} referenced by native LINKNO "
            f"{int(native_ids[connected_row])}"
        )
    rows[connected] = positions
    self_links = np.flatnonzero(rows == np.arange(len(rows)))
    if len(self_links):
        raise Disagreement(
            f"streamnet self-link at native LINKNO {int(native_ids[int(self_links[0])])}"
        )
    return rows


def _topological_order(native_ids: np.ndarray, downstream_rows: np.ndarray) -> np.ndarray:
    connected_rows = np.flatnonzero(downstream_rows >= 0).astype("int64")
    remaining = np.bincount(
        downstream_rows[connected_rows], minlength=len(native_ids)
    ).astype("int64")
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
        raise Disagreement(
            "streamnet cycle detected: " + " -> ".join(str(value) for value in cycle)
        )
    return topology


def _match_endpoints(
    native_ids: np.ndarray,
    downstream_rows: np.ndarray,
    endpoints: np.ndarray,
    degenerate: np.ndarray,
    progress: Progress,
) -> tuple[np.ndarray, np.ndarray]:
    matches = np.zeros((len(native_ids), 2, 2), dtype=bool)
    current_indexes = np.zeros((len(native_ids), 2), dtype=bool)
    connected_rows = np.flatnonzero(downstream_rows >= 0)
    for offset, row_value in enumerate(connected_rows, start=1):
        row = int(row_value)
        successor = int(downstream_rows[row])
        current_candidates = (0,) if degenerate[row] else (0, 1)
        successor_candidates = (0,) if degenerate[successor] else (0, 1)
        for current_index in current_candidates:
            for successor_index in successor_candidates:
                if (
                    math.dist(
                        endpoints[row, current_index],
                        endpoints[successor, successor_index],
                    )
                    <= ENDPOINT_TOLERANCE
                ):
                    matches[row, current_index, successor_index] = True
                    current_indexes[row, current_index] = True
        indexes = np.flatnonzero(current_indexes[row])
        if len(indexes) == 0:
            raise Disagreement(
                "orientation proof for native LINKNO "
                f"{int(native_ids[row])} and downstream LINKNO "
                f"{int(native_ids[successor])} is non-coincident"
            )
        if len(indexes) > 1:
            separation = math.dist(endpoints[row, 0], endpoints[row, 1])
            if separation > AMBIGUITY_LIMIT:
                raise Disagreement(
                    "orientation proof for native LINKNO "
                    f"{int(native_ids[row])} and downstream LINKNO "
                    f"{int(native_ids[successor])} is reach-side ambiguous: "
                    "both current endpoints coincide within tolerance but endpoint "
                    f"separation {separation} exceeds near-degenerate limit {AMBIGUITY_LIMIT}"
                )
        progress.phase(
            f"matched endpoints for {offset}/{len(connected_rows)} connected reaches"
        )
    progress.phase("all connected endpoint matches validated", force=True)
    return matches, current_indexes


def _raise_before_3825(
    native_ids: np.ndarray,
    downstream_rows: np.ndarray,
    areas_km2: np.ndarray,
    degenerate: np.ndarray,
    downstream_endpoints: np.ndarray,
    endpoints: np.ndarray,
    row: int,
    successor: int,
) -> tuple[int, int, int, int]:
    current_id = int(native_ids[row])
    successor_id = int(native_ids[successor])
    current_area = float(areas_km2[row])
    successor_area = float(areas_km2[successor])
    if current_area == successor_area:
        raise Disagreement(
            "orientation proof for native LINKNO "
            f"{current_id} and downstream LINKNO {successor_id} cannot use "
            "source vertex order: DSContArea evidence is tied at "
            f"{current_area!r} km2"
        )
    if current_area > successor_area:
        raise Disagreement(
            "orientation proof for native LINKNO "
            f"{current_id} and downstream LINKNO {successor_id} contradicts "
            "source vertex order: upstream DSContArea "
            f"{current_area!r} km2 exceeds downstream DSContArea "
            f"{successor_area!r} km2"
        )
    if degenerate[successor]:
        raise Disagreement(
            "orientation proof for native LINKNO "
            f"{current_id} and downstream LINKNO {successor_id} cannot use "
            "source vertex order: downstream-nondecreasing DSContArea "
            f"{current_area!r} km2 -> {successor_area!r} km2 does not "
            "distinguish the successor endpoint side"
        )
    if downstream_rows[successor] < 0:
        raise Disagreement(
            "orientation proof for native LINKNO "
            f"{current_id} and downstream LINKNO {successor_id} cannot use "
            "source vertex order: downstream-nondecreasing DSContArea "
            f"{current_area!r} km2 -> {successor_area!r} km2 cannot determine "
            f"the upstream endpoint of root successor LINKNO {successor_id}"
        )
    if np.isnan(downstream_endpoints[successor]).any():
        raise Disagreement(
            "internal compact-topology orientation is missing for native LINKNO "
            f"{current_id} and downstream LINKNO {successor_id}"
        )
    successor_downstream_index = next(
        (
            index
            for index in (0, 1)
            if np.array_equal(
                endpoints[successor, index], downstream_endpoints[successor]
            )
        ),
        -1,
    )
    if successor_downstream_index < 0:
        raise Disagreement(
            "internal compact-topology orientation is invalid for native LINKNO "
            f"{current_id} and downstream LINKNO {successor_id}"
        )
    successor_upstream_index = 1 - successor_downstream_index
    return (
        current_id,
        successor_id,
        successor_upstream_index,
        successor_downstream_index,
    )


def _traversal_positions(
    topology: np.ndarray,
    downstream_rows: np.ndarray,
    refusal_row: int,
    target_row: int,
) -> tuple[int, int]:
    refusal_position: int | None = None
    target_position: int | None = None
    position = 0
    for row_value in topology[::-1]:
        row = int(row_value)
        if downstream_rows[row] < 0:
            continue
        position += 1
        if row == refusal_row:
            refusal_position = position
        if row == target_row:
            target_position = position
    if refusal_position is None or target_position is None:
        raise Disagreement(
            "unable to locate refusal and target edges in connected reverse-topology traversal"
        )
    return refusal_position, target_position


def _simulate_until_refusal(
    native_ids: np.ndarray,
    downstream_rows: np.ndarray,
    topology: np.ndarray,
    areas_km2: np.ndarray,
    endpoints: np.ndarray,
    degenerate: np.ndarray,
    matches: np.ndarray,
    current_indexes: np.ndarray,
    expected_refusal_position: int,
    progress: Progress,
) -> None:
    downstream_endpoints = np.full((len(native_ids), 2), np.nan)
    successor_match = np.full(len(native_ids), -1, dtype="int8")
    successor_conflict = np.zeros(len(native_ids), dtype=bool)
    indeterminate = np.zeros(len(native_ids), dtype=bool)
    short_resolved = np.zeros(len(native_ids), dtype=bool)
    near_resolved = np.zeros(len(native_ids), dtype=bool)
    endpoint_proven = 0
    position = 0
    for row_value in topology[::-1]:
        row = int(row_value)
        successor = int(downstream_rows[row])
        if successor < 0:
            continue
        position += 1
        indexes = [int(value) for value in np.flatnonzero(current_indexes[row])]
        if len(indexes) == 1:
            current_index = indexes[0]
            if not degenerate[row]:
                endpoint_proven += 1
        else:
            (
                current_id,
                successor_id,
                successor_upstream_index,
                successor_downstream_index,
            ) = _raise_before_3825(
                native_ids,
                downstream_rows,
                areas_km2,
                degenerate,
                downstream_endpoints,
                endpoints,
                row,
                successor,
            )
            source_successor_indexes = [
                int(value) for value in np.flatnonzero(matches[row, 1])
            ]
            if source_successor_indexes == [successor_downstream_index]:
                actual_message = (
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} contradicts "
                    "source vertex order: downstream-nondecreasing DSContArea "
                    f"{float(areas_km2[row])!r} km2 -> "
                    f"{float(areas_km2[successor])!r} km2 identifies "
                    f"successor endpoint {successor_upstream_index} as upstream, but "
                    "source endpoint 1 matches successor downstream endpoint "
                    f"{successor_downstream_index}"
                )
                if (
                    current_id != REFUSAL_LINKNO
                    or successor_id != REFUSAL_DSLINKNO
                    or successor_upstream_index
                    != EXPECTED_SUCCESSOR_UPSTREAM_INDEX
                    or successor_downstream_index
                    != EXPECTED_SUCCESSOR_DOWNSTREAM_INDEX
                    or float(areas_km2[row]) != EXPECTED_CURRENT_AREA_KM2
                    or float(areas_km2[successor])
                    != EXPECTED_SUCCESSOR_AREA_KM2
                    or position != expected_refusal_position
                ):
                    raise Disagreement(actual_message)
                return
            if source_successor_indexes != [successor_upstream_index]:
                raise Disagreement(
                    "orientation proof for native LINKNO "
                    f"{current_id} and downstream LINKNO {successor_id} cannot use "
                    "source vertex order: downstream-nondecreasing DSContArea "
                    f"{float(areas_km2[row])!r} km2 -> "
                    f"{float(areas_km2[successor])!r} km2 does not "
                    "distinguish the successor endpoint side"
                )
            current_index = 1
            near_resolved[row] = True
        downstream_endpoints[row] = endpoints[row, current_index]
        successor_indexes = [
            int(value) for value in np.flatnonzero(matches[row, current_index])
        ]
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
                and len(indexes) == 1
            ):
                short_resolved[row] = True
        progress.phase(f"simulated {position} connected traversal edges")
    raise Disagreement("reverse-topology traversal completed without a line-3825 match")


def _reconstruct(
    basins_path: Path, streamnet_path: Path, progress: Progress
) -> dict[str, object]:
    _verify_inputs(basins_path, streamnet_path, progress)
    with _connect(basins_path) as basins_connection:
        basin_ids = _read_basin_ids(basins_connection, progress)
    with _connect(streamnet_path) as streamnet_connection:
        native_ids, downstream_ids, areas, endpoints, degenerate = _read_streamnet(
            streamnet_connection, progress
        )
    basin_positions = np.searchsorted(native_ids, basin_ids)
    safe_positions = np.minimum(basin_positions, len(native_ids) - 1)
    missing_basin = (basin_positions == len(native_ids)) | (
        native_ids[safe_positions] != basin_ids
    )
    if np.any(missing_basin):
        raise Disagreement(
            "basins.streamID does not join to streamnet.LINKNO: "
            f"{int(basin_ids[int(np.flatnonzero(missing_basin)[0])])}"
        )
    refusal_row = _row_for_edge(
        native_ids,
        downstream_ids,
        REFUSAL_LINKNO,
        REFUSAL_DSLINKNO,
    )
    target_row = _row_for_edge(
        native_ids,
        downstream_ids,
        TARGET_LINKNO,
        TARGET_DSLINKNO,
    )
    downstream_rows = _downstream_rows(native_ids, downstream_ids)
    topology = _topological_order(native_ids, downstream_rows)
    if len(topology) != STREAM_ROW_COUNT:
        raise Disagreement(
            f"topology row count differs: expected {STREAM_ROW_COUNT}, got {len(topology)}"
        )
    refusal_position, target_position = _traversal_positions(
        topology, downstream_rows, refusal_row, target_row
    )
    progress.phase(
        "tie-broken topology and connected traversal positions reconstructed", force=True
    )
    matches, current_indexes = _match_endpoints(
        native_ids, downstream_rows, endpoints, degenerate, progress
    )
    areas_km2 = areas / 1_000_000
    _simulate_until_refusal(
        native_ids,
        downstream_rows,
        topology,
        areas_km2,
        endpoints,
        degenerate,
        matches,
        current_indexes,
        refusal_position,
        progress,
    )
    if target_position <= refusal_position:
        raise Disagreement(
            "target edge is not strictly later than refusal edge: "
            f"target position {target_position}, refusal position {refusal_position}"
        )
    edges_before = refusal_position - 1
    if edges_before < 0 or edges_before != refusal_position - 1:
        raise Disagreement(
            f"edges before refusal is inconsistent with position {refusal_position}"
        )
    _verify_inputs(basins_path, streamnet_path, progress)
    return {
        "algorithm_source_revision": ALGORITHM_SOURCE_REVISION,
        "condition_3825_matching_edge_count": None,
        "does_not_establish": LIMITATIONS,
        "edges_evaluated_before_refusal": edges_before,
        "first_3825_match": {
            "DSLINKNO": REFUSAL_DSLINKNO,
            "LINKNO": REFUSAL_LINKNO,
        },
        "implementation_path": IMPLEMENTATION_PATH,
        "imports_build_adapter": False,
        "position_basis": POSITION_BASIS,
        "recorded_refusal_is_first_3825_match": True,
        "refusal_traversal_position": refusal_position,
        "status": "agrees",
        "target_strictly_later": True,
        "target_traversal_position": target_position,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Independently reconstruct basin 2020003440 topology ordering."
    )
    parser.add_argument("--basins", type=Path, required=True)
    parser.add_argument("--streamnet", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    progress = Progress()
    try:
        result = _reconstruct(args.basins, args.streamnet, progress)
    except Disagreement as error:
        print(
            json.dumps(
                {"disagreement": str(error), "status": "disagrees"},
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
