#!/usr/bin/env python3
"""Prove fixed-domain GeoPandas Hilbert-key parity across pinned environments."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import platform
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from dataclasses import dataclass
from itertools import zip_longest
from pathlib import Path
from typing import Sequence

REFERENCE = "a3e8a2b"
WORLD_BOUNDS = [-180, -90, 180, 90]
HILBERT_LEVEL = 16
RANDOM_SEED = 20260724
RANDOM_GEOMETRY_COUNT = 100_000
TDX_SOURCE_CELL_DEGREES = 0.4 / 3600.0

PACKAGE_NAMES = (
    "geopandas",
    "numpy",
    "pandas",
    "shapely",
    "pyproj",
    "packaging",
    "pyogrio",
)
EXPECTED_ENVIRONMENTS = {
    "vm-stack-geopandas-1.1.3": {
        "python": "3.12.11",
        "geopandas": "1.1.3",
        "numpy": "2.4.6",
        "pandas": "3.0.3",
        "shapely": "2.1.2",
        "pyproj": "3.7.2",
        "packaging": "26.2",
        "pyogrio": "0.12.1",
    },
    "vm-stack-geopandas-1.1.4": {
        "python": "3.12.11",
        "geopandas": "1.1.4",
        "numpy": "2.4.6",
        "pandas": "3.0.3",
        "shapely": "2.1.2",
        "pyproj": "3.7.2",
        "packaging": "26.2",
        "pyogrio": "0.12.1",
    },
    "adapter-lock-geopandas-1.1.4": {
        "python": "3.13.8",
        "geopandas": "1.1.4",
        "numpy": "2.5.1",
        "pandas": "3.0.3",
        "shapely": "2.1.2",
        "pyproj": "3.7.2",
        "packaging": "26.2",
        "pyogrio": "0.13.0",
    },
}


class ParityError(RuntimeError):
    """Raised when parity verification cannot complete."""


class CorpusError(ParityError):
    """Raised when deterministic corpus data violates its contract."""


class PairFileError(ParityError):
    """Raised when a Hilbert pair file is malformed."""


class PairComparisonError(ParityError):
    """Raised when two Hilbert pair files diverge."""


class ResultDocumentError(ParityError):
    """Raised when VM confirmation hashes cannot be read uniquely."""


class HashMismatchError(ParityError):
    """Raised when a VM-confirm artifact hash differs from its reference."""


@dataclass(frozen=True)
class CorpusRecord:
    """Store one parsed corpus record."""

    index: int
    category: str
    wkb_hex: str


@dataclass(frozen=True)
class ArtifactResult:
    """Describe a deterministic file artifact."""

    row_count: int
    sha256: str


@dataclass(frozen=True)
class PairRecord:
    """Store one parsed Hilbert pair."""

    index: int
    key: int


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _package_versions() -> dict[str, str]:
    return {name: importlib.metadata.version(name) for name in PACKAGE_NAMES}


def _assert_environment(label: str) -> dict[str, str]:
    try:
        expected = EXPECTED_ENVIRONMENTS[label]
    except KeyError as error:
        raise ParityError(f"unknown worker environment label: {label}") from error
    actual = {"python": platform.python_version(), **_package_versions()}
    mismatches = [
        f"{name}: expected {expected[name]}, actual {actual[name]}"
        for name in expected
        if expected[name] != actual[name]
    ]
    if mismatches:
        raise ParityError(f"{label} environment mismatch: {'; '.join(mismatches)}")
    return actual


def tie_groups() -> tuple[tuple[object, ...], tuple[object, ...]]:
    """Return the two fixed groups of geometries with identical centroids."""
    from shapely.geometry import LineString, Point, Polygon

    return (
        (
            Point(0, 0),
            LineString([(-1, 0), (1, 0)]),
            Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)]),
            Polygon([(0, -2), (2, 0), (0, 2), (-2, 0)]),
        ),
        (
            Point(45, -30),
            LineString([(44, -30), (46, -30)]),
            Polygon([(44, -31), (46, -31), (46, -29), (44, -29)]),
            Polygon([(45, -32), (47, -30), (45, -28), (43, -30)]),
        ),
    )


def _curated_geometries() -> list[tuple[str, object]]:
    from shapely.geometry import LineString, Point, Polygon

    records: list[tuple[str, object]] = []
    polygon_vertices = (
        (
            (-123.4, 48.7),
            (-122.8, 48.9),
            (-122.1, 48.4),
            (-121.7, 47.8),
            (-122.0, 47.1),
            (-122.7, 46.8),
            (-123.2, 47.3),
        ),
        (
            (5.8, 47.9),
            (7.1, 48.5),
            (8.4, 48.1),
            (9.0, 47.2),
            (8.2, 46.4),
            (7.0, 46.1),
            (6.2, 46.8),
            (6.8, 47.3),
        ),
        (
            (31.2, -2.0),
            (32.4, -1.1),
            (33.1, -1.8),
            (33.6, -3.0),
            (32.9, -4.2),
            (31.7, -4.6),
            (30.9, -3.5),
        ),
        (
            (146.0, -38.2),
            (147.3, -37.5),
            (148.1, -38.0),
            (148.6, -39.1),
            (147.8, -40.0),
            (146.5, -39.6),
            (145.8, -38.9),
        ),
    )
    records.extend(("real-polygon", Polygon(vertices)) for vertices in polygon_vertices)

    line_vertices = (
        (
            (-123.3, 48.8),
            (-123.0, 48.2),
            (-122.6, 47.9),
            (-122.8, 47.4),
            (-122.2, 46.9),
        ),
        (
            (6.0, 48.4),
            (6.7, 48.0),
            (7.4, 47.7),
            (7.1, 47.1),
            (8.0, 46.6),
            (8.8, 46.3),
        ),
        (
            (31.3, -1.2),
            (31.8, -1.9),
            (31.6, -2.8),
            (32.4, -3.3),
            (33.0, -4.1),
        ),
        (
            (146.1, -37.7),
            (146.8, -38.1),
            (147.2, -38.8),
            (147.9, -39.1),
            (148.3, -39.8),
        ),
    )
    records.extend(("real-line", LineString(vertices)) for vertices in line_vertices)

    corners = ((-180, -90), (-180, 90), (180, -90), (180, 90))
    edge_midpoints = ((-180, 0), (180, 0), (0, -90), (0, 90))
    records.extend(("world-boundary", Point(point)) for point in corners + edge_midpoints)
    inward_points = (
        (math.nextafter(-180.0, 0.0), 0),
        (math.nextafter(180.0, 0.0), 0),
        (0, math.nextafter(-90.0, 0.0)),
        (0, math.nextafter(90.0, 0.0)),
    )
    records.extend(("world-boundary", Point(point)) for point in inward_points)
    edge_lines = (
        LineString([(-180, -1), (-180, 1)]),
        LineString([(180, -1), (180, 1)]),
        LineString([(-1, -90), (1, -90)]),
        LineString([(-1, 90), (1, 90)]),
    )
    records.extend(("world-boundary", geometry) for geometry in edge_lines)
    corner_polygons = (
        Polygon([(-180, -90), (-180 + 2**-20, -90), (-180, -90 + 2**-20)]),
        Polygon([(-180, 90), (-180 + 2**-20, 90), (-180, 90 - 2**-20)]),
        Polygon([(180, -90), (180 - 2**-20, -90), (180, -90 + 2**-20)]),
        Polygon([(180, 90), (180 - 2**-20, 90), (180, 90 - 2**-20)]),
    )
    records.extend(("world-boundary", geometry) for geometry in corner_polygons)

    half_cell = TDX_SOURCE_CELL_DEGREES / 2
    cell = TDX_SOURCE_CELL_DEGREES
    west = (
        -180.0,
        -180.0 + half_cell,
        -180.0 - half_cell,
        -180.0 - cell,
        math.nextafter(-180.0 - cell, -math.inf),
    )
    east = (
        180.0,
        180.0 - half_cell,
        180.0 + half_cell,
        180.0 + cell,
        math.nextafter(180.0 + cell, math.inf),
    )
    south = (
        -90.0,
        -90.0 + half_cell,
        -90.0 - half_cell,
        -90.0 - cell,
        math.nextafter(-90.0 - cell, -math.inf),
    )
    north = (
        90.0,
        90.0 - half_cell,
        90.0 + half_cell,
        90.0 + cell,
        math.nextafter(90.0 + cell, math.inf),
    )
    records.extend(("clamp-adjacent", Point(x, 0)) for x in west + east)
    records.extend(("clamp-adjacent", Point(0, y)) for y in south + north)
    records.extend(
        ("clamp-adjacent", Point(x, y))
        for x, y in (
            (-180.0 - half_cell, -90.0 - half_cell),
            (-180.0 - half_cell, 90.0 + half_cell),
            (180.0 + half_cell, -90.0 - half_cell),
            (180.0 + half_cell, 90.0 + half_cell),
        )
    )

    groups = tie_groups()
    for group in groups:
        records.extend(("hilbert-tie", geometry) for geometry in group)

    tiny = (
        LineString([(12.0, 34.0), (12.0 + 2**-40, 34.0 + 2**-40)]),
        Polygon([(12.0, 34.0), (12.0 + 2**-36, 34.0), (12.0, 34.0 + 2**-36)]),
        LineString([(-180.0, 0.0), (-180.0 + 2**-40, 2**-40)]),
        Polygon(
            [
                (180.0 - 2**-35, 90.0 - 2**-35),
                (180.0, 90.0 - 2**-35),
                (180.0 - 2**-35, 90.0),
            ]
        ),
    )
    records.extend(("tiny-valid", geometry) for geometry in tiny)
    return records


def _randomized_geometries(
    seed: int, random_count: int
) -> list[tuple[str, object]]:
    from shapely.geometry import LineString, Point, Polygon

    if random_count < 4:
        raise CorpusError("randomized corpus requires at least four records")
    scale = 2**20
    max_offset = 2**14
    rng = random.Random(seed)
    geometries: list[object] = [
        Point((-180 * scale + 1) / scale, 0),
        Point((180 * scale - 1) / scale, 0),
        Point(0, (-90 * scale + 1) / scale),
        Point(0, (90 * scale - 1) / scale),
    ]

    def coordinate(x_value: int, y_value: int) -> tuple[float, float]:
        x = min(180.0, max(-180.0, x_value / scale))
        y = min(90.0, max(-90.0, y_value / scale))
        return x, y

    for index in range(4, random_count):
        dx = rng.randint(1, max_offset)
        dy = rng.randint(1, max_offset)
        x = rng.randint(-180 * scale + 2 * max_offset, 180 * scale - 2 * max_offset)
        y = rng.randint(-90 * scale + 2 * max_offset, 90 * scale - 2 * max_offset)
        constructor = (index - 4) % 3
        if constructor == 0:
            geometry = Point(coordinate(x, y))
        elif constructor == 1:
            geometry = LineString(
                [
                    coordinate(x - dx, y - dy),
                    coordinate(x, y + dy),
                    coordinate(x + dx, y - dy),
                ]
            )
        else:
            geometry = Polygon(
                [
                    coordinate(x - 2 * dx, y),
                    coordinate(x - dx, y - dy),
                    coordinate(x + dx, y - dy),
                    coordinate(x + 2 * dx, y),
                    coordinate(x, y + 2 * dy),
                ]
            )
        geometries.append(geometry)

    quadrants = {
        (1 if geometry.centroid.x >= 0 else -1, 1 if geometry.centroid.y >= 0 else -1)
        for geometry in geometries
    }
    if quadrants != {(-1, -1), (-1, 1), (1, -1), (1, 1)}:
        raise CorpusError("randomized batch does not span all four quadrants")
    edge_coverage = (
        any(geometry.bounds[0] <= -180 + TDX_SOURCE_CELL_DEGREES for geometry in geometries),
        any(geometry.bounds[2] >= 180 - TDX_SOURCE_CELL_DEGREES for geometry in geometries),
        any(geometry.bounds[1] <= -90 + TDX_SOURCE_CELL_DEGREES for geometry in geometries),
        any(geometry.bounds[3] >= 90 - TDX_SOURCE_CELL_DEGREES for geometry in geometries),
    )
    if not all(edge_coverage):
        raise CorpusError("randomized batch does not cover every world edge")
    return [("randomized", geometry) for geometry in geometries]


def _validate_geometries(records: Sequence[tuple[str, object]]) -> None:
    for index, (category, geometry) in enumerate(records):
        if geometry is None or geometry.is_empty:
            raise CorpusError(f"empty or missing geometry at index {index} ({category})")
        if not geometry.is_valid:
            raise CorpusError(f"invalid geometry at index {index} ({category})")


def _assert_tie_centroids(groups: Sequence[Sequence[object]]) -> None:
    import shapely
    from shapely.geometry import Point

    for group_number, group in enumerate(groups):
        centroid_wkb = {
            shapely.to_wkb(
                Point(geometry.centroid.x + 0.0, geometry.centroid.y + 0.0),
                hex=True,
                byte_order=0,
                output_dimension=2,
            )
            for geometry in group
        }
        if len(centroid_wkb) != 1:
            raise CorpusError(f"tie group {group_number} centroids are not identical")


def generate_corpus(
    path: Path,
    *,
    seed: int = RANDOM_SEED,
    random_count: int = RANDOM_GEOMETRY_COUNT,
) -> ArtifactResult:
    """Generate the deterministic big-endian WKB corpus."""
    import shapely

    groups = tie_groups()
    _assert_tie_centroids(groups)
    records = _curated_geometries() + _randomized_geometries(seed, random_count)
    _validate_geometries(records)
    lines = []
    for index, (category, geometry) in enumerate(records):
        wkb_hex = shapely.to_wkb(
            geometry, hex=True, byte_order=0, output_dimension=2
        )
        lines.append(f"{index}\t{category}\t{wkb_hex}\n")
    data = "".join(lines).encode("utf-8")
    path.write_bytes(data)
    return ArtifactResult(row_count=len(records), sha256=_sha256(data))


_CORPUS_RECORD = re.compile(r"([0-9]+)\t([a-z][a-z0-9-]*)\t([0-9A-F]+)\n")


def read_corpus(
    path: Path, *, expected_random_count: int = RANDOM_GEOMETRY_COUNT
) -> list[CorpusRecord]:
    """Read and validate a deterministic WKB corpus."""
    import shapely

    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError as error:
        raise CorpusError(f"corpus is not UTF-8: {path}") from error
    records: list[CorpusRecord] = []
    for position, line in enumerate(text.splitlines(keepends=True)):
        match = _CORPUS_RECORD.fullmatch(line)
        if match is None:
            raise CorpusError(f"malformed corpus record at line {position + 1}")
        index = int(match.group(1))
        if index != position:
            raise CorpusError(
                f"non-contiguous or duplicate corpus index: expected {position}, got {index}"
            )
        wkb_hex = match.group(3)
        try:
            geometry = shapely.from_wkb(bytes.fromhex(wkb_hex))
        except (ValueError, shapely.errors.GEOSException) as error:
            raise CorpusError(f"malformed WKB at corpus index {index}") from error
        if geometry is None or geometry.is_empty:
            raise CorpusError(f"empty or missing geometry at corpus index {index}")
        records.append(CorpusRecord(index, match.group(2), wkb_hex))
    if not records:
        raise CorpusError("corpus is empty")
    random_count = sum(record.category == "randomized" for record in records)
    if random_count != expected_random_count:
        raise CorpusError(
            f"randomized record count: expected {expected_random_count}, got {random_count}"
        )
    return records


def calculate_pairs(
    corpus_path: Path, pairs_path: Path
) -> tuple[ArtifactResult, str]:
    """Calculate Hilbert pairs through the public GeoPandas API."""
    import geopandas
    import shapely

    records = read_corpus(corpus_path)
    geometries = geopandas.GeoSeries(
        [shapely.from_wkb(bytes.fromhex(record.wkb_hex)) for record in records]
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Geometry is in a geographic CRS.*",
            category=UserWarning,
        )
        keys = geometries.centroid.hilbert_distance(total_bounds=WORLD_BOUNDS)
    if len(keys) != len(records):
        raise PairFileError(
            f"Hilbert output count: expected {len(records)}, got {len(keys)}"
        )
    integer_keys: list[int] = []
    for index, key in enumerate(keys):
        integer_key = int(key)
        if integer_key != key or not 0 <= integer_key <= 2**32 - 1:
            raise PairFileError(f"non-uint32 Hilbert key at index {index}: {key!r}")
        integer_keys.append(integer_key)
    tie_indices = [
        index
        for index, record in enumerate(records)
        if record.category == "hilbert-tie"
    ]
    for group_start in range(0, len(tie_indices), 4):
        group_keys = {
            integer_keys[index] for index in tie_indices[group_start : group_start + 4]
        }
        if len(group_keys) != 1:
            raise PairFileError(
                f"tie group at corpus index {tie_indices[group_start]} emitted unequal keys"
            )
    data = "".join(
        f"{record.index}\t{key}\n"
        for record, key in zip(records, integer_keys, strict=True)
    ).encode("utf-8")
    pairs_path.write_bytes(data)
    return ArtifactResult(len(records), _sha256(data)), _sha256(corpus_path.read_bytes())


_PAIR_RECORD = re.compile(r"([0-9]+)\t([0-9]+)\n")


def read_pair_file(path: Path) -> list[PairRecord]:
    """Read and validate a contiguous Hilbert pair file."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeError as error:
        raise PairFileError(f"pair file is not UTF-8: {path}") from error
    records: list[PairRecord] = []
    for position, line in enumerate(text.splitlines(keepends=True)):
        match = _PAIR_RECORD.fullmatch(line)
        if match is None:
            raise PairFileError(f"malformed pair record at line {position + 1}: {path}")
        index = int(match.group(1))
        key = int(match.group(2))
        if index != position:
            raise PairFileError(
                f"non-contiguous or duplicate pair index: expected {position}, got {index}"
            )
        if key > 2**32 - 1:
            raise PairFileError(f"pair key outside uint32 range at index {index}")
        records.append(PairRecord(index, key))
    if not records:
        raise PairFileError(f"pair file is empty: {path}")
    return records


def compare_pair_files(
    left_path: Path, left_label: str, right_path: Path, right_label: str
) -> ArtifactResult:
    """Require two complete pair files to be byte-identical."""
    left_bytes = left_path.read_bytes()
    right_bytes = right_path.read_bytes()
    left_records = read_pair_file(left_path)
    right_records = read_pair_file(right_path)
    if left_bytes != right_bytes:
        for left, right in zip_longest(left_records, right_records):
            if left is None:
                index = right.index
                left_key = "<missing>"
                right_key = str(right.key)
            elif right is None:
                index = left.index
                left_key = str(left.key)
                right_key = "<missing>"
            elif left.key != right.key:
                index = left.index
                left_key = str(left.key)
                right_key = str(right.key)
            else:
                continue
            raise PairComparisonError(
                f"{left_label} vs {right_label} diverged at index {index}: "
                f"{left_label}={left_key}, {right_label}={right_key}"
            )
        raise PairComparisonError(
            f"{left_label} vs {right_label} pair bytes differ without a key divergence"
        )
    return ArtifactResult(len(left_records), _sha256(left_bytes))


_CORPUS_HASH_FIELD = re.compile(
    r"^- VM-confirm corpus SHA-256: (.*)$", re.MULTILINE
)
_PAIR_HASH_FIELD = re.compile(
    r"^- VM-confirm macOS pair SHA-256: (.*)$", re.MULTILINE
)


def parse_vm_confirmation_hashes(text: str) -> tuple[str, str]:
    """Parse the two unique VM-confirm hashes from the result document."""
    corpus_matches = _CORPUS_HASH_FIELD.findall(text)
    pair_matches = _PAIR_HASH_FIELD.findall(text)
    if len(corpus_matches) != 1:
        raise ResultDocumentError(
            "result document must contain exactly one VM-confirm corpus SHA-256 field"
        )
    if len(pair_matches) != 1:
        raise ResultDocumentError(
            "result document must contain exactly one VM-confirm macOS pair SHA-256 field"
        )
    parsed: list[str] = []
    for name, field in (
        ("VM-confirm corpus SHA-256", corpus_matches[0]),
        ("VM-confirm macOS pair SHA-256", pair_matches[0]),
    ):
        match = re.fullmatch(r"`([0-9a-f]{64})`\.", field)
        if match is None:
            raise ResultDocumentError(f"{name} must be 64 lowercase hexadecimal characters")
        parsed.append(match.group(1))
    return parsed[0], parsed[1]


def require_hash_match(kind: str, expected: str, actual: str) -> None:
    """Require an artifact hash to equal its committed reference."""
    if expected != actual:
        raise HashMismatchError(
            f"{kind} mismatch: expected {expected}, actual {actual}"
        )


def _write_metadata(
    path: Path,
    label: str,
    versions: dict[str, str],
    corpus_result: ArtifactResult,
    pair_result: ArtifactResult,
) -> None:
    metadata = {
        "corpus_row_count": corpus_result.row_count,
        "corpus_sha256": corpus_result.sha256,
        "effective_hilbert_level": HILBERT_LEVEL,
        "label": label,
        "packages": {name: versions[name] for name in PACKAGE_NAMES},
        "pair_row_count": pair_result.row_count,
        "pair_sha256": pair_result.sha256,
        "python": versions["python"],
        "reference": REFERENCE,
        "world_bounds": WORLD_BOUNDS,
    }
    path.write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _worker(args: argparse.Namespace) -> int:
    versions = _assert_environment(args.label)
    pair_result, corpus_hash = calculate_pairs(args.corpus, args.pairs)
    corpus_records = read_corpus(args.corpus)
    corpus_result = ArtifactResult(len(corpus_records), corpus_hash)
    _write_metadata(
        args.metadata, args.label, versions, corpus_result, pair_result
    )
    print(args.metadata.read_text(encoding="utf-8").rstrip())
    return 0


def _quoted(command: Sequence[str]) -> str:
    return shlex.join(command)


def _run_worker(label: str, command: list[str]) -> str:
    print(f"COMMAND {label}: {_quoted(command)}")
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        diagnostics = (
            f"worker {label} failed with return code {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        raise ParityError(diagnostics)
    metadata = result.stdout.strip()
    if result.stderr:
        raise ParityError(f"worker {label} emitted unexpected stderr:\n{result.stderr}")
    print(f"METADATA {label}: {metadata}")
    return metadata


def _orchestrate() -> int:
    started = time.monotonic()
    _assert_environment("adapter-lock-geopandas-1.1.4")
    uv = shutil.which("uv")
    if uv is None:
        raise ParityError("uv executable not found")
    verifier = Path(__file__).resolve()
    with tempfile.TemporaryDirectory(prefix="hfx-hilbert-parity-") as directory:
        run_dir = Path(directory)
        corpus = run_dir / "corpus.tsv"
        corpus_result = generate_corpus(corpus)
        artifacts = {
            label: (
                run_dir / f"{label}.pairs.tsv",
                run_dir / f"{label}.metadata.json",
            )
            for label in EXPECTED_ENVIRONMENTS
        }
        common = [
            "--with",
            "numpy==2.4.6",
            "--with",
            "pandas==3.0.3",
            "--with",
            "shapely==2.1.2",
            "--with",
            "pyproj==3.7.2",
            "--with",
            "packaging==26.2",
            "--with",
            "pyogrio==0.12.1",
        ]
        commands: dict[str, list[str]] = {}
        for label, geopandas_version in (
            ("vm-stack-geopandas-1.1.3", "1.1.3"),
            ("vm-stack-geopandas-1.1.4", "1.1.4"),
        ):
            pairs, metadata = artifacts[label]
            commands[label] = [
                uv,
                "run",
                "--no-project",
                "--python",
                "3.12.11",
                "--with",
                f"geopandas=={geopandas_version}",
                *common,
                "python",
                str(verifier),
                "worker",
                "--corpus",
                str(corpus),
                "--pairs",
                str(pairs),
                "--metadata",
                str(metadata),
                "--label",
                label,
            ]
        adapter_label = "adapter-lock-geopandas-1.1.4"
        adapter_pairs, adapter_metadata = artifacts[adapter_label]
        commands[adapter_label] = [
            sys.executable,
            str(verifier),
            "worker",
            "--corpus",
            str(corpus),
            "--pairs",
            str(adapter_pairs),
            "--metadata",
            str(adapter_metadata),
            "--label",
            adapter_label,
        ]
        for label in EXPECTED_ENVIRONMENTS:
            _run_worker(label, commands[label])

        comparison_specs = (
            ("vm-stack-geopandas-1.1.3", "vm-stack-geopandas-1.1.4"),
            ("vm-stack-geopandas-1.1.3", "adapter-lock-geopandas-1.1.4"),
            ("vm-stack-geopandas-1.1.4", "adapter-lock-geopandas-1.1.4"),
        )
        pair_results = {
            label: ArtifactResult(
                row_count=len(read_pair_file(paths[0])),
                sha256=_sha256(paths[0].read_bytes()),
            )
            for label, paths in artifacts.items()
        }
        comparison_results = []
        for left, right in comparison_specs:
            compare_pair_files(artifacts[left][0], left, artifacts[right][0], right)
            comparison_results.append((left, right))

        print(
            f"CORPUS: rows={corpus_result.row_count} sha256={corpus_result.sha256}"
        )
        for label, result in pair_results.items():
            print(f"PAIRS {label}: rows={result.row_count} sha256={result.sha256}")
        for left, right in comparison_results:
            print(f"BYTE-IDENTICAL: {left} vs {right}")
        print(f"ELAPSED_SECONDS: {time.monotonic() - started:.3f}")
        print(
            "GEOPANDAS VERSION PARITY ON MACOS: PASS - all three GeoPandas "
            "Hilbert outputs are byte-identical on macOS."
        )
    return 0


def _vm_confirm() -> int:
    required = {
        "python": "3.12.11",
        "geopandas": "1.1.3",
        "numpy": "2.4.6",
        "pandas": "3.0.3",
        "shapely": "2.1.2",
    }
    actual = {"python": platform.python_version()}
    actual.update(
        {
            name: importlib.metadata.version(name)
            for name in ("geopandas", "numpy", "pandas", "shapely")
        }
    )
    mismatches = [
        f"{name}: expected {required[name]}, actual {actual[name]}"
        for name in required
        if required[name] != actual[name]
    ]
    if mismatches:
        raise ParityError(f"vm-confirm environment mismatch: {'; '.join(mismatches)}")
    document = Path(__file__).resolve().with_name("GEOPANDAS-HILBERT-PARITY.md")
    if not document.is_file():
        raise ResultDocumentError(f"result document is missing: {document}")
    expected_corpus_hash, expected_pair_hash = parse_vm_confirmation_hashes(
        document.read_text(encoding="utf-8")
    )
    with tempfile.TemporaryDirectory(prefix="hfx-hilbert-vm-confirm-") as directory:
        run_dir = Path(directory)
        corpus = run_dir / "corpus.tsv"
        pairs = run_dir / "pairs.tsv"
        corpus_result = generate_corpus(corpus)
        require_hash_match(
            "corpus SHA-256", expected_corpus_hash, corpus_result.sha256
        )
        print(f"BYTE-IDENTICAL corpus SHA-256: {corpus_result.sha256}")
        pair_result, parsed_corpus_hash = calculate_pairs(corpus, pairs)
        require_hash_match(
            "corpus SHA-256", expected_corpus_hash, parsed_corpus_hash
        )
        require_hash_match("pair SHA-256", expected_pair_hash, pair_result.sha256)
        print(f"BYTE-IDENTICAL pair SHA-256: {pair_result.sha256}")
    print("VM-NATIVE HILBERT CONFIRMATION: PASS")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command")
    worker = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    worker.add_argument("--corpus", type=Path, required=True)
    worker.add_argument("--pairs", type=Path, required=True)
    worker.add_argument("--metadata", type=Path, required=True)
    worker.add_argument("--label", required=True)
    subparsers.add_parser(
        "vm-confirm",
        help="confirm committed corpus and pair hashes in the bootstrap VM environment",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run orchestration, an internal worker, or standalone VM confirmation."""
    args = _parser().parse_args(argv)
    try:
        if args.command == "worker":
            return _worker(args)
        if args.command == "vm-confirm":
            return _vm_confirm()
        return _orchestrate()
    except ParityError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
