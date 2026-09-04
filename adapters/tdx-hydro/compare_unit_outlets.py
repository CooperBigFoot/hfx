#!/usr/bin/env python3
"""Compare two compiled outputs of one processing basin unit by unit.

compare : (ReferenceDataset, CandidateDataset, AdjudicatedOutletDifference, OrientReport?)
          -> Accepted | Refused                        (total; refuses on any unpinned difference)

Both datasets are per-basin HFX outputs of the same TDX-Hydro processing
basin. The comparison accepts the candidate only when every drainage unit,
every same-level graph row, every polygon, and every non-outlet attribute is
identical to the reference, and the set of units whose outlet moved equals
exactly the adjudicated set of native LINKNOs pinned in the expected record,
with every shift within the pinned maximum. Snap stems may differ only for
units inside that set. The orientation digest of each dataset is recomputed
from its own graph and outlet columns and must equal the pinned digest for its
side. Nothing is written except the report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from build_adapter import TDX_LINKNO_SENTINEL, load_header_crosswalk

REPORT_SCHEMA_VERSION = 1
EXPECTED_SCHEMA_VERSION = 1
GEOMETRY_BATCH_SIZE = 4096
SHIFT_TOLERANCE_DEGREES = 1e-12
_SHA256_HEX_LENGTH = 64

CATCHMENT_OUTLET_COLUMNS = ("outlet_lon", "outlet_lat")
CATCHMENT_ATTRIBUTE_COLUMNS = ("level", "parent_id", "area_km2", "up_area_km2", "bbox")
GRAPH_COLUMNS = ("level", "upstream_ids", "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy")
SNAP_COLUMNS = ("id", "weight", "stem_role", "bbox")
MANIFEST_TIMESTAMP_FIELD = "created_at"


class ComparisonRefusal(ValueError):
    """Raised when the candidate differs from the reference beyond the adjudicated set."""


@dataclass(frozen=True)
class AdjudicatedOutletDifference:
    """The pinned, maintainer-adjudicated difference between two builds of one basin."""

    processing_basin_id: str
    unit_count: int
    reference_orientation_digest: str
    candidate_orientation_digest: str
    downstream_differences: int
    outlet_differences: int
    max_shift_deg: float
    outlet_difference_native_linknos: tuple[int, ...]

    @classmethod
    def from_record(cls, record: Mapping[str, object]) -> "AdjudicatedOutletDifference":
        if record.get("schema_version") != EXPECTED_SCHEMA_VERSION:
            raise ComparisonRefusal(
                f"expected record schema_version must be {EXPECTED_SCHEMA_VERSION}"
            )
        basin_id = record.get("processing_basin_id")
        if not isinstance(basin_id, str) or not basin_id.isdigit():
            raise ComparisonRefusal("expected record processing_basin_id must be a digit string")
        unit_count = _require_int(record, "unit_count", minimum=1)
        reference_digest = _require_sha256(record, "planetary_orientation_digest")
        candidate_digest = _require_sha256(record, "corrected_orientation_digest")
        downstream = _require_int(record, "downstream_differences", minimum=0)
        outlets = _require_int(record, "outlet_differences", minimum=0)
        max_shift = record.get("max_shift_deg")
        if (
            isinstance(max_shift, bool)
            or not isinstance(max_shift, (int, float))
            or not math.isfinite(max_shift)
            or max_shift < 0
        ):
            raise ComparisonRefusal("expected record max_shift_deg must be a finite non-negative number")
        linknos = record.get("outlet_difference_native_linknos")
        if not isinstance(linknos, list) or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in linknos
        ):
            raise ComparisonRefusal(
                "expected record outlet_difference_native_linknos must be a list of positive integers"
            )
        if linknos != sorted(set(linknos)):
            raise ComparisonRefusal(
                "expected record outlet_difference_native_linknos must be sorted and unique"
            )
        if len(linknos) != outlets:
            raise ComparisonRefusal(
                "expected record outlet_differences must equal the LINKNO list length"
            )
        if reference_digest == candidate_digest and outlets > 0:
            raise ComparisonRefusal(
                "expected record pins equal orientation digests with a nonempty difference set"
            )
        return cls(
            processing_basin_id=basin_id,
            unit_count=unit_count,
            reference_orientation_digest=reference_digest,
            candidate_orientation_digest=candidate_digest,
            downstream_differences=downstream,
            outlet_differences=outlets,
            max_shift_deg=float(max_shift),
            outlet_difference_native_linknos=tuple(linknos),
        )


def _require_int(record: Mapping[str, object], field: str, *, minimum: int) -> int:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ComparisonRefusal(f"expected record {field} must be an integer >= {minimum}")
    return value


def _require_sha256(record: Mapping[str, object], field: str) -> str:
    value = record.get(field)
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_HEX_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ComparisonRefusal(f"expected record {field} must be a lowercase SHA-256 hex digest")
    return value


def load_expected_record(path: Path) -> AdjudicatedOutletDifference:
    """Parse the tracked adjudication record; refuse on any malformed field."""
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ComparisonRefusal(f"expected record is unreadable: {path}: {error}") from error
    if not isinstance(record, Mapping):
        raise ComparisonRefusal("expected record must be a JSON object")
    return AdjudicatedOutletDifference.from_record(record)


@dataclass(frozen=True)
class DatasetUnits:
    """The per-unit content of one compiled per-basin output, keyed by row order."""

    ids: np.ndarray
    catchment_attributes: dict[str, np.ndarray]
    outlet_lons: np.ndarray
    outlet_lats: np.ndarray
    geometry_digests: np.ndarray
    graph_columns: dict[str, np.ndarray]
    graph_upstream: list[tuple[int, ...]]
    snap_by_unit: dict[int, tuple[tuple[object, ...], ...]]
    manifest: dict[str, object]


def _dataset_root(path: Path, label: str) -> Path:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        raise ComparisonRefusal(f"--{label} must be an absolute path: {path}")
    if resolved.is_symlink() or not resolved.is_dir():
        raise ComparisonRefusal(f"--{label} is not a regular directory: {path}")
    return resolved.resolve(strict=True)


def _read_columns(path: Path, columns: Sequence[str]) -> pa.Table:
    schema = pq.read_schema(path)
    missing = [column for column in columns if column not in schema.names]
    if missing:
        raise ComparisonRefusal(f"{path} lacks required columns: {missing}")
    return pq.read_table(path, columns=list(columns))


def _flatten_column(table: pa.Table, name: str) -> dict[str, np.ndarray]:
    column = table.column(name)
    if pa.types.is_struct(column.type):
        combined = column.combine_chunks()
        return {
            f"{name}.{field.name}": combined.field(index).to_numpy(zero_copy_only=False)
            for index, field in enumerate(column.type)
        }
    return {name: column.to_numpy(zero_copy_only=False)}


def _geometry_digests(path: Path, expected_ids: np.ndarray) -> np.ndarray:
    digests: list[str] = []
    ids: list[int] = []
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=GEOMETRY_BATCH_SIZE, columns=["id", "geometry"]):
        ids.extend(batch.column(0).to_pylist())
        for wkb in batch.column(1).to_pylist():
            if wkb is None:
                raise ComparisonRefusal(f"{path} carries a null geometry")
            digests.append(hashlib.sha256(wkb).hexdigest())
    if not np.array_equal(np.asarray(ids, dtype="int64"), expected_ids):
        raise ComparisonRefusal(f"{path} geometry scan disagrees with its id column")
    return np.asarray(digests, dtype=object)


def _manifest(root: Path) -> dict[str, object]:
    path = root / "manifest.json"
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ComparisonRefusal(f"manifest is unreadable: {path}: {error}") from error
    if not isinstance(manifest, dict) or MANIFEST_TIMESTAMP_FIELD not in manifest:
        raise ComparisonRefusal(f"manifest is not an object with {MANIFEST_TIMESTAMP_FIELD}: {path}")
    return manifest


def read_dataset_units(root: Path) -> DatasetUnits:
    """Load every per-unit column of one per-basin output into memory, geometry as digests."""
    catchments = _read_columns(
        root / "catchments.parquet",
        ("id",) + CATCHMENT_ATTRIBUTE_COLUMNS + CATCHMENT_OUTLET_COLUMNS,
    )
    ids = catchments.column("id").to_numpy(zero_copy_only=False).astype("int64", copy=False)
    if len(np.unique(ids)) != len(ids):
        raise ComparisonRefusal(f"{root}: catchments.parquet repeats a unit id")
    attributes: dict[str, np.ndarray] = {}
    for name in CATCHMENT_ATTRIBUTE_COLUMNS:
        attributes.update(_flatten_column(catchments, name))

    graph = _read_columns(root / "graph.parquet", ("id",) + GRAPH_COLUMNS)
    graph_ids = graph.column("id").to_numpy(zero_copy_only=False).astype("int64", copy=False)
    if not np.array_equal(graph_ids, ids):
        raise ComparisonRefusal(f"{root}: graph.parquet ids or row order differ from catchments.parquet")
    graph_columns: dict[str, np.ndarray] = {}
    for name in GRAPH_COLUMNS:
        if name == "upstream_ids":
            continue
        graph_columns.update(_flatten_column(graph, name))
    upstream = [tuple(int(value) for value in row) for row in graph.column("upstream_ids").to_pylist()]

    snap_path = root / "aux" / "snap_stems.parquet"
    snap_by_unit: dict[int, list[tuple[object, ...]]] = {}
    if snap_path.exists():
        snap = _read_columns(snap_path, ("unit_id",) + SNAP_COLUMNS)
        bbox = _flatten_column(snap, "bbox")
        unit_ids = snap.column("unit_id").to_pylist()
        stem_ids = snap.column("id").to_pylist()
        weights = snap.column("weight").to_pylist()
        roles = snap.column("stem_role").to_pylist()
        digests = _geometry_digests(snap_path, np.asarray(stem_ids, dtype="int64"))
        bbox_rows = list(zip(*(bbox[key].tolist() for key in sorted(bbox))))
        for index, unit_id in enumerate(unit_ids):
            snap_by_unit.setdefault(int(unit_id), []).append(
                (stem_ids[index], weights[index], roles[index], bbox_rows[index], digests[index])
            )
        unknown = set(snap_by_unit) - set(ids.tolist())
        if unknown:
            raise ComparisonRefusal(f"{root}: snap stems reference unknown units: {sorted(unknown)[:5]}")

    return DatasetUnits(
        ids=ids,
        catchment_attributes=attributes,
        outlet_lons=catchments.column("outlet_lon").to_numpy(zero_copy_only=False).astype("float64"),
        outlet_lats=catchments.column("outlet_lat").to_numpy(zero_copy_only=False).astype("float64"),
        geometry_digests=_geometry_digests(root / "catchments.parquet", ids),
        graph_columns=graph_columns,
        graph_upstream=upstream,
        snap_by_unit={unit: tuple(sorted(stems, key=repr)) for unit, stems in snap_by_unit.items()},
        manifest=_manifest(root),
    )


def downstream_native_ids(units: DatasetUnits, header_number: int) -> tuple[np.ndarray, np.ndarray]:
    """Invert the upstream lists into (sorted native ids, downstream native id or sentinel)."""
    offset = header_number * 10_000_000
    downstream: dict[int, int] = {}
    for unit_id, upstream in zip(units.ids.tolist(), units.graph_upstream):
        for upstream_id in upstream:
            if upstream_id in downstream:
                raise ComparisonRefusal(f"unit {upstream_id} has more than one downstream unit")
            downstream[upstream_id] = unit_id
    unknown = set(downstream) - set(units.ids.tolist())
    if unknown:
        raise ComparisonRefusal(f"graph names upstream units absent from the dataset: {sorted(unknown)[:5]}")
    order = np.argsort(units.ids, kind="stable")
    sorted_ids = units.ids[order]
    native = sorted_ids - offset
    if np.any(native <= 0) or np.any(native >= 10_000_000):
        raise ComparisonRefusal(f"unit ids do not carry header number {header_number}")
    downstream_native = np.asarray(
        [
            TDX_LINKNO_SENTINEL if unit_id not in downstream else downstream[unit_id] - offset
            for unit_id in sorted_ids.tolist()
        ],
        dtype="int64",
    )
    return native.astype("int64"), downstream_native


def orientation_digest(units: DatasetUnits, header_number: int) -> str:
    """SHA-256 over sorted native LINKNO, downstream native LINKNO, and outlet coordinates.

    This is the same byte layout `build_adapter.py orient` digests from source
    topology, so a compiled output and an orient report can be compared.
    """
    native, downstream = downstream_native_ids(units, header_number)
    order = np.argsort(units.ids, kind="stable")
    digest = hashlib.sha256()
    for values, dtype in (
        (native, "int64"),
        (downstream, "int64"),
        (units.outlet_lons[order], "float64"),
        (units.outlet_lats[order], "float64"),
    ):
        digest.update(np.ascontiguousarray(values, dtype=dtype).tobytes())
    return digest.hexdigest()


def _values_equal(left: np.ndarray, right: np.ndarray) -> np.ndarray:
    if left.dtype.kind == "f" and right.dtype.kind == "f":
        return (left == right) | (np.isnan(left) & np.isnan(right))
    if left.dtype.kind == "O" or right.dtype.kind == "O":
        return np.asarray([a == b for a, b in zip(left.tolist(), right.tolist())], dtype=bool)
    return left == right


def _manifest_without_timestamp(manifest: Mapping[str, object]) -> str:
    return json.dumps(
        {key: value for key, value in manifest.items() if key != MANIFEST_TIMESTAMP_FIELD},
        sort_keys=True,
    )


def compare_unit_outlets(
    reference: DatasetUnits,
    candidate: DatasetUnits,
    expected: AdjudicatedOutletDifference,
    *,
    header_number: int,
    orient_report_digest: str | None = None,
) -> dict[str, object]:
    """Compare candidate against reference and the pinned adjudicated difference.

    Returns the comparison report. Raises ComparisonRefusal on the first
    difference the record does not pin; the report carries `verdict`
    `accepted` only when every check passed.
    """
    offset = header_number * 10_000_000
    refusals: list[str] = []

    if len(reference.ids) != expected.unit_count:
        refusals.append(f"reference unit count {len(reference.ids)} differs from pinned {expected.unit_count}")
    if len(candidate.ids) != len(reference.ids) or not np.array_equal(reference.ids, candidate.ids):
        refusals.append("candidate unit ids or row order differ from reference")
        raise ComparisonRefusal("; ".join(refusals))

    manifest_equal = _manifest_without_timestamp(reference.manifest) == _manifest_without_timestamp(
        candidate.manifest
    )
    if not manifest_equal:
        refusals.append(f"manifest differs beyond {MANIFEST_TIMESTAMP_FIELD}")

    attribute_differences: dict[str, int] = {}
    for name in sorted(reference.catchment_attributes):
        if name not in candidate.catchment_attributes:
            refusals.append(f"candidate catchments.parquet lacks column {name}")
            continue
        unequal = ~_values_equal(reference.catchment_attributes[name], candidate.catchment_attributes[name])
        count = int(np.count_nonzero(unequal))
        attribute_differences[name] = count
        if count:
            refusals.append(f"catchments.parquet column {name} differs for {count} units")
    geometry_differences = int(
        np.count_nonzero(~_values_equal(reference.geometry_digests, candidate.geometry_digests))
    )
    if geometry_differences:
        refusals.append(f"catchments.parquet geometry differs for {geometry_differences} units")

    graph_differences: dict[str, int] = {}
    for name in sorted(reference.graph_columns):
        if name not in candidate.graph_columns:
            refusals.append(f"candidate graph.parquet lacks column {name}")
            continue
        count = int(np.count_nonzero(~_values_equal(reference.graph_columns[name], candidate.graph_columns[name])))
        graph_differences[name] = count
        if count:
            refusals.append(f"graph.parquet column {name} differs for {count} units")
    upstream_differences = sum(
        1 for left, right in zip(reference.graph_upstream, candidate.graph_upstream) if left != right
    )
    graph_differences["upstream_ids"] = upstream_differences
    if upstream_differences:
        refusals.append(f"graph.parquet upstream_ids differ for {upstream_differences} units")
    reference_native, reference_downstream = downstream_native_ids(reference, header_number)
    candidate_native, candidate_downstream = downstream_native_ids(candidate, header_number)
    downstream_differences = int(np.count_nonzero(reference_downstream != candidate_downstream))
    if not np.array_equal(reference_native, candidate_native):
        refusals.append("native LINKNO sets differ")
    if downstream_differences != expected.downstream_differences:
        refusals.append(
            f"downstream differences {downstream_differences} differ from pinned {expected.downstream_differences}"
        )

    outlet_moved = (reference.outlet_lons != candidate.outlet_lons) | (
        reference.outlet_lats != candidate.outlet_lats
    )
    moved_rows = np.flatnonzero(outlet_moved)
    observed_linknos = sorted(int(value) for value in (reference.ids[moved_rows] - offset).tolist())
    expected_linknos = list(expected.outlet_difference_native_linknos)
    unexpected = sorted(set(observed_linknos) - set(expected_linknos))
    missing = sorted(set(expected_linknos) - set(observed_linknos))
    if unexpected:
        refusals.append(f"{len(unexpected)} units moved outlets outside the adjudicated set: {unexpected[:10]}")
    if missing:
        refusals.append(f"{len(missing)} adjudicated units kept their reference outlet: {missing[:10]}")
    shifts = np.hypot(
        candidate.outlet_lons[moved_rows] - reference.outlet_lons[moved_rows],
        candidate.outlet_lats[moved_rows] - reference.outlet_lats[moved_rows],
    )
    max_shift = float(shifts.max()) if len(shifts) else 0.0
    if max_shift > expected.max_shift_deg + SHIFT_TOLERANCE_DEGREES:
        refusals.append(f"max outlet shift {max_shift!r} deg exceeds pinned {expected.max_shift_deg!r}")

    adjudicated_set = set(expected_linknos)
    snap_outside = 0
    snap_inside = 0
    for unit_id in reference.ids.tolist():
        left = reference.snap_by_unit.get(unit_id, ())
        right = candidate.snap_by_unit.get(unit_id, ())
        if left == right:
            continue
        if unit_id - offset in adjudicated_set:
            snap_inside += 1
        else:
            snap_outside += 1
    if snap_outside:
        refusals.append(f"snap stems differ for {snap_outside} units outside the adjudicated set")
    if set(candidate.snap_by_unit) != set(reference.snap_by_unit):
        refusals.append("snap stem unit coverage differs")

    reference_digest = orientation_digest(reference, header_number)
    candidate_digest = orientation_digest(candidate, header_number)
    if reference_digest != expected.reference_orientation_digest:
        refusals.append("reference orientation digest differs from the pinned planetary digest")
    if candidate_digest != expected.candidate_orientation_digest:
        refusals.append("candidate orientation digest differs from the pinned corrected digest")
    if orient_report_digest is not None and orient_report_digest != candidate_digest:
        refusals.append("orient report digest differs from the candidate dataset digest")

    report: dict[str, object] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "processing_basin_id": expected.processing_basin_id,
        "header_number": header_number,
        "unit_count": int(len(reference.ids)),
        "manifest_equal_except_created_at": manifest_equal,
        "catchment_attribute_differences": attribute_differences,
        "catchment_geometry_differences": geometry_differences,
        "graph_differences": graph_differences,
        "downstream_differences": downstream_differences,
        "outlet_differences": int(len(moved_rows)),
        "outlet_differences_expected": expected.outlet_differences,
        "outlet_differences_outside_adjudicated_set": len(unexpected),
        "adjudicated_units_without_difference": len(missing),
        "max_shift_deg": max_shift,
        "max_shift_deg_pinned": expected.max_shift_deg,
        "snap_stem_differences_inside_adjudicated_set": snap_inside,
        "snap_stem_differences_outside_adjudicated_set": snap_outside,
        "reference_orientation_digest": reference_digest,
        "candidate_orientation_digest": candidate_digest,
        "orient_report_digest": orient_report_digest,
        "refusals": refusals,
        "verdict": "accepted" if not refusals else "refused",
    }
    return report


def _orient_report_digest(path: Path) -> str:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ComparisonRefusal(f"orient report is unreadable: {path}: {error}") from error
    if not isinstance(report, dict) or report.get("outcome") != "resolved":
        raise ComparisonRefusal(f"orient report did not resolve: {path}")
    digest = report.get("orientation_digest")
    if not isinstance(digest, str) or len(digest) != _SHA256_HEX_LENGTH:
        raise ComparisonRefusal(f"orient report lacks an orientation_digest: {path}")
    return digest


def _write_report(report: Mapping[str, object], path: Path) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare two compiled outputs of one processing basin against an adjudicated outlet difference."
    )
    parser.add_argument("--reference", required=True, type=Path, help="reference per-basin HFX root")
    parser.add_argument("--candidate", required=True, type=Path, help="candidate per-basin HFX root")
    parser.add_argument("--expected", required=True, type=Path, help="adjudicated difference record (JSON)")
    parser.add_argument("--report", required=True, type=Path, help="comparison report to write (JSON)")
    parser.add_argument(
        "--orient-report",
        type=Path,
        default=None,
        help="orient report whose orientation_digest must equal the candidate's",
    )
    arguments = parser.parse_args(argv)
    try:
        expected = load_expected_record(arguments.expected)
        header_number = load_header_crosswalk()[expected.processing_basin_id]
        reference_root = _dataset_root(arguments.reference, "reference")
        candidate_root = _dataset_root(arguments.candidate, "candidate")
        if reference_root == candidate_root:
            raise ComparisonRefusal("reference and candidate resolve to the same directory")
        orient_digest = (
            _orient_report_digest(arguments.orient_report) if arguments.orient_report is not None else None
        )
        report = compare_unit_outlets(
            read_dataset_units(reference_root),
            read_dataset_units(candidate_root),
            expected,
            header_number=header_number,
            orient_report_digest=orient_digest,
        )
    except (ComparisonRefusal, KeyError) as error:
        report = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "verdict": "refused",
            "refusals": [str(error)],
        }
        _write_report(report, arguments.report)
        print(json.dumps(report, indent=2, sort_keys=True))
        print(f"compare-unit-outlets: refused: {error}", file=sys.stderr)
        return 1
    _write_report(report, arguments.report)
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["verdict"] != "accepted":
        print("compare-unit-outlets: refused: " + "; ".join(report["refusals"]), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
