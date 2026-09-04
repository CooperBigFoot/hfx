"""Test the per-unit comparison of two compiled outputs against an adjudicated outlet set."""

from __future__ import annotations

import copy
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import LineString, Polygon

import compare_unit_outlets as compare

BASIN_ID = "7020000010"
HEADER_NUMBER = 71
OFFSET = HEADER_NUMBER * 10_000_000


def _bbox_type() -> pa.DataType:
    return pa.struct(
        [
            pa.field("xmin", pa.float32(), nullable=False),
            pa.field("ymin", pa.float32(), nullable=False),
            pa.field("xmax", pa.float32(), nullable=False),
            pa.field("ymax", pa.float32(), nullable=False),
        ]
    )


def _bbox_array(bounds: list[tuple[float, float, float, float]]) -> pa.StructArray:
    columns = list(zip(*bounds))
    return pa.StructArray.from_arrays(
        [pa.array(np.asarray(column, dtype="float32"), type=pa.float32()) for column in columns],
        fields=list(_bbox_type()),
    )


def _unit(native: int, downstream: int | None, x: float, y: float) -> dict[str, object]:
    polygon = Polygon([(x, y), (x + 0.01, y), (x + 0.01, y + 0.01), (x, y + 0.01)])
    return {
        "native": native,
        "downstream": downstream,
        "parent_id": None,
        "area_km2": 1.0 + native,
        "up_area_km2": 10.0 + native,
        "outlet": (x + 0.005, y + 0.005),
        "bbox": polygon.bounds,
        "geometry": polygon.wkb,
        "stems": [
            (
                native,
                10.0 + native,
                None,
                (x, y, x + 0.01, y + 0.01),
                LineString([(x, y), (x + 0.01, y + 0.01)]).wkb,
            )
        ],
    }


def synthetic_units() -> list[dict[str, object]]:
    # A five-unit tree: 5 -> 3 -> 1 (root), 4 -> 1, plus isolated root 2.
    return [
        _unit(1, None, 0.0, 0.0),
        _unit(2, None, 1.0, 1.0),
        _unit(3, 1, 0.0, 0.02),
        _unit(4, 1, 0.02, 0.0),
        _unit(5, 3, 0.0, 0.04),
    ]


def write_dataset(root: Path, units: list[dict[str, object]], *, created_at: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "aux").mkdir(exist_ok=True)
    ids = [OFFSET + int(unit["native"]) for unit in units]
    upstream = {unit_id: [] for unit_id in ids}
    for unit in units:
        if unit["downstream"] is not None:
            upstream[OFFSET + int(unit["downstream"])].append(OFFSET + int(unit["native"]))
    catchments = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "level": pa.array([0] * len(units), type=pa.int16()),
            "parent_id": pa.array([unit["parent_id"] for unit in units], type=pa.int64()),
            "area_km2": pa.array([unit["area_km2"] for unit in units], type=pa.float32()),
            "up_area_km2": pa.array([unit["up_area_km2"] for unit in units], type=pa.float32()),
            "outlet_lon": pa.array([unit["outlet"][0] for unit in units], type=pa.float64()),
            "outlet_lat": pa.array([unit["outlet"][1] for unit in units], type=pa.float64()),
            "bbox": _bbox_array([unit["bbox"] for unit in units]),
            "geometry": pa.array([unit["geometry"] for unit in units], type=pa.binary()),
        }
    )
    pq.write_table(catchments, root / "catchments.parquet")
    bounds = [unit["bbox"] for unit in units]
    graph = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "level": pa.array([0] * len(units), type=pa.int16()),
            "upstream_ids": pa.array([sorted(upstream[unit_id]) for unit_id in ids], type=pa.list_(pa.int64())),
            "bbox_minx": pa.array([b[0] for b in bounds], type=pa.float32()),
            "bbox_miny": pa.array([b[1] for b in bounds], type=pa.float32()),
            "bbox_maxx": pa.array([b[2] for b in bounds], type=pa.float32()),
            "bbox_maxy": pa.array([b[3] for b in bounds], type=pa.float32()),
        }
    )
    pq.write_table(graph, root / "graph.parquet")
    stems = [(OFFSET + int(unit["native"]), stem) for unit in units for stem in unit["stems"]]
    snap = pa.table(
        {
            "id": pa.array([stem[0] for _, stem in stems], type=pa.int64()),
            "unit_id": pa.array([unit_id for unit_id, _ in stems], type=pa.int64()),
            "weight": pa.array([stem[1] for _, stem in stems], type=pa.float32()),
            "stem_role": pa.array([stem[2] for _, stem in stems], type=pa.string()),
            "bbox": _bbox_array([stem[3] for _, stem in stems]),
            "geometry": pa.array([stem[4] for _, stem in stems], type=pa.binary()),
        }
    )
    pq.write_table(snap, root / "aux" / "snap_stems.parquet")
    manifest = {
        "format_version": "0.3.0",
        "region": BASIN_ID,
        "unit_count": len(units),
        "created_at": created_at,
        "bbox": [0.0, 0.0, 1.01, 1.01],
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def move_outlet(units: list[dict[str, object]], native: int, dx: float, dy: float) -> None:
    for unit in units:
        if unit["native"] == native:
            x, y = unit["outlet"]
            unit["outlet"] = (x + dx, y + dy)
            return
    raise KeyError(native)


def expected_record(
    reference_root: Path,
    candidate_root: Path,
    linknos: list[int],
    *,
    max_shift: float,
) -> dict[str, object]:
    reference = compare.read_dataset_units(reference_root)
    candidate = compare.read_dataset_units(candidate_root)
    return {
        "schema_version": 1,
        "processing_basin_id": BASIN_ID,
        "unit_count": len(reference.ids),
        "planetary_orientation_digest": compare.orientation_digest(reference, HEADER_NUMBER),
        "corrected_orientation_digest": compare.orientation_digest(candidate, HEADER_NUMBER),
        "downstream_differences": 0,
        "outlet_differences": len(linknos),
        "max_shift_deg": max_shift,
        "outlet_difference_native_linknos": sorted(linknos),
    }


class Fixture:
    """A reference build, a candidate build differing on units 3 and 2, and the matching record."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.reference_units = synthetic_units()
        self.candidate_units = copy.deepcopy(self.reference_units)
        move_outlet(self.candidate_units, 3, 0.0006, -0.0008)  # shift 0.001
        move_outlet(self.candidate_units, 2, 0.0003, 0.0004)  # shift 0.0005
        self.reference = directory / "reference"
        self.candidate = directory / "candidate"
        write_dataset(self.reference, self.reference_units, created_at="2026-08-19T14:03:32+00:00")
        write_dataset(self.candidate, self.candidate_units, created_at="2026-08-19T14:03:32+00:00")
        self.record = expected_record(self.reference, self.candidate, [2, 3], max_shift=0.0010000000000000002)
        self.expected_path = directory / "expected.json"
        self.expected_path.write_text(json.dumps(self.record, indent=2) + "\n")

    def write_candidate(self, units: list[dict[str, object]], *, created_at: str = "2026-08-19T14:03:32+00:00") -> Path:
        root = self.directory / "candidate-variant"
        write_dataset(root, units, created_at=created_at)
        return root

    def run(self, candidate: Path | None = None, expected: Path | None = None, *extra: str) -> tuple[int, dict[str, object], str]:
        report_path = self.directory / "report.json"
        argv = [
            "--reference",
            str(self.reference),
            "--candidate",
            str(candidate or self.candidate),
            "--expected",
            str(expected or self.expected_path),
            "--report",
            str(report_path),
            *extra,
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = compare.main(argv)
        return status, json.loads(report_path.read_text()), stderr.getvalue()


class ComparisonTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.fixture = Fixture(Path(self._tmp.name))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_exact_adjudicated_difference_is_accepted(self) -> None:
        status, report, _ = self.fixture.run()
        self.assertEqual(status, 0, report)
        self.assertEqual(report["verdict"], "accepted")
        self.assertEqual(report["outlet_differences"], 2)
        self.assertEqual(report["downstream_differences"], 0)
        self.assertEqual(report["catchment_geometry_differences"], 0)
        self.assertTrue(report["manifest_equal_except_created_at"])
        self.assertEqual(report["snap_stem_differences_inside_adjudicated_set"], 0)
        self.assertAlmostEqual(report["max_shift_deg"], 0.001, places=12)
        self.assertEqual(report["header_number"], HEADER_NUMBER)

    def test_created_at_difference_alone_is_accepted(self) -> None:
        variant = self.fixture.write_candidate(self.fixture.candidate_units, created_at="2026-09-10T00:00:00+00:00")
        status, report, _ = self.fixture.run(variant)
        self.assertEqual(status, 0, report)

    def test_orientation_digest_matches_orient_report_digest(self) -> None:
        candidate = compare.read_dataset_units(self.fixture.candidate)
        digest = compare.orientation_digest(candidate, HEADER_NUMBER)
        report_path = self.fixture.directory / "orient.json"
        report_path.write_text(json.dumps({"outcome": "resolved", "orientation_digest": digest}))
        status, report, _ = self.fixture.run(None, None, "--orient-report", str(report_path))
        self.assertEqual(status, 0, report)
        self.assertEqual(report["orient_report_digest"], digest)

        report_path.write_text(json.dumps({"outcome": "resolved", "orientation_digest": "0" * 64}))
        status, report, stderr = self.fixture.run(None, None, "--orient-report", str(report_path))
        self.assertEqual(status, 1)
        self.assertIn("orient report digest differs", stderr)

        report_path.write_text(json.dumps({"outcome": "refused", "orientation_digest": digest}))
        status, _, stderr = self.fixture.run(None, None, "--orient-report", str(report_path))
        self.assertEqual(status, 1)
        self.assertIn("did not resolve", stderr)

    def test_outlet_moved_outside_adjudicated_set_refuses(self) -> None:
        units = copy.deepcopy(self.fixture.candidate_units)
        move_outlet(units, 4, 0.0001, 0.0)
        status, report, stderr = self.fixture.run(self.fixture.write_candidate(units))
        self.assertEqual(status, 1)
        self.assertEqual(report["outlet_differences_outside_adjudicated_set"], 1)
        self.assertIn("outside the adjudicated set: [4]", stderr)
        self.assertIn("candidate orientation digest differs", stderr)

    def test_adjudicated_unit_without_difference_refuses(self) -> None:
        units = copy.deepcopy(self.fixture.candidate_units)
        for unit in units:
            if unit["native"] == 2:
                unit["outlet"] = next(u["outlet"] for u in self.fixture.reference_units if u["native"] == 2)
        status, report, stderr = self.fixture.run(self.fixture.write_candidate(units))
        self.assertEqual(status, 1)
        self.assertEqual(report["adjudicated_units_without_difference"], 1)
        self.assertIn("kept their reference outlet: [2]", stderr)

    def test_shift_beyond_pinned_maximum_refuses(self) -> None:
        record = dict(self.fixture.record)
        record["max_shift_deg"] = 0.0009
        path = self.fixture.directory / "tight.json"
        path.write_text(json.dumps(record))
        status, _, stderr = self.fixture.run(None, path)
        self.assertEqual(status, 1)
        self.assertIn("exceeds pinned", stderr)

    def test_downstream_change_refuses(self) -> None:
        units = copy.deepcopy(self.fixture.candidate_units)
        for unit in units:
            if unit["native"] == 4:
                unit["downstream"] = 3
        status, report, stderr = self.fixture.run(self.fixture.write_candidate(units))
        self.assertEqual(status, 1)
        self.assertEqual(report["downstream_differences"], 1)
        self.assertIn("upstream_ids differ for 2 units", stderr)
        self.assertIn("downstream differences 1 differ from pinned 0", stderr)

    def test_polygon_or_attribute_change_refuses(self) -> None:
        units = copy.deepcopy(self.fixture.candidate_units)
        for unit in units:
            if unit["native"] == 5:
                unit["geometry"] = Polygon([(0, 0.04), (0.02, 0.04), (0.02, 0.06), (0, 0.06)]).wkb
                unit["area_km2"] = 99.0
        status, report, stderr = self.fixture.run(self.fixture.write_candidate(units))
        self.assertEqual(status, 1)
        self.assertEqual(report["catchment_geometry_differences"], 1)
        self.assertEqual(report["catchment_attribute_differences"]["area_km2"], 1)
        self.assertIn("geometry differs for 1 units", stderr)
        self.assertIn("column area_km2 differs for 1 units", stderr)

    def test_snap_stem_difference_is_tolerated_only_inside_the_set(self) -> None:
        inside = copy.deepcopy(self.fixture.candidate_units)
        for unit in inside:
            if unit["native"] == 3:
                stem = list(unit["stems"][0])
                stem[4] = LineString([(0.0, 0.02), (0.005, 0.025), (0.01, 0.03)]).wkb
                unit["stems"] = [tuple(stem)]
        status, report, _ = self.fixture.run(self.fixture.write_candidate(inside))
        self.assertEqual(status, 0, report)
        self.assertEqual(report["snap_stem_differences_inside_adjudicated_set"], 1)

        outside = copy.deepcopy(self.fixture.candidate_units)
        for unit in outside:
            if unit["native"] == 5:
                stem = list(unit["stems"][0])
                stem[1] = 1234.0
                unit["stems"] = [tuple(stem)]
        status, report, stderr = self.fixture.run(self.fixture.write_candidate(outside))
        self.assertEqual(status, 1)
        self.assertEqual(report["snap_stem_differences_outside_adjudicated_set"], 1)
        self.assertIn("snap stems differ for 1 units outside", stderr)

    def test_manifest_field_difference_refuses(self) -> None:
        variant = self.fixture.write_candidate(self.fixture.candidate_units)
        manifest = json.loads((variant / "manifest.json").read_text())
        manifest["unit_count"] = 99
        (variant / "manifest.json").write_text(json.dumps(manifest))
        status, _, stderr = self.fixture.run(variant)
        self.assertEqual(status, 1)
        self.assertIn("manifest differs beyond created_at", stderr)

    def test_unit_set_or_order_difference_refuses(self) -> None:
        units = copy.deepcopy(self.fixture.candidate_units)
        units.append(_unit(6, 1, 0.5, 0.5))
        status, _, stderr = self.fixture.run(self.fixture.write_candidate(units))
        self.assertEqual(status, 1)
        self.assertIn("unit ids or row order differ", stderr)

        reordered = copy.deepcopy(self.fixture.candidate_units)
        reordered.reverse()
        status, _, stderr = self.fixture.run(self.fixture.write_candidate(reordered))
        self.assertEqual(status, 1)
        self.assertIn("unit ids or row order differ", stderr)

    def test_pinned_digest_mismatch_refuses(self) -> None:
        record = dict(self.fixture.record)
        record["planetary_orientation_digest"] = "f" * 64
        path = self.fixture.directory / "wrong-reference.json"
        path.write_text(json.dumps(record))
        status, _, stderr = self.fixture.run(None, path)
        self.assertEqual(status, 1)
        self.assertIn("reference orientation digest differs", stderr)

    def test_same_directory_refuses(self) -> None:
        status, _, stderr = self.fixture.run(self.fixture.reference)
        self.assertEqual(status, 1)
        self.assertIn("same directory", stderr)


class ExpectedRecordTests(unittest.TestCase):
    def setUp(self) -> None:
        self.record = {
            "schema_version": 1,
            "processing_basin_id": BASIN_ID,
            "unit_count": 5,
            "planetary_orientation_digest": "a" * 64,
            "corrected_orientation_digest": "b" * 64,
            "downstream_differences": 0,
            "outlet_differences": 2,
            "max_shift_deg": 0.001,
            "outlet_difference_native_linknos": [2, 3],
        }

    def assert_refuses(self, message: str, **changes: object) -> None:
        record = dict(self.record)
        record.update(changes)
        with self.assertRaises(compare.ComparisonRefusal) as raised:
            compare.AdjudicatedOutletDifference.from_record(record)
        self.assertIn(message, str(raised.exception))

    def test_well_formed_record_parses(self) -> None:
        parsed = compare.AdjudicatedOutletDifference.from_record(self.record)
        self.assertEqual(parsed.outlet_difference_native_linknos, (2, 3))
        self.assertEqual(parsed.max_shift_deg, 0.001)

    def test_malformed_records_refuse(self) -> None:
        self.assert_refuses("schema_version", schema_version=2)
        self.assert_refuses("processing_basin_id", processing_basin_id=7020000010)
        self.assert_refuses("unit_count", unit_count=0)
        self.assert_refuses("planetary_orientation_digest", planetary_orientation_digest="A" * 64)
        self.assert_refuses("max_shift_deg", max_shift_deg="0.001")
        self.assert_refuses("max_shift_deg", max_shift_deg=float("nan"))
        self.assert_refuses("sorted and unique", outlet_difference_native_linknos=[3, 2])
        self.assert_refuses("sorted and unique", outlet_difference_native_linknos=[2, 2])
        self.assert_refuses("positive integers", outlet_difference_native_linknos=[2, "3"])
        self.assert_refuses("LINKNO list length", outlet_differences=3)
        self.assert_refuses("equal orientation digests", corrected_orientation_digest="a" * 64)

    def test_unreadable_record_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "expected.json"
            path.write_text("{")
            with self.assertRaises(compare.ComparisonRefusal):
                compare.load_expected_record(path)
            path.write_text("[]")
            with self.assertRaises(compare.ComparisonRefusal):
                compare.load_expected_record(path)


if __name__ == "__main__":
    unittest.main()
