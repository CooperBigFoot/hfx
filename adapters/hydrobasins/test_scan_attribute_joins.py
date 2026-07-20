"""Tests for the geometry-free HydroBASINS attribute-join scanner."""

from __future__ import annotations

import contextlib
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import pyogrio
from shapely.geometry import Point, box

import scan_attribute_joins


REGIONS = ("af", "ar", "as", "au", "eu", "gr", "na", "sa", "si")
LEVELS = tuple(f"{level:02d}" for level in range(1, 13))


def basin_id(region: str, level: str) -> int:
    return (REGIONS.index(region) + 1) * 100 + int(level)


def pfaf_id(region: str, level: str) -> int:
    digit = str(REGIONS.index(region) + 1)
    return int(digit * int(level))


def write_basin(
    root: Path,
    region: str,
    level: str,
    *,
    ids: list[object] | None = None,
    pfafs: list[object] | None = None,
) -> None:
    target = root / f"extract/hybas_{region}/hybas_{region}_lev{level}_v1c.shp"
    target.parent.mkdir(parents=True, exist_ok=True)
    values = ids if ids is not None else [basin_id(region, level)]
    frame = gpd.GeoDataFrame(
        {
            "HYBAS_ID": values,
            "PFAF_ID": pfafs if pfafs is not None else [pfaf_id(region, level)],
        },
        geometry=[box(index, 0, index + 1, 1) for index in range(len(values))],
        crs="EPSG:4326",
    )
    frame.to_file(target, driver="ESRI Shapefile", engine="pyogrio", index=False)


def write_pour(root: Path, level: str, ids: list[object] | None = None) -> None:
    target = root / f"extract/pour/hybas_pour_lev{level}_v1.shp"
    target.parent.mkdir(parents=True, exist_ok=True)
    values = ids if ids is not None else [basin_id(region, level) for region in REGIONS]
    frame = gpd.GeoDataFrame(
        {"HYBAS_ID": values},
        geometry=[Point(index, 0) for index in range(len(values))],
        crs="EPSG:4326",
    )
    frame.to_file(target, driver="ESRI Shapefile", engine="pyogrio", index=False)


def write_complete_fixture(root: Path) -> None:
    for region in REGIONS:
        for level in LEVELS:
            write_basin(root, region, level)
    for level in LEVELS:
        write_pour(root, level)


def copy_fixture(source: Path, destination: Path) -> None:
    shutil.copytree(source / "extract", destination / "extract")


def run_main(root: Path) -> tuple[int, str]:
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        result = scan_attribute_joins.main(["--source-root", str(root)])
    return result, stdout.getvalue()


class ScannerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._fixture_temp = tempfile.TemporaryDirectory()
        cls.fixture_root = Path(cls._fixture_temp.name)
        write_complete_fixture(cls.fixture_root)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._fixture_temp.cleanup()

    def test_generates_exact_fixed_path_inventory(self) -> None:
        basins, pours = scan_attribute_joins.generate_input_specs()

        expected_basins = [
            f"extract/hybas_{region}/hybas_{region}_lev{level}_v1c.shp"
            for region in REGIONS
            for level in LEVELS
        ]
        expected_pours = [
            f"extract/pour/hybas_pour_lev{level}_v1.shp" for level in LEVELS
        ]
        self.assertEqual([item.layer for item in basins], expected_basins)
        self.assertEqual([item.layer for item in pours], expected_pours)
        self.assertEqual(len(basins), 108)
        self.assertEqual(len(pours), 12)
        self.assertFalse(any("staged/" in path for path in expected_basins + expected_pours))
        self.assertTrue(all("_v1c.shp" in path for path in expected_basins))
        self.assertTrue(all("_v1.shp" in path for path in expected_pours))
        self.assertEqual(sum("lev12" in path for path in expected_basins), 9)

    def test_complete_global_pour_fixture_passes_and_ignores_decoys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            decoys = [
                root / "extract/hybas_af/hybas_af_lev01_v1.shp",
                root / "extract/hybas_af/nested/hybas_af_lev01_v1c.shp",
                root / "extract/hybas_af/hybas_pour_lev01_v1.shp",
                root / "staged/standard/hybas_af_lev01_v1c.shp",
                root / "staged/pour-points/hybas_pour_lev01_v1.shp",
                root / "recursive/hybas_pour_lev01_v1.shp",
            ]
            for decoy in decoys:
                decoy.parent.mkdir(parents=True, exist_ok=True)
                decoy.touch()

            calls: list[Path] = []
            original = pyogrio.read_dataframe

            def recording_read(path: Path, **kwargs: object):
                calls.append(Path(path))
                return original(path, **kwargs)

            with patch.object(pyogrio, "read_dataframe", side_effect=recording_read):
                code, output = run_main(root)

            report = json.loads(output)
            self.assertEqual(code, 0)
            self.assertEqual(report["status"], "pass")
            for level in LEVELS:
                expected = root / f"extract/pour/hybas_pour_lev{level}_v1.shp"
                self.assertEqual(calls.count(expected), 1)
            self.assertTrue(all(path not in calls for path in decoys))

    def test_inventory_first_missing_layers_never_read_attributes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            (root / "extract/hybas_af/hybas_af_lev02_v1c.shp").unlink()
            (root / "extract/pour/hybas_pour_lev01_v1.shp").unlink()
            with patch.object(pyogrio, "read_dataframe") as read:
                code, output = run_main(root)

            report = json.loads(output)
            self.assertNotEqual(code, 0)
            self.assertEqual(report["status"], "input-not-staged")
            self.assertFalse(report["inventory_complete"])
            self.assertTrue(all(check["status"] == "not-evaluated" for check in report["checks"]))
            self.assertEqual(report["findings"]["source_contract"], [])
            self.assertEqual(
                [(finding["layer_type"], finding["level"]) for finding in report["findings"]["input_not_staged"]],
                [("basin", "02"), ("pour-point", "01")],
            )
            self.assertTrue(all(item["feature_count"] is None for item in report["inputs"]["basins"]))
            read.assert_not_called()
            self.assertNotIn(str(root), output)

    def test_ambiguous_resolver_cardinality_is_an_inventory_finding(self) -> None:
        basins, pours = scan_attribute_joins.generate_input_specs()

        def resolver(root: Path, layer: str) -> list[Path]:
            if layer == basins[0].layer:
                return [root / layer, root / "duplicate" / Path(layer).name]
            return [root / layer]

        report, resolved = scan_attribute_joins.inventory_layers(
            Path("/not/reported"), basins, pours, resolver=resolver
        )
        self.assertIsNone(resolved)
        self.assertEqual(report["status"], "input-not-staged")
        self.assertEqual(
            report["findings"]["input_not_staged"][0],
            {
                "kind": "ambiguous-required-layer",
                "layer_type": "basin",
                "region": "af",
                "level": "01",
                "layer": "extract/hybas_af/hybas_af_lev01_v1c.shp",
                "matches": [
                    "duplicate/hybas_af_lev01_v1c.shp",
                    "extract/hybas_af/hybas_af_lev01_v1c.shp",
                ],
            },
        )

    def test_parent_findings_cover_unresolved_and_ambiguous(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            write_basin(root, "af", "02", pfafs=[22])
            code, output = run_main(root)
            report = json.loads(output)
            finding = report["findings"]["source_contract"][0]
            self.assertNotEqual(code, 0)
            self.assertEqual(report["status"], "source-contract-failed")
            self.assertEqual(
                finding,
                {
                    "check": "adjacent-level-pfaf-parent",
                    "kind": "unresolved-parent",
                    "region": "af",
                    "child_level": "02",
                    "parent_level": "01",
                    "child_hybas_id": 102,
                    "child_pfaf_id": 22,
                    "parent_pfaf_id": 2,
                    "match_count": 0,
                },
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            write_basin(root, "af", "01", ids=[101, 999], pfafs=[1, 1])
            code, output = run_main(root)
            findings = json.loads(output)["findings"]["source_contract"]
            parent = next(item for item in findings if item.get("kind") == "ambiguous-parent")
            self.assertNotEqual(code, 0)
            self.assertEqual(parent["child_hybas_id"], 102)
            self.assertEqual(parent["match_count"], 2)

    def test_coverage_and_collision_findings_are_stable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            write_pour(root, "03", [basin_id(region, "03") for region in REGIONS if region != "au"])
            write_basin(root, "ar", "04", ids=[103], pfafs=[2222])
            code, output = run_main(root)

            report = json.loads(output)
            self.assertNotEqual(code, 0)
            coverage = next(
                item
                for item in report["findings"]["source_contract"]
                if item["kind"] == "uncovered-basin-id" and item["region"] == "au"
            )
            collision = next(item for item in report["findings"]["source_contract"] if item["kind"] == "duplicate-hybas-id")
            self.assertEqual(coverage, {
                "check": "per-level-pour-point-coverage",
                "kind": "uncovered-basin-id",
                "region": "au",
                "level": "03",
                "hybas_id": 403,
            })
            self.assertEqual(collision["hybas_id"], 103)
            self.assertEqual(
                [(item["region"], item["level"]) for item in collision["occurrences"]],
                [("af", "03"), ("ar", "04")],
            )

    def test_reads_exact_attribute_projections_without_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            calls: list[tuple[Path, dict[str, object]]] = []
            original = pyogrio.read_dataframe

            def recording_read(path: Path, **kwargs: object):
                calls.append((Path(path), kwargs))
                return original(path, **kwargs)

            with patch.object(pyogrio, "read_dataframe", side_effect=recording_read):
                code, _ = run_main(root)

            self.assertEqual(code, 0)
            self.assertEqual(len(calls), 120)
            for path, kwargs in calls:
                self.assertIs(kwargs.get("read_geometry"), False)
                expected = ["HYBAS_ID"] if "/pour/" in path.as_posix() else ["HYBAS_ID", "PFAF_ID"]
                self.assertEqual(kwargs.get("columns"), expected)

    def test_attribute_input_failure_blocks_relevant_checks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            target = root / "extract/hybas_af/hybas_af_lev01_v1c.shp"
            frame = gpd.GeoDataFrame(
                {"PFAF_ID": [1]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326"
            )
            frame.to_file(target, driver="ESRI Shapefile", engine="pyogrio", index=False)
            code, output = run_main(root)

            report = json.loads(output)
            self.assertNotEqual(code, 0)
            self.assertEqual(report["status"], "source-contract-failed")
            finding = report["findings"]["source_contract"][0]
            self.assertEqual(finding["check"], "attribute-input")
            self.assertEqual(finding["kind"], "missing-required-column")
            self.assertEqual(finding["column"], "HYBAS_ID")
            self.assertTrue(all(check["status"] == "fail" for check in report["checks"]))

    def test_numeric_normalization_rejects_every_invalid_value_class(self) -> None:
        invalid = [
            (None, "column contains a null value"),
            ("not-a-number", "column contains a non-numeric value"),
            (1.5, "column contains a non-integral value"),
            (0, "column contains a nonpositive value"),
            (-1, "column contains a nonpositive value"),
            (2**63, "column contains a value outside signed int64 range"),
        ]
        for value, detail in invalid:
            with self.subTest(value=value):
                self.assertEqual(scan_attribute_joins._as_integral(value), (None, detail))
        self.assertEqual(scan_attribute_joins._as_integral("9223372036854775807"), (2**63 - 1, None))

    def test_child_pfaf_requires_a_nonempty_decimal_parent_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            copy_fixture(self.fixture_root, root)
            write_basin(root, "af", "02", pfafs=[1])
            code, output = run_main(root)

            report = json.loads(output)
            self.assertNotEqual(code, 0)
            finding = report["findings"]["source_contract"][0]
            self.assertEqual(finding["kind"], "invalid-attribute-value")
            self.assertEqual(finding["column"], "PFAF_ID")
            self.assertEqual(
                finding["detail"],
                "column contains a child code without a nonempty parent prefix",
            )

    def test_serialization_is_byte_stable_across_roots_and_repeats(self) -> None:
        outputs: list[str] = []
        roots: list[str] = []
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            for directory in (first, second):
                root = Path(directory)
                roots.append(str(root))
                copy_fixture(self.fixture_root, root)
                for _ in range(2):
                    code, output = run_main(root)
                    self.assertEqual(code, 0)
                    outputs.append(output)

        self.assertTrue(all(output == outputs[0] for output in outputs))
        self.assertTrue(all(root not in outputs[0] for root in roots))
        self.assertTrue(outputs[0].endswith("\n"))
        self.assertFalse(outputs[0].endswith("\n\n"))
        report = json.loads(outputs[0])
        self.assertEqual([item["region"] for item in report["inputs"]["basins"][:12]], ["af"] * 12)
        self.assertEqual([item["level"] for item in report["inputs"]["pour_points"]], list(LEVELS))


if __name__ == "__main__":
    unittest.main()
