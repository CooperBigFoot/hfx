"""Test the deterministic rehearsal corpus: products, manifest, compilability, and refusals."""

from __future__ import annotations

import hashlib
import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

import pyogrio

import build_adapter
import synthesize_rehearsal_corpus as synthesize

BASIN_IDS = ("7020000010", "1020000010")
FABRIC_VERSION = "NGA-TDX-Hydro-20230126"


def run_synthesize(argv: list[str]) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        status = synthesize.main(argv)
    return status, stdout.getvalue(), stderr.getvalue()


def basin_args(out: Path, *basin_ids: str) -> list[str]:
    argv = ["--out", str(out)]
    for basin_id in basin_ids:
        argv.extend(["--basin", basin_id])
    return argv


class SynthesizeCorpusTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_two_ids_write_four_products_and_a_matching_manifest(self) -> None:
        out = self.root / "corpus"
        status, stdout, stderr = run_synthesize(basin_args(out, *BASIN_IDS))
        self.assertEqual(status, 0, stderr)
        expected_names = sorted(f"{basin_id}-{product}.gpkg" for basin_id in BASIN_IDS for product in ("basins", "streamnet"))
        self.assertEqual(sorted(path.name for path in out.glob("*.gpkg")), expected_names)
        manifest = out / synthesize.DEFAULT_MANIFEST_NAME
        lines = manifest.read_text(encoding="utf-8").splitlines()
        self.assertEqual([line.split("  ./")[1] for line in lines], expected_names)
        for line in lines:
            digest, name = line.split("  ./")
            self.assertEqual(digest, hashlib.sha256((out / name).read_bytes()).hexdigest())
        printed = stdout.splitlines()
        self.assertEqual(sorted(Path(line).name for line in printed[:-1]), expected_names)
        self.assertEqual(printed[-1], f"manifest: {manifest.resolve()}")
        for basin_id in BASIN_IDS:
            basins = out / f"{basin_id}-basins.gpkg"
            streamnet = out / f"{basin_id}-streamnet.gpkg"
            self.assertEqual(pyogrio.list_layers(basins).tolist(), [["basins", "Polygon"]])
            self.assertEqual(pyogrio.list_layers(streamnet).tolist(), [["streamnet", "LineString"]])
            self.assertEqual(pyogrio.read_info(basins)["fields"].tolist(), ["streamID"])
            self.assertEqual(pyogrio.read_info(streamnet)["fields"].tolist(), ["LINKNO", "DSLINKNO", "DSContArea"])

    def test_extents_are_disjoint_and_content_is_deterministic(self) -> None:
        first = self.root / "first"
        second = self.root / "second"
        self.assertEqual(run_synthesize(basin_args(first, *BASIN_IDS))[0], 0)
        self.assertEqual(run_synthesize(basin_args(second, *reversed(BASIN_IDS)))[0], 0)
        self.assertEqual(
            (first / synthesize.DEFAULT_MANIFEST_NAME).read_text(),
            (second / synthesize.DEFAULT_MANIFEST_NAME).read_text(),
        )
        low = pyogrio.read_bounds(first / "1020000010-basins.gpkg")[1]
        high = pyogrio.read_bounds(first / "7020000010-basins.gpkg")[1]
        self.assertLess(low[2].max(), high[0].min())

    def test_each_synthesized_basin_compiles_to_two_units(self) -> None:
        out = self.root / "corpus"
        self.assertEqual(run_synthesize(basin_args(out, *BASIN_IDS))[0], 0)
        for basin_id in BASIN_IDS:
            output = self.root / f"{basin_id}-out"
            report_path = self.root / f"{basin_id}-report.json"
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                status = build_adapter.main(
                    [
                        "build",
                        "--basins", str(out / f"{basin_id}-basins.gpkg"),
                        "--streamnet", str(out / f"{basin_id}-streamnet.gpkg"),
                        "--out", str(output),
                        "--report", str(report_path),
                        "--processing-basin-id", basin_id,
                        "--fabric-version", FABRIC_VERSION,
                    ]
                )
            self.assertEqual(status, 0, stdout.getvalue())
            report = json.loads(report_path.read_text())
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest["unit_count"], 2)
            self.assertEqual(manifest["region"], basin_id)
            self.assertEqual(manifest["fabric_version"], FABRIC_VERSION)
            self.assertEqual(report["build_identity"]["processing_basin_id"], basin_id)
            self.assertEqual(report["diagnostics"]["ingestion"]["dscontarea"]["checked_polygon_bearing_link_count"], 2)

    def test_unknown_id_refuses(self) -> None:
        out = self.root / "corpus"
        status, _, stderr = run_synthesize(basin_args(out, "7020000010", "9999999999"))
        self.assertEqual(status, 1)
        self.assertIn("not in the TDX-Hydro header crosswalk: 9999999999", stderr)
        self.assertFalse(out.exists())
        status, _, stderr = run_synthesize(basin_args(out, "702000001"))
        self.assertEqual(status, 1)
        self.assertIn("10-digit", stderr)

    def test_missing_parent_directory_refuses(self) -> None:
        out = self.root / "missing" / "corpus"
        status, _, stderr = run_synthesize(basin_args(out, *BASIN_IDS))
        self.assertEqual(status, 1)
        self.assertIn("parent directory does not exist", stderr)
        self.assertFalse(out.parent.exists())

    def test_no_basin_refuses(self) -> None:
        status, _, stderr = run_synthesize(["--out", str(self.root / "corpus")])
        self.assertEqual(status, 1)
        self.assertIn("at least one --basin", stderr)


if __name__ == "__main__":
    unittest.main()
