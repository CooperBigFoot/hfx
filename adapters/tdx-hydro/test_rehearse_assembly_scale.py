import hashlib
import json
import subprocess
import sys
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pyarrow.parquet as pq

import rehearse_assembly_scale
from test_build_adapter import (
    MERGE_RUN_A,
    MERGE_RUN_B,
    assembly_snap_schema,
    merge_hilbert_keys,
    rewrite_snap_rows,
    write_assembly_fixture,
)

PLANETARY_ROWS = [
    MERGE_RUN_A[0],
    MERGE_RUN_B[0],
    MERGE_RUN_A[1],
    MERGE_RUN_B[1],
    MERGE_RUN_A[2],
    MERGE_RUN_B[2],
    MERGE_RUN_B[3],
]
PLANETARY_KEYS = [
    (7_054_384, 710_000_101),
    (238_609_294, 720_000_201),
    (517_598_622, 710_000_102),
    (622_261_220, 720_000_202),
    (715_827_882, 710_000_103),
    (715_827_882, 720_000_203),
    (1_008_396_555, 720_000_204),
]
PLANETARY_SNAP_ROWS = [
    (1, 710_000_101, -170.0, -80.0, 1.0),
    (2, 720_000_201, -120.0, -80.0, 4.0),
    (3, 710_000_102, -80.0, -60.0, 3.0),
    (4, 720_000_202, -20.0, -40.0, 9.0),
    (5, 710_000_103, 0.0, 0.0, 6.0),
    (6, 720_000_203, 0.0, 0.0, 15.0),
    (7, 720_000_204, -170.0, -20.0, 22.0),
]


class RehearseAssemblyScaleTests(unittest.TestCase):
    def make_planetary_fixture(self, root: Path) -> None:
        write_assembly_fixture(
            root,
            "7020000010",
            PLANETARY_ROWS,
            PLANETARY_SNAP_ROWS,
        )

    def assert_verification_failure(
        self, root: Path, expected_message: str
    ) -> None:
        with self.assertRaises(
            rehearse_assembly_scale.VerificationError
        ) as raised:
            rehearse_assembly_scale.verify_planetary_output(
                root, batch_size=2
            )
        self.assertEqual(str(raised.exception), expected_message)

    def read_snap_rows(self, root: Path) -> list[dict[str, object]]:
        return (
            pq.ParquetFile(root / "aux" / "snap_stems.parquet")
            .read()
            .to_pylist()
        )

    def test_monotonic_catchment_order_passes_across_batches_and_row_groups(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            catchments = pq.ParquetFile(root / "catchments.parquet")
            snaps = pq.ParquetFile(root / "aux" / "snap_stems.parquet")
            self.assertEqual(catchments.num_row_groups, 4)
            self.assertEqual(snaps.num_row_groups, 4)
            self.assertGreater(
                len(list(catchments.iter_batches(batch_size=2))), 1
            )
            self.assertGreater(
                len(list(snaps.iter_batches(batch_size=2))), 1
            )
            catchment_rows = catchments.read().to_pylist()
            keys = merge_hilbert_keys(
                [row["geometry"] for row in catchment_rows]
            )
            self.assertEqual(
                list(zip(keys, [row["id"] for row in catchment_rows])),
                PLANETARY_KEYS,
            )
            self.assertIsNone(
                rehearse_assembly_scale.verify_planetary_output(
                    root, batch_size=2
                )
            )

    def test_non_monotonic_pair_across_row_group_boundary_fails(self) -> None:
        rows = [MERGE_RUN_A[0], MERGE_RUN_A[2], MERGE_RUN_A[1]]
        snap_rows = [
            (1, 710_000_101, -170.0, -80.0, 1.0),
            (2, 710_000_103, 0.0, 0.0, 6.0),
            (3, 710_000_102, -80.0, -60.0, 3.0),
        ]
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            write_assembly_fixture(root, "7020000010", rows, snap_rows)
            parquet = pq.ParquetFile(root / "catchments.parquet")
            self.assertEqual(parquet.num_row_groups, 2)
            authored = parquet.read().to_pylist()
            keys = merge_hilbert_keys(
                [row["geometry"] for row in authored]
            )
            self.assertEqual(
                (keys[1], authored[1]["id"]),
                (715_827_882, 710_000_103),
            )
            self.assertEqual(
                (keys[2], authored[2]["id"]),
                (517_598_622, 710_000_102),
            )
            self.assert_verification_failure(
                root, rehearse_assembly_scale.CATCHMENT_ORDER_FAILURE
            )

    def test_equal_hilbert_with_decreasing_id_fails(self) -> None:
        rows = [MERGE_RUN_B[2], MERGE_RUN_A[2]]
        snap_rows = [
            (1, 720_000_203, 0.0, 0.0, 15.0),
            (2, 710_000_103, 0.0, 0.0, 6.0),
        ]
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            write_assembly_fixture(root, "7020000010", rows, snap_rows)
            authored = (
                pq.ParquetFile(root / "catchments.parquet")
                .read()
                .to_pylist()
            )
            keys = merge_hilbert_keys(
                [row["geometry"] for row in authored]
            )
            self.assertEqual(
                list(zip(keys, [row["id"] for row in authored])),
                [
                    (715_827_882, 720_000_203),
                    (715_827_882, 710_000_103),
                ],
            )
            self.assert_verification_failure(
                root, rehearse_assembly_scale.CATCHMENT_ORDER_FAILURE
            )

    def test_sequential_snap_ids_pass_across_batches_and_row_groups(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            self.assertIsNone(
                rehearse_assembly_scale.verify_planetary_output(
                    root, batch_size=2
                )
            )

    def test_snap_id_gap_duplicate_and_wrong_start_fail(self) -> None:
        cases = {
            "gap": [1, 2, 4, 5, 6, 7, 8],
            "duplicate": [1, 2, 2, 4, 5, 6, 7],
            "wrong_start": [0, 1, 2, 3, 4, 5, 6],
        }
        for name, ids in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as temporary:
                root = Path(temporary) / "dataset"
                self.make_planetary_fixture(root)
                rows = self.read_snap_rows(root)
                for row, snap_id in zip(rows, ids):
                    row["id"] = snap_id
                rewrite_snap_rows(root, rows, schema=assembly_snap_schema())
                self.assert_verification_failure(
                    root, rehearse_assembly_scale.SNAP_ID_FAILURE
                )

    def test_required_manifest_auxiliary_declaration_passes(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            self.assertIsNone(
                rehearse_assembly_scale.verify_planetary_output(
                    root, batch_size=2
                )
            )

    def test_wrong_references_levels_fails(self) -> None:
        for levels in ([1], [0, 1], [False]):
            with (
                self.subTest(levels=levels),
                TemporaryDirectory() as temporary,
            ):
                root = Path(temporary) / "dataset"
                self.make_planetary_fixture(root)
                manifest_path = root / "manifest.json"
                manifest = json.loads(manifest_path.read_text())
                manifest["auxiliary"][0]["metadata"][
                    "references_levels"
                ] = levels
                manifest_path.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n"
                )
                self.assert_verification_failure(
                    root, rehearse_assembly_scale.MANIFEST_AUX_FAILURE
                )

    def test_resolved_unique_snap_unit_ids_pass(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            self.assertIsNone(
                rehearse_assembly_scale.verify_planetary_output(
                    root, batch_size=2
                )
            )

    def test_dangling_snap_unit_id_fails(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            rows = self.read_snap_rows(root)
            rows[0]["unit_id"] = 999_999_999
            rewrite_snap_rows(root, rows, schema=assembly_snap_schema())
            self.assert_verification_failure(
                root, rehearse_assembly_scale.SNAP_REFERENCE_FAILURE
            )

    def test_duplicate_snap_unit_id_fails_without_second_large_set(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            rows = self.read_snap_rows(root)
            rows[1]["unit_id"] = 710_000_101
            rewrite_snap_rows(root, rows, schema=assembly_snap_schema())
            self.assert_verification_failure(
                root, rehearse_assembly_scale.SNAP_REFERENCE_FAILURE
            )

    def test_manifest_schema_validation_is_mandatory(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["format_version"] = "9.9.9"
            manifest_path.write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n"
            )
            self.assert_verification_failure(
                root, rehearse_assembly_scale.MANIFEST_SCHEMA_FAILURE
            )

    def test_missing_uv_maps_to_manifest_schema_failure(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "dataset"
            self.make_planetary_fixture(root)
            with patch.object(
                rehearse_assembly_scale.subprocess,
                "run",
                side_effect=FileNotFoundError,
            ):
                self.assert_verification_failure(
                    root, rehearse_assembly_scale.MANIFEST_SCHEMA_FAILURE
                )

    def test_cli_verify_success_and_exact_failure_stderr(self) -> None:
        script = Path(rehearse_assembly_scale.__file__).resolve()
        with TemporaryDirectory() as valid_temporary:
            valid_root = Path(valid_temporary) / "dataset"
            self.make_planetary_fixture(valid_root)
            success = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "verify",
                    str(valid_root),
                    "--batch-size",
                    "2",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(success.returncode, 0)
            self.assertEqual(success.stderr, "")

        with TemporaryDirectory() as invalid_temporary:
            invalid_root = Path(invalid_temporary) / "dataset"
            self.make_planetary_fixture(invalid_root)
            rows = self.read_snap_rows(invalid_root)
            for row, snap_id in zip(rows, [0, 1, 2, 3, 4, 5, 6]):
                row["id"] = snap_id
            rewrite_snap_rows(
                invalid_root, rows, schema=assembly_snap_schema()
            )
            failure = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "verify",
                    str(invalid_root),
                    "--batch-size",
                    "2",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(failure.returncode, 0)
            self.assertEqual(
                failure.stderr,
                rehearse_assembly_scale.SNAP_ID_FAILURE + "\n",
            )

    def test_main_failure_stderr_has_no_traceback(self) -> None:
        stderr = StringIO()
        with redirect_stderr(stderr):
            result = rehearse_assembly_scale.main(
                ["verify", "/definitely/missing/dataset"]
            )
        self.assertNotEqual(result, 0)
        self.assertNotIn("Traceback", stderr.getvalue())


class SyntheticAssemblyRehearsalTests(unittest.TestCase):
    script = Path(rehearse_assembly_scale.__file__).resolve()

    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary = TemporaryDirectory()
        cls.scratch = Path(cls.temporary.name) / "scratch"
        result = subprocess.run(
            [
                sys.executable,
                str(cls.script),
                "rehearse",
                "--scratch-root",
                str(cls.scratch),
                "--basin-count",
                "3",
                "--total-units",
                "12291",
                "--distribution",
                "even",
                "--seed",
                "20260723",
                "--generator-batch-size",
                "512",
                "--verify-batch-size",
                "512",
                "--sample-interval-ms",
                "5",
                "--rss-ceiling-bytes",
                "32212254720",
                "--scratch-ceiling-bytes",
                "68719476736",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        cls.report = json.loads(result.stdout)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary.cleanup()

    @staticmethod
    def file_hashes(root: Path) -> dict[str, str]:
        return {
            str(path.relative_to(root)): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sorted(root.rglob("*"))
            if path.is_file()
        }

    def run_generate(self, output: Path, seed: int) -> dict[str, object]:
        result = subprocess.run(
            [
                sys.executable,
                str(self.script),
                "generate",
                "--out",
                str(output),
                "--basin-count",
                "3",
                "--total-units",
                "51",
                "--distribution",
                "seeded-skew",
                "--seed",
                str(seed),
                "--generator-batch-size",
                "7",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        return json.loads(result.stdout)

    def test_generation_is_byte_deterministic_and_seeded(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            changed = root / "changed"
            first_report = self.run_generate(first, 73421)
            second_report = self.run_generate(second, 73421)
            changed_report = self.run_generate(changed, 73422)
            self.assertEqual(
                first_report["basin_unit_counts"], [15, 14, 22]
            )
            self.assertEqual(first_report, second_report)
            self.assertEqual(
                changed_report["basin_unit_counts"], [20, 12, 19]
            )
            first_hashes = self.file_hashes(first / "inputs")
            second_hashes = self.file_hashes(second / "inputs")
            changed_hashes = self.file_hashes(changed / "inputs")
            self.assertEqual(first_hashes, second_hashes)
            self.assertEqual(set(first_hashes), set(changed_hashes))
            self.assertTrue(
                any(
                    first_hashes[path] != changed_hashes[path]
                    for path in first_hashes
                )
            )

    def test_scaled_end_to_end_uses_real_assemble_and_verifier(self) -> None:
        regions = ["1020000010", "1020011530", "1020018110"]
        self.assertEqual(self.report["status"], "passed")
        assembly = self.report["assembly"]
        expected_argv = [
            sys.executable,
            str(self.script.with_name("build_adapter.py").resolve()),
            "assemble",
        ]
        for region in regions:
            expected_argv.extend(
                [
                    "--input",
                    str((self.scratch / "inputs" / region).resolve()),
                ]
            )
        expected_argv.extend(
            ["--out", str((self.scratch / "assembled").resolve())]
        )
        self.assertEqual(assembly["argv"], expected_argv)
        self.assertEqual(len(assembly["argv"]), 11)
        self.assertEqual(assembly["returncode"], 0)
        catchment_schema, _, graph_read_schema, snap_schema = (
            rehearse_assembly_scale._schemas()
        )
        for region in regions:
            basin = self.scratch / "inputs" / region
            manifest = json.loads((basin / "manifest.json").read_text())
            self.assertEqual(manifest["unit_count"], 4097)
            self.assertEqual(manifest["region"], region)
            for relative, schema in (
                ("catchments.parquet", catchment_schema),
                ("graph.parquet", graph_read_schema),
                ("aux/snap_stems.parquet", snap_schema),
            ):
                parquet = pq.ParquetFile(basin / relative)
                self.assertEqual(parquet.metadata.num_rows, 4097)
                self.assertTrue(
                    parquet.schema_arrow.equals(
                        schema, check_metadata=True
                    )
                )
        assembled = self.scratch / "assembled"
        self.assertTrue((assembled / "manifest.json").is_file())
        verifier = subprocess.run(
            [
                sys.executable,
                str(self.script),
                "verify",
                str(assembled),
                "--batch-size",
                "512",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(verifier.returncode, 0)
        self.assertEqual(verifier.stderr, "")

    def test_interleaving_proof_covers_batches_and_row_groups(self) -> None:
        metrics = self.report["interleaving"]
        self.assertEqual(metrics["input_batches_per_basin"], [5, 5, 5])
        for key in (
            "catchment_input_row_groups_per_basin",
            "graph_input_row_groups_per_basin",
            "snap_input_row_groups_per_basin",
        ):
            self.assertEqual(metrics[key], [9, 9, 9])
        self.assertEqual(metrics["output_catchment_row_groups"], 3)
        self.assertEqual(metrics["output_graph_row_groups"], 3)
        self.assertEqual(metrics["output_snap_row_groups"], 3)
        self.assertEqual(metrics["basin_origin_transitions"], 12290)
        self.assertEqual(
            metrics["post_first_input_batch_origin_transitions"], 9218
        )
        self.assertEqual(
            metrics["catchment_row_groups_with_multiple_basins"], 3
        )
        self.assertEqual(
            metrics["snap_row_groups_with_multiple_basins"], 3
        )
        self.assertTrue(metrics["passed"])

        assembled = self.scratch / "assembled"
        catchments = pq.ParquetFile(assembled / "catchments.parquet")
        graph = pq.ParquetFile(assembled / "graph.parquet")
        snaps = pq.ParquetFile(
            assembled / "aux" / "snap_stems.parquet"
        )
        known_headers = {11, 12, 13}
        basin_groups = {header: 0 for header in known_headers}
        snap_groups = {header: 0 for header in known_headers}
        for index in range(catchments.num_row_groups):
            catchment_ids = catchments.read_row_group(
                index, columns=["id"]
            ).column("id").to_pylist()
            graph_ids = graph.read_row_group(
                index, columns=["id"]
            ).column("id").to_pylist()
            self.assertEqual(catchment_ids, graph_ids)
            headers = {
                int(identifier) // 10_000_000
                for identifier in catchment_ids
            }
            self.assertGreaterEqual(len(headers), 2)
            for header in headers:
                basin_groups[header] += 1
        for index in range(snaps.num_row_groups):
            unit_ids = snaps.read_row_group(
                index, columns=["unit_id"]
            ).column("unit_id").to_pylist()
            headers = {
                int(identifier) // 10_000_000 for identifier in unit_ids
            }
            self.assertGreaterEqual(len(headers), 2)
            for header in headers:
                snap_groups[header] += 1
        self.assertTrue(all(groups > 1 for groups in basin_groups.values()))
        self.assertTrue(all(groups > 1 for groups in snap_groups.values()))

    def test_measurement_report_is_complete_and_bounded(self) -> None:
        self.assertEqual(
            set(self.report),
            {
                "schema_version",
                "status",
                "configuration",
                "generation",
                "assembly",
                "interleaving",
                "verification",
            },
        )
        configuration = self.report["configuration"]
        self.assertEqual(
            set(configuration),
            {
                "seed",
                "basin_count",
                "total_units",
                "distribution",
                "generator_batch_size",
                "verify_batch_size",
                "sample_interval_ms",
                "assemble_input_batch_size",
                "assemble_row_group_min",
                "assemble_row_group_max",
                "rss_ceiling_bytes",
                "scratch_ceiling_bytes",
            },
        )
        generation = self.report["generation"]
        self.assertEqual(
            set(generation),
            {
                "regions",
                "basin_unit_counts",
                "basin_snap_counts",
                "peak_authored_rows_bound",
            },
        )
        self.assertEqual(sum(generation["basin_unit_counts"]), 12291)
        self.assertEqual(
            generation["basin_snap_counts"],
            generation["basin_unit_counts"],
        )
        self.assertEqual(generation["peak_authored_rows_bound"], 1536)
        assembly = self.report["assembly"]
        self.assertEqual(
            set(assembly),
            {
                "argv",
                "returncode",
                "wall_time_seconds",
                "process_tree_peak_rss_bytes",
                "referential_proof_peak_rss_bytes",
                "scratch_tree_peak_bytes",
                "final_scratch_tree_bytes",
            },
        )
        for key in (
            "wall_time_seconds",
            "process_tree_peak_rss_bytes",
            "referential_proof_peak_rss_bytes",
            "scratch_tree_peak_bytes",
            "final_scratch_tree_bytes",
        ):
            self.assertIsInstance(assembly[key], (int, float))
            self.assertNotIsInstance(assembly[key], bool)
            self.assertGreater(assembly[key], 0)
        self.assertGreaterEqual(
            assembly["process_tree_peak_rss_bytes"],
            assembly["referential_proof_peak_rss_bytes"],
        )
        self.assertGreaterEqual(
            assembly["scratch_tree_peak_bytes"],
            assembly["final_scratch_tree_bytes"],
        )
        self.assertLessEqual(
            assembly["process_tree_peak_rss_bytes"],
            configuration["rss_ceiling_bytes"],
        )
        self.assertLessEqual(
            assembly["scratch_tree_peak_bytes"],
            configuration["scratch_ceiling_bytes"],
        )
        interleaving = self.report["interleaving"]
        self.assertLessEqual(
            interleaving["basin_origin_transitions"], 12290
        )
        self.assertLessEqual(
            interleaving["post_first_input_batch_origin_transitions"],
            12291 - 3 * 1024 - 1,
        )
        self.assertLessEqual(
            interleaving["catchment_row_groups_with_multiple_basins"],
            interleaving["output_catchment_row_groups"],
        )
        self.assertLessEqual(
            interleaving["snap_row_groups_with_multiple_basins"],
            interleaving["output_snap_row_groups"],
        )
        self.assertEqual(
            self.report["verification"],
            {"batch_size": 512, "passed": True},
        )

    def test_exact_rss_ceiling_failure(self) -> None:
        with TemporaryDirectory() as temporary:
            result = subprocess.run(
                [
                    sys.executable,
                    str(self.script),
                    "rehearse",
                    "--scratch-root",
                    str(Path(temporary) / "rss"),
                    "--basin-count",
                    "2",
                    "--total-units",
                    "8194",
                    "--generator-batch-size",
                    "512",
                    "--sample-interval-ms",
                    "5",
                    "--rss-ceiling-bytes",
                    "1",
                    "--scratch-ceiling-bytes",
                    "68719476736",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertEqual(
            result.stderr,
            "rehearsal failed: process-tree peak RSS exceeded ceiling\n",
        )

    def test_exact_generation_scratch_ceiling_failure(self) -> None:
        with TemporaryDirectory() as temporary:
            result = subprocess.run(
                [
                    sys.executable,
                    str(self.script),
                    "rehearse",
                    "--scratch-root",
                    str(Path(temporary) / "scratch"),
                    "--basin-count",
                    "2",
                    "--total-units",
                    "8194",
                    "--generator-batch-size",
                    "512",
                    "--sample-interval-ms",
                    "5",
                    "--rss-ceiling-bytes",
                    "32212254720",
                    "--scratch-ceiling-bytes",
                    "1",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertEqual(
            result.stderr,
            "rehearsal failed: scratch-tree peak usage exceeded ceiling\n",
        )


if __name__ == "__main__":
    unittest.main()
