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


if __name__ == "__main__":
    unittest.main()
