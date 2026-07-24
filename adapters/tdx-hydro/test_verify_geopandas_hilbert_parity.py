"""Test deterministic GeoPandas Hilbert parity verification helpers."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import shapely
from shapely.geometry import Point

import verify_geopandas_hilbert_parity as parity


class CorpusTests(unittest.TestCase):
    def test_same_seed_produces_identical_corpus(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.tsv"
            second = Path(directory) / "second.tsv"
            first_result = parity.generate_corpus(first, seed=1234, random_count=64)
            second_result = parity.generate_corpus(second, seed=1234, random_count=64)

            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(first_result.sha256, second_result.sha256)
            self.assertEqual(
                first_result.sha256, hashlib.sha256(first.read_bytes()).hexdigest()
            )

    def test_different_seed_changes_corpus(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.tsv"
            second = Path(directory) / "second.tsv"
            first_result = parity.generate_corpus(first, seed=1234, random_count=64)
            second_result = parity.generate_corpus(second, seed=5678, random_count=64)

            self.assertNotEqual(first.read_bytes(), second.read_bytes())
            self.assertNotEqual(first_result.sha256, second_result.sha256)

    def test_small_corpus_has_all_categories_and_valid_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            corpus = Path(directory) / "corpus.tsv"
            parity.generate_corpus(corpus, seed=1234, random_count=64)
            records = parity.read_corpus(corpus, expected_random_count=64)

            self.assertEqual(
                {record.category for record in records},
                {
                    "real-polygon",
                    "real-line",
                    "world-boundary",
                    "clamp-adjacent",
                    "hilbert-tie",
                    "tiny-valid",
                    "randomized",
                },
            )
            self.assertEqual([record.index for record in records], list(range(len(records))))
            self.assertEqual(
                sum(record.category == "randomized" for record in records), 64
            )
            for record in records:
                geometry = shapely.from_wkb(bytes.fromhex(record.wkb_hex))
                self.assertFalse(geometry.is_empty)
                self.assertTrue(geometry.is_valid)

    def test_tie_group_centroids_are_identical(self) -> None:
        groups = parity.tie_groups()
        self.assertEqual(len(groups), 2)
        for group in groups:
            centroid_wkb = [
                shapely.to_wkb(
                    Point(geometry.centroid.x + 0.0, geometry.centroid.y + 0.0),
                    hex=True,
                    byte_order=0,
                    output_dimension=2,
                )
                for geometry in group
            ]
            self.assertEqual(len(set(centroid_wkb)), 1)


class PairComparisonTests(unittest.TestCase):
    def test_identical_pair_files_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left = Path(directory) / "left.tsv"
            right = Path(directory) / "right.tsv"
            pairs = b"0\t10\n1\t20\n"
            left.write_bytes(pairs)
            right.write_bytes(pairs)

            result = parity.compare_pair_files(left, "left", right, "right")

            self.assertEqual(result.sha256, hashlib.sha256(pairs).hexdigest())
            self.assertEqual(result.row_count, 2)

    def test_key_mismatch_names_first_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left = Path(directory) / "left.tsv"
            right = Path(directory) / "right.tsv"
            left.write_text("0\t10\n1\t20\n2\t30\n", encoding="utf-8")
            right.write_text("0\t10\n1\t21\n2\t30\n", encoding="utf-8")

            with self.assertRaisesRegex(
                parity.PairComparisonError,
                r"left vs right.*index 1.*left=20.*right=21",
            ):
                parity.compare_pair_files(left, "left", right, "right")

    def test_truncated_output_reports_missing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left = Path(directory) / "left.tsv"
            right = Path(directory) / "right.tsv"
            left.write_text("0\t10\n1\t20\n", encoding="utf-8")
            right.write_text("0\t10\n", encoding="utf-8")

            with self.assertRaisesRegex(
                parity.PairComparisonError,
                r"left vs right.*index 1.*left=20.*right=<missing>",
            ):
                parity.compare_pair_files(left, "left", right, "right")

    def test_malformed_and_duplicate_pair_records_fail(self) -> None:
        malformed_inputs = (
            "not-a-record\n",
            "0\t10\n0\t11\n",
            "0\t10\n2\t11\n",
            "0\t-1\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            for sequence, contents in enumerate(malformed_inputs):
                with self.subTest(contents=contents):
                    path = Path(directory) / f"pairs-{sequence}.tsv"
                    path.write_text(contents, encoding="utf-8")
                    with self.assertRaises(parity.PairFileError):
                        parity.read_pair_file(path)


class ResultDocumentTests(unittest.TestCase):
    CORPUS_HASH = "a" * 64
    PAIR_HASH = "b" * 64

    def test_accepts_unique_well_formed_hashes(self) -> None:
        text = (
            f"- VM-confirm corpus SHA-256: `{self.CORPUS_HASH}`.\n"
            f"- VM-confirm macOS pair SHA-256: `{self.PAIR_HASH}`.\n"
        )
        self.assertEqual(
            parity.parse_vm_confirmation_hashes(text),
            (self.CORPUS_HASH, self.PAIR_HASH),
        )

    def test_rejects_missing_duplicate_malformed_and_conflicting_fields(self) -> None:
        valid_corpus = f"- VM-confirm corpus SHA-256: `{self.CORPUS_HASH}`.\n"
        valid_pair = f"- VM-confirm macOS pair SHA-256: `{self.PAIR_HASH}`.\n"
        cases = (
            "",
            valid_corpus,
            valid_pair,
            valid_corpus + valid_corpus + valid_pair,
            valid_corpus
            + f"- VM-confirm corpus SHA-256: `{'c' * 64}`.\n"
            + valid_pair,
            valid_corpus + "- VM-confirm corpus SHA-256: `ABC`.\n" + valid_pair,
            "- VM-confirm corpus SHA-256: `ABC`.\n" + valid_pair,
            valid_corpus + "- VM-confirm macOS pair SHA-256: `1234`.\n",
        )
        for text in cases:
            with self.subTest(text=text):
                with self.assertRaises(parity.ResultDocumentError):
                    parity.parse_vm_confirmation_hashes(text)

    def test_hash_mismatch_names_kind_expected_and_actual(self) -> None:
        for kind in ("corpus SHA-256", "pair SHA-256"):
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(
                    parity.HashMismatchError,
                    rf"{kind}.*expected {'a' * 64}.*actual {'b' * 64}",
                ):
                    parity.require_hash_match(kind, "a" * 64, "b" * 64)


if __name__ == "__main__":
    unittest.main()
