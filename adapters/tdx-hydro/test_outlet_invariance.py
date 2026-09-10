"""Tiny real-Parquet proofs of exhaustive outlet-only artifact invariance."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import outlet_invariance as invariance
from test_compare_unit_outlets import Fixture, HEADER_NUMBER, OFFSET, BASIN_ID


class OutletInvarianceTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(__file__).resolve().parents[2] / ".worktrees" / "test-outlet-invariance"
        scratch.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.root = Path(self.tmp.name)
        self.fixture = Fixture(self.root)
        self.index = np.zeros(5, dtype=invariance.OUTLET_DTYPE)
        for row, unit in zip(self.index, self.fixture.candidate_units):
            row["id"] = OFFSET + unit["native"]
            row["downstream_id"] = -1 if unit["downstream"] is None else OFFSET + unit["downstream"]
            row["outlet_lon"], row["outlet_lat"] = unit["outlet"]
        self.path = self.root / "outlets.npy"
        np.save(self.path, self.index)
        native_path = self.root / "native-resolution.npy"
        resolution = np.zeros(5, dtype=[("native_id", "<i8"), ("resolution", "i1")])
        resolution["native_id"] = np.arange(1, 6)
        np.save(native_path, resolution)
        self.provenance = {"basins": [{"processing_basin_id": BASIN_ID,
            "header_number": HEADER_NUMBER,
            "orientation_digest": self.fixture.record["corrected_orientation_digest"],
            "source_sha256": "a" * 64,
            "unit_resolution_path": str(native_path), "resolution_code_names": ["unique-successor"],
            "unit_resolution_sha256": hashlib.sha256(native_path.read_bytes()).hexdigest(),
            "native_evidence_sha256": "b" * 64}]}

    def tearDown(self):
        self.tmp.cleanup()

    def verify(self, **kwargs):
        return invariance.verify_outlet_invariance(
            self.fixture.reference, self.fixture.candidate, self.path,
            self.provenance, self.root / "evidence", batch_size=2, **kwargs)

    def rewrite(self, column, values):
        path = self.fixture.candidate / "catchments.parquet"
        table = pq.read_table(path)
        i = table.schema.get_field_index(column)
        table = table.set_column(i, table.schema.field(i), pa.array(values, type=table.column(i).type))
        pq.write_table(table, path, row_group_size=3)

    def test_exhaustive_changes_and_digests(self):
        report = self.verify()
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["unit_count"], 5)
        self.assertEqual(report["changed_unit_count"], 2)
        self.assertEqual(report["basins"][0]["changed_unit_count"], 2)
        self.assertAlmostEqual(report["basins"][0]["max_shift_deg"], .001)
        changes = [json.loads(line) for line in (self.root / "evidence/changes.jsonl").read_text().splitlines()]
        self.assertEqual({r["native_id"] for r in changes}, {2, 3})
        self.assertEqual(changes[0]["processing_basin_id"], BASIN_ID)
        self.assertEqual(changes[0]["resolution"], "unique-successor")
        from pyproj import Geod
        expected = []
        for change in changes:
            _, _, shift_m = Geod(ellps="WGS84").inv(*change["old_outlet"], *change["new_outlet"])
            self.assertAlmostEqual(change["shift_m"], shift_m)
            expected.append(shift_m)
        self.assertAlmostEqual(report["max_shift_m"], max(expected))
        self.assertAlmostEqual(report["basins"][0]["max_shift_m"], max(expected))

    def test_unknown_existing_column_is_compared_and_float_bits_preserved(self):
        for root in (self.fixture.reference, self.fixture.candidate):
            path = root / "catchments.parquet"
            table = pq.read_table(path).append_column("future_attribute", pa.array([float("nan"), -0., 1., None, 2.]))
            pq.write_table(table, path)
        self.verify()
        shutil.rmtree(self.root / "evidence")
        self.rewrite("future_attribute", [float("nan"), 0., 1., None, 2.])
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "future_attribute"):
            self.verify()

    def test_actual_moved_unit_snap_fields_refuse(self):
        path = self.fixture.candidate / "aux/snap_stems.parquet"
        original = pq.read_table(path)
        for name in original.schema.names:
            with self.subTest(column=name):
                values = original[name].to_pylist()
                # Unit 3 is moved, so the historical comparator would allow this.
                values[2] = None if values[2] is not None else "changed"
                i = original.schema.get_field_index(name)
                table = original.set_column(i, original.schema.field(i), pa.array(values, type=original[name].type))
                pq.write_table(table, path)
                with self.assertRaisesRegex(invariance.InvarianceRefusal, "snap_stems"):
                    self.verify()
                shutil.rmtree(self.root / "evidence", ignore_errors=True)

    def test_provenance_missing_extra_duplicate_and_invalid_refuse(self):
        valid = copy.deepcopy(self.provenance)
        variants = [{"basins": []}, {"basins": valid["basins"] * 2}]
        wrong = copy.deepcopy(valid)
        wrong["basins"][0]["native_evidence_sha256"] = "missing"
        variants.append(wrong)
        wrong = copy.deepcopy(valid)
        wrong["basins"][0]["header_number"] += 1
        variants.append(wrong)
        for provenance in variants:
            self.provenance = provenance
            with self.assertRaises(invariance.InvarianceRefusal):
                self.verify()

    def test_duplicate_reference_and_candidate_ids_refuse(self):
        for root in (self.fixture.reference, self.fixture.candidate):
            path = root / "catchments.parquet"
            table = pq.read_table(path)
            i = table.schema.get_field_index("id")
            table = table.set_column(i, table.schema.field(i), pa.array([OFFSET + 1] * 5))
            pq.write_table(table, path)
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "duplicate"):
            self.verify()

    def test_native_zero_id_and_downstream_are_admitted(self):
        from test_compare_unit_outlets import write_dataset
        for units, root in ((self.fixture.reference_units, self.fixture.reference),
                            (self.fixture.candidate_units, self.fixture.candidate)):
            for unit in units:
                unit["native"] -= 1
                if unit["downstream"] is not None:
                    unit["downstream"] -= 1
            write_dataset(root, units, created_at="same")
        self.index["id"] -= 1
        linked = self.index["downstream_id"] != -1
        self.index["downstream_id"][linked] -= 1
        np.save(self.path, self.index)
        digest = hashlib.sha256()
        for name in self.index.dtype.names:
            values = self.index[name].copy()
            if name in ("id", "downstream_id"):
                values[values != -1] -= OFFSET
            digest.update(values.tobytes())
        record = self.provenance["basins"][0]
        record["orientation_digest"] = digest.hexdigest()
        path = Path(record["unit_resolution_path"])
        rows = np.load(path)
        rows["native_id"] -= 1
        np.save(path, rows)
        record["unit_resolution_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(self.verify()["unit_count"], 5)

    def test_noop(self):
        shutil.copyfile(self.fixture.candidate / "catchments.parquet", self.fixture.reference / "catchments.parquet")
        self.assertEqual(self.verify()["changed_unit_count"], 0)

    def test_each_non_outlet_column_refuses(self):
        path = self.fixture.candidate / "catchments.parquet"
        original = path.read_bytes()
        for name in pq.read_schema(path).names:
            if name.startswith("outlet_"):
                continue
            with self.subTest(column=name):
                path.write_bytes(original)
                vals = pq.read_table(path, columns=[name])[name].to_pylist()
                vals[0] = None if vals[0] is not None else 1
                self.rewrite(name, vals)
                with self.assertRaises(invariance.InvarianceRefusal):
                    self.verify()
                self.assertFalse((self.root / "evidence/summary.json").exists())
                shutil.rmtree(self.root / "evidence", ignore_errors=True)

    def test_unknown_column_schema_metadata_nullability_and_row_order_refuse(self):
        path = self.fixture.candidate / "catchments.parquet"
        original = pq.read_table(path)
        variants = [original.append_column("unknown", pa.array([1] * 5)),
                    original.replace_schema_metadata({b"changed": b"yes"}),
                    original.take(pa.array([4, 3, 2, 1, 0]))]
        fields = list(original.schema)
        fields[0] = fields[0].with_nullable(False)
        variants.append(pa.Table.from_arrays(original.columns, schema=pa.schema(fields)))
        fields = list(original.schema)
        fields[0] = fields[0].with_metadata({b"field": b"changed"})
        variants.append(pa.Table.from_arrays(original.columns, schema=pa.schema(fields)))
        for table in variants:
            pq.write_table(table, path)
            with self.assertRaises(invariance.InvarianceRefusal):
                self.verify()
            shutil.rmtree(self.root / "evidence", ignore_errors=True)

    def test_every_other_file_including_moved_unit_snap_refuses(self):
        for relative in ("graph.parquet", "aux/snap_stems.parquet", "manifest.json"):
            path = self.fixture.candidate / relative
            original = path.read_bytes()
            path.write_bytes(original + b"mutation")
            with self.assertRaises(invariance.InvarianceRefusal):
                self.verify()
            path.write_bytes(original)
            shutil.rmtree(self.root / "evidence", ignore_errors=True)

    def test_reference_candidate_hardlink_alias_refuses(self):
        for name in ("aux/snap_stems.parquet", "catchments.parquet"):
            with self.subTest(artifact=name):
                reference = self.fixture.reference / name
                candidate = self.fixture.candidate / name
                original = candidate.read_bytes()
                # Equal outlets for catchments isolate alias refusal from derivation.
                if name == "catchments.parquet":
                    reference.write_bytes(original)
                candidate.unlink()
                candidate.hardlink_to(reference)
                try:
                    with self.assertRaisesRegex(invariance.InvarianceRefusal, "hardlink"):
                        self.verify()
                finally:
                    candidate.unlink()
                    candidate.write_bytes(original)
                    shutil.rmtree(self.root / "evidence", ignore_errors=True)

    def test_extra_file_refuses(self):
        (self.fixture.candidate / "extra").write_text("unapproved")
        with self.assertRaises(invariance.InvarianceRefusal):
            self.verify()

    def test_missing_duplicate_extra_unsorted_index_refuse(self):
        variants = [self.index[:-1], np.concatenate([self.index, self.index[-1:]]),
                    self.index[::-1], np.concatenate([self.index, self.index[-1:]])]
        variants[-1][-1]["id"] += 1
        for rows in variants:
            np.save(self.path, rows)
            with self.assertRaises(invariance.InvarianceRefusal):
                self.verify()
            shutil.rmtree(self.root / "evidence", ignore_errors=True)

    def test_wrong_outlet_and_orientation_digest_refuse(self):
        self.rewrite("outlet_lon", [99.] * 5)
        with self.assertRaises(invariance.InvarianceRefusal):
            self.verify()
        shutil.rmtree(self.root / "evidence", ignore_errors=True)
        self.provenance["basins"][0]["orientation_digest"] = "f" * 64
        with self.assertRaises(invariance.InvarianceRefusal):
            self.verify()

    def test_graph_lookups_are_per_batch_not_per_unit(self):
        from unittest.mock import patch
        original = invariance._positions
        calls = []
        def record(index, ids, context):
            if context == "graph upstream_ids":
                calls.append(len(ids))
            return original(index, ids, context)
        with patch.object(invariance, "_positions", side_effect=record):
            self.verify()
        self.assertEqual(len(calls), 3)  # Five graph rows in batches of two.
        self.assertEqual(sum(calls), 3)  # Every edge, including two from one target.

    def test_index_graph_edge_mismatch_refuses(self):
        self.index[4]["downstream_id"] = OFFSET + 1
        np.save(self.path, self.index)
        # Supply a self-consistent index digest: the actual graph must still refuse.
        digest = hashlib.sha256()
        for name in self.index.dtype.names:
            values = self.index[name].copy()
            if name in ("id", "downstream_id"):
                values[values != -1] -= OFFSET
            digest.update(values.tobytes())
        self.provenance["basins"][0]["orientation_digest"] = digest.hexdigest()
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "graph"):
            self.verify()

    def test_existing_evidence_alias_and_symlink_refuse(self):
        evidence = self.root / "evidence"
        evidence.mkdir()
        with self.assertRaises(invariance.InvarianceRefusal):
            self.verify()
        evidence.rmdir()
        evidence.symlink_to(self.fixture.candidate, target_is_directory=True)
        with self.assertRaises(invariance.InvarianceRefusal):
            self.verify()

    def test_exact_pinned_replacement_readme_only(self):
        (self.fixture.reference / "README.md").write_text("Historical mixed convention.\n")
        replacement = self.root / "replacement.md"
        replacement.write_text("Corrected native outlets for all processing basins.\n")
        candidate_readme = self.fixture.candidate / "README.md"
        shutil.copyfile(replacement, candidate_readme)
        self.provenance["replacement_readme"] = {"path": str(replacement),
            "bytes": replacement.stat().st_size, "sha256": hashlib.sha256(replacement.read_bytes()).hexdigest()}
        report = self.verify()
        self.assertEqual(report["replacement_readme"], self.provenance["replacement_readme"])
        self.assertNotIn("README.md", report["unchanged_artifact_sha256"])
        shutil.rmtree(self.root / "evidence")
        candidate_readme.write_text("Unpinned README mutation\n")
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "README"):
            self.verify()
        shutil.copyfile(replacement, candidate_readme)
        replacement.write_text("Source pin drift\n")
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "README"):
            self.verify()

    def test_readme_mutation_without_explicit_pin_refuses(self):
        (self.fixture.reference / "README.md").write_text("Historical\n")
        (self.fixture.candidate / "README.md").write_text("Unpinned replacement\n")
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "README"):
            self.verify()

    def test_concurrent_artifact_mutation_refuses_without_success_marker(self):
        from unittest.mock import patch
        original = invariance._compare_batch
        mutated = False
        def mutate(left, right, row_offset):
            nonlocal mutated
            original(left, right, row_offset)
            if not mutated:
                with (self.fixture.candidate / "manifest.json").open("a") as stream:
                    stream.write(" ")
                mutated = True
        with patch.object(invariance, "_compare_batch", side_effect=mutate):
            with self.assertRaisesRegex(invariance.InvarianceRefusal, "changed during verification"):
                self.verify()
        self.assertFalse((self.root / "evidence/summary.json").exists())

    def test_resolution_digest_and_coverage_refuse(self):
        record = self.provenance["basins"][0]
        path = Path(record["unit_resolution_path"])
        path.write_bytes(path.read_bytes() + b"changed")
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "resolution digest"):
            self.verify()
        shutil.rmtree(self.root / "evidence")
        rows = np.zeros(4, dtype=[("native_id", "<i8"), ("resolution", "i1")])
        np.save(path, rows)
        record["unit_resolution_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(invariance.InvarianceRefusal, "resolution count"):
            self.verify()

    def test_large_wkb_uses_bounded_batches(self):
        # 3 MiB across five rows is a lifetime probe, not a scale benchmark.
        from shapely.geometry import Polygon
        angle = np.linspace(0, 2 * np.pi, 32768)
        blob = Polygon(np.column_stack((np.cos(angle), np.sin(angle)))).wkb
        for root in (self.fixture.reference, self.fixture.candidate):
            path = root / "catchments.parquet"
            table = pq.read_table(path)
            i = table.schema.get_field_index("geometry")
            table = table.set_column(i, table.schema.field(i), pa.array([blob] * 5))
            pq.write_table(table, path, row_group_size=3)
        from unittest.mock import patch
        original = invariance._compare_batch
        observed = []
        def inspect_batches(left, right, *args):
            observed.append((left.num_rows, right.num_rows, left.nbytes))
            return original(left, right, *args)
        with patch.object(invariance, "_compare_batch", side_effect=inspect_batches):
            self.verify()
        self.assertEqual(sum(n for n, _, _ in observed), 5)
        self.assertLessEqual(max(n for n, _, _ in observed), 2)
        self.assertLessEqual(max(n for _, n, _ in observed), 2)


if __name__ == "__main__":
    unittest.main()
