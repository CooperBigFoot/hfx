"""Native-source regeneration equals corrected compilation on real artifacts."""

import hashlib
import importlib
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import LineString, Polygon

import build_adapter


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_streamnet(path, rows):
    if path.exists():
        path.unlink()
    gpd.GeoDataFrame(rows, crs="EPSG:4326").to_file(
        path, layer="streamnet", driver="GPKG", engine="pyogrio"
    )


def native_rows(reverse=False):
    rows = []
    for identity, downstream, coordinates in (
        (1, 2, [(0, 0), (.01, 0)]),
        (2, 3, [(.01, 0), (.02, 0)]),
        (3, -1, [(.02, 0), (.03, 0)]),
        (4, -1, [(.1, 0), (.11, 0)]),
        (5, -1, [(.2, 0), (.2, 0)]),
    ):
        rows.append(dict(LINKNO=identity, DSLINKNO=downstream,
                         DSContArea={1: .3077, 2: .46, 3: .6154, 4: .3077, 5: .3077}[identity],
                         geometry=LineString(coordinates[::-1] if reverse else coordinates)))
    return rows


def compile_reference(root, reverse=False, *, source_rows=None, polygon_ids=(1, 3, 4, 5)):
    """Compile once, then remap unchanged native content to every header."""
    source = root / "streamnet.gpkg"
    write_streamnet(source, native_rows(reverse) if source_rows is None else source_rows)
    basins = root / "basins.gpkg"
    polygons = []
    for identity, x in ((1, 0), (3, .02), (4, .1), (5, .2)):
        if identity not in polygon_ids:
            continue
        polygons.append(dict(streamID=identity, geometry=Polygon(
            [(x, 0), (x+.005, 0), (x+.005, .005), (x, .005), (x, 0)])))
    gpd.GeoDataFrame(polygons, crs="EPSG:4326").to_file(
        basins, layer="basins", driver="GPKG", engine="pyogrio")
    crosswalk = build_adapter.load_header_crosswalk()
    basin, header = next(iter(crosswalk.items()))
    compiled = root / "compiled"
    build_adapter.build_dataset(
        basins, source, compiled, root / "compile-report.json",
        processing_basin_id=basin, fabric_version="synthetic",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    reference = root / "reference"
    shutil.copytree(compiled, reference)
    offset = header * 10_000_000
    for relative in ("catchments.parquet", "graph.parquet", "aux/snap_stems.parquet"):
        table = pq.read_table(compiled / relative)
        authored = []
        for new_header in crosswalk.values():
            delta = new_header * 10_000_000 - offset
            for row in table.to_pylist():
                row["id"] += delta
                if "unit_id" in row:
                    row["unit_id"] += delta
                if "upstream_ids" in row:
                    row["upstream_ids"] = [value + delta for value in row["upstream_ids"]]
                authored.append(row)
        pq.write_table(pa.Table.from_pylist(authored, schema=table.schema),
                       reference / relative, row_group_size=7)
    manifest = json.loads((reference / "manifest.json").read_text())
    manifest.pop("region", None)
    manifest["bbox"] = [-180., -90., 180., 90.]
    manifest["unit_count"] *= len(crosswalk)
    (reference / "manifest.json").write_text(json.dumps(manifest))
    for name in ("NOTICE", "CITATION.txt"):
        shutil.copyfile(Path(build_adapter.__file__).parent / name, reference / name)
    (reference / "README.md").write_text("Synthetic authenticated reference.\n")
    return reference, source


def inventory(reference, source):
    adapter = Path(build_adapter.__file__).parent
    readme = source.parent / "corrected-readme.md"
    if not readme.exists():
        readme.write_text("Synthetic replacement: native-derived corrected outlets in all 62 processing basins.\n")
    return dict(schema_version=1, build_identity="synthetic-regeneration-test",
                replacement_readme=dict(path=str(readme.resolve()),
                                       bytes=readme.stat().st_size, sha256=digest(readme)),
                crosswalk_sha256=digest(adapter / "data/tdx_header_numbers.json"),
                reference_files=[dict(path=str(path.relative_to(reference)),
                                      bytes=path.stat().st_size, sha256=digest(path))
                                 for path in sorted(reference.rglob("*")) if path.is_file()],
                sources=[dict(processing_basin_id=basin, path=str(source.resolve()),
                              bytes=source.stat().st_size, sha256=digest(source))
                         for basin in build_adapter.load_header_crosswalk()])


class NativeOutletRegenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="native-regeneration-")
        cls.root = Path(cls.temporary.name)
        cls.reference, cls.source = compile_reference(cls.root)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def setUp(self):
        self.case = Path(tempfile.mkdtemp(dir=self.root, prefix="case-"))
        self.reference = self.case / "reference"
        shutil.copytree(type(self).reference, self.reference)
        self.source = self.case / "streamnet.gpkg"
        shutil.copyfile(type(self).source, self.source)
        self.destination = self.case / "candidate"
        self.evidence = self.case / "evidence"
        self.inventory_path = self.case / "inventory.json"
        self.authenticate()

    def authenticate(self):
        self.inventory_path.write_text(json.dumps(inventory(self.reference, self.source)))

    def regenerate(self):
        module = importlib.import_module("regenerate_outlets")
        return module.regenerate(self.reference, self.destination,
                                 self.inventory_path, self.evidence, batch_size=3,
                                 inventory_sha256=digest(self.inventory_path))

    def refuse(self):
        # Every refusal must reach its intended gate, rather than fail because
        # a previous subtest already wrote an evidence directory.
        while self.evidence.exists():
            self.evidence = self.evidence.with_name(self.evidence.name + "-next")
        with self.assertRaises((ValueError, RuntimeError, OSError)):
            self.regenerate()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_full_native_regeneration_matches_corrected_build(self):
        expected = pq.read_table(self.reference / "catchments.parquet")
        rows = expected.to_pylist()
        for row in rows:
            row["outlet_lon"] += .0001
        pq.write_table(pa.Table.from_pylist(rows, schema=expected.schema),
                       self.reference / "catchments.parquet", row_group_size=7)
        self.authenticate()
        before = {str(p.relative_to(self.reference)): digest(p)
                  for p in self.reference.rglob("*") if p.is_file()}
        native_membership = []
        real_reader = build_adapter._read_streamnet_topology_columns

        def read_native(path):
            columns = real_reader(path)
            native_membership.append(columns.native_ids.tolist())
            return columns

        with patch.object(build_adapter, "_read_streamnet_topology_columns",
                          side_effect=read_native) as reader:
            report = self.regenerate()
        self.assertEqual(native_membership, [[1, 2, 3, 4, 5]] * 62)
        self.assertIsInstance(report, dict)
        self.assertEqual(reader.call_count, 62)
        self.assertTrue(all(Path(call.args[0]) == self.source for call in reader.call_args_list))
        actual = pq.read_table(self.destination / "catchments.parquet")
        self.assertTrue(actual.equals(expected, check_metadata=True))
        for relative, sha in before.items():
            self.assertEqual(digest(self.reference / relative), sha)
            if relative not in {"catchments.parquet", "README.md", "manifest.json"}:
                self.assertEqual(digest(self.destination / relative), sha)
        self.assertEqual(json.loads((self.destination / "manifest.json").read_text()),
                         json.loads((self.reference / "manifest.json").read_text()))
        self.assertTrue(self.evidence.exists())
        record = json.loads(self.inventory_path.read_text())
        self.assertEqual(digest(self.destination / "README.md"),
                         record["replacement_readme"]["sha256"])
        self.assertEqual(digest(self.evidence / "historical-README.md"),
                         before["README.md"])
        provenance = json.loads((self.evidence / "provenance.json").read_text())
        self.assertEqual(len(provenance["basins"]), 62)
        for basin in provenance["basins"]:
            self.assertEqual(basin["native_reach_count"], 5)
            self.assertEqual(basin["polygonless_reach_count"], 1)
            self.assertEqual(basin["source_sha256"], digest(self.source))
            native_path = Path(basin["native_evidence_path"])
            self.assertEqual(digest(native_path), basin["native_evidence_sha256"])
            with np.load(native_path, allow_pickle=False) as native:
                self.assertEqual(native["native_ids"].tolist(), [1, 2, 3, 4, 5])
                self.assertEqual(native["polygon_native_ids"].tolist(), [1, 3, 4, 5])
                self.assertEqual(len(native["resolution"]), 5)
                self.assertEqual(len(native["downstream_endpoint_index"]), 5)

    def test_noop_still_derives_all_processing_basins(self):
        with patch.object(build_adapter, "_read_streamnet_topology_columns",
                          wraps=build_adapter._read_streamnet_topology_columns) as reader:
            self.regenerate()
        self.assertEqual(reader.call_count, 62)
        self.assertTrue(pq.read_table(self.destination / "catchments.parquet").equals(
            pq.read_table(self.reference / "catchments.parquet"), check_metadata=True))

    def test_reversed_digitization_matches_corrected_full_build(self):
        reverse_root = self.case / "reversed"
        reverse_root.mkdir()
        self.reference, self.source = compile_reference(reverse_root, reverse=True)
        self.authenticate()
        self.regenerate()
        self.assertTrue(pq.read_table(self.destination / "catchments.parquet").equals(
            pq.read_table(self.reference / "catchments.parquet"), check_metadata=True))

    def test_successor_side_and_exact_coincidence_match_full_build(self):
        scenarios = {
            "successor-side": ([(1, 2, .3077, [(0.0025, 0), (0, 0)]),
                                (2, 3, .6, [(0.002, 0), (0.0005, 0)]),
                                (3, -1, .9, [(0.002, 0), (0.01, 0)])], (1,)),
            "exact-coincidence": ([(1, 2, .3077, [(0.0015, 0), (0.0018, 0)]),
                                   (2, 3, .6, [(0.01, 0), (0.0015, 0)]),
                                   (3, -1, .9, [(0.02, 0), (0.01, 0)])], (1,)),
        }
        for name, (links, polygon_ids) in scenarios.items():
            with self.subTest(resolution=name):
                scenario = self.case / name
                scenario.mkdir()
                rows = [dict(LINKNO=i, DSLINKNO=d, DSContArea=a,
                             geometry=LineString(coords)) for i, d, a, coords in links]
                self.reference, self.source = compile_reference(
                    scenario, source_rows=rows, polygon_ids=polygon_ids)
                self.destination = scenario / "candidate"
                self.evidence = scenario / "evidence"
                self.authenticate()
                self.regenerate()
                self.assertTrue(pq.read_table(self.destination / "catchments.parquet").equals(
                    pq.read_table(self.reference / "catchments.parquet"), check_metadata=True))

    def test_inventory_pin_mismatch_refuses(self):
        module = importlib.import_module("regenerate_outlets")
        with self.assertRaises((ValueError, RuntimeError)):
            module.regenerate(self.reference, self.destination,
                              self.inventory_path, self.evidence, batch_size=3,
                              inventory_sha256="0" * 64)
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_source_record_order_is_irrelevant(self):
        record = json.loads(self.inventory_path.read_text())
        record["sources"].reverse()
        self.inventory_path.write_text(json.dumps(record))
        self.regenerate()
        self.assertTrue((self.destination / "manifest.json").exists())

    def test_missing_extra_and_duplicate_source_records_refuse(self):
        original = json.loads(self.inventory_path.read_text())
        for mode in ("missing", "extra", "duplicate"):
            with self.subTest(mode=mode):
                record = json.loads(json.dumps(original))
                if mode == "missing":
                    record["sources"].pop()
                elif mode == "extra":
                    record["sources"].append(dict(record["sources"][0], processing_basin_id="unknown"))
                else:
                    record["sources"][-1] = record["sources"][0]
                self.inventory_path.write_text(json.dumps(record))
                self.refuse()

    def test_source_mutation_during_real_reader_refuses(self):
        real_reader = build_adapter._read_streamnet_topology_columns
        reads = []

        def mutate(path):
            columns = real_reader(path)
            reads.append(path)
            with Path(path).open("ab") as handle:
                handle.write(b"mutation after real native read")
            return columns

        with patch.object(build_adapter, "_read_streamnet_topology_columns", side_effect=mutate):
            with self.assertRaisesRegex(ValueError, "source changed"):
                self.regenerate()
        self.assertEqual(len(reads), 1)
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_source_and_reference_symlinks_refuse(self):
        source_alias = self.case / "source-alias.gpkg"
        source_alias.symlink_to(self.source)
        record = json.loads(self.inventory_path.read_text())
        record["sources"][0]["path"] = str(source_alias)
        self.inventory_path.write_text(json.dumps(record))
        self.refuse()
        self.authenticate()
        reference_alias = self.case / "reference-alias"
        reference_alias.symlink_to(self.reference, target_is_directory=True)
        self.reference = reference_alias
        self.refuse()

    def test_verify_rederives_native_outlets_and_rejects_candidate_tampering(self):
        module = importlib.import_module("regenerate_outlets")
        self.regenerate()
        path = self.destination / "catchments.parquet"
        table = pq.read_table(path)
        rows = table.to_pylist()
        rows[0]["outlet_lon"] += .0001
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
        with patch.object(build_adapter, "_read_streamnet_topology_columns",
                          wraps=build_adapter._read_streamnet_topology_columns) as reader:
            with self.assertRaisesRegex(ValueError, "outlet"):
                module.verify(self.reference, self.destination, self.inventory_path,
                              self.case / "verify-evidence", batch_size=3,
                              inventory_sha256=digest(self.inventory_path))
        self.assertEqual(reader.call_count, 62)

    def test_changed_source_bytes_refuse(self):
        with self.source.open("ab") as handle:
            handle.write(b"identity drift")
        self.refuse()

    def test_changed_replacement_readme_bytes_refuse(self):
        record = json.loads(self.inventory_path.read_text())
        Path(record["replacement_readme"]["path"]).write_text("unapproved replacement text")
        self.refuse()

    def test_changed_reference_bytes_refuse(self):
        with (self.reference / "NOTICE").open("a") as handle:
            handle.write("identity drift")
        self.refuse()

    def test_wrong_crosswalk_identity_refuses(self):
        record = json.loads(self.inventory_path.read_text())
        record["crosswalk_sha256"] = "0" * 64
        self.inventory_path.write_text(json.dumps(record))
        self.refuse()

    def test_wrong_manifest_count_refuses(self):
        path = self.reference / "manifest.json"
        record = json.loads(path.read_text())
        record["unit_count"] += 1
        path.write_text(json.dumps(record))
        self.authenticate()
        self.refuse()

    def test_missing_extra_duplicate_and_invalid_unit_identity_refuse(self):
        path = self.reference / "catchments.parquet"
        original = pq.read_table(path)
        for mode in ("missing", "extra", "duplicate", "invalid"):
            with self.subTest(mode=mode):
                rows = original.to_pylist()
                if mode == "missing":
                    rows.pop()
                elif mode == "extra":
                    rows.append(dict(rows[0], id=rows[0]["id"] + 6))
                elif mode == "duplicate":
                    rows[-1]["id"] = rows[0]["id"]
                else:
                    rows[-1]["id"] = -1
                pq.write_table(pa.Table.from_pylist(rows, schema=original.schema), path)
                self.authenticate()
                self.refuse()

    def test_changed_contracted_graph_refuses(self):
        path = self.reference / "graph.parquet"
        table = pq.read_table(path)
        rows = table.to_pylist()
        edge = next(row for row in rows if row["upstream_ids"])
        edge["upstream_ids"] = []
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
        self.authenticate()
        self.refuse()

    def test_native_link_integrity_refuses(self):
        for mode in ("missing", "duplicate", "dangling", "self", "cycle"):
            with self.subTest(mode=mode):
                rows = native_rows()
                if mode == "missing":
                    rows = rows[1:]
                elif mode == "duplicate":
                    rows.append(rows[0])
                elif mode == "dangling":
                    rows[0]["DSLINKNO"] = 999
                elif mode == "self":
                    rows[0]["DSLINKNO"] = 1
                else:
                    rows[2]["DSLINKNO"] = 1
                write_streamnet(self.source, rows)
                self.authenticate()
                self.refuse()

    def test_native_ambiguity_and_area_guards_refuse_real_sources(self):
        module = importlib.import_module("regenerate_outlets")
        header = next(iter(build_adapter.load_header_crosswalk().values()))
        for mode, pattern in (("tied", "tied"), ("decrease", "exceeds"),
                              ("noncoincidence", "ambiguous"),
                              ("isolated", "polarity|evidence")):
            with self.subTest(guard=mode):
                rows = [
                    dict(LINKNO=1, DSLINKNO=2, DSContArea=.1,
                         geometry=LineString([(.0015, 0), (.0018, 0)])),
                    dict(LINKNO=2, DSLINKNO=3, DSContArea=.2,
                         geometry=LineString([(.01, 0), (.0015, 0)])),
                    dict(LINKNO=3, DSLINKNO=-1, DSContArea=.3,
                         geometry=LineString([(.02, 0), (.01, 0)])),
                ]
                if mode == "tied":
                    rows[0]["DSContArea"] = .2
                elif mode == "decrease":
                    rows[0]["DSContArea"] = .4
                elif mode == "noncoincidence":
                    rows[0]["geometry"] = LineString([(.0014, 0), (.0017, 0)])
                else:
                    rows = [dict(rows[0], DSLINKNO=-1)]
                write_streamnet(self.source, rows)
                with self.assertRaisesRegex(ValueError, pattern):
                    module.derive_native_outlets(np.array([1], dtype="int64"),
                                                 self.source, header)

    def test_polygonless_evidence_cannot_be_replaced_with_delivered_links(self):
        # Removing native reach 2 and contracting 1 -> 3 reproduces delivered
        # connectivity, but removes the endpoint evidence required to orient 1.
        rows = [row for row in native_rows() if row["LINKNO"] != 2]
        rows[0]["DSLINKNO"] = 3
        write_streamnet(self.source, rows)
        self.authenticate()
        self.refuse()

    def test_large_wkb_rewrite_uses_bounded_real_parquet_batches(self):
        module = importlib.import_module("regenerate_outlets")
        source = self.case / "large-wkb.parquet"
        destination = self.case / "rewritten-large-wkb.parquet"
        expected = pq.read_table(self.reference / "catchments.parquet").slice(0, 5)
        angles = np.linspace(0, 2 * np.pi, 40_001)
        geometry = Polygon(np.column_stack((.001 * np.cos(angles),
                                           .001 * np.sin(angles)))).wkb
        self.assertGreater(len(geometry), 600_000)
        rows = expected.to_pylist()
        for row in rows:
            row["geometry"] = geometry
        original = pa.Table.from_pylist(rows, schema=expected.schema)
        pq.write_table(original, source, row_group_size=5)
        outlet_index = np.empty(5, dtype=[("id", "<i8"), ("outlet_lon", "<f8"),
                                         ("outlet_lat", "<f8")])
        for position, row in enumerate(sorted(rows, key=lambda row: row["id"])):
            outlet_index[position] = (row["id"], row["outlet_lon"] + .0001,
                                      row["outlet_lat"])
        index_path = self.case / "large-wkb-outlets.npy"
        np.save(index_path, outlet_index, allow_pickle=False)
        real_write = pq.ParquetWriter.write_batch
        real_batches = pq.ParquetFile.iter_batches
        reads, writes = [], []

        def iter_batches(reader, *args, **kwargs):
            for batch in real_batches(reader, *args, **kwargs):
                reads.append((batch.num_rows, batch.nbytes))
                yield batch

        def write_batch(writer, batch, *args, **kwargs):
            writes.append((batch.num_rows, batch.nbytes))
            return real_write(writer, batch, *args, **kwargs)

        with patch.object(pq.ParquetFile, "iter_batches", iter_batches), \
             patch.object(pq.ParquetWriter, "write_batch", write_batch):
            module.rewrite_catchments(source, destination, index_path, batch_size=2)
        self.assertEqual([rows for rows, _ in reads], [2, 2, 1])
        self.assertEqual([rows for rows, _ in writes], [2, 2, 1])
        self.assertLess(max(size for _, size in reads + writes), 1_400_000)
        actual = pq.read_table(destination)
        for name in original.column_names:
            if name != "outlet_lon":
                self.assertTrue(actual[name].equals(original[name]), name)
        self.assertTrue(actual.schema.equals(original.schema, check_metadata=True))
        np.testing.assert_allclose(actual["outlet_lon"].to_numpy(),
                                   original["outlet_lon"].to_numpy() + .0001)

    def test_interrupted_real_rewrite_cannot_publish_success(self):
        module = importlib.import_module("regenerate_outlets")
        real_rewrite = module.rewrite_catchments
        writes = []

        def interrupt(*args, **kwargs):
            result = real_rewrite(*args, **kwargs)
            writes.append(Path(args[1]))
            raise OSError("injected interruption after real Parquet rewrite")

        with patch.object(module, "rewrite_catchments", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "after real Parquet rewrite"):
                self.regenerate()
        self.assertEqual(len(writes), 1)
        self.assertTrue(writes[0].exists())
        self.assertFalse((self.destination / "manifest.json").exists())
        self.assertFalse((writes[0].parent / "manifest.json").exists())
        self.assertEqual(json.loads((self.evidence / "status.json").read_text())["status"],
                         "refused")

    def test_insufficient_scratch_refuses_before_output(self):
        module = importlib.import_module("regenerate_outlets")
        usage = shutil.disk_usage(self.case)
        with patch.object(module.shutil, "disk_usage", return_value=usage._replace(free=0)):
            with self.assertRaisesRegex(ValueError, "insufficient scratch"):
                self.regenerate()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_atomic_directory_publication_never_replaces_existing_destinations(self):
        module = importlib.import_module("regenerate_outlets")
        source = self.case / "publication-source"
        source.mkdir()
        (source / "manifest.json").write_text("source marker")
        for kind in ("empty", "occupied", "symlink"):
            with self.subTest(destination=kind):
                destination = self.case / ("publication-" + kind)
                if kind == "symlink":
                    target = self.case / "publication-symlink-target"
                    target.mkdir()
                    destination.symlink_to(target, target_is_directory=True)
                else:
                    destination.mkdir()
                    if kind == "occupied":
                        (destination / "keep.txt").write_text("keep")
                before = destination.lstat().st_ino
                with self.assertRaises(OSError):
                    module._publish_new_directory(source, destination)
                self.assertEqual(destination.lstat().st_ino, before)
                self.assertEqual((source / "manifest.json").read_text(), "source marker")
                self.assertFalse((destination / "manifest.json").exists())
                if kind == "occupied":
                    self.assertEqual((destination / "keep.txt").read_text(), "keep")
                if kind == "symlink":
                    self.assertTrue(destination.is_symlink())

    def test_existing_destination_is_never_reused(self):
        self.destination.mkdir()
        sentinel = self.destination / "owned.txt"
        sentinel.write_text("keep")
        self.refuse()
        self.assertEqual(sentinel.read_text(), "keep")

    def test_input_output_alias_and_symlink_refuse_without_mutation(self):
        original = digest(self.reference / "manifest.json")
        for alias in (self.reference, self.case / "alias"):
            if alias != self.reference:
                alias.symlink_to(self.reference, target_is_directory=True)
            self.destination = alias
            with self.assertRaises((ValueError, RuntimeError, OSError)):
                self.regenerate()
            self.assertEqual(digest(self.reference / "manifest.json"), original)

    def test_evidence_path_inside_reference_refuses(self):
        self.evidence = self.reference / "evidence"
        self.refuse()
        self.assertFalse(self.evidence.exists())


if __name__ == "__main__":
    unittest.main()
