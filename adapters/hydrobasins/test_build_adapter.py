"""Tests for HydroBASINS Pfaf-12 polygon ingestion and serialization."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from geoparquet_io.core.validate import validate_geoparquet
from shapely.geometry import LineString, Point, Polygon, box

import build_adapter


LAYER_NAME = "hybas_gr_lev12_v1.shp"
POUR_POINTS_LAYER_NAME = "hybas_pour_lev12_v1.shp"


def _write_layer(
    basins_dir: Path,
    *,
    ids: list[object] | None = None,
    next_down: list[object] | None = None,
    endo: list[object] | None = None,
    omit: str | None = None,
    crs: str = "EPSG:4326",
    geometries: list[Polygon] | None = None,
) -> None:
    values = ids if ids is not None else [30, 10, 20, 40]
    rows: dict[str, list[object]] = {
        "HYBAS_ID": values,
        "SUB_AREA": [3.5, 1.5, 2.5, 4.5],
        "UP_AREA": [30.5, 10.5, 20.5, 40.5],
        "NEXT_DOWN": next_down if next_down is not None else [20, 20, 0, 20],
        "ENDO": endo if endo is not None else [0, 0, 0, 2],
        "PFAF_ID": [111, 112, 113, 114],
    }
    if omit is not None:
        del rows[omit]
    if geometries is None:
        geometries = [
            box(0, 0, 1, 1),
            box(0, 0, 1, 1),
            box(10, 0, 11, 1),
            box(20, 0, 21, 1),
        ]
    frame = gpd.GeoDataFrame(rows, geometry=geometries, crs=crs)
    frame.to_file(
        basins_dir / LAYER_NAME,
        driver="ESRI Shapefile",
        engine="pyogrio",
        index=False,
    )


def _write_pour_points(
    pour_points_dir: Path,
    *,
    ids: list[object],
    points: list[object],
    key_name: str = "HYBAS_ID",
    crs: str | None = "EPSG:4326",
    extra_columns: dict[str, list[object]] | None = None,
) -> None:
    pour_points_dir.mkdir(parents=True, exist_ok=True)
    rows: dict[str, list[object]] = {key_name: ids}
    if extra_columns is not None:
        rows.update(extra_columns)
    frame = gpd.GeoDataFrame(rows, geometry=points, crs=crs)
    frame.to_file(
        pour_points_dir / POUR_POINTS_LAYER_NAME,
        driver="ESRI Shapefile",
        engine="pyogrio",
        index=False,
    )


def _build_args(basins_dir: Path, pour_points_dir: Path, out_dir: Path) -> list[str]:
    return [
        "build",
        "--region",
        "gr",
        "--basins",
        str(basins_dir),
        "--pour-points",
        str(pour_points_dir),
        "--out",
        str(out_dir),
    ]


def _ordinary_points() -> tuple[list[int], list[Point]]:
    return (
        [30, 10, 20, 40],
        [Point(0.30, 0.30), Point(0.10, 0.10), Point(10.20, 0.20), Point(20.20, 0.20)],
    )


class LoadRegionUnitsTests(unittest.TestCase):
    """Exercise normalization through real ESRI Shapefile I/O."""

    def test_missing_required_column_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, omit="UP_AREA")

            with self.assertRaisesRegex(build_adapter.AdapterError, "UP_AREA"):
                build_adapter.load_region_units("gr", basins_dir)

    def test_missing_next_down_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, omit="NEXT_DOWN")

            with self.assertRaisesRegex(build_adapter.AdapterError, "NEXT_DOWN"):
                build_adapter.load_region_units("gr", basins_dir)

    def test_missing_endo_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, omit="ENDO")

            with self.assertRaisesRegex(build_adapter.AdapterError, "ENDO"):
                build_adapter.load_region_units("gr", basins_dir)

    def test_non_positive_and_non_integral_ids_are_rejected(self) -> None:
        cases = ([0, 10, 20, 40], [-1, 10, 20, 40], [1.5, 10, 20, 40])
        for ids in cases:
            with self.subTest(ids=ids), tempfile.TemporaryDirectory() as temporary:
                basins_dir = Path(temporary)
                _write_layer(basins_dir, ids=ids)

                with self.assertRaises(build_adapter.AdapterError):
                    build_adapter.load_region_units("gr", basins_dir)

    def test_duplicate_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, ids=[30, 10, 20, 10])

            with self.assertRaisesRegex(build_adapter.AdapterError, "duplicate"):
                build_adapter.load_region_units("gr", basins_dir)

    def test_attributes_are_mapped_with_contract_dtypes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir)

            units = build_adapter.load_region_units("gr", basins_dir)

        by_id = units.set_index("id")
        self.assertEqual(by_id.loc[10, "area_km2"], 1.5)
        self.assertEqual(by_id.loc[20, "up_area_km2"], 20.5)
        self.assertEqual(units["id"].dtype, pd.Int64Dtype().numpy_dtype)
        self.assertEqual(units["area_km2"].dtype, "float64")
        self.assertEqual(units["up_area_km2"].dtype, "float64")
        self.assertIn("NEXT_DOWN", units.columns)
        self.assertIn("ENDO", units.columns)
        self.assertEqual(units["NEXT_DOWN"].dtype, "int64")
        self.assertTrue(pd.api.types.is_integer_dtype(units["ENDO"].dtype))
        self.assertIn("PFAF_ID", units.columns)

    def test_single_level_defaults_use_nullable_parent_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir)

            units = build_adapter.load_region_units("gr", basins_dir)

        self.assertTrue((units["level"] == 0).all())
        self.assertTrue(pd.api.types.is_integer_dtype(units["level"].dtype))
        self.assertTrue(units["parent_id"].isna().all())
        self.assertEqual(units["parent_id"].dtype, pd.Int64Dtype())

    def test_non_4326_crs_is_transformed(self) -> None:
        geometries = [
            box(0, 0, 1000, 1000),
            box(0, 0, 1000, 1000),
            box(10000, 0, 11000, 1000),
            box(20000, 0, 21000, 1000),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, crs="EPSG:3857", geometries=geometries)

            units = build_adapter.load_region_units("gr", basins_dir)

        self.assertEqual(units.crs.to_epsg(), 4326)
        self.assertLess(units.total_bounds[2], 1.0)
        self.assertNotAlmostEqual(units.total_bounds[2], 21000.0)

    def test_invalid_polygon_is_repaired(self) -> None:
        geometries = [
            Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)]),
            box(3, 0, 4, 1),
            box(5, 0, 6, 1),
            box(7, 0, 8, 1),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, geometries=geometries)

            units = build_adapter.load_region_units("gr", basins_dir)

        self.assertTrue(units.geometry.is_valid.all())
        self.assertTrue((~units.geometry.is_empty).all())
        self.assertTrue(units.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all())

    def test_hilbert_order_is_deterministic_with_id_tie_break(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir)

            first = build_adapter.load_region_units("gr", basins_dir)
            second = build_adapter.load_region_units("gr", basins_dir)

        self.assertEqual(first["id"].tolist(), second["id"].tolist())
        distances = first.geometry.centroid.hilbert_distance(
            total_bounds=first.total_bounds
        )
        self.assertTrue((distances.diff().dropna() >= 0).all())
        tied_ids = first.loc[first["id"].isin([10, 30]), "id"].tolist()
        self.assertEqual(tied_ids, [10, 30])
        self.assertEqual(first.index.tolist(), list(range(len(first))))


class BuildCatchmentsTests(unittest.TestCase):
    """Prove the command path writes the required GeoParquet slice."""

    def test_build_writes_conformant_catchments_slice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "basins"
            pour_points_dir = root / "pour_points"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)

            expected = build_adapter.load_region_units("gr", basins_dir)
            return_code = build_adapter.main(
                _build_args(basins_dir, pour_points_dir, out_dir)
            )
            catchments_path = out_dir / "catchments.parquet"
            graph_path = out_dir / "graph.parquet"
            self.assertEqual(return_code, 0)
            self.assertTrue(catchments_path.is_file())
            self.assertTrue(graph_path.is_file())

            parquet_file = pq.ParquetFile(catchments_path)
            table = pq.read_table(catchments_path)
            schema = parquet_file.schema_arrow
            expected_names = [
                "id",
                "level",
                "parent_id",
                "area_km2",
                "up_area_km2",
                "outlet_lon",
                "outlet_lat",
                "bbox",
                "geometry",
            ]
            self.assertEqual(schema.names, expected_names)
            expected_fields = [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("level", pa.int16(), nullable=False),
                pa.field("parent_id", pa.int64(), nullable=True),
                pa.field("area_km2", pa.float32(), nullable=False),
                pa.field("up_area_km2", pa.float32(), nullable=True),
                pa.field("outlet_lon", pa.float64(), nullable=False),
                pa.field("outlet_lat", pa.float64(), nullable=False),
                pa.field("bbox", build_adapter.bbox_struct_type(), nullable=False),
                pa.field("geometry", pa.binary(), nullable=False),
            ]
            for actual, required in zip(schema, expected_fields, strict=True):
                self.assertEqual(actual, required)

            bbox = table.column("bbox").combine_chunks()
            self.assertEqual(bbox.null_count, 0)
            self.assertEqual(
                [field.name for field in bbox.type],
                list(build_adapter.BBOX_LEAF_NAMES),
            )
            for field in bbox.type:
                self.assertEqual(field.type, pa.float32())
                self.assertFalse(field.nullable)
            for index, name in enumerate(build_adapter.BBOX_LEAF_NAMES):
                self.assertEqual(bbox.field(index).null_count, 0, name)

            output = gpd.read_parquet(catchments_path)
            output_bounds = output.geometry.bounds
            source_names = ("minx", "miny", "maxx", "maxy")
            for index, (stored_name, source_name) in enumerate(
                zip(build_adapter.BBOX_LEAF_NAMES, source_names, strict=True)
            ):
                stored = bbox.field(index).to_numpy()
                wanted = output_bounds[source_name].to_numpy(dtype="float32")
                np.testing.assert_allclose(stored, wanted, err_msg=stored_name)

            geo = json.loads(schema.metadata[b"geo"])
            self.assertEqual(
                geo,
                {
                    "version": "1.1.0",
                    "primary_column": "geometry",
                    "columns": {
                        "geometry": {
                            "encoding": "WKB",
                            "geometry_types": ["Polygon", "MultiPolygon"],
                            "covering": {
                                "bbox": {
                                    name: ["bbox", name]
                                    for name in build_adapter.BBOX_LEAF_NAMES
                                }
                            },
                        }
                    },
                },
            )

            expected_outlets = {
                identifier: (point.x, point.y)
                for identifier, point in zip(ids, points, strict=True)
            }
            for name, coordinate_index, lower, upper in (
                ("outlet_lon", 0, -180.0, 180.0),
                ("outlet_lat", 1, -90.0, 90.0),
            ):
                outlet = table.column(name)
                values = outlet.to_numpy()
                self.assertEqual(outlet.type, pa.float64())
                self.assertEqual(outlet.null_count, 0)
                self.assertTrue(np.isfinite(values).all())
                self.assertFalse(np.isnan(values).any())
                self.assertTrue(((values >= lower) & (values <= upper)).all())
                wanted = [
                    expected_outlets[identifier][coordinate_index]
                    for identifier in table.column("id").to_pylist()
                ]
                np.testing.assert_allclose(values, wanted)

            expected_ids = expected["id"].tolist()
            self.assertEqual(table.column("id").to_pylist(), expected_ids)
            self.assertEqual(output["id"].tolist(), expected_ids)
            distances = output.geometry.centroid.hilbert_distance(
                total_bounds=output.total_bounds
            )
            self.assertTrue((distances.diff().dropna() >= 0).all())
            self.assertEqual(
                output.loc[output["id"].isin([10, 30]), "id"].tolist(),
                [10, 30],
            )

            self.assertEqual(parquet_file.metadata.num_row_groups, 1)
            row_group = parquet_file.metadata.row_group(0)
            self.assertEqual(row_group.num_rows, 4)
            physical_columns = {
                row_group.column(index).path_in_schema: row_group.column(index)
                for index in range(row_group.num_columns)
            }
            for index, name in enumerate(build_adapter.BBOX_LEAF_NAMES):
                physical = physical_columns[f"bbox.{name}"]
                self.assertIsNotNone(physical.statistics)
                self.assertTrue(physical.statistics.has_min_max)
                stored = bbox.field(index).to_numpy()
                self.assertAlmostEqual(physical.statistics.min, float(stored.min()))
                self.assertAlmostEqual(physical.statistics.max, float(stored.max()))

            validation = validate_geoparquet(
                str(catchments_path), target_version="1.1"
            )
            failures = [
                f"{check.name}: {check.message}"
                for check in validation.checks
                if check.status.value == "failed"
            ]
            self.assertTrue(validation.is_valid, "; ".join(failures))

            graph_file = pq.ParquetFile(graph_path)
            graph = pq.read_table(graph_path)
            graph_schema = graph_file.schema_arrow
            list_type = pa.list_(pa.field("item", pa.int64(), nullable=True))
            graph_fields = [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("level", pa.int16(), nullable=False),
                pa.field("upstream_ids", list_type, nullable=False),
                pa.field("bbox_minx", pa.float32(), nullable=False),
                pa.field("bbox_miny", pa.float32(), nullable=False),
                pa.field("bbox_maxx", pa.float32(), nullable=False),
                pa.field("bbox_maxy", pa.float32(), nullable=False),
            ]
            self.assertEqual(graph_schema.names, [field.name for field in graph_fields])
            for actual, required in zip(graph_schema, graph_fields, strict=True):
                self.assertEqual(actual, required)
            for column in graph.columns:
                self.assertEqual(column.null_count, 0)

            upstream_column = graph.column("upstream_ids").combine_chunks()
            self.assertEqual(upstream_column.type, list_type)
            self.assertEqual(upstream_column.flatten().type, pa.int64())
            self.assertEqual(upstream_column.flatten().null_count, 0)

            graph_ids = graph.column("id").to_pylist()
            graph_levels = graph.column("level").to_pylist()
            catchment_ids = table.column("id").to_pylist()
            catchment_levels = table.column("level").to_pylist()
            self.assertEqual(graph.num_rows, table.num_rows)
            self.assertEqual(len(graph_ids), len(set(graph_ids)))
            self.assertEqual(set(graph_ids), set(catchment_ids))
            self.assertEqual(graph_ids, catchment_ids)
            self.assertEqual(graph_levels, catchment_levels)
            self.assertEqual(graph_levels, [0, 0, 0, 0])

            catchments_by_id = output.set_index("id")
            for row in graph.to_pylist():
                referenced = catchments_by_id.loc[row["id"]]
                self.assertEqual(row["level"], int(referenced["level"]))
                wanted_bounds = np.asarray(
                    referenced.geometry.bounds,
                    dtype="float32",
                )
                stored_bounds = np.asarray(
                    [
                        row["bbox_minx"],
                        row["bbox_miny"],
                        row["bbox_maxx"],
                        row["bbox_maxy"],
                    ],
                    dtype="float32",
                )
                np.testing.assert_array_equal(stored_bounds, wanted_bounds)

            levels_by_id = dict(zip(graph_ids, graph_levels, strict=True))
            adjacency = dict(
                zip(graph_ids, upstream_column.to_pylist(), strict=True)
            )
            for downstream_id, upstream_ids in adjacency.items():
                self.assertEqual(upstream_ids, sorted(upstream_ids))
                self.assertTrue(set(upstream_ids) <= set(graph_ids))
                self.assertTrue(
                    all(
                        levels_by_id[upstream_id] == levels_by_id[downstream_id]
                        for upstream_id in upstream_ids
                    )
                )
            self.assertEqual(adjacency[20], [10, 30])
            self.assertEqual(adjacency[10], [])
            self.assertEqual(adjacency[30], [])
            self.assertEqual(adjacency[40], [])
            self.assertNotIn(40, adjacency[20])

            downstream_counts = {identifier: 0 for identifier in graph_ids}
            for upstream_ids in adjacency.values():
                for upstream_id in upstream_ids:
                    downstream_counts[upstream_id] += 1
            self.assertTrue(all(count <= 1 for count in downstream_counts.values()))

            self.assertEqual(graph_file.metadata.num_row_groups, 1)
            graph_row_group = graph_file.metadata.row_group(0)
            self.assertEqual(graph_row_group.num_rows, 4)
            graph_physical_columns = {
                graph_row_group.column(index).path_in_schema: graph_row_group.column(index)
                for index in range(graph_row_group.num_columns)
            }
            for name in ("bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"):
                physical = graph_physical_columns[name]
                self.assertIsNotNone(physical.statistics)
                self.assertTrue(physical.statistics.has_min_max)
                stored = graph.column(name).to_numpy()
                self.assertAlmostEqual(physical.statistics.min, float(stored.min()))
                self.assertAlmostEqual(physical.statistics.max, float(stored.max()))


class BuildManifestTests(unittest.TestCase):
    """Exercise manifest serialization through the public build command."""

    def test_regional_manifest_describes_built_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "basins"
            pour_points_dir = root / "pour_points"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            units = build_adapter.load_region_units("gr", basins_dir)

            return_code = build_adapter.main(
                _build_args(basins_dir, pour_points_dir, out_dir)
            )

            manifest_path = out_dir / "manifest.json"
            self.assertEqual(return_code, 0)
            self.assertTrue(manifest_path.is_file())
            with manifest_path.open(encoding="utf-8") as manifest_file:
                manifest = json.load(manifest_file)
            catchments = gpd.read_parquet(out_dir / "catchments.parquet")

        self.assertEqual(manifest["format_version"], "0.3.0")
        self.assertEqual(manifest["fabric_name"], "hydrobasins")
        self.assertEqual(manifest["fabric_version"], "v1c")
        self.assertEqual(manifest["crs"], "EPSG:4326")
        self.assertIs(manifest["has_up_area"], True)
        self.assertEqual(manifest["topology"], "tree")
        self.assertEqual(manifest["adapter_version"], "0.1.0")
        self.assertEqual(manifest["unit_count"], len(units))
        self.assertEqual(manifest["unit_count"], len(catchments))
        self.assertEqual(manifest["region"], "gr")
        expected_bbox = [float(value) for value in units.geometry.total_bounds]
        np.testing.assert_allclose(manifest["bbox"], expected_bbox)
        self.assertNotIn("auxiliary", manifest)
        datetime.fromisoformat(manifest["created_at"])

    def test_planetary_manifest_omits_region_and_uses_global_bbox(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "basins"
            pour_points_dir = root / "pour_points"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            args = _build_args(basins_dir, pour_points_dir, out_dir)
            args.append("--planetary")

            return_code = build_adapter.main(args)

            manifest_path = out_dir / "manifest.json"
            self.assertEqual(return_code, 0)
            self.assertTrue(manifest_path.is_file())
            with manifest_path.open(encoding="utf-8") as manifest_file:
                manifest = json.load(manifest_file)
            catchments = gpd.read_parquet(out_dir / "catchments.parquet")

        self.assertEqual(manifest["format_version"], "0.3.0")
        self.assertEqual(manifest["fabric_name"], "hydrobasins")
        self.assertEqual(manifest["fabric_version"], "v1c")
        self.assertEqual(manifest["crs"], "EPSG:4326")
        self.assertIs(manifest["has_up_area"], True)
        self.assertEqual(manifest["topology"], "tree")
        self.assertEqual(manifest["adapter_version"], "0.1.0")
        self.assertEqual(manifest["unit_count"], len(ids))
        self.assertEqual(manifest["unit_count"], len(catchments))
        self.assertNotIn("region", manifest)
        self.assertEqual(manifest["bbox"], [-180.0, -90.0, 180.0, 90.0])
        self.assertNotIn("auxiliary", manifest)
        datetime.fromisoformat(manifest["created_at"])


class BuildGraphErrorTests(unittest.TestCase):
    """Reject invalid topology through the public build command."""

    def _run_invalid_build(
        self,
        *,
        next_down: list[object],
        endo: list[object],
    ) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        basins_dir = root / "basins"
        pour_points_dir = root / "pour_points"
        out_dir = root / "out"
        basins_dir.mkdir()
        _write_layer(basins_dir, next_down=next_down, endo=endo)
        ids, points = _ordinary_points()
        _write_pour_points(pour_points_dir, ids=ids, points=points)
        build_adapter.main(_build_args(basins_dir, pour_points_dir, out_dir))

    def test_two_node_cycle_is_rejected(self) -> None:
        with self.assertRaisesRegex(build_adapter.AdapterError, "cycle|acyclic"):
            self._run_invalid_build(
                next_down=[10, 30, 0, 0],
                endo=[0, 0, 0, 0],
            )

    def test_self_link_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            build_adapter.AdapterError,
            "30.*(?:self-link|cycle)|(?:self-link|cycle).*30",
        ):
            self._run_invalid_build(
                next_down=[30, 0, 0, 0],
                endo=[0, 0, 0, 0],
            )

    def test_out_of_set_downstream_is_rejected(self) -> None:
        with self.assertRaisesRegex(build_adapter.AdapterError, "999"):
            self._run_invalid_build(
                next_down=[999, 0, 0, 0],
                endo=[0, 0, 0, 0],
            )


class PourPointTests(unittest.TestCase):
    """Exercise pour-point source normalization and outlet assignment."""

    def _build_fixture(
        self,
        root: Path,
        *,
        ids: list[object],
        points: list[object],
        key_name: str = "HYBAS_ID",
        crs: str | None = "EPSG:4326",
        extra_columns: dict[str, list[object]] | None = None,
    ) -> Path:
        basins_dir = root / "basins"
        pour_points_dir = root / "pour_points"
        out_dir = root / "out"
        basins_dir.mkdir()
        _write_layer(basins_dir)
        _write_pour_points(
            pour_points_dir,
            ids=ids,
            points=points,
            key_name=key_name,
            crs=crs,
            extra_columns=extra_columns,
        )
        build_adapter.main(_build_args(basins_dir, pour_points_dir, out_dir))
        return out_dir / "catchments.parquet"

    def test_unique_case_insensitive_join_key_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ids, points = _ordinary_points()
            path = self._build_fixture(
                Path(temporary), ids=ids, points=points, key_name="Hybas_ID"
            )
            output = gpd.read_parquet(path).set_index("id")

        for identifier, point in zip(ids, points, strict=True):
            self.assertEqual(output.loc[identifier, "outlet_lon"], point.x)
            self.assertEqual(output.loc[identifier, "outlet_lat"], point.y)

    def test_missing_join_key_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(build_adapter.AdapterError, "HYBAS_ID.*available"):
                self._build_fixture(
                    root,
                    ids=[30, 10, 20, 40],
                    points=_ordinary_points()[1],
                    key_name="OTHER_ID",
                )

    def test_ambiguous_case_insensitive_join_keys_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "basins"
            pour_points_dir = root / "pour_points"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(
                pour_points_dir,
                ids=ids,
                points=points,
                key_name="Hybas_ID",
                extra_columns={"other_id": ids},
            )
            dbf_path = pour_points_dir / "hybas_pour_lev12_v1.dbf"
            dbf = bytearray(dbf_path.read_bytes())
            dbf[64:75] = b"hybas_id\0\0\0"
            dbf_path.write_bytes(dbf)

            with self.assertRaisesRegex(
                build_adapter.AdapterError, "ambiguous.*Hybas_ID.*hybas_id"
            ):
                build_adapter.main(_build_args(basins_dir, pour_points_dir, out_dir))

    def test_exact_join_key_is_preferred(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "basins"
            pour_points_dir = root / "pour_points"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(
                pour_points_dir,
                ids=ids,
                points=points,
                extra_columns={"other_id": [999, 999, 999, 999]},
            )
            dbf_path = pour_points_dir / "hybas_pour_lev12_v1.dbf"
            dbf = bytearray(dbf_path.read_bytes())
            dbf[64:75] = b"hybas_id\0\0\0"
            dbf_path.write_bytes(dbf)
            build_adapter.main(_build_args(basins_dir, pour_points_dir, out_dir))
            path = out_dir / "catchments.parquet"
            output = gpd.read_parquet(path)

        self.assertEqual(set(output["id"]), set(ids))

    def test_missing_unit_is_rejected_and_unrelated_id_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            ids = [30, 10, 40, 999]
            points = [
                Point(0.30, 0.30),
                Point(0.10, 0.10),
                Point(20.20, 0.20),
                Point(30.0, 30.0),
            ]
            with self.assertRaises(build_adapter.AdapterError) as caught:
                self._build_fixture(Path(temporary), ids=ids, points=points)
            self.assertIn("20", str(caught.exception))
            self.assertNotIn("999", str(caught.exception))

    def test_duplicate_points_choose_nearest_then_lexicographic_ties(self) -> None:
        cases = (
            ([Point(0.1, 0.1), Point(0.4, 0.5)], (0.4, 0.5)),
            ([Point(0.25, 0.5), Point(0.75, 0.5)], (0.25, 0.5)),
            ([Point(0.5, 0.25), Point(0.5, 0.75)], (0.5, 0.25)),
        )
        for candidates, wanted in cases:
            with self.subTest(candidates=candidates), tempfile.TemporaryDirectory() as temporary:
                path = self._build_fixture(
                    Path(temporary),
                    ids=[30, 30, 10, 20, 40],
                    points=candidates
                    + [Point(0.1, 0.1), Point(10.2, 0.2), Point(20.2, 0.2)],
                )
                row = gpd.read_parquet(path).set_index("id").loc[30]
                self.assertEqual((row["outlet_lon"], row["outlet_lat"]), wanted)

    def test_invalid_selected_coordinates_are_rejected(self) -> None:
        for bad_point in (Point(181.0, 0.0), Point(0.0, 91.0)):
            with self.subTest(point=bad_point), tempfile.TemporaryDirectory() as temporary:
                ids, points = _ordinary_points()
                points[0] = bad_point
                with self.assertRaisesRegex(build_adapter.AdapterError, "30.*invalid"):
                    self._build_fixture(Path(temporary), ids=ids, points=points)

        with tempfile.TemporaryDirectory() as temporary:
            ids, points = _ordinary_points()
            path = self._build_fixture(
                Path(temporary),
                ids=ids + [999],
                points=points + [Point(181.0, 91.0)],
            )
            self.assertEqual(set(gpd.read_parquet(path)["id"]), set(ids))

    def test_point_layer_match_count_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary)
            with self.assertRaisesRegex(build_adapter.AdapterError, "found 0"):
                build_adapter.load_pour_points(pour_points_dir)

        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary)
            nested = pour_points_dir / "nested"
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_pour_points(nested, ids=ids, points=points)
            with self.assertRaisesRegex(build_adapter.AdapterError, "found 2"):
                build_adapter.load_pour_points(pour_points_dir)

    def test_point_layer_requires_declared_crs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points, crs=None)
            with self.assertRaisesRegex(build_adapter.AdapterError, "no declared CRS"):
                build_adapter.load_pour_points(pour_points_dir)

    def test_point_layer_crs_is_transformed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary)
            _write_pour_points(
                pour_points_dir,
                ids=[30],
                points=[Point(111319.490793, 111325.142866)],
                crs="EPSG:3857",
            )
            points = build_adapter.load_pour_points(pour_points_dir)

        self.assertEqual(points.crs.to_epsg(), 4326)
        self.assertAlmostEqual(points.loc[0, "outlet_lon"], 1.0, places=6)
        self.assertAlmostEqual(points.loc[0, "outlet_lat"], 1.0, places=6)
        self.assertEqual(points.loc[0, "outlet_lon"], points.geometry.iloc[0].x)
        self.assertEqual(points.loc[0, "outlet_lat"], points.geometry.iloc[0].y)

    def test_invalid_point_geometries_are_rejected(self) -> None:
        cases = (None, Point(), LineString([(0, 0), (1, 1)]))
        for geometry in cases:
            with self.subTest(geometry=geometry), tempfile.TemporaryDirectory() as temporary:
                pour_points_dir = Path(temporary)
                _write_pour_points(pour_points_dir, ids=[30], points=[geometry])
                with self.assertRaisesRegex(build_adapter.AdapterError, "geometry"):
                    build_adapter.load_pour_points(pour_points_dir)


if __name__ == "__main__":
    unittest.main()
