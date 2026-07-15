"""Tests for HydroBASINS Pfaf-12 polygon ingestion and serialization."""

from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from geoparquet_io.core.validate import validate_geoparquet
from shapely.geometry import Polygon, box

import build_adapter


LAYER_NAME = "hybas_gr_lev12_v1.shp"


def _write_layer(
    basins_dir: Path,
    *,
    ids: list[object] | None = None,
    omit: str | None = None,
    crs: str = "EPSG:4326",
    geometries: list[Polygon] | None = None,
) -> None:
    values = ids if ids is not None else [30, 10, 20, 40]
    rows: dict[str, list[object]] = {
        "HYBAS_ID": values,
        "SUB_AREA": [3.5, 1.5, 2.5, 4.5],
        "UP_AREA": [30.5, 10.5, 20.5, 40.5],
        "NEXT_DOWN": [20, 20, 0, 20],
        "ENDO": [0, 0, 0, 2],
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


class LoadRegionUnitsTests(unittest.TestCase):
    """Exercise normalization through real ESRI Shapefile I/O."""

    def test_missing_required_column_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            _write_layer(basins_dir, omit="UP_AREA")

            with self.assertRaisesRegex(build_adapter.AdapterError, "UP_AREA"):
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
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)

            expected = build_adapter.load_region_units("gr", basins_dir)
            return_code = build_adapter.main(
                [
                    "build",
                    "--region",
                    "gr",
                    "--basins",
                    str(basins_dir),
                    "--out",
                    str(out_dir),
                ]
            )
            catchments_path = out_dir / "catchments.parquet"
            self.assertEqual(return_code, 0)
            self.assertTrue(catchments_path.is_file())

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

            for name in ("outlet_lon", "outlet_lat"):
                outlet = table.column(name)
                self.assertEqual(outlet.type, pa.float64())
                self.assertEqual(outlet.null_count, 0)
                self.assertTrue(
                    np.isnan(outlet.to_numpy()).all(),
                    "M1 deliberately stubs outlets with NaN; M2 adds real outlets",
                )

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


if __name__ == "__main__":
    unittest.main()
