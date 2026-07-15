"""Tests for HydroBASINS Pfaf-12 polygon ingestion."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, box

import build_adapter


LAYER_NAME = "hybas_gr_lev12_v1.shp"


class LoadRegionUnitsTests(unittest.TestCase):
    """Exercise normalization through real ESRI Shapefile I/O."""

    def _write_layer(
        self,
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

    def test_missing_required_column_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            self._write_layer(basins_dir, omit="UP_AREA")

            with self.assertRaisesRegex(build_adapter.AdapterError, "UP_AREA"):
                build_adapter.load_region_units("gr", basins_dir)

    def test_non_positive_and_non_integral_ids_are_rejected(self) -> None:
        cases = ([0, 10, 20, 40], [-1, 10, 20, 40], [1.5, 10, 20, 40])
        for ids in cases:
            with self.subTest(ids=ids), tempfile.TemporaryDirectory() as temporary:
                basins_dir = Path(temporary)
                self._write_layer(basins_dir, ids=ids)

                with self.assertRaises(build_adapter.AdapterError):
                    build_adapter.load_region_units("gr", basins_dir)

    def test_duplicate_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            self._write_layer(basins_dir, ids=[30, 10, 20, 10])

            with self.assertRaisesRegex(build_adapter.AdapterError, "duplicate"):
                build_adapter.load_region_units("gr", basins_dir)

    def test_attributes_are_mapped_with_contract_dtypes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            self._write_layer(basins_dir)

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
            self._write_layer(basins_dir)

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
            self._write_layer(basins_dir, crs="EPSG:3857", geometries=geometries)

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
            self._write_layer(basins_dir, geometries=geometries)

            units = build_adapter.load_region_units("gr", basins_dir)

        self.assertTrue(units.geometry.is_valid.all())
        self.assertTrue((~units.geometry.is_empty).all())
        self.assertTrue(units.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all())

    def test_hilbert_order_is_deterministic_with_id_tie_break(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary)
            self._write_layer(basins_dir)

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


if __name__ == "__main__":
    unittest.main()
