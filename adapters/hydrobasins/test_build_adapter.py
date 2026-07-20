"""Tests for HydroBASINS Pfaf-12 polygon ingestion and serialization."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from geoparquet_io.core.validate import validate_geoparquet
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    Point,
    Polygon,
    box,
)

import build_adapter


LAYER_NAME = "hybas_gr_lev12_v1c.shp"
POUR_POINTS_LAYER_NAME = "hybas_pour_lev12_v1.shp"
RIVERS_LAYER_NAME = "HydroRIVERS_v10_gr.shp"


def _write_layer(
    basins_dir: Path,
    *,
    region: str = "gr",
    level: int = 12,
    ids: list[object] | None = None,
    pfaf_ids: list[object] | None = None,
    next_down: list[object] | None = None,
    endo: list[object] | None = None,
    omit: str | None = None,
    crs: str = "EPSG:4326",
    geometries: list[Polygon] | None = None,
) -> None:
    values = ids if ids is not None else [30, 10, 20, 40]
    size = len(values)
    default_fixture = size == 4
    rows: dict[str, list[object]] = {
        "HYBAS_ID": values,
        "SUB_AREA": (
            [3.5, 1.5, 2.5, 4.5]
            if default_fixture
            else [float(index + 1) for index in range(size)]
        ),
        "UP_AREA": (
            [30.5, 10.5, 20.5, 40.5]
            if default_fixture
            else [float(index + 1) for index in range(size)]
        ),
        "NEXT_DOWN": next_down if next_down is not None else (
            [20, 20, 0, 20] if default_fixture else [0] * size
        ),
        "ENDO": endo if endo is not None else (
            [0, 0, 0, 2] if default_fixture else [0] * size
        ),
        "PFAF_ID": pfaf_ids if pfaf_ids is not None else (
            [111, 112, 113, 114] if default_fixture else list(range(1, size + 1))
        ),
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
    region_dir = basins_dir / f"hybas_{region}"
    region_dir.mkdir(parents=True, exist_ok=True)
    frame.to_file(
        region_dir / f"hybas_{region}_lev{level:02d}_v1c.shp",
        driver="ESRI Shapefile",
        engine="pyogrio",
        index=False,
    )


def _selector_args(selector: list[str], root: Path) -> list[str]:
    return [
        "build",
        *selector,
        "--basins",
        str(root / "extract"),
        "--pour-points",
        str(root / "extract" / "pour"),
        "--out",
        str(root / "out"),
    ]


def _write_pour_points(
    pour_points_dir: Path,
    *,
    ids: list[object],
    points: list[object],
    level: int = 12,
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
        pour_points_dir / f"hybas_pour_lev{level:02d}_v1.shp",
        driver="ESRI Shapefile",
        engine="pyogrio",
        index=False,
    )


def _write_rivers(
    rivers_dir: Path,
    *,
    layer_path: Path | None = None,
    hyriv_id: list[object] | None = None,
    hybas_l12: list[object] | None = None,
    upland_skm: list[object] | None = None,
    next_down: list[object] | None = None,
    geometries: list[object] | None = None,
    crs: str | None = "EPSG:4326",
    omit: set[str] | None = None,
    extra_columns: dict[str, list[object]] | None = None,
) -> None:
    target = layer_path or rivers_dir / "nested" / RIVERS_LAYER_NAME
    target.parent.mkdir(parents=True, exist_ok=True)
    row_count = len(hybas_l12) if hybas_l12 is not None else 4
    rows: dict[str, list[object]] = {
        "HYRIV_ID": (
            hyriv_id
            if hyriv_id is not None
            else list(range(1, row_count + 1))
        ),
        "HYBAS_L12": hybas_l12 if hybas_l12 is not None else [30, 20, 999, 10],
        "UPLAND_SKM": (
            upland_skm
            if upland_skm is not None
            else [30.25, 20.5, 999.0, 10.75]
        ),
        "NEXT_DOWN": next_down if next_down is not None else [0, 0, 0, 0],
    }
    for field in omit or set():
        rows.pop(field, None)
    for field, values in (extra_columns or {}).items():
        if any(field.casefold() == existing.casefold() for existing in rows):
            raise ValueError(f"duplicate HydroRIVERS field: {field}")
        rows[field] = values
    frame = gpd.GeoDataFrame(
        rows,
        geometry=geometries if geometries is not None else [
            LineString([(-0.25, 0.20), (1.25, 0.80)]),
            LineString([(9.75, 0.15), (11.25, 0.85)]),
            LineString([(29.75, 0.10), (31.25, 0.90)]),
            LineString([(-0.20, 0.35), (1.20, 0.65)]),
        ],
        crs=crs,
    )
    frame.to_file(
        target,
        driver="ESRI Shapefile",
        engine="pyogrio",
        index=False,
    )


def _rename_dbf_field(dbf_path: Path, old: str, new: str) -> None:
    data = bytearray(dbf_path.read_bytes())
    header_length = int.from_bytes(data[8:10], "little")
    replacement = new.encode("ascii").ljust(11, b"\0")
    for offset in range(32, header_length - 1, 32):
        name = bytes(data[offset : offset + 11]).split(b"\0", 1)[0].decode("ascii")
        if name == old:
            data[offset : offset + 11] = replacement
            dbf_path.write_bytes(data)
            return
    raise AssertionError(f"DBF field {old!r} not found in {dbf_path}")


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


def _nested_args(root: Path) -> list[str]:
    return [
        *_selector_args(["--regions", "gr,af"], root),
        "--levels",
        "6-7",
    ]


def _write_nested_fixture(
    root: Path,
    *,
    unresolved_greek_parent: bool = False,
    missing_level_7_pour: bool = False,
    duplicate_greek_parent: bool = False,
) -> None:
    extract_dir = root / "extract"
    _write_layer(
        extract_dir,
        region="gr",
        level=6,
        ids=[6001, 6002],
        pfaf_ids=[123456, 123456 if duplicate_greek_parent else 654321],
        geometries=[box(0, 0, 1, 1), box(2, 0, 3, 1)],
    )
    _write_layer(
        extract_dir,
        region="gr",
        level=7,
        ids=[7001, 7002, 7003],
        pfaf_ids=[
            9999991 if unresolved_greek_parent else 1234561,
            1234562,
            6543211,
        ],
        geometries=[
            box(0, 0, 0.4, 0.4),
            box(0.6, 0.6, 1, 1),
            box(2, 0, 3, 1),
        ],
    )
    _write_layer(
        extract_dir,
        region="af",
        level=6,
        ids=[16001, 16002],
        pfaf_ids=[999999 if unresolved_greek_parent else 123456, 777777],
        geometries=[box(20, 0, 21, 1), box(22, 0, 23, 1)],
    )
    _write_layer(
        extract_dir,
        region="af",
        level=7,
        ids=[17001, 17002],
        pfaf_ids=[
            9999991 if unresolved_greek_parent else 1234563,
            7777771,
        ],
        geometries=[box(20, 0, 21, 1), box(22, 0, 23, 1)],
    )
    _write_pour_points(
        extract_dir / "pour",
        level=6,
        ids=[6001, 6001, 6002, 16001, 16002],
        points=[
            Point(0.25, 0.50),
            Point(0.75, 0.50),
            Point(2.50, 0.50),
            Point(20.50, 0.50),
            Point(22.50, 0.50),
        ],
    )
    point_rows = [
        (7001, Point(0.90, 0.90)),
        (7002, Point(0.10, 0.10)),
        (7003, Point(2.60, 0.50)),
        (7003, Point(2.90, 0.90)),
        (17001, Point(20.60, 0.60)),
        (17002, Point(22.60, 0.60)),
    ]
    if missing_level_7_pour:
        point_rows = [row for row in point_rows if row[0] != 7002]
        point_rows.append((99999, Point(10, 10)))
    _write_pour_points(
        extract_dir / "pour",
        level=7,
        ids=[row[0] for row in point_rows],
        points=[row[1] for row in point_rows],
    )
    _write_layer(
        extract_dir,
        region="gr",
        level=12,
        ids=[12001],
        pfaf_ids=[123456100001],
        geometries=[box(0, 0, 1, 1)],
    )
    _write_layer(
        extract_dir,
        region="af",
        level=12,
        ids=[12002],
        pfaf_ids=[123456300001],
        geometries=[box(20, 0, 21, 1)],
    )
    _write_pour_points(
        extract_dir / "pour",
        level=12,
        ids=[12001, 12002],
        points=[Point(0.5, 0.5), Point(20.5, 0.5)],
    )


def _prepare_nested(root: Path) -> dict[int, gpd.GeoDataFrame]:
    args = build_adapter.build_arg_parser().parse_args(_nested_args(root))
    regions = build_adapter.resolve_build_regions(args)
    return build_adapter._prepare_level_frames(args, regions)


def _write_two_region_snap_fixture(root: Path) -> dict[str, LineString]:
    basins_dir = root / "extract"
    pour_points_dir = basins_dir / "pour"
    rivers_dir = root / "rivers"
    basins_dir.mkdir()

    _write_layer(
        basins_dir,
        region="gr",
        ids=[101, 103],
        next_down=[0, 0],
        geometries=[box(0, 0, 1, 1), box(10, 0, 11, 1)],
    )
    _write_layer(
        basins_dir,
        region="af",
        ids=[202, 204],
        next_down=[0, 0],
        geometries=[box(20, 0, 21, 1), box(30, 0, 31, 1)],
    )
    _write_pour_points(
        pour_points_dir,
        ids=[101, 103, 202, 204],
        points=[
            Point(0.5, 0.5), Point(10.5, 0.5),
            Point(20.5, 0.5), Point(30.5, 0.5),
        ],
    )

    gr_tied = LineString([(4.8, 0.2), (5.2, 0.8)])
    af_tied = LineString([(4.8, 0.8), (5.2, 0.2)])
    unresolved_inside_af_unit = LineString([(30.1, 0.2), (30.9, 0.8)])
    af_local = LineString([(20.1, 0.2), (20.9, 0.8)])

    _write_rivers(
        rivers_dir / "gr",
        layer_path=rivers_dir / "gr" / "nested" / "HydroRIVERS_v10_gr.shp",
        hyriv_id=[1001, 1002],
        hybas_l12=[101, 999],
        upland_skm=[11.0, 99.0],
        next_down=[0, 0],
        geometries=[gr_tied, unresolved_inside_af_unit],
    )
    _write_rivers(
        rivers_dir / "af",
        layer_path=rivers_dir / "af" / "nested" / "HydroRIVERS_v10_af.shp",
        hyriv_id=[2001, 2002],
        hybas_l12=[202, 204],
        upland_skm=[22.0, 33.0],
        next_down=[0, 0],
        geometries=[af_tied, af_local],
    )
    return {
        "gr_tied": gr_tied,
        "af_tied": af_tied,
        "unresolved": unresolved_inside_af_unit,
        "af_local": af_local,
    }


def _build_synthetic_dataset(
    tmpdir: Path,
    *,
    geometries: list[Polygon] | None = None,
) -> Path:
    basins_dir = tmpdir / "extract"
    pour_points_dir = tmpdir / "extract" / "pour"
    rivers_dir = tmpdir / "rivers"
    out_dir = tmpdir / "out"
    basins_dir.mkdir()
    _write_layer(basins_dir, geometries=geometries)
    ids, points = _ordinary_points()
    _write_pour_points(pour_points_dir, ids=ids, points=points)
    _write_rivers(rivers_dir)
    return_code = build_adapter.main(
        [
            "build",
            "--region",
            "gr",
            "--basins",
            str(basins_dir),
            "--pour-points",
            str(pour_points_dir),
            "--rivers",
            str(rivers_dir),
            "--out",
            str(out_dir),
        ]
    )
    if return_code != 0:
        raise AssertionError(f"synthetic build returned {return_code}")
    for filename in (
        "catchments.parquet",
        "graph.parquet",
        "manifest.json",
        "aux/snap_stems.parquet",
    ):
        if not (out_dir / filename).is_file():
            raise AssertionError(f"synthetic build did not create {filename}")
    return out_dir


class MultiLevelBuildOrchestrationTests(unittest.TestCase):
    def test_explicit_levels_load_every_region_layer_and_each_global_pour_layer_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root)
            args = build_adapter.build_arg_parser().parse_args(_nested_args(root))
            regions = build_adapter.resolve_build_regions(args)
            with (
                patch(
                    "build_adapter.load_regional_layer",
                    wraps=build_adapter.load_regional_layer,
                ) as regional_loader,
                patch(
                    "build_adapter.load_pour_points",
                    wraps=build_adapter.load_pour_points,
                ) as pour_loader,
            ):
                prepared = build_adapter._prepare_level_frames(args, regions)

            regional_calls = [
                (call.args[0], call.kwargs["source_level"])
                for call in regional_loader.call_args_list
            ]
            pour_calls = [
                call.kwargs["source_level"]
                for call in pour_loader.call_args_list
            ]
            self.assertEqual(
                regional_calls,
                [("gr", 6), ("af", 6), ("gr", 7), ("af", 7)],
            )
            self.assertEqual(pour_calls, [6, 7])
            self.assertEqual(list(prepared), [6, 7])
            self.assertEqual(set(prepared[6]["level"]), {0})
            self.assertEqual(set(prepared[7]["level"]), {1})
            self.assertEqual(set(prepared[6]["id"]), {6001, 6002, 16001, 16002})
            self.assertEqual(set(prepared[7]["id"]), {7001, 7002, 7003, 17001, 17002})
            for frame in prepared.values():
                for column in ("id", "PFAF_ID", "NEXT_DOWN", "ENDO"):
                    self.assertTrue(pd.api.types.is_integer_dtype(frame[column]))
                self.assertEqual(frame["SUB_AREA"].dtype, np.dtype("float64"))
                self.assertEqual(frame["UP_AREA"].dtype, np.dtype("float64"))
                self.assertEqual(str(frame["parent_id"].dtype), "Int64")
                self.assertEqual(frame["outlet_lon"].dtype, np.dtype("float64"))
                self.assertEqual(frame["outlet_lat"].dtype, np.dtype("float64"))

    def test_explicit_multi_level_build_stops_before_artifact_emission(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root)
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(_nested_args(root))
            self.assertIn("multi-level artifact emission", str(caught.exception))
            self.assertIn("deferred to M2-S3", str(caught.exception))
            self.assertFalse((root / "out").exists())


class NestedParentAssignmentTests(unittest.TestCase):
    def test_explicit_levels_assign_region_scoped_prefix_parents(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root)
            prepared = _prepare_nested(root)
            self.assertTrue(prepared[6]["parent_id"].isna().all())
            parents = {
                int(row.id): int(row.parent_id)
                for row in prepared[7].itertuples(index=False)
            }
            self.assertEqual(
                parents,
                {7001: 6001, 7002: 6001, 7003: 6002, 17001: 16001, 17002: 16002},
            )
            self.assertEqual(
                int(prepared[7].loc[prepared[7]["PFAF_ID"] == 1234561, "parent_id"].iloc[0]),
                6001,
            )
            self.assertEqual(
                int(prepared[7].loc[prepared[7]["PFAF_ID"] == 1234563, "parent_id"].iloc[0]),
                16001,
            )

    def test_unresolved_parent_is_fatal_before_artifact_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root, unresolved_greek_parent=True)
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(_nested_args(root))
            message = str(caught.exception)
            for detail in ("7001", "9999991", "gr", "999999", "6", "7"):
                self.assertIn(detail, message)
            self.assertFalse((root / "out").exists())

    def test_duplicate_coarser_pfaf_id_is_fatal_before_parent_resolution_or_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root, duplicate_greek_parent=True)
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(_nested_args(root))
            message = str(caught.exception)
            self.assertIn("duplicate PFAF_ID values", message)
            self.assertIn("123456", message)
            self.assertFalse((root / "out").exists())


class LevelSpecificOutletTests(unittest.TestCase):
    def test_each_level_uses_its_own_global_pour_points_and_independent_collapse(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root)
            prepared = _prepare_nested(root)
            outlets_6 = {
                int(row.id): (float(row.outlet_lon), float(row.outlet_lat))
                for row in prepared[6].itertuples(index=False)
            }
            outlets_7 = {
                int(row.id): (float(row.outlet_lon), float(row.outlet_lat))
                for row in prepared[7].itertuples(index=False)
            }
            self.assertEqual(outlets_6[6001], (0.25, 0.50))
            self.assertNotIn(outlets_6[6001], {outlets_7[7001], outlets_7[7002]})
            self.assertEqual(
                outlets_7,
                {
                    7001: (0.90, 0.90),
                    7002: (0.10, 0.10),
                    7003: (2.60, 0.50),
                    17001: (20.60, 0.60),
                    17002: (22.60, 0.60),
                },
            )

    def test_missing_pour_point_at_any_selected_level_is_fatal_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root, missing_level_7_pour=True)
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(_nested_args(root))
            message = str(caught.exception)
            self.assertIn("7002", message)
            self.assertNotIn("99999", message)
            self.assertFalse((root / "out").exists())

    def test_per_level_polygon_and_pour_layers_clamp_before_parent_and_outlet_joins(self) -> None:
        tolerance = build_adapter.COORDINATE_DOMAIN_TOLERANCE_DEGREES
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_nested_fixture(root)
            _write_layer(
                root / "extract",
                region="gr",
                level=6,
                ids=[6001, 6002],
                pfaf_ids=[123456, 654321],
                geometries=[
                    box(179.5, 0, 180 + tolerance / 2, 1),
                    box(2, 0, 3, 1),
                ],
            )
            _write_pour_points(
                root / "extract" / "pour",
                level=6,
                ids=[6001, 6002, 16001, 16002],
                points=[
                    Point(180 + tolerance / 2, 0.5),
                    Point(2.5, 0.5),
                    Point(20.5, 0.5),
                    Point(22.5, 0.5),
                ],
            )
            prepared = _prepare_nested(root)
            unit = prepared[6].loc[prepared[6]["id"] == 6001].iloc[0]
            self.assertEqual(unit.geometry.bounds[2], 180.0)
            self.assertEqual(unit.outlet_lon, 180.0)

        for layer in ("polygon", "pour"):
            with self.subTest(layer=layer), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                _write_nested_fixture(root)
                if layer == "polygon":
                    _write_layer(
                        root / "extract",
                        region="gr",
                        level=6,
                        ids=[6001, 6002],
                        pfaf_ids=[123456, 654321],
                        geometries=[
                            box(179.5, 0, 180 + tolerance * 2, 1),
                            box(2, 0, 3, 1),
                        ],
                    )
                else:
                    _write_pour_points(
                        root / "extract" / "pour",
                        level=6,
                        ids=[6001, 6002, 16001, 16002],
                        points=[
                            Point(180 + tolerance * 2, 0.5),
                            Point(2.5, 0.5),
                            Point(20.5, 0.5),
                            Point(22.5, 0.5),
                        ],
                    )
                with self.assertRaises(build_adapter.AdapterError) as caught:
                    build_adapter.main(_nested_args(root))
                self.assertIn("coordinate-domain", str(caught.exception))
                self.assertFalse((root / "out").exists())


class LevelRangeTests(unittest.TestCase):
    """Exercise the build selector's central source-level representation."""

    def test_documented_singleton_and_contiguous_ranges_parse_and_map_levels(self) -> None:
        root = Path("/unused")
        cases = (
            ("12", (12, 12), (12,), ((12, 0),)),
            ("6-12", (6, 12), tuple(range(6, 13)), ((6, 0), (12, 6))),
            ("1-12", (1, 12), tuple(range(1, 13)), ((1, 0), (12, 11))),
        )
        for value, bounds, source_levels, mappings in cases:
            with self.subTest(value=value):
                args = build_adapter.build_arg_parser().parse_args(
                    [*_selector_args(["--region", "gr"], root), "--levels", value]
                )
                self.assertIsInstance(args.levels, build_adapter.LevelRange)
                self.assertEqual((args.levels.minimum, args.levels.maximum), bounds)
                self.assertEqual(args.levels.source_levels, source_levels)
                for source_level, hfx_level in mappings:
                    self.assertEqual(args.levels.hfx_level(source_level), hfx_level)

    def test_build_parser_defaults_to_singleton_level_12(self) -> None:
        args = build_adapter.build_arg_parser().parse_args(
            _selector_args(["--region", "gr"], Path("/unused"))
        )
        self.assertEqual(args.levels, build_adapter.LevelRange(minimum=12, maximum=12))
        self.assertEqual(args.levels.source_levels, (12,))
        self.assertEqual(args.levels.hfx_level(12), 0)

    def test_invalid_level_selectors_raise_exact_adapter_errors(self) -> None:
        syntax_values = ("", "six", "6-", "-12", "1-2-3", "1,2", "1,3", "1 3", " 12 ")
        cases = [
            *(
                (value, f"invalid --levels '{value}': expected a singleton N or contiguous range N-M")
                for value in syntax_values
            ),
            ("12-6", "invalid --levels '12-6': range must be ascending"),
            *(
                (value, f"invalid --levels '{value}': levels must be within 1-12")
                for value in ("0", "13", "0-12", "1-13")
            ),
        ]
        parser = build_adapter.build_arg_parser()
        for value, message in cases:
            with self.subTest(value=value), self.assertRaises(build_adapter.AdapterError) as caught:
                parser.parse_args(
                    [*_selector_args(["--region", "gr"], Path("/unused")), "--levels", value]
                )
            self.assertEqual(str(caught.exception), message)


class CanonicalSourceLayoutTests(unittest.TestCase):
    """Pin direct, level-specific scanner-proven source paths."""

    def test_source_paths_resolve_extract_layout_for_requested_level(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            extract_dir = Path(temporary) / "extract"
            region_dir = extract_dir / "hybas_gr"
            region_dir.mkdir(parents=True)
            expected = {}
            for level in (1, 6, 12):
                path = region_dir / f"hybas_gr_lev{level:02d}_v1c.shp"
                path.touch()
                expected[level] = path
            for level, path in expected.items():
                self.assertEqual(build_adapter._source_path("gr", extract_dir, level), path)

            (extract_dir / "hybas_gr_lev12_v1.shp").touch()
            expected[12].unlink()
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter._source_path("gr", extract_dir, 12)
            self.assertIn(str(expected[12]), str(caught.exception))
            self.assertIn("found 0", str(caught.exception))

    def test_global_pour_path_is_direct_and_level_specific(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_dir = Path(temporary) / "extract" / "pour"
            pour_dir.mkdir(parents=True)
            expected = {}
            for level in (1, 6, 12):
                path = pour_dir / f"hybas_pour_lev{level:02d}_v1.shp"
                path.touch()
                expected[level] = path
            decoy = pour_dir / "gr" / POUR_POINTS_LAYER_NAME
            decoy.parent.mkdir()
            decoy.touch()
            for level, path in expected.items():
                self.assertEqual(build_adapter._pour_points_source_path(pour_dir, level), path)

            expected[12].unlink()
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter._pour_points_source_path(pour_dir, 12)
            self.assertIn(POUR_POINTS_LAYER_NAME, str(caught.exception))
            self.assertIn("found 0", str(caught.exception))

    def test_default_and_explicit_level_12_builds_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            extract_dir = root / "extract"
            pour_dir = extract_dir / "pour"
            _write_layer(extract_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_dir, ids=ids, points=points)
            default_out = root / "default"
            explicit_out = root / "explicit"
            instant = datetime.fromisoformat("2026-07-20T12:00:00+00:00")
            with patch("build_adapter.datetime") as frozen_datetime:
                frozen_datetime.now.return_value = instant
                self.assertEqual(
                    build_adapter.main(_build_args(extract_dir, pour_dir, default_out)), 0
                )
                explicit_args = _build_args(extract_dir, pour_dir, explicit_out)
                explicit_args.extend(["--levels", "12"])
                self.assertEqual(build_adapter.main(explicit_args), 0)

            default_files = sorted(path.relative_to(default_out) for path in default_out.rglob("*") if path.is_file())
            explicit_files = sorted(path.relative_to(explicit_out) for path in explicit_out.rglob("*") if path.is_file())
            self.assertEqual(default_files, explicit_files)
            for relative_path in default_files:
                self.assertEqual(
                    (default_out / relative_path).read_bytes(),
                    (explicit_out / relative_path).read_bytes(),
                )


class ExtractCommandTests(unittest.TestCase):
    """Exercise read-only source inspection through the public CLI."""

    def test_extract_reports_sources_without_writing_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = basins_dir / "pour"
            sentinel = root / "candidate-output"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(
                pour_points_dir,
                ids=ids,
                points=points,
                key_name="Hybas_ID",
            )
            with tempfile.TemporaryFile(mode="w+") as stdout:
                with patch("sys.stdout", stdout):
                    return_code = build_adapter.main(
                        [
                            "extract",
                            "--region",
                            "gr",
                            "--basins",
                            str(basins_dir),
                            "--pour-points",
                            str(pour_points_dir),
                        ]
                    )
                stdout.seek(0)
                inspection = stdout.read()

            self.assertEqual(return_code, 0)
            self.assertIn("basins features: 4", inspection)
            self.assertIn("pour points features: 4", inspection)
            self.assertEqual(inspection.count("CRS: EPSG:4326"), 2)
            for column in ("HYBAS_ID", "SUB_AREA", "UP_AREA", "NEXT_DOWN", "ENDO"):
                self.assertIn(column, inspection)
            self.assertIn("pour points join key: Hybas_ID", inspection)
            self.assertFalse(sentinel.exists())
            for filename in ("catchments.parquet", "graph.parquet", "manifest.json"):
                self.assertEqual(list(root.rglob(filename)), [])

    def test_extract_rejects_missing_polygon_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            basins_dir.mkdir()
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            with self.assertRaisesRegex(build_adapter.AdapterError, "found 0"):
                build_adapter.main(
                    ["extract", "--region", "gr", "--basins", str(basins_dir),
                     "--pour-points", str(pour_points_dir)]
                )

    def test_extract_rejects_missing_pour_points_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            basins_dir.mkdir()
            pour_points_dir.mkdir()
            _write_layer(basins_dir)
            with self.assertRaisesRegex(build_adapter.AdapterError, "found 0"):
                build_adapter.main(
                    ["extract", "--region", "gr", "--basins", str(basins_dir),
                     "--pour-points", str(pour_points_dir)]
                )

    def test_extract_translates_unreadable_polygon_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            basins_dir.mkdir()
            region_dir = basins_dir / "hybas_gr"
            region_dir.mkdir()
            (region_dir / LAYER_NAME).write_text(
                "not a shapefile", encoding="utf-8"
            )
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            with self.assertRaisesRegex(
                build_adapter.AdapterError, "regional HydroBASINS polygon layer"
            ):
                build_adapter.main(
                    ["extract", "--region", "gr", "--basins", str(basins_dir),
                     "--pour-points", str(pour_points_dir)]
                )

    def test_extract_translates_unreadable_pour_points_layer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            basins_dir.mkdir()
            pour_points_dir.mkdir()
            _write_layer(basins_dir)
            (pour_points_dir / POUR_POINTS_LAYER_NAME).write_text(
                "not a shapefile", encoding="utf-8"
            )
            with self.assertRaisesRegex(
                build_adapter.AdapterError, "HydroBASINS pour-points layer"
            ):
                build_adapter.main(
                    ["extract", "--region", "gr", "--basins", str(basins_dir),
                     "--pour-points", str(pour_points_dir)]
                )


class ValidateCommandTests(unittest.TestCase):
    """Exercise strict validator command construction and report persistence."""

    @patch("build_adapter.subprocess.run")
    def test_validate_writes_text_and_json_reports(self, run) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "dataset"
            report_dir = root / "reports"
            dataset.mkdir()
            run.side_effect = [
                subprocess.CompletedProcess([], 0, stdout="text output\n", stderr="text warning\n"),
                subprocess.CompletedProcess([], 0, stdout='{"valid": true}\n', stderr=""),
            ]

            return_code = build_adapter.main(
                ["validate", str(dataset), "--report-dir", str(report_dir)]
            )

            self.assertEqual(return_code, 0)
            self.assertEqual(run.call_count, 2)
            expected_commands = [
                ["cargo", "run", "-p", "hfx-cli", "--", str(dataset),
                 "--format", "text", "--strict", "--sample-pct", "100"],
                ["cargo", "run", "-p", "hfx-cli", "--", str(dataset),
                 "--format", "json", "--strict", "--sample-pct", "100"],
            ]
            for call, command in zip(run.call_args_list, expected_commands):
                self.assertEqual(call.args, (command,))
                self.assertEqual(call.kwargs["cwd"], Path(build_adapter.__file__).parents[2])
                self.assertEqual(call.kwargs["env"], dict(os.environ))
                self.assertIs(call.kwargs["capture_output"], True)
                self.assertIs(call.kwargs["text"], True)
                self.assertIs(call.kwargs["check"], False)
            self.assertEqual(
                (report_dir / "validator-report.text").read_text(encoding="utf-8"),
                "text output\n",
            )
            self.assertEqual(
                (report_dir / "validator-report.json").read_text(encoding="utf-8"),
                '{"valid": true}\n',
            )
            self.assertEqual(
                (report_dir / "validator-report.text.stderr").read_text(encoding="utf-8"),
                "text warning\n",
            )
            self.assertFalse((report_dir / "validator-report.json.stderr").exists())

    @patch("build_adapter.subprocess.run")
    def test_validate_defaults_report_directory(self, run) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dataset = Path(temporary) / "dataset"
            dataset.mkdir()
            run.side_effect = [
                subprocess.CompletedProcess([], 0, stdout="text", stderr=""),
                subprocess.CompletedProcess([], 0, stdout="json", stderr=""),
            ]

            self.assertEqual(build_adapter.main(["validate", str(dataset)]), 0)

            self.assertEqual(
                (dataset / "validation" / "validator-report.text").read_text(
                    encoding="utf-8"
                ),
                "text",
            )
            self.assertEqual(
                (dataset / "validation" / "validator-report.json").read_text(
                    encoding="utf-8"
                ),
                "json",
            )

    @patch("build_adapter.subprocess.run")
    def test_validate_persists_failed_report_before_raising(self, run) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "dataset"
            report_dir = root / "reports"
            dataset.mkdir()
            run.return_value = subprocess.CompletedProcess(
                [], 1, stdout="invalid\n", stderr="failure detail\n"
            )

            with self.assertRaisesRegex(
                build_adapter.AdapterError, str(report_dir)
            ):
                build_adapter.main(
                    ["validate", str(dataset), "--report-dir", str(report_dir)]
                )

            self.assertEqual(run.call_count, 1)
            self.assertEqual(
                (report_dir / "validator-report.text").read_text(encoding="utf-8"),
                "invalid\n",
            )
            self.assertEqual(
                (report_dir / "validator-report.text.stderr").read_text(encoding="utf-8"),
                "failure detail\n",
            )


class ConformanceTests(unittest.TestCase):
    """Run authoritative strict conformance when a prebuilt validator is supplied."""

    @unittest.skipUnless(os.environ.get("HFX_BIN"), "HFX_BIN not set")
    def test_synthetic_dataset_passes_strict_conformance(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            dataset = _build_synthetic_dataset(Path(temporary))
            result = subprocess.run(
                [os.environ["HFX_BIN"], str(dataset), "--strict", "--sample-pct", "100"],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout)

    @unittest.skipUnless(os.environ.get("HFX_BIN"), "HFX_BIN not set")
    def test_regional_float32_bbox_round_up_passes_strict_conformance(self) -> None:
        max_y = 81.85897607
        self.assertGreater(float(np.float32(max_y)), max_y)
        geometries = [
            Polygon([
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            Polygon([
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
            ]),
            Polygon([
                (10.0, 0.0),
                (11.0, 0.0),
                (11.0, 1.0),
                (10.0, 1.0),
                (10.0, 0.0),
            ]),
            Polygon([
                (20.0, 0.0),
                (21.0, 0.0),
                (21.0, max_y),
                (20.0, max_y),
                (20.0, 0.0),
            ]),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            dataset = _build_synthetic_dataset(
                Path(temporary), geometries=geometries
            )
            result = subprocess.run(
                [
                    os.environ["HFX_BIN"],
                    str(dataset),
                    "--strict",
                    "--sample-pct",
                    "100",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            report = result.stdout + result.stderr
            self.assertEqual(result.returncode, 0, report)
            self.assertIn("Result: VALID", result.stdout)


class HydroRiversSnapTests(unittest.TestCase):
    """Exercise HydroRIVERS snap emission through the public build command."""

    def _assert_stem_role_vocabulary(self, table: pa.Table) -> None:
        roles = table.column("stem_role").to_pylist()
        self.assertLessEqual(
            set(roles),
            {"mainstem", "tributary", "unknown"},
        )
        self.assertNotIn("distributary", roles)

    def _build(self) -> tuple[Path, list[str]]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        basins_dir = root / "extract"
        pour_points_dir = root / "extract" / "pour"
        rivers_dir = root / "rivers"
        out_dir = root / "out"
        basins_dir.mkdir()
        _write_layer(basins_dir)
        ids, points = _ordinary_points()
        _write_pour_points(pour_points_dir, ids=ids, points=points)
        _write_rivers(rivers_dir)
        with self.assertLogs(build_adapter.__name__, level="INFO") as captured:
            return_code = build_adapter.main(
                _build_args(basins_dir, pour_points_dir, out_dir)
                + ["--rivers", str(rivers_dir)]
            )
        self.assertEqual(return_code, 0)
        return out_dir, captured.output

    def test_well_formed_confluence_emits_largest_upstream_as_mainstem(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hybas_l12=[30, 10, 20],
                upland_skm=[80.0, 20.0, 100.0],
                next_down=[200, 200, 0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                    LineString([(-0.20, 0.35), (1.20, 0.65)]),
                    LineString([(9.75, 0.15), (11.25, 0.85)]),
                ],
                hyriv_id=[101, 102, 200],
            )

            return_code = build_adapter.main(
                _build_args(basins_dir, pour_points_dir, out_dir)
                + ["--rivers", str(rivers_dir)]
            )

            self.assertEqual(return_code, 0)
            table = pq.read_table(out_dir / "aux" / "snap_stems.parquet")
            roles_by_unit = dict(
                zip(
                    table.column("unit_id").to_pylist(),
                    table.column("stem_role").to_pylist(),
                    strict=True,
                )
            )
            self.assertEqual(roles_by_unit[30], "mainstem")
            self.assertEqual(roles_by_unit[10], "tributary")
            self.assertEqual(roles_by_unit[20], "mainstem")
            self._assert_stem_role_vocabulary(table)

    def test_equal_area_confluence_uses_higher_reach_id_deterministically(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hyriv_id=[101, 102, 200],
                hybas_l12=[30, 10, 20],
                upland_skm=[50.0, 50.0, 100.0],
                next_down=[200, 200, 0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                    LineString([(-0.20, 0.35), (1.20, 0.65)]),
                    LineString([(9.75, 0.15), (11.25, 0.85)]),
                ],
            )

            tables = []
            for output_name in ("out_first", "out_second"):
                out_dir = root / output_name
                self.assertEqual(
                    build_adapter.main(
                        _build_args(basins_dir, pour_points_dir, out_dir)
                        + ["--rivers", str(rivers_dir)]
                    ),
                    0,
                )
                table = pq.read_table(out_dir / "aux" / "snap_stems.parquet")
                self._assert_stem_role_vocabulary(table)
                tables.append(table)

            mappings = [
                dict(
                    zip(
                        table.column("unit_id").to_pylist(),
                        table.column("stem_role").to_pylist(),
                        strict=True,
                    )
                )
                for table in tables
            ]
            self.assertEqual(
                mappings[0],
                {10: "mainstem", 20: "mainstem", 30: "tributary"},
            )
            self.assertEqual(mappings[0], mappings[1])
            paired_roles = [
                sorted(
                    zip(
                        table.column("unit_id").to_pylist(),
                        table.column("stem_role").to_pylist(),
                        strict=True,
                    )
                )
                for table in tables
            ]
            self.assertEqual(paired_roles[0], paired_roles[1])

    def test_single_contributor_chain_remains_mainstem(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hyriv_id=[101, 200],
                hybas_l12=[30, 20],
                upland_skm=[25.0, 50.0],
                next_down=[200, 0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                    LineString([(9.75, 0.15), (11.25, 0.85)]),
                ],
            )

            self.assertEqual(
                build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                    + ["--rivers", str(rivers_dir)]
                ),
                0,
            )
            table = pq.read_table(out_dir / "aux" / "snap_stems.parquet")
            roles_by_unit = dict(
                zip(
                    table.column("unit_id").to_pylist(),
                    table.column("stem_role").to_pylist(),
                    strict=True,
                )
            )
            self.assertEqual(roles_by_unit, {20: "mainstem", 30: "mainstem"})
            self._assert_stem_role_vocabulary(table)

    def test_terminal_zero_remains_mainstem(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hyriv_id=[101],
                hybas_l12=[30],
                upland_skm=[25.0],
                next_down=[0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                ],
            )

            self.assertEqual(
                build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                    + ["--rivers", str(rivers_dir)]
                ),
                0,
            )
            table = pq.read_table(out_dir / "aux" / "snap_stems.parquet")
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.column("unit_id").to_pylist(), [30])
            self.assertEqual(table.column("stem_role").to_pylist(), ["mainstem"])
            self._assert_stem_role_vocabulary(table)

    def test_null_next_down_is_rejected_without_snap_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hyriv_id=[101, 200],
                hybas_l12=[30, 20],
                upland_skm=[25.0, 50.0],
                next_down=[None, 0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                    LineString([(9.75, 0.15), (11.25, 0.85)]),
                ],
            )

            with self.assertRaisesRegex(
                build_adapter.AdapterError,
                r"NEXT_DOWN.*(?:non-integral|non-numeric)",
            ):
                build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                    + ["--rivers", str(rivers_dir)]
                )
            self.assertFalse((out_dir / "aux" / "snap_stems.parquet").exists())

    def test_missing_next_down_is_rejected_without_snap_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hyriv_id=[101],
                hybas_l12=[30],
                upland_skm=[25.0],
                next_down=[0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                ],
                omit={"NEXT_DOWN"},
            )

            with self.assertRaisesRegex(
                build_adapter.AdapterError,
                r"missing required field NEXT_DOWN",
            ):
                build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                    + ["--rivers", str(rivers_dir)]
                )
            self.assertFalse((out_dir / "aux" / "snap_stems.parquet").exists())

    def test_dangling_positive_next_down_emits_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_rivers(
                rivers_dir,
                hyriv_id=[101],
                hybas_l12=[30],
                upland_skm=[25.0],
                next_down=[999],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                ],
            )

            self.assertEqual(
                build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                    + ["--rivers", str(rivers_dir)]
                ),
                0,
            )
            table = pq.read_table(out_dir / "aux" / "snap_stems.parquet")
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.column("unit_id").to_pylist(), [30])
            self.assertEqual(table.column("stem_role").to_pylist(), ["unknown"])
            self._assert_stem_role_vocabulary(table)

    def test_build_writes_strict_valid_snap_stems(self) -> None:
        out_dir, logs = self._build()
        snap_path = out_dir / "aux" / "snap_stems.parquet"
        for path in (
            out_dir / "catchments.parquet",
            out_dir / "graph.parquet",
            out_dir / "manifest.json",
            snap_path,
        ):
            self.assertTrue(path.is_file(), path)

        self.assertTrue(
            any(
                "dropped 1 HydroRIVERS reaches with HYBAS_L12 absent from the unit set"
                in message
                for message in logs
            ),
            logs,
        )
        parquet_file = pq.ParquetFile(snap_path)
        table = pq.read_table(snap_path)
        expected_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("unit_id", pa.int64(), nullable=False),
                pa.field("weight", pa.float32(), nullable=False),
                pa.field("stem_role", pa.string(), nullable=True),
                pa.field(
                    "bbox",
                    pa.struct(
                        [
                            pa.field("xmin", pa.float32(), nullable=False),
                            pa.field("ymin", pa.float32(), nullable=False),
                            pa.field("xmax", pa.float32(), nullable=False),
                            pa.field("ymax", pa.float32(), nullable=False),
                        ]
                    ),
                    nullable=True,
                ),
                pa.field("geometry", pa.binary(), nullable=False),
            ]
        ).with_metadata(
            {
                b"geo": json.dumps(
                    {
                        "version": "1.1.0",
                        "primary_column": "geometry",
                        "columns": {
                            "geometry": {
                                "encoding": "WKB",
                                "geometry_types": ["LineString"],
                                "covering": {
                                    "bbox": {
                                        "xmin": ["bbox", "xmin"],
                                        "ymin": ["bbox", "ymin"],
                                        "xmax": ["bbox", "xmax"],
                                        "ymax": ["bbox", "ymax"],
                                    }
                                },
                            }
                        },
                    }
                ).encode("utf-8")
            }
        )
        schema = pq.read_schema(snap_path)
        self.assertEqual(schema, expected_schema)
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("id").to_pylist(), [1, 2, 3])
        self.assertEqual(table.column("unit_id").to_pylist(), [20, 30, 10])
        self.assertEqual(table.column("weight").type, pa.float32())
        np.testing.assert_array_equal(
            table.column("weight").to_numpy(),
            np.asarray([20.5, 30.25, 10.75], dtype="float32"),
        )
        self.assertEqual(
            table.column("stem_role").to_pylist(),
            ["mainstem", "mainstem", "mainstem"],
        )
        self._assert_stem_role_vocabulary(table)
        for name in ("id", "unit_id", "weight", "geometry"):
            self.assertEqual(table.column(name).null_count, 0)
        catchment_ids = set(
            pq.read_table(out_dir / "catchments.parquet", columns=["id"])
            .column("id")
            .to_pylist()
        )
        self.assertNotIn(999, table.column("unit_id").to_pylist())
        self.assertTrue(set(table.column("unit_id").to_pylist()) <= catchment_ids)

        output = gpd.read_parquet(snap_path)
        self.assertTrue(output.crs.equals("EPSG:4326", ignore_axis_order=True))
        self.assertEqual(output.geometry.geom_type.tolist(), ["LineString"] * 3)
        expected_geometry = [
            LineString([(9.75, 0.15), (11.25, 0.85)]),
            LineString([(-0.25, 0.20), (1.25, 0.80)]),
            LineString([(-0.20, 0.35), (1.20, 0.65)]),
        ]
        self.assertTrue(
            all(actual.equals_exact(expected, 0.0) for actual, expected in zip(
                output.geometry, expected_geometry, strict=True
            ))
        )
        catchments = gpd.read_parquet(out_dir / "catchments.parquet")
        distances = output.geometry.centroid.hilbert_distance(
            total_bounds=catchments.geometry.total_bounds
        )
        self.assertTrue((distances.diff().dropna() >= 0).all())

        bbox = table.column("bbox").combine_chunks()
        self.assertEqual(bbox.null_count, 0)
        expected_bounds = output.geometry.bounds.to_numpy(dtype="float32")
        stored_bounds = np.column_stack(
            [
                bbox.field(index).to_numpy()
                for index in range(len(build_adapter.BBOX_LEAF_NAMES))
            ]
        )
        self.assertTrue(np.all(stored_bounds[:, 0] <= stored_bounds[:, 2]))
        self.assertTrue(np.all(stored_bounds[:, 1] <= stored_bounds[:, 3]))
        for index, name in enumerate(build_adapter.BBOX_LEAF_NAMES):
            leaf = bbox.field(index)
            self.assertEqual(leaf.null_count, 0)
            np.testing.assert_array_equal(leaf.to_numpy(), expected_bounds[:, index])
            np.testing.assert_allclose(
                leaf.to_numpy(),
                expected_bounds[:, index],
                atol=build_adapter.SNAP_BBOX_EPSILON,
                rtol=0,
            )
        for group_index in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(group_index)
            physical = {
                row_group.column(index).path_in_schema: row_group.column(index)
                for index in range(row_group.num_columns)
            }
            for name in build_adapter.BBOX_LEAF_NAMES:
                statistics = physical[f"bbox.{name}"].statistics
                self.assertIsNotNone(statistics)
                self.assertTrue(statistics.has_min_max)
            encodings = set(physical["stem_role"].encodings)
            self.assertTrue(
                {"RLE_DICTIONARY", "PLAIN_DICTIONARY"} & encodings,
                encodings,
            )
        self.assertEqual(table.column("stem_role").type, pa.string())

        self.assertEqual(
            json.loads(schema.metadata[b"geo"]),
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["LineString"],
                        "covering": {
                            "bbox": {
                                "xmin": ["bbox", "xmin"],
                                "ymin": ["bbox", "ymin"],
                                "xmax": ["bbox", "xmax"],
                                "ymax": ["bbox", "ymax"],
                            }
                        },
                    }
                },
            },
        )
        validation = validate_geoparquet(str(snap_path), target_version="1.1")
        failures = [
            f"{check.name}: {check.message}"
            for check in validation.checks
            if check.status.value == "failed"
        ]
        self.assertTrue(validation.is_valid, "; ".join(failures))

        with (out_dir / "manifest.json").open(encoding="utf-8") as stream:
            manifest = json.load(stream)
        self.assertEqual(
            manifest["auxiliary"],
            [
                {
                    "schema": "hfx.aux.snap.v2",
                    "artifacts": {"snap": "aux/snap_stems.parquet"},
                    "metadata": {
                        "name": "stems",
                        "description": "Unclipped HydroRIVERS reach centerlines for HydroBASINS Pfaf-12 snapping. HydroRIVERS and HydroBASINS are HydroSHEDS products covered by the HydroSHEDS License Agreement. weight = UPLAND_SKM (km^2). stem_role = mainstem/tributary derived from NEXT_DOWN confluences.",
                        "references_levels": [0],
                        "weight_semantics": "drainage_area_km2",
                    },
                }
            ],
        )

    def test_snap_order_is_stable_for_hilbert_ties_after_drops(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            authored_geometries = [
                LineString([(-0.25, 0.20), (1.25, 0.80)]),
                LineString([(29.75, 0.10), (31.25, 0.90)]),
                LineString([(9.75, 0.15), (11.25, 0.85)]),
                LineString([(-0.20, 0.35), (1.20, 0.65)]),
            ]
            authored_ids = [30, 999, 20, 10]
            _write_rivers(
                rivers_dir,
                hybas_l12=authored_ids,
                upland_skm=[30.25, 999.0, 20.5, 10.75],
                next_down=[0, 0, 0, 0],
                geometries=authored_geometries,
                crs="EPSG:4326",
            )

            tables = []
            for output_name in ("out_first", "out_second"):
                out_dir = root / output_name
                self.assertEqual(
                    build_adapter.main(
                        _build_args(basins_dir, pour_points_dir, out_dir)
                        + ["--rivers", str(rivers_dir)]
                    ),
                    0,
                )
                tables.append(pq.read_table(out_dir / "aux" / "snap_stems.parquet"))

            first, second = tables
            for name in ("id", "unit_id", "geometry"):
                self.assertEqual(first.column(name), second.column(name), name)
            self.assertEqual(
                first.column("id").to_pylist(),
                list(range(1, first.num_rows + 1)),
            )
            self.assertEqual(first.column("id").to_pylist(), [1, 2, 3])

            unit_ids = first.column("unit_id").to_pylist()
            self.assertLess(unit_ids.index(30), unit_ids.index(10))
            output = gpd.GeoSeries.from_wkb(first.column("geometry").to_pylist())
            authored_by_id = dict(zip(authored_ids, authored_geometries, strict=True))
            for unit_id, geometry in zip(unit_ids, output, strict=True):
                self.assertTrue(geometry.equals_exact(authored_by_id[unit_id], 0.0))

            tied = output[[unit_ids.index(30), unit_ids.index(10)]]
            self.assertTrue(
                tied.iloc[0].centroid.equals_exact(tied.iloc[1].centroid, 0.0)
            )
            catchments = gpd.read_parquet(root / "out_first" / "catchments.parquet")
            tied_distances = tied.centroid.hilbert_distance(
                total_bounds=catchments.geometry.total_bounds
            )
            self.assertEqual(tied_distances.iloc[0], tied_distances.iloc[1])

    def test_two_region_snap_order_is_stable_when_hilbert_and_source_order_collide(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            authored = _write_two_region_snap_fixture(root)
            tables: list[pa.Table] = []
            frames: list[gpd.GeoDataFrame] = []

            for output_name in ("out_first", "out_second"):
                out_dir = root / output_name
                args = _selector_args(["--regions", "gr,af"], root)[:-1] + [
                    str(out_dir),
                    "--rivers",
                    str(root / "rivers"),
                ]
                self.assertEqual(build_adapter.main(args), 0)
                snap_path = out_dir / "aux" / "snap_stems.parquet"
                tables.append(pq.read_table(snap_path))
                frames.append(gpd.read_parquet(snap_path))

            first, second = tables
            self.assertEqual(first.column("id").to_pylist(), [1, 2, 3])
            self.assertEqual(second.column("id").to_pylist(), [1, 2, 3])
            self.assertTrue(first.equals(second))

            mappings = [
                list(
                    zip(
                        table.column("id").to_pylist(),
                        table.column("unit_id").to_pylist(),
                        table.column("geometry").to_pylist(),
                        strict=True,
                    )
                )
                for table in tables
            ]
            self.assertEqual(mappings[0], mappings[1])

            unit_ids = first.column("unit_id").to_pylist()
            self.assertLess(unit_ids.index(101), unit_ids.index(202))
            tied_rows = [unit_ids.index(101), unit_ids.index(202)]
            tied = frames[0].geometry.iloc[tied_rows]
            self.assertFalse(tied.iloc[0].equals_exact(tied.iloc[1], 0.0))
            self.assertTrue(tied.iloc[0].equals_exact(authored["gr_tied"], 0.0))
            self.assertTrue(tied.iloc[1].equals_exact(authored["af_tied"], 0.0))

            catchments = gpd.read_parquet(root / "out_first" / "catchments.parquet")
            distances = tied.centroid.hilbert_distance(
                total_bounds=catchments.geometry.total_bounds
            )
            self.assertEqual(distances.iloc[0], distances.iloc[1])
            self.assertFalse(
                any(
                    geometry.equals_exact(authored["unresolved"], 0.0)
                    for geometry in frames[0].geometry
                )
            )

    def test_two_region_unresolved_reach_is_dropped_against_merged_units_without_reassignment(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            authored = _write_two_region_snap_fixture(root)
            args = _selector_args(["--regions", "gr,af"], root) + [
                "--rivers",
                str(root / "rivers"),
            ]
            with self.assertLogs(build_adapter.__name__, level="INFO") as captured:
                self.assertEqual(build_adapter.main(args), 0)

            expected = (
                "dropped 1 HydroRIVERS reaches with HYBAS_L12 absent "
                "from the unit set"
            )
            self.assertEqual(sum(expected in message for message in captured.output), 1)
            catchment_ids = set(
                pq.read_table(root / "out" / "catchments.parquet", columns=["id"])
                .column("id")
                .to_pylist()
            )
            snap = gpd.read_parquet(root / "out" / "aux" / "snap_stems.parquet")
            self.assertTrue(set(snap["unit_id"]) <= catchment_ids)
            self.assertEqual(set(snap["unit_id"]), {101, 202, 204})
            self.assertNotIn(999, snap["unit_id"].tolist())
            self.assertFalse(
                any(
                    geometry.equals_exact(authored["unresolved"], 0.0)
                    for geometry in snap.geometry
                )
            )
            expected_by_unit = {
                101: authored["gr_tied"],
                202: authored["af_tied"],
                204: authored["af_local"],
            }
            for unit_id, geometry in zip(snap["unit_id"], snap.geometry, strict=True):
                self.assertTrue(
                    geometry.equals_exact(expected_by_unit[unit_id], 0.0)
                )

    def test_all_regions_resolves_one_rivers_layer_per_selected_region(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            authored = _write_two_region_snap_fixture(root)
            args = _selector_args(["--all-regions"], root) + [
                "--rivers",
                str(root / "rivers"),
            ]
            self.assertEqual(build_adapter.main(args), 0)

            with (root / "out" / "manifest.json").open(encoding="utf-8") as stream:
                manifest = json.load(stream)
            snap = gpd.read_parquet(root / "out" / "aux" / "snap_stems.parquet")
            self.assertEqual(manifest["region"], "af,gr")
            self.assertEqual(len(manifest["auxiliary"]), 1)
            self.assertEqual(set(snap["unit_id"]), {101, 202, 204})
            for expected in (authored["gr_tied"], authored["af_tied"]):
                self.assertTrue(
                    any(
                        geometry.equals_exact(expected, 0.0)
                        for geometry in snap.geometry
                    )
                )

    def test_multi_region_rivers_layer_count_is_enforced_per_selected_directory(
        self,
    ) -> None:
        selectors = (["--regions", "gr,af"], ["--all-regions"])
        for selector in selectors:
            for match_count in (0, 2):
                with self.subTest(selector=selector, match_count=match_count):
                    with tempfile.TemporaryDirectory() as temporary:
                        root = Path(temporary)
                        _write_two_region_snap_fixture(root)
                        if match_count == 0:
                            selected_dir = root / "rivers" / "gr"
                            for path in selected_dir.rglob("*"):
                                if path.is_file():
                                    path.unlink()
                        else:
                            selected_dir = root / "rivers" / "af"
                            _write_rivers(
                                selected_dir,
                                layer_path=selected_dir / "second" / "duplicate.shp",
                                hyriv_id=[9001],
                                hybas_l12=[202],
                                upland_skm=[1.0],
                                next_down=[0],
                                geometries=[
                                    LineString([(20.2, 0.2), (20.8, 0.8)])
                                ],
                            )
                        args = _selector_args(selector, root) + [
                            "--rivers",
                            str(root / "rivers"),
                        ]
                        with self.assertRaises(build_adapter.AdapterError) as caught:
                            build_adapter.main(args)
                        self.assertIn(str(selected_dir), str(caught.exception))
                        self.assertIn(f"found {match_count}", str(caught.exception))
                        self.assertFalse((root / "out").exists())

    def test_two_region_build_without_rivers_remains_core_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_two_region_snap_fixture(root)
            self.assertEqual(
                build_adapter.main(_selector_args(["--regions", "gr,af"], root)),
                0,
            )

            out_dir = root / "out"
            for filename in ("catchments.parquet", "graph.parquet", "manifest.json"):
                self.assertTrue((out_dir / filename).is_file())
            self.assertFalse((out_dir / "aux").exists())
            with (out_dir / "manifest.json").open(encoding="utf-8") as stream:
                manifest = json.load(stream)
            self.assertNotIn("auxiliary", manifest)
            catchments = pq.read_table(out_dir / "catchments.parquet")
            graph = pq.read_table(out_dir / "graph.parquet")
            catchment_ids = catchments.column("id").to_pylist()
            self.assertEqual(set(catchment_ids), {101, 103, 202, 204})
            self.assertEqual(graph.column("id").to_pylist(), catchment_ids)
            self.assertEqual(manifest["region"], "gr,af")
            self.assertEqual(manifest["unit_count"], 4)

    def test_no_rivers_preserves_core_only_artifacts(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        basins_dir = root / "extract"
        pour_points_dir = root / "extract" / "pour"
        out_dir = root / "out"
        basins_dir.mkdir()
        _write_layer(basins_dir)
        ids, points = _ordinary_points()
        _write_pour_points(pour_points_dir, ids=ids, points=points)

        self.assertEqual(
            build_adapter.main(_build_args(basins_dir, pour_points_dir, out_dir)),
            0,
        )
        self.assertFalse((out_dir / "aux").exists())
        with (out_dir / "manifest.json").open(encoding="utf-8") as stream:
            self.assertNotIn("auxiliary", json.load(stream))

    def test_two_regions_merge_rivers_against_merged_unit_set(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = basins_dir / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()

            _write_layer(
                basins_dir,
                region="gr",
                ids=[101, 103],
                next_down=[202, 0],
                geometries=[box(20, 0, 21, 1), box(0, 0, 1, 1)],
            )
            _write_layer(
                basins_dir,
                region="af",
                ids=[202, 204],
                next_down=[0, 0],
                geometries=[box(10, 0, 11, 1), box(30, 0, 31, 1)],
            )
            _write_pour_points(
                pour_points_dir,
                ids=[101, 103, 202, 204],
                points=[
                    Point(20.5, 0.5), Point(0.5, 0.5),
                    Point(10.5, 0.5), Point(30.5, 0.5),
                ],
            )

            gr_local_geometry = LineString([(20.1, 0.2), (20.9, 0.8)])
            cross_region_geometry = LineString([(10.1, 0.2), (10.9, 0.8)])
            af_local_geometry = LineString([(30.1, 0.2), (30.9, 0.8)])
            _write_rivers(
                rivers_dir / "gr",
                layer_path=rivers_dir / "gr" / "nested" / "HydroRIVERS_v10_gr.shp",
                hyriv_id=[1001, 1002],
                hybas_l12=[101, 202],
                upland_skm=[11.0, 22.0],
                next_down=[0, 0],
                geometries=[gr_local_geometry, cross_region_geometry],
            )
            _write_rivers(
                rivers_dir / "af",
                layer_path=rivers_dir / "af" / "nested" / "HydroRIVERS_v10_af.shp",
                hyriv_id=[2001],
                hybas_l12=[204],
                upland_skm=[33.0],
                next_down=[0],
                geometries=[af_local_geometry],
            )

            args = _selector_args(["--regions", "gr,af"], root) + [
                "--rivers",
                str(rivers_dir),
            ]
            self.assertEqual(build_adapter.main(args), 0)

            catchment_ids = set(
                pq.read_table(out_dir / "catchments.parquet", columns=["id"])
                .column("id")
                .to_pylist()
            )
            snap = gpd.read_parquet(out_dir / "aux" / "snap_stems.parquet")
            self.assertEqual(set(snap["unit_id"]), {101, 202, 204})
            self.assertEqual(snap["id"].tolist(), list(range(1, len(snap) + 1)))
            self.assertTrue(set(snap["unit_id"]) <= catchment_ids)
            self.assertTrue(
                any(
                    geometry.equals_exact(gr_local_geometry, 0.0)
                    for geometry in snap.geometry
                )
            )
            self.assertTrue(
                any(
                    geometry.equals_exact(cross_region_geometry, 0.0)
                    for geometry in snap.geometry
                )
            )
            self.assertTrue(
                any(
                    geometry.equals_exact(af_local_geometry, 0.0)
                    for geometry in snap.geometry
                )
            )

            with (out_dir / "manifest.json").open(encoding="utf-8") as stream:
                manifest = json.load(stream)
            self.assertEqual(manifest["region"], "gr,af")
            self.assertEqual(len(manifest["auxiliary"]), 1)
            self.assertEqual(manifest["auxiliary"][0]["schema"], "hfx.aux.snap.v2")
            self.assertEqual(
                manifest["auxiliary"][0]["artifacts"],
                {"snap": "aux/snap_stems.parquet"},
            )


class HydroRiversNormalizationTests(unittest.TestCase):
    """Exercise HydroRIVERS validation at its source boundary."""

    @staticmethod
    def _frame(
        *,
        reach_ids: list[object] | None = None,
        ids: list[object] | None = None,
        weights: list[object] | None = None,
        next_down: list[object] | None = None,
        geometries: list[object] | None = None,
        crs: str | None = "EPSG:4326",
    ) -> gpd.GeoDataFrame:
        ids = ids if ids is not None else [30]
        return gpd.GeoDataFrame(
            {
                "HYRIV_ID": (
                    reach_ids
                    if reach_ids is not None
                    else list(range(1, len(ids) + 1))
                ),
                "HYBAS_L12": ids,
                "UPLAND_SKM": weights if weights is not None else [30.25] * len(ids),
                "NEXT_DOWN": next_down if next_down is not None else [0] * len(ids),
            },
            geometry=geometries if geometries is not None else [
                LineString([(-0.25, 0.20), (1.25, 0.80)])
            ] * len(ids),
            crs=crs,
        )

    def test_source_discovery_reports_exact_recursive_match_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "rivers"
            (root / "nested").mkdir(parents=True)
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter._rivers_source_path(root)
            self.assertIn(str(root), str(caught.exception))
            self.assertIn("found 0", str(caught.exception))

            _write_rivers(
                root,
                layer_path=root / "a" / RIVERS_LAYER_NAME,
                hybas_l12=[30],
                upland_skm=[30.25],
                next_down=[0],
                geometries=[LineString([(-0.25, 0.20), (1.25, 0.80)])],
            )
            _write_rivers(
                root,
                layer_path=root / "b" / "deeper" / "duplicate.shp",
                hybas_l12=[20],
                upland_skm=[20.5],
                next_down=[0],
                geometries=[LineString([(9.75, 0.15), (11.25, 0.85)])],
            )
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter._rivers_source_path(root)
            self.assertIn(str(root), str(caught.exception))
            self.assertIn("found 2", str(caught.exception))

    def test_source_path_preconditions_and_reader_failure_are_clear(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for path in (root / "does-not-exist", root / "rivers"):
                if path.name == "rivers":
                    path.write_text("not a directory\n", encoding="utf-8")
                with self.subTest(path=path), self.assertRaises(
                    build_adapter.AdapterError
                ) as caught:
                    build_adapter._rivers_source_path(path)
                self.assertIn(str(path), str(caught.exception))
                self.assertIn("readable directory", str(caught.exception))

            rivers = root / "broken-rivers"
            rivers.mkdir()
            broken = rivers / "broken.shp"
            broken.write_text("not a shapefile\n", encoding="utf-8")
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.load_rivers(rivers)
            self.assertIn("failed to read HydroRIVERS layer", str(caught.exception))
            self.assertIn(str(broken), str(caught.exception))

    def test_required_attributes_are_resolved_uniquely_case_insensitively(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "rivers"
            required = ("HYRIV_ID", "HYBAS_L12", "UPLAND_SKM", "NEXT_DOWN")
            for missing in required:
                with self.subTest(missing=missing):
                    case_root = root / missing
                    _write_rivers(
                        case_root,
                        hybas_l12=[30],
                        upland_skm=[30.25],
                        next_down=[0],
                        geometries=[LineString([(-0.25, 0.20), (1.25, 0.80)])],
                        omit={missing},
                    )
                    with self.assertRaises(build_adapter.AdapterError) as caught:
                        build_adapter.load_rivers(case_root)
                    self.assertIn(missing, str(caught.exception))
                    self.assertIn("available", str(caught.exception))

            case_root = root / "case"
            _write_rivers(
                case_root,
                hybas_l12=[30],
                upland_skm=[30.25],
                next_down=[0],
                geometries=[LineString([(-0.25, 0.20), (1.25, 0.80)])],
            )
            dbf = next(case_root.rglob("*.dbf"))
            variants = ("hyriv_id", "hybas_l12", "upland_skm", "next_down")
            for canonical, variant in zip(required, variants, strict=True):
                _rename_dbf_field(dbf, canonical, variant)
            normalized = build_adapter.load_rivers(case_root)
            self.assertTrue(set(required) <= set(normalized.columns))

    def test_ambiguous_required_attributes_name_both_candidates(self) -> None:
        cases = (
            ("HYRIV_ID", "oth_reach", "hyriv_id", [2]),
            ("HYBAS_L12", "other_id", "hybas_l12", [20]),
            ("UPLAND_SKM", "other_area", "upland_skm", [31.0]),
            ("NEXT_DOWN", "other_next", "next_down", [99]),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for canonical, extra, variant, values in cases:
                with self.subTest(field=canonical):
                    rivers = root / canonical
                    _write_rivers(
                        rivers,
                        hybas_l12=[30],
                        upland_skm=[30.25],
                        next_down=[0],
                        geometries=[LineString([(-0.25, 0.20), (1.25, 0.80)])],
                        extra_columns={extra: values},
                    )
                    _rename_dbf_field(next(rivers.rglob("*.dbf")), extra, variant)
                    with self.assertRaises(build_adapter.AdapterError) as caught:
                        build_adapter.load_rivers(rivers)
                    message = str(caught.exception)
                    self.assertIn("ambiguous", message)
                    self.assertIn(canonical, message)
                    self.assertIn(variant, message)

    def test_geometry_schema_requires_one_active_geometry(self) -> None:
        plain = pd.DataFrame(
            {
                "HYRIV_ID": [1],
                "HYBAS_L12": [30],
                "UPLAND_SKM": [30.25],
                "NEXT_DOWN": [0],
            }
        )
        with self.assertRaisesRegex(build_adapter.AdapterError, "no.*geometry"):
            build_adapter._normalize_rivers_layer(plain)

        frame = self._frame()
        frame["alternate_geometry"] = gpd.GeoSeries(
            [LineString([(-0.20, 0.35), (1.20, 0.65)])], crs="EPSG:4326"
        )
        with self.assertRaises(build_adapter.AdapterError) as caught:
            build_adapter._normalize_rivers_layer(frame)
        self.assertIn("ambiguous", str(caught.exception))
        self.assertIn("geometry", str(caught.exception))
        self.assertIn("alternate_geometry", str(caught.exception))

    def test_crs_is_required_and_web_mercator_line_is_reprojected_unclipped(self) -> None:
        with self.assertRaisesRegex(build_adapter.AdapterError, "no declared CRS"):
            build_adapter._normalize_rivers_layer(self._frame(crs=None))

        frame = self._frame(
            geometries=[LineString([(0.0, 0.0), (111319.49079327357, 111325.1428663851)])],
            crs="EPSG:3857",
        )
        normalized = build_adapter._normalize_rivers_layer(frame)
        self.assertEqual(normalized.crs.to_epsg(), 4326)
        self.assertEqual(normalized.geometry.iloc[0].geom_type, "LineString")
        coordinates = list(normalized.geometry.iloc[0].coords)
        self.assertEqual(len(coordinates), 2)
        np.testing.assert_allclose(coordinates, [(0.0, 0.0), (1.0, 1.0)], atol=1e-6)

    def test_any_invalid_geometry_rejects_the_complete_layer(self) -> None:
        in_memory_invalid = (
            (None, "null"),
            (LineString(), "empty"),
            (
                GeometryCollection([
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                    Point(0.30, 0.30),
                ]),
                "GeometryCollection",
            ),
        )
        for geometry, diagnostic in in_memory_invalid:
            with self.subTest(geometry=diagnostic), self.assertRaises(
                build_adapter.AdapterError
            ) as caught:
                build_adapter._normalize_rivers_layer(self._frame(geometries=[geometry]))
            self.assertIn(diagnostic, str(caught.exception))

        file_invalid = (
            (
                MultiLineString([
                    [(-0.25, 0.20), (0.50, 0.50)],
                    [(0.50, 0.50), (1.25, 0.80)],
                ]),
                "MultiLineString",
            ),
            (Point(0.30, 0.30), "Point"),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for geometry, diagnostic in file_invalid:
                with self.subTest(geometry=diagnostic):
                    rivers = root / diagnostic
                    _write_rivers(
                        rivers,
                        hybas_l12=[30],
                        upland_skm=[30.25],
                        next_down=[0],
                        geometries=[geometry],
                    )
                    with self.assertRaises(build_adapter.AdapterError) as caught:
                        build_adapter.load_rivers(rivers)
                    self.assertIn(diagnostic, str(caught.exception))

        mixed = self._frame(
            ids=[30, 20],
            weights=[30.25, 20.5],
            geometries=[
                LineString([(-0.25, 0.20), (1.25, 0.80)]),
                Point(0.30, 0.30),
            ],
        )
        with self.assertRaisesRegex(build_adapter.AdapterError, "Point"):
            build_adapter._normalize_rivers_layer(mixed)

    def test_hybas_l12_parsing_is_strict_and_duplicates_are_allowed(self) -> None:
        cases = (
            (None, "non-numeric"),
            (0, "strictly positive"),
            (-1, "strictly positive"),
            (10.5, "non-integral"),
            (9223372036854775808, "int64 range"),
        )
        for value, diagnostic in cases:
            with self.subTest(value=value), self.assertRaises(
                build_adapter.AdapterError
            ) as caught:
                build_adapter._normalize_rivers_layer(self._frame(ids=[value]))
            self.assertIn("HYBAS_L12", str(caught.exception))
            self.assertIn(diagnostic, str(caught.exception))

        normalized = build_adapter._normalize_rivers_layer(
            self._frame(ids=[30.0, "20"], weights=[30.25, 20.5])
        )
        pd.testing.assert_series_equal(
            normalized["HYBAS_L12"].reset_index(drop=True),
            pd.Series([30, 20], dtype="int64", name="HYBAS_L12"),
        )
        duplicates = build_adapter._normalize_rivers_layer(
            self._frame(ids=[30, 30], weights=[30.25, 20.5])
        )
        self.assertEqual(duplicates["HYBAS_L12"].tolist(), [30, 30])

    def test_reach_and_downstream_ids_normalize_to_int64(self) -> None:
        normalized = build_adapter._normalize_rivers_layer(
            self._frame(
                reach_ids=["101", 102.0],
                ids=[30, 20],
                weights=[80.0, 20.0],
                next_down=["200", 0.0],
            )
        )

        pd.testing.assert_series_equal(
            normalized["HYRIV_ID"].reset_index(drop=True),
            pd.Series([101, 102], dtype="int64", name="HYRIV_ID"),
        )
        pd.testing.assert_series_equal(
            normalized["NEXT_DOWN"].reset_index(drop=True),
            pd.Series([200, 0], dtype="int64", name="NEXT_DOWN"),
        )

    def test_upland_skm_requires_non_negative_finite_float32(self) -> None:
        cases = (
            (None, "finite float32"),
            (float("nan"), "finite float32"),
            (float("inf"), "finite float32"),
            (float("-inf"), "finite float32"),
            (-0.25, "non-negative"),
            (3.5e38, "finite float32"),
        )
        for value, diagnostic in cases:
            with self.subTest(value=value), self.assertRaises(
                build_adapter.AdapterError
            ) as caught:
                build_adapter._normalize_rivers_layer(self._frame(weights=[value]))
            self.assertIn("UPLAND_SKM", str(caught.exception))
            self.assertIn(diagnostic, str(caught.exception))

        normalized = build_adapter._normalize_rivers_layer(
            self._frame(ids=[30, 20], weights=[0, 30.25])
        )
        self.assertEqual(normalized["UPLAND_SKM"].dtype, np.dtype("float32"))
        np.testing.assert_array_equal(
            normalized["UPLAND_SKM"].to_numpy(),
            np.asarray([0.0, 30.25], dtype="float32"),
        )

    def test_unresolved_ids_are_counted_once_without_spatial_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            unresolved = [
                LineString([(0.10, 0.10), (0.90, 0.90)]),
                LineString([(10.10, 0.10), (10.90, 0.90)]),
            ]
            _write_rivers(
                rivers_dir,
                hybas_l12=[30, 999, 998, 20],
                upland_skm=[30.25, 999.0, 998.0, 20.5],
                next_down=[0, 0, 0, 0],
                geometries=[
                    LineString([(-0.25, 0.20), (1.25, 0.80)]),
                    *unresolved,
                    LineString([(9.75, 0.15), (11.25, 0.85)]),
                ],
            )
            with self.assertLogs(build_adapter.__name__, level="INFO") as captured:
                self.assertEqual(
                    build_adapter.main(
                        _build_args(basins_dir, pour_points_dir, out_dir)
                        + ["--rivers", str(rivers_dir)]
                    ),
                    0,
                )
            expected = (
                "dropped 2 HydroRIVERS reaches with HYBAS_L12 absent "
                "from the unit set"
            )
            self.assertEqual(sum(expected in message for message in captured.output), 1)
            snap = gpd.read_parquet(out_dir / "aux" / "snap_stems.parquet")
            self.assertEqual(len(snap), 2)
            self.assertEqual(set(snap["unit_id"]), {30, 20})
            self.assertFalse(
                any(
                    geometry.equals(candidate)
                    for geometry in snap.geometry
                    for candidate in unresolved
                )
            )


class BuildSelectorTests(unittest.TestCase):
    """Exercise required build-region selection without changing extract."""

    def _parse_and_resolve(self, selector: list[str], root: Path) -> list[str]:
        args = build_adapter.build_arg_parser().parse_args(
            _selector_args(selector, root)
        )
        return build_adapter.resolve_build_regions(args)

    def test_single_region_resolves_without_changing_root_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(self._parse_and_resolve(["--region", "gr"], root), ["gr"])

    def test_strict_build_defaults_false_and_is_opt_in(self) -> None:
        parser = build_adapter.build_arg_parser()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            default_args = parser.parse_args(
                _selector_args(["--region", "gr"], root)
            )
            strict_args = parser.parse_args(
                _selector_args(["--region", "gr"], root) + ["--strict-build"]
            )

        self.assertIs(default_args.strict_build, False)
        self.assertIs(strict_args.strict_build, True)

    def test_regions_accept_commas_and_space_tokens_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(
                self._parse_and_resolve(["--regions", "gr,af"], root),
                ["gr", "af"],
            )
            self.assertEqual(
                self._parse_and_resolve(["--regions", "gr", "af"], root),
                ["gr", "af"],
            )

    def test_all_regions_selects_present_layers_in_standard_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            basins_dir.mkdir()
            for region in ("si", "af", "eu"):
                region_dir = basins_dir / f"hybas_{region}"
                region_dir.mkdir()
                (region_dir / f"hybas_{region}_lev12_v1c.shp").touch()
            self.assertEqual(
                self._parse_and_resolve(["--all-regions"], root),
                ["af", "eu", "si"],
            )

    def test_selector_is_required_and_mutually_exclusive(self) -> None:
        parser = build_adapter.build_arg_parser()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(SystemExit, "2"):
                parser.parse_args(_selector_args([], root))
            pairs = (
                ["--region", "gr", "--regions", "af"],
                ["--region", "gr", "--all-regions"],
                ["--regions", "gr", "--all-regions"],
            )
            for selectors in pairs:
                with self.subTest(selectors=selectors), self.assertRaisesRegex(SystemExit, "2"):
                    parser.parse_args(_selector_args(selectors, root))

    def test_repeated_and_empty_regions_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(build_adapter.AdapterError, "gr"):
                self._parse_and_resolve(["--regions", "gr,af,gr"], root)
            with self.assertRaisesRegex(build_adapter.AdapterError, "empty|region"):
                self._parse_and_resolve(["--regions", ", \t ,"], root)

    def test_all_regions_rejects_an_empty_basins_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "extract").mkdir()
            with self.assertRaisesRegex(build_adapter.AdapterError, "no regions found"):
                self._parse_and_resolve(["--all-regions"], root)

    def test_extract_retains_singular_required_region(self) -> None:
        parser = build_adapter.build_arg_parser()
        args = parser.parse_args(
            [
                "extract",
                "--region",
                "gr",
                "--basins",
                "basins",
                "--pour-points",
                "pour_points",
            ]
        )
        self.assertEqual(args.command, "extract")
        self.assertEqual(args.region, "gr")
        self.assertFalse(hasattr(args, "regions"))
        self.assertFalse(hasattr(args, "all_regions"))


class MergedBuildTests(unittest.TestCase):
    """Exercise deterministic normalization and graph construction across regions."""

    def _write_two_regions(self, root: Path) -> tuple[Path, Path]:
        basins_dir = root / "extract"
        pour_points_dir = basins_dir / "pour"
        basins_dir.mkdir()
        _write_layer(
            basins_dir,
            region="gr",
            ids=[101, 103],
            next_down=[202, 0],
            geometries=[box(20, 0, 21, 1), box(0, 0, 1, 1)],
        )
        _write_layer(
            basins_dir,
            region="af",
            ids=[202, 204],
            geometries=[box(10, 0, 11, 1), box(30, 0, 31, 1)],
        )
        _write_pour_points(
            pour_points_dir,
            ids=[101, 103, 202, 204],
            points=[
                Point(20.5, 0.5), Point(0.5, 0.5),
                Point(10.5, 0.5), Point(30.5, 0.5),
            ],
        )
        return basins_dir, pour_points_dir

    def test_two_regions_merge_sort_graph_and_manifest_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir, pour_points_dir = self._write_two_regions(root)
            output_orders: list[list[int]] = []
            for name in ("out-first", "out-second"):
                out_dir = root / name
                return_code = build_adapter.main(
                    _selector_args(["--regions", "gr,af"], root)[:-1]
                    + [str(out_dir)]
                )
                self.assertEqual(return_code, 0)
                catchments = gpd.read_parquet(out_dir / "catchments.parquet")
                graph = pq.read_table(out_dir / "graph.parquet")
                output_orders.append(catchments["id"].tolist())

                self.assertEqual(len(catchments), 4)
                self.assertEqual(set(catchments["id"]), {101, 103, 202, 204})
                distances = catchments.geometry.centroid.hilbert_distance(
                    total_bounds=catchments.total_bounds
                )
                self.assertTrue((distances.diff().dropna() >= 0).all())
                tied = pd.DataFrame(
                    {"distance": distances, "id": catchments["id"]}
                )
                for _, group in tied.groupby("distance", sort=False):
                    self.assertEqual(group["id"].tolist(), sorted(group["id"]))
                self.assertNotEqual(catchments["id"].tolist(), [103, 101, 202, 204])
                self.assertEqual(graph.column("id").to_pylist(), catchments["id"].tolist())
                adjacency = {
                    row["id"]: row["upstream_ids"] for row in graph.to_pylist()
                }
                self.assertIn(101, adjacency[202])

                with (out_dir / "manifest.json").open(encoding="utf-8") as stream:
                    manifest = json.load(stream)
                self.assertEqual(manifest["region"], "gr,af")
                self.assertEqual(manifest["unit_count"], 4)
                self.assertEqual(
                    manifest["bbox"],
                    [
                        float(np.float32(v))
                        for v in catchments.geometry.total_bounds
                    ],
                )
                schema = pq.ParquetFile(out_dir / "catchments.parquet").schema_arrow
                geo = json.loads(schema.metadata[b"geo"])
                self.assertEqual(geo["version"], "1.1.0")
                self.assertIn("covering", geo["columns"]["geometry"])

            self.assertEqual(output_orders[0], output_orders[1])

    def test_duplicate_ids_across_regions_are_rejected_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = basins_dir / "pour"
            basins_dir.mkdir()
            for region, offset in (("gr", 0), ("af", 10)):
                _write_layer(
                    basins_dir,
                    region=region,
                    ids=[22, 11],
                    geometries=[box(offset, 0, offset + 1, 1), box(offset + 2, 0, offset + 3, 1)],
                )
            _write_pour_points(
                pour_points_dir,
                ids=[22, 11],
                points=[Point(0.5, 0.5), Point(2.5, 0.5)],
            )
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(_selector_args(["--regions", "gr,af"], root))
            message = str(caught.exception)
            self.assertIn("duplicate HYBAS_ID values", message)
            self.assertIn("[11, 22]", message)
            self.assertFalse((root / "out").exists())

    def test_all_regions_builds_every_present_standard_region_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = basins_dir / "pour"
            basins_dir.mkdir()
            fixtures = (("si", 303, 30), ("af", 101, 0), ("eu", 202, 15))
            for region, identifier, x in fixtures:
                _write_layer(
                    basins_dir,
                    region=region,
                    ids=[identifier],
                    geometries=[box(x, 0, x + 1, 1)],
                )
            _write_pour_points(
                pour_points_dir,
                ids=[identifier for _, identifier, _ in fixtures],
                points=[Point(x + 0.5, 0.5) for _, _, x in fixtures],
            )
            self.assertEqual(
                build_adapter.main(_selector_args(["--all-regions"], root)), 0
            )
            catchments = gpd.read_parquet(root / "out" / "catchments.parquet")
            with (root / "out" / "manifest.json").open(encoding="utf-8") as stream:
                manifest = json.load(stream)
            self.assertEqual(set(catchments["id"]), {101, 202, 303})
            self.assertEqual(manifest["region"], "af,eu,si")
            self.assertEqual(manifest["unit_count"], 3)


class AntimeridianGuardTests(unittest.TestCase):
    """Exercise antimeridian detection through complete synthetic builds."""

    @staticmethod
    def _adapter_warnings(captured: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
        return [
            warning
            for warning in captured
            if "antimeridian-wrap candidates" in str(warning.message)
        ]

    @staticmethod
    def _assert_artifacts(test: unittest.TestCase, out_dir: Path) -> None:
        for filename in ("catchments.parquet", "graph.parquet", "manifest.json"):
            test.assertTrue((out_dir / filename).is_file())

    @staticmethod
    def _assert_no_artifacts(test: unittest.TestCase, out_dir: Path) -> None:
        for filename in ("catchments.parquet", "graph.parquet", "manifest.json"):
            test.assertFalse((out_dir / filename).exists())

    def _write_wrapping_region(self, root: Path) -> tuple[Path, Path]:
        basins_dir = root / "extract"
        pour_points_dir = basins_dir / "pour"
        basins_dir.mkdir()
        _write_layer(
            basins_dir,
            ids=[901],
            next_down=[0],
            endo=[0],
            geometries=[box(-179.0, 0.0, 179.0, 1.0)],
        )
        _write_pour_points(
            pour_points_dir,
            ids=[901],
            points=[Point(0.0, 0.5)],
        )
        return basins_dir, pour_points_dir

    def _write_two_regions_with_wrap(self, root: Path) -> None:
        basins_dir = root / "extract"
        pour_points_dir = basins_dir / "pour"
        basins_dir.mkdir()
        _write_layer(
            basins_dir,
            region="gr",
            ids=[101, 103],
            next_down=[902, 0],
            geometries=[box(20, 0, 21, 1), box(0, 0, 1, 1)],
        )
        _write_layer(
            basins_dir,
            region="af",
            ids=[902, 204],
            next_down=[0, 0],
            endo=[0, 0],
            geometries=[box(-179.0, 0.0, 179.0, 1.0), box(30, 0, 31, 1)],
        )
        _write_pour_points(
            pour_points_dir,
            ids=[101, 103, 902, 204],
            points=[
                Point(20.5, 0.5), Point(0.5, 0.5),
                Point(0.0, 0.5), Point(30.5, 0.5),
            ],
        )

    def test_ordinary_geometry_is_silent_in_both_modes(self) -> None:
        for strict_build in (False, True):
            with self.subTest(strict_build=strict_build), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                basins_dir = root / "extract"
                pour_points_dir = root / "extract" / "pour"
                out_dir = root / "out"
                basins_dir.mkdir()
                _write_layer(basins_dir)
                ids, points = _ordinary_points()
                _write_pour_points(pour_points_dir, ids=ids, points=points)
                args = _build_args(basins_dir, pour_points_dir, out_dir)
                if strict_build:
                    args.append("--strict-build")

                with warnings.catch_warnings(record=True) as captured:
                    warnings.simplefilter("always")
                    return_code = build_adapter.main(args)

                self.assertEqual(return_code, 0)
                self._assert_artifacts(self, out_dir)
                self.assertEqual(self._adapter_warnings(captured), [])

    def test_default_single_region_warns_and_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir, pour_points_dir = self._write_wrapping_region(root)
            out_dir = root / "out"

            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always")
                return_code = build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                )

            adapter_warnings = self._adapter_warnings(captured)
            self.assertEqual(return_code, 0)
            self.assertEqual(len(adapter_warnings), 1)
            self.assertIs(adapter_warnings[0].category, UserWarning)
            self.assertIn("count=1", str(adapter_warnings[0].message))
            self.assertIn("901", str(adapter_warnings[0].message))
            self._assert_artifacts(self, out_dir)

    def test_strict_single_region_rejects_before_serialization(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir, pour_points_dir = self._write_wrapping_region(root)
            out_dir = root / "out"

            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(
                    _build_args(basins_dir, pour_points_dir, out_dir)
                    + ["--strict-build"]
                )

            message = str(caught.exception)
            self.assertIn("antimeridian-wrap candidates", message)
            self.assertIn("count=1", message)
            self.assertIn("901", message)
            self._assert_no_artifacts(self, out_dir)

    def test_combined_regions_warn_or_reject_before_serialization(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_two_regions_with_wrap(root)
            default_out = root / "out-default"
            strict_out = root / "out-strict"

            default_args = _selector_args(["--regions", "gr,af"], root)
            default_args[-1] = str(default_out)
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter("always")
                return_code = build_adapter.main(default_args)

            adapter_warnings = self._adapter_warnings(captured)
            self.assertEqual(return_code, 0)
            self.assertEqual(len(adapter_warnings), 1)
            self.assertIs(adapter_warnings[0].category, UserWarning)
            self.assertIn("count=1", str(adapter_warnings[0].message))
            self.assertIn("HYBAS_IDs=[902]", str(adapter_warnings[0].message))
            self._assert_artifacts(self, default_out)

            strict_args = _selector_args(["--regions", "gr,af"], root)
            strict_args[-1] = str(strict_out)
            strict_args.append("--strict-build")
            with self.assertRaises(build_adapter.AdapterError) as caught:
                build_adapter.main(strict_args)

            message = str(caught.exception)
            self.assertIn("antimeridian-wrap candidates", message)
            self.assertIn("count=1", message)
            self.assertIn("HYBAS_IDs=[902]", message)
            self._assert_no_artifacts(self, strict_out)


class LargeRowGroupTests(unittest.TestCase):
    """Prove both writers balance a roughly 12,000-unit real build."""

    def test_large_build_has_balanced_row_groups_and_bbox_statistics(self) -> None:
        count = 12_000
        columns = 120
        width = 0.005
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            basins_dir.mkdir()
            ids = list(range(1, count + 1))
            geometries = [
                box(
                    -10 + (index % columns) * 0.01,
                    -10 + (index // columns) * 0.01,
                    -10 + (index % columns) * 0.01 + width,
                    -10 + (index // columns) * 0.01 + width,
                )
                for index in range(count)
            ]
            points = [geometry.centroid for geometry in geometries]
            _write_layer(basins_dir, ids=ids, geometries=geometries)
            _write_pour_points(pour_points_dir, ids=ids, points=points)

            self.assertEqual(
                build_adapter.main(_build_args(basins_dir, pour_points_dir, root / "out")),
                0,
            )

            for filename, bbox_names in (
                (
                    "catchments.parquet",
                    tuple(f"bbox.{name}" for name in build_adapter.BBOX_LEAF_NAMES),
                ),
                (
                    "graph.parquet",
                    ("bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"),
                ),
            ):
                metadata = pq.ParquetFile(root / "out" / filename).metadata
                self.assertGreater(metadata.num_row_groups, 1)
                self.assertEqual(
                    sum(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)),
                    count,
                )
                for group_index in range(metadata.num_row_groups):
                    row_group = metadata.row_group(group_index)
                    self.assertGreaterEqual(row_group.num_rows, 4096)
                    self.assertLessEqual(row_group.num_rows, 8192)
                    columns_by_name = {
                        row_group.column(index).path_in_schema: row_group.column(index)
                        for index in range(row_group.num_columns)
                    }
                    for name in bbox_names:
                        statistics = columns_by_name[name].statistics
                        self.assertIsNotNone(statistics)
                        self.assertTrue(statistics.has_min_max)

    def test_large_snap_has_balanced_row_groups_and_bbox_statistics(self) -> None:
        count = 12_000
        columns = 120
        width = 0.005
        ids = list(range(1, count + 1))
        geometries = [
            LineString(
                [
                    (
                        -10 + (index % columns) * 0.01,
                        -10 + (index // columns) * 0.01,
                    ),
                    (
                        -10 + (index % columns) * 0.01 + width,
                        -10 + (index // columns) * 0.01 + width,
                    ),
                ]
            )
            for index in range(count)
        ]
        rivers = gpd.GeoDataFrame(
            {
                "HYBAS_L12": pd.Series(ids, dtype="int64"),
                "UPLAND_SKM": pd.Series(
                    [float(identifier) for identifier in ids], dtype="float32"
                ),
                "NEXT_DOWN": pd.Series([0] * count, dtype="int64"),
                "stem_role": pd.Series(["mainstem"] * count, dtype="string"),
                "_source_order": pd.Series(range(count), dtype="int64"),
            },
            geometry=geometries,
            crs="EPSG:4326",
        )
        units = gpd.GeoDataFrame(
            {"id": [1]},
            geometry=[box(-10.0, -10.0, -8.8, -9.0)],
            crs="EPSG:4326",
        )

        with tempfile.TemporaryDirectory() as temporary:
            snap_path = build_adapter.write_snap_stems(
                rivers, units, Path(temporary)
            )
            schema = pq.read_schema(snap_path)
            parquet_file = pq.ParquetFile(snap_path)
            metadata = parquet_file.metadata

            expected_schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("unit_id", pa.int64(), nullable=False),
                    pa.field("weight", pa.float32(), nullable=False),
                    pa.field("stem_role", pa.string(), nullable=True),
                    pa.field(
                        "bbox",
                        pa.struct(
                            [
                                pa.field("xmin", pa.float32(), nullable=False),
                                pa.field("ymin", pa.float32(), nullable=False),
                                pa.field("xmax", pa.float32(), nullable=False),
                                pa.field("ymax", pa.float32(), nullable=False),
                            ]
                        ),
                        nullable=True,
                    ),
                    pa.field("geometry", pa.binary(), nullable=False),
                ]
            ).with_metadata(
                {
                    b"geo": json.dumps(
                        {
                            "version": "1.1.0",
                            "primary_column": "geometry",
                            "columns": {
                                "geometry": {
                                    "encoding": "WKB",
                                    "geometry_types": ["LineString"],
                                    "covering": {
                                        "bbox": {
                                            "xmin": ["bbox", "xmin"],
                                            "ymin": ["bbox", "ymin"],
                                            "xmax": ["bbox", "xmax"],
                                            "ymax": ["bbox", "ymax"],
                                        }
                                    },
                                }
                            },
                        }
                    ).encode("utf-8")
                }
            )
            self.assertEqual(schema, expected_schema)
            self.assertGreaterEqual(metadata.num_row_groups, 1)
            self.assertEqual(
                sum(
                    metadata.row_group(i).num_rows
                    for i in range(metadata.num_row_groups)
                ),
                count,
            )
            group_sizes = [
                metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)
            ]
            self.assertEqual(group_sizes, [6000, 6000])
            for group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(group_index)
                self.assertGreaterEqual(row_group.num_rows, build_adapter.ROW_GROUP_MIN)
                self.assertLessEqual(row_group.num_rows, build_adapter.ROW_GROUP_MAX)
                columns_by_name = {
                    row_group.column(index).path_in_schema: row_group.column(index)
                    for index in range(row_group.num_columns)
                }
                for name in ("bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax"):
                    statistics = columns_by_name[name].statistics
                    self.assertIsNotNone(statistics)
                    self.assertTrue(statistics.has_min_max)


class LoadRegionalLayerTests(unittest.TestCase):
    """Exercise normalization through real ESRI Shapefile I/O."""

    def test_missing_required_column_names_field(self) -> None:
        for field in ("UP_AREA", "PFAF_ID"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                basins_dir = Path(temporary) / "extract"
                _write_layer(basins_dir, omit=field)

                with self.assertRaisesRegex(build_adapter.AdapterError, field):
                    build_adapter.load_regional_layer(
                        "gr", basins_dir, source_level=12,
                        levels=build_adapter.LevelRange(12, 12),
                    )

    def test_missing_next_down_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir, omit="NEXT_DOWN")

            with self.assertRaisesRegex(build_adapter.AdapterError, "NEXT_DOWN"):
                build_adapter.load_regional_layer(
                    "gr", basins_dir, source_level=12,
                    levels=build_adapter.LevelRange(12, 12),
                )

    def test_missing_endo_names_field(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir, omit="ENDO")

            with self.assertRaisesRegex(build_adapter.AdapterError, "ENDO"):
                build_adapter.load_regional_layer(
                    "gr", basins_dir, source_level=12,
                    levels=build_adapter.LevelRange(12, 12),
                )

    def test_non_positive_and_non_integral_ids_are_rejected(self) -> None:
        cases = ([0, 10, 20, 40], [-1, 10, 20, 40], [1.5, 10, 20, 40])
        for ids in cases:
            with self.subTest(ids=ids), tempfile.TemporaryDirectory() as temporary:
                basins_dir = Path(temporary) / "extract"
                _write_layer(basins_dir, ids=ids)

                with self.assertRaises(build_adapter.AdapterError):
                    build_adapter.load_regional_layer(
                        "gr", basins_dir, source_level=12,
                        levels=build_adapter.LevelRange(12, 12),
                    )

    def test_duplicate_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir, ids=[30, 10, 20, 10])

            with self.assertRaisesRegex(build_adapter.AdapterError, "duplicate"):
                build_adapter.load_regional_layer(
                    "gr", basins_dir, source_level=12,
                    levels=build_adapter.LevelRange(12, 12),
                )

    def test_attributes_are_mapped_with_contract_dtypes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir)

            units = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )

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
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir)

            units = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )

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
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir, crs="EPSG:3857", geometries=geometries)

            units = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )

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
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir, geometries=geometries)

            units = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )

        self.assertTrue(units.geometry.is_valid.all())
        self.assertTrue((~units.geometry.is_empty).all())
        self.assertTrue(units.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all())

    def test_hilbert_order_is_deterministic_with_id_tie_break(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir)

            first = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )
            second = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )

        self.assertEqual(first["id"].tolist(), second["id"].tolist())
        distances = first.geometry.centroid.hilbert_distance(
            total_bounds=first.total_bounds
        )
        self.assertTrue((distances.diff().dropna() >= 0).all())
        tied_ids = first.loc[first["id"].isin([10, 30]), "id"].tolist()
        self.assertEqual(tied_ids, [10, 30])
        self.assertEqual(first.index.tolist(), list(range(len(first))))

    def test_requested_level_sets_hfx_level_and_normalizes_one_to_twelve_digit_pfaf_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            extract_dir = Path(temporary) / "extract"
            pfaf_ids = [1, 12, 123456, 123456789012]
            _write_layer(extract_dir, level=6, pfaf_ids=pfaf_ids)
            levels = build_adapter.LevelRange(1, 12)
            units = build_adapter.load_regional_layer(
                "gr", extract_dir, source_level=6, levels=levels
            )
            self.assertIn("lev06_v1c", str(build_adapter._source_path("gr", extract_dir, 6)))
            self.assertTrue((units["level"] == 5).all())
            self.assertEqual(sorted(units["PFAF_ID"].tolist()), sorted(pfaf_ids))
            self.assertEqual(units["PFAF_ID"].dtype, "int64")

            _write_layer(extract_dir, level=1)
            level_one = build_adapter.load_regional_layer(
                "gr", extract_dir, source_level=1, levels=levels
            )
            _write_layer(extract_dir, level=12)
            level_twelve = build_adapter.load_regional_layer(
                "gr", extract_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )
            self.assertTrue((level_one["level"] == 0).all())
            self.assertTrue((level_twelve["level"] == 0).all())

    def test_requested_coarse_level_preserves_crs_repair_and_coordinate_clamp(self) -> None:
        tolerance = build_adapter.COORDINATE_DOMAIN_TOLERANCE_DEGREES
        geometries = [
            Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)]),
            box(3, 0, 4, 1),
            box(5, 0, 6, 1),
            box(7, 0, 8, 1),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            extract_dir = Path(temporary) / "extract"
            _write_layer(
                extract_dir, level=6, crs="EPSG:3857", geometries=geometries
            )
            repaired = build_adapter.load_regional_layer(
                "gr", extract_dir, source_level=6,
                levels=build_adapter.LevelRange(1, 12),
            )
            self.assertEqual(repaired.crs.to_epsg(), 4326)
            self.assertTrue(repaired.geometry.is_valid.all())
            self.assertTrue(repaired.geometry.geom_type.isin(["Polygon", "MultiPolygon"]).all())

            marginal = [
                box(179.0, 0.0, 180.0 + tolerance / 2, 1.0),
                box(3, 0, 4, 1), box(5, 0, 6, 1), box(7, 0, 8, 1),
            ]
            _write_layer(extract_dir, level=6, geometries=marginal)
            clamped = build_adapter.load_regional_layer(
                "gr", extract_dir, source_level=6,
                levels=build_adapter.LevelRange(1, 12),
            )
            self.assertLessEqual(clamped.total_bounds[2], 180.0)

            excessive = [
                box(179.0, 0.0, 180.0 + tolerance * 2, 1.0),
                box(3, 0, 4, 1), box(5, 0, 6, 1), box(7, 0, 8, 1),
            ]
            _write_layer(extract_dir, level=6, geometries=excessive)
            with self.assertRaises(build_adapter.AdapterError):
                build_adapter.load_regional_layer(
                    "gr", extract_dir, source_level=6,
                    levels=build_adapter.LevelRange(1, 12),
                )


class CoordinateDomainClampTests(unittest.TestCase):
    """Exercise coordinate-domain normalization through real source layers."""

    def test_au_fiji_polygon_vertices_are_clamped(self) -> None:
        fiji_ids = [5120082160, 5120082230]
        fiji_polygons = [
            Polygon([
                (179.8, -17.9),
                (179.9, -18.0),
                (180.0006, -17.9),
                (179.9, -17.8),
                (179.8, -17.9),
            ]),
            Polygon([
                (179.7, -16.9),
                (179.8, -17.0),
                (180.0006, -16.9),
                (179.8, -16.8),
                (179.7, -16.9),
            ]),
        ]
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(
                basins_dir,
                region="au",
                ids=fiji_ids,
                geometries=fiji_polygons,
            )

            with self.assertLogs(build_adapter.__name__, level="WARNING") as captured:
                units = build_adapter.load_regional_layer(
                    "au", basins_dir, source_level=12,
                    levels=build_adapter.LevelRange(12, 12),
                )

        self.assertLessEqual(
            units.total_bounds[2],
            180.0,
            "180.0006 remained above 180.0 after source normalization",
        )
        self.assertEqual(set(units["id"]), set(fiji_ids))
        self.assertTrue(units.geometry.is_valid.all())
        self.assertTrue(units.geometry.geom_type.eq("Polygon").all())
        self.assertTrue(
            all(
                -180.0 <= x <= 180.0 and -90.0 <= y <= 90.0
                for polygon in units.geometry
                for x, y in polygon.exterior.coords
            )
        )
        self.assertEqual(
            sum(
                x == 180.0
                for polygon in units.geometry
                for x, _ in polygon.exterior.coords
            ),
            2,
        )
        warning = "\n".join(captured.output)
        self.assertIn("HydroBASINS units", warning)
        self.assertIn("altered_vertices=2", warning)
        self.assertIn("HYBAS_IDs", warning)
        for identifier in fiji_ids:
            self.assertIn(str(identifier), warning)

    def test_marginal_overshoot_is_clamped_at_point_and_river_boundaries(
        self,
    ) -> None:
        fiji_ids = [5120082160, 5120082230]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pour_points_dir = root / "extract" / "pour"
            rivers_dir = root / "rivers"
            _write_pour_points(
                pour_points_dir,
                ids=fiji_ids,
                points=[Point(180.0006, -17.9), Point(180.0006, -16.9)],
            )
            _write_rivers(
                rivers_dir,
                hyriv_id=[9001, 9002],
                hybas_l12=fiji_ids,
                upland_skm=[1.0, 2.0],
                next_down=[0, 0],
                geometries=[
                    LineString([(179.9, -17.9), (180.0006, -17.8)]),
                    LineString([(179.8, -16.9), (180.0006, -16.8)]),
                ],
            )

            pour_points = build_adapter.load_pour_points(
                pour_points_dir, source_level=12
            )
            rivers = build_adapter.load_rivers(rivers_dir)

        self.assertEqual(pour_points.geometry.x.tolist(), [180.0, 180.0])
        self.assertEqual(pour_points["outlet_lon"].tolist(), [180.0, 180.0])
        self.assertEqual(pour_points["outlet_lat"].tolist(), [-17.9, -16.9])
        self.assertTrue(
            all(
                -180.0 <= point.x <= 180.0 and -90.0 <= point.y <= 90.0
                for point in pour_points.geometry
            )
        )
        self.assertTrue(rivers.geometry.geom_type.eq("LineString").all())
        self.assertTrue(
            all(
                -180.0 <= x <= 180.0 and -90.0 <= y <= 90.0
                for line in rivers.geometry
                for x, y in line.coords
            )
        )
        self.assertEqual([line.coords[-1][0] for line in rivers.geometry], [180.0, 180.0])

    def test_over_tolerance_coordinates_are_rejected_at_the_source_boundary(
        self,
    ) -> None:
        cases = (
            (Point(180.005, 0.0), "180.005"),
            (Point(-180.005, 0.0), "-180.005"),
            (Point(0.0, 90.005), "90.005"),
            (Point(0.0, -90.005), "-90.005"),
        )
        for point, coordinate_text in cases:
            with self.subTest(point=point), tempfile.TemporaryDirectory() as temporary:
                pour_points_dir = Path(temporary) / "extract" / "pour"
                _write_pour_points(
                    pour_points_dir,
                    ids=[5120082160],
                    points=[point],
                )

                with self.assertRaises(build_adapter.AdapterError) as caught:
                    build_adapter.load_pour_points(
                        pour_points_dir, source_level=12
                    )

                message = str(caught.exception)
                self.assertIn("HydroBASINS pour points", message)
                self.assertIn("HYBAS_ID=5120082160", message)
                self.assertIn(coordinate_text, message)
                self.assertIn("excess", message)
                self.assertIn("tolerance", message)

    def test_in_domain_geometry_does_not_emit_a_clamp_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            basins_dir = Path(temporary) / "extract"
            _write_layer(basins_dir)

            with patch.object(build_adapter.LOGGER, "warning") as warning:
                build_adapter.load_regional_layer(
                    "gr", basins_dir, source_level=12,
                    levels=build_adapter.LevelRange(12, 12),
                )

        warning.assert_not_called()


class BuildCatchmentsTests(unittest.TestCase):
    """Prove the command path writes the required GeoParquet slice."""

    def test_build_writes_conformant_catchments_slice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)

            expected = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )
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
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
            out_dir = root / "out"
            basins_dir.mkdir()
            _write_layer(basins_dir)
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            units = build_adapter.load_regional_layer(
                "gr", basins_dir, source_level=12,
                levels=build_adapter.LevelRange(12, 12),
            )

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
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
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
        basins_dir = root / "extract"
        pour_points_dir = root / "extract" / "pour"
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
        basins_dir = root / "extract"
        pour_points_dir = root / "extract" / "pour"
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
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
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
            basins_dir = root / "extract"
            pour_points_dir = root / "extract" / "pour"
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
                with self.assertRaises(build_adapter.AdapterError) as caught:
                    self._build_fixture(Path(temporary), ids=ids, points=points)
                message = str(caught.exception)
                self.assertIn("HydroBASINS pour points", message)
                self.assertIn("HYBAS_ID=30", message)
                self.assertIn("excess", message)
                self.assertIn("tolerance", message)

        with tempfile.TemporaryDirectory() as temporary:
            ids, points = _ordinary_points()
            path = self._build_fixture(
                Path(temporary),
                ids=ids + [999],
                points=points + [Point(170.0, 80.0)],
            )
            self.assertEqual(set(gpd.read_parquet(path)["id"]), set(ids))

    def test_point_layer_uses_exact_direct_level_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary) / "extract" / "pour"
            with self.assertRaisesRegex(build_adapter.AdapterError, "found 0"):
                build_adapter.load_pour_points(
                    pour_points_dir, source_level=12
                )

        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary) / "extract" / "pour"
            nested = pour_points_dir / "nested"
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points)
            _write_pour_points(nested, ids=ids, points=points)
            points_frame = build_adapter.load_pour_points(
                pour_points_dir, source_level=12
            )
            self.assertEqual(len(points_frame), 4)

    def test_point_layer_requires_declared_crs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary) / "extract" / "pour"
            ids, points = _ordinary_points()
            _write_pour_points(pour_points_dir, ids=ids, points=points, crs=None)
            with self.assertRaisesRegex(build_adapter.AdapterError, "no declared CRS"):
                build_adapter.load_pour_points(
                    pour_points_dir, source_level=12
                )

    def test_point_layer_crs_is_transformed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            pour_points_dir = Path(temporary) / "extract" / "pour"
            _write_pour_points(
                pour_points_dir,
                ids=[30],
                points=[Point(111319.490793, 111325.142866)],
                crs="EPSG:3857",
            )
            points = build_adapter.load_pour_points(
                pour_points_dir, source_level=12
            )

        self.assertEqual(points.crs.to_epsg(), 4326)
        self.assertAlmostEqual(points.loc[0, "outlet_lon"], 1.0, places=6)
        self.assertAlmostEqual(points.loc[0, "outlet_lat"], 1.0, places=6)
        self.assertEqual(points.loc[0, "outlet_lon"], points.geometry.iloc[0].x)
        self.assertEqual(points.loc[0, "outlet_lat"], points.geometry.iloc[0].y)

    def test_invalid_point_geometries_are_rejected(self) -> None:
        cases = (None, Point(), LineString([(0, 0), (1, 1)]))
        for geometry in cases:
            with self.subTest(geometry=geometry), tempfile.TemporaryDirectory() as temporary:
                pour_points_dir = Path(temporary) / "extract" / "pour"
                _write_pour_points(pour_points_dir, ids=[30], points=[geometry])
                with self.assertRaisesRegex(build_adapter.AdapterError, "geometry"):
                    build_adapter.load_pour_points(
                        pour_points_dir, source_level=12
                    )


if __name__ == "__main__":
    unittest.main()
