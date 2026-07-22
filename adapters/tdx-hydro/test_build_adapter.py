import json
import math
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from geoparquet_io.core.validate import validate_geoparquet
from jsonschema import Draft202012Validator, FormatChecker
from pyproj import Geod
from shapely import from_wkb, get_coordinates
from shapely.geometry import LineString, MultiLineString, Point, Polygon

import build_adapter
from build_adapter import (
    ADAPTER_VERSION,
    BBOX_LEAF_NAMES,
    COORDINATE_DOMAIN_TOLERANCE_DEGREES,
    CRS,
    FABRIC_NAME,
    FORMAT_VERSION,
    HAS_UP_AREA,
    LayerClampDiagnostics,
    TOPOLOGY,
    CoreBuildResult,
    StreamnetDiagnostics,
    StreamnetUnit,
    build_dataset,
    build_diagnostics_report,
    build_streamnet_model,
    compile_core_hfx,
    global_linkno,
    load_header_crosswalk,
    load_tdx_geopackages,
    main,
)


def canonical_frames() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, float, float]:
    polygon_100 = Polygon([
        (0.00, 0.00), (0.01, 0.00), (0.01, 0.01),
        (0.00, 0.01), (0.00, 0.00),
    ])
    polygon_200 = Polygon([
        (0.01, 0.00), (0.02, 0.00), (0.02, 0.01),
        (0.01, 0.01), (0.01, 0.00),
    ])
    reach_100 = LineString([(0.00, 0.00), (0.01, 0.00)])
    reach_200 = LineString([(0.01, 0.00), (0.02, 0.00)])
    geod = Geod(ellps="WGS84")
    area_100_m2 = abs(geod.geometry_area_perimeter(polygon_100)[0])
    area_200_m2 = abs(geod.geometry_area_perimeter(polygon_200)[0])
    basins = gpd.GeoDataFrame(
        {"streamID": [200, 100], "label": ["downstream", "upstream"]},
        geometry=[polygon_200, polygon_100],
        crs="EPSG:4326",
    )
    streamnet = gpd.GeoDataFrame(
        {
            "LINKNO": [200, 100],
            "DSLINKNO": [-1, 200],
            "DSContArea": [
                (area_100_m2 + area_200_m2) / 1_000_000,
                area_100_m2 / 1_000_000,
            ],
            "label": ["downstream", "upstream"],
        },
        geometry=[reach_200, reach_100],
        crs="EPSG:4326",
    )
    return basins, streamnet, area_100_m2, area_200_m2


def write_pair(
    directory: Path,
    basins: gpd.GeoDataFrame,
    streamnet: gpd.GeoDataFrame,
) -> tuple[Path, Path]:
    basins_path = directory / "basins.gpkg"
    streamnet_path = directory / "streamnet.gpkg"
    basins.to_file(basins_path, layer="basins", driver="GPKG", engine="pyogrio")
    streamnet.to_file(
        streamnet_path, layer="streamnet", driver="GPKG", engine="pyogrio"
    )
    return basins_path, streamnet_path


def build_cli_frames() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
    streamnet = gpd.GeoDataFrame(
        {
            "LINKNO": [200, 100, 150],
            "DSLINKNO": [-1, 150, 200],
            "DSContArea": [
                (area_100_m2 + area_200_m2) / 1_000_000,
                area_100_m2 / 1_000_000,
                area_100_m2 / 1_000_000,
            ],
            "label": ["downstream", "upstream", "polygon-less"],
        },
        geometry=[
            LineString([(0.01, 0.00), (0.02, 0.00)]),
            LineString([(0.00, 0.00), (0.01, 0.00)]),
            LineString([(0.01, 0.00), (0.01, 0.002)]),
        ],
        crs="EPSG:4326",
    )
    return basins, streamnet


class GeoPackageIngestionTests(unittest.TestCase):
    def test_accepts_exact_two_identical_vertex_streamnet_linestring(self) -> None:
        basins, streamnet, _, _ = canonical_frames()
        streamnet.loc[streamnet["LINKNO"] == 200, "geometry"] = LineString(
            [(0.01, 0.00), (0.01, 0.00)]
        )
        with TemporaryDirectory() as temp_dir:
            paths = write_pair(Path(temp_dir), basins, streamnet)
            source = load_tdx_geopackages(*paths)

        self.assertEqual(source.streamnet["LINKNO"].tolist(), [200, 100])
        self.assertEqual(
            list(
                source.streamnet.loc[
                    source.streamnet["LINKNO"] == 200, "geometry"
                ].iloc[0].coords
            ),
            [(0.01, 0.0), (0.01, 0.0)],
        )
        self.assertFalse(
            source.streamnet.loc[
                source.streamnet["LINKNO"] == 200, "geometry"
            ].iloc[0].is_valid
        )

    def test_loads_and_normalizes_real_geopackages(self) -> None:
        basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
        with TemporaryDirectory() as temp_dir:
            paths = write_pair(Path(temp_dir), basins, streamnet)
            with self.assertNoLogs("tdx-hydro", level="WARNING"):
                source = load_tdx_geopackages(*paths)

        self.assertIsInstance(source.basins, gpd.GeoDataFrame)
        self.assertIsInstance(source.streamnet, gpd.GeoDataFrame)
        self.assertEqual(source.basins.crs.to_epsg(), 4326)
        self.assertEqual(source.streamnet.crs.to_epsg(), 4326)
        self.assertEqual(source.basins["streamID"].tolist(), [200, 100])
        self.assertEqual(source.streamnet["LINKNO"].tolist(), [200, 100])
        self.assertEqual(source.basins["label"].tolist(), ["downstream", "upstream"])
        self.assertEqual(source.streamnet["label"].tolist(), ["downstream", "upstream"])
        self.assertEqual(str(source.basins["streamID"].dtype), "int64")
        self.assertEqual(str(source.streamnet["LINKNO"].dtype), "int64")
        self.assertEqual(str(source.streamnet["DSLINKNO"].dtype), "int64")
        self.assertEqual(str(source.streamnet["DSContArea"].dtype), "float64")
        self.assertEqual(str(source.streamnet["DSContArea_km2"].dtype), "float64")
        self.assertEqual(
            source.streamnet["DSContArea_km2"].tolist(),
            streamnet["DSContArea"].tolist(),
        )
        self.assertEqual(source.diagnostics.basins_clamp, LayerClampDiagnostics(0, ()))
        self.assertEqual(source.diagnostics.streamnet_clamp, LayerClampDiagnostics(0, ()))
        expected_200_m2 = math.fsum([area_100_m2, area_200_m2])
        expected_100_m2 = area_100_m2
        expected_sum = math.fsum([expected_200_m2, expected_100_m2])
        raw_sum = math.fsum([
            (area_100_m2 + area_200_m2) / 1_000_000,
            area_100_m2 / 1_000_000,
        ])
        diagnostics = source.diagnostics.dscontarea
        self.assertEqual(diagnostics.source_unit, "km2")
        self.assertEqual(diagnostics.checked_polygon_bearing_link_count, 2)
        self.assertEqual(diagnostics.geodesic_upstream_area_sum_m2, expected_sum)
        self.assertEqual(diagnostics.dscontarea_sum_raw, raw_sum)
        expected_m2_error = math.fsum([
            abs((area_100_m2 + area_200_m2) / 1_000_000 - expected_200_m2),
            abs(area_100_m2 / 1_000_000 - expected_100_m2),
        ]) / expected_sum
        self.assertEqual(diagnostics.m2_relative_error, expected_m2_error)
        self.assertEqual(diagnostics.km2_relative_error, 0.0)
        self.assertEqual(diagnostics.selected_relative_error, 0.0)

    def test_detects_dscontarea_in_square_metres(self) -> None:
        basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
        streamnet["DSContArea"] = [area_100_m2 + area_200_m2, area_100_m2]
        with TemporaryDirectory() as temp_dir:
            source = load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))
        expected_200_m2 = math.fsum([area_100_m2, area_200_m2])
        expected_100_m2 = area_100_m2
        diagnostics = source.diagnostics.dscontarea
        self.assertEqual(diagnostics.source_unit, "m2")
        self.assertEqual(diagnostics.checked_polygon_bearing_link_count, 2)
        self.assertEqual(
            diagnostics.dscontarea_sum_raw,
            math.fsum([area_100_m2 + area_200_m2, area_100_m2]),
        )
        self.assertEqual(diagnostics.m2_relative_error, 0.0)
        self.assertEqual(diagnostics.selected_relative_error, 0.0)
        self.assertEqual(
            source.streamnet["DSContArea_km2"].tolist(),
            [(area_100_m2 + area_200_m2) / 1_000_000, area_100_m2 / 1_000_000],
        )
        self.assertEqual(
            diagnostics.geodesic_upstream_area_sum_m2,
            math.fsum([expected_200_m2, expected_100_m2]),
        )

    def test_reprojects_declared_crs_before_normalization(self) -> None:
        basins, streamnet, _, _ = canonical_frames()
        with TemporaryDirectory() as temp_dir:
            source = load_tdx_geopackages(
                *write_pair(Path(temp_dir), basins.to_crs(3857), streamnet.to_crs(3857))
            )
        self.assertEqual(source.basins.crs.to_epsg(), 4326)
        self.assertEqual(source.streamnet.crs.to_epsg(), 4326)
        self.assertEqual(source.basins["streamID"].tolist(), [200, 100])
        self.assertEqual(source.streamnet["DSLINKNO"].tolist(), [-1, 200])
        for actual, expected in zip(source.basins.geometry, basins.geometry, strict=True):
            self.assertTrue(actual.equals_exact(expected, 1e-9))
        for actual, expected in zip(source.streamnet.geometry, streamnet.geometry, strict=True):
            self.assertTrue(actual.equals_exact(expected, 1e-9))
        self.assertEqual(source.diagnostics.dscontarea.source_unit, "km2")
        self.assertLess(source.diagnostics.dscontarea.selected_relative_error, 1e-9)

    def test_clamps_one_tdx_cell_envelope_and_reports_native_ids(self) -> None:
        tolerance = COORDINATE_DOMAIN_TOLERANCE_DEGREES
        clamped_polygon = Polygon([
            (179.99, 0.00), (180.0, 0.00), (180.0, 0.01),
            (179.99, 0.01), (179.99, 0.00),
        ])
        edge_area_m2 = abs(Geod(ellps="WGS84").geometry_area_perimeter(clamped_polygon)[0])
        basins = gpd.GeoDataFrame(
            {"streamID": [100], "label": ["edge"]},
            geometry=[Polygon([
                (179.99, 0.00), (180 + tolerance / 2, 0.00),
                (180 + tolerance / 2, 0.01), (179.99, 0.01), (179.99, 0.00),
            ])], crs="EPSG:4326",
        )
        streamnet = gpd.GeoDataFrame(
            {"LINKNO": [100], "DSLINKNO": [-1],
             "DSContArea": [edge_area_m2 / 1_000_000], "label": ["edge"]},
            geometry=[LineString([(179.99, 0.00), (180 + tolerance / 2, 0.00)])],
            crs="EPSG:4326",
        )
        with TemporaryDirectory() as temp_dir:
            with self.assertLogs("tdx-hydro", level="WARNING") as captured:
                source = load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))
        self.assertEqual(source.diagnostics.basins_clamp, LayerClampDiagnostics(2, (100,)))
        self.assertEqual(source.diagnostics.streamnet_clamp, LayerClampDiagnostics(1, (100,)))
        self.assertEqual(len(captured.records), 2)
        messages = [record.getMessage() for record in captured.records]
        self.assertIn(
            "diagnostic=basins_clamp.altered_vertex_count count=2 native_ids=(100,)",
            messages,
        )
        self.assertIn(
            "diagnostic=streamnet_clamp.altered_vertex_count count=1 native_ids=(100,)",
            messages,
        )
        self.assertEqual(get_coordinates(source.basins.geometry).max(axis=0)[0], 180.0)
        self.assertEqual(get_coordinates(source.streamnet.geometry).max(axis=0)[0], 180.0)
        self.assertAlmostEqual(source.diagnostics.dscontarea.selected_relative_error, 0.0)

    def test_rejects_overshoot_beyond_one_tdx_cell(self) -> None:
        tolerance = COORDINATE_DOMAIN_TOLERANCE_DEGREES
        clamped_polygon = Polygon([(179.99, 0.0), (180.0, 0.0), (180.0, 0.01),
                                   (179.99, 0.01), (179.99, 0.0)])
        area = abs(Geod(ellps="WGS84").geometry_area_perimeter(clamped_polygon)[0])
        basins = gpd.GeoDataFrame(
            {"streamID": [100], "label": ["edge"]},
            geometry=[Polygon([(179.99, 0.0), (180 + tolerance / 2, 0.0),
                               (180 + tolerance / 2, 0.01), (179.99, 0.01),
                               (179.99, 0.0)])], crs="EPSG:4326")
        streamnet = gpd.GeoDataFrame(
            {"LINKNO": [100], "DSLINKNO": [-1], "DSContArea": [area / 1_000_000],
             "label": ["edge"]},
            geometry=[LineString([(179.99, 0.0), (180 + tolerance + 1e-9, 0.0)])],
            crs="EPSG:4326")
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(
                ValueError,
                rf"(?=.*streamnet)(?=.*LINKNO=100)(?=.*longitude_excess)(?=.*{tolerance!r})",
            ):
                load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))

    def test_rejects_schema_crs_and_geometry_contract_violations(self) -> None:
        def assert_invalid(
            basins: gpd.GeoDataFrame,
            streamnet: gpd.GeoDataFrame,
            pattern: str,
        ) -> None:
            with TemporaryDirectory() as temp_dir:
                paths = write_pair(Path(temp_dir), basins, streamnet)
                with self.assertRaisesRegex(ValueError, pattern):
                    load_tdx_geopackages(*paths)

        for layer, column in (("basins", "streamID"), ("streamnet", "LINKNO"),
                              ("streamnet", "DSLINKNO"), ("streamnet", "DSContArea")):
            with self.subTest(layer=layer, column=column):
                basins, streamnet, _, _ = canonical_frames()
                frame = basins if layer == "basins" else streamnet
                frame.drop(columns=column, inplace=True)
                assert_invalid(basins, streamnet, rf"(?=.*{layer})(?=.*{column})")
        for layer in ("basins", "streamnet"):
            with self.subTest(layer=layer, violation="CRS"):
                basins, streamnet, _, _ = canonical_frames()
                (basins if layer == "basins" else streamnet).set_crs(None, allow_override=True, inplace=True)
                assert_invalid(basins, streamnet, rf"(?=.*{layer})(?=.*CRS)")
        geometry_cases = (
            ("basins", Point(0.0, 0.0), "Polygon"),
            ("streamnet", MultiLineString([[(0.0, 0.0), (0.01, 0.0)]]), "LineString"),
            (
                "streamnet",
                LineString([(0.00, 0.00), (0.00, 0.00), (0.00, 0.00)]),
                "valid",
            ),
            (
                "basins",
                Polygon([
                    (0.00, 0.00),
                    (0.01, 0.01),
                    (0.01, 0.00),
                    (0.00, 0.01),
                    (0.00, 0.00),
                ]),
                "valid",
            ),
            ("basins", None, "geometry"), ("streamnet", None, "geometry"),
            ("basins", Polygon(), "geometry"), ("streamnet", LineString(), "geometry"),
        )
        for layer, geometry, expected in geometry_cases:
            with self.subTest(layer=layer, geometry=repr(geometry)):
                basins, streamnet, _, _ = canonical_frames()
                frame = basins if layer == "basins" else streamnet
                frame.at[frame.index[-1], "geometry"] = geometry
                assert_invalid(basins, streamnet, rf"(?=.*{layer})(?=.*{expected})")
        basins, streamnet, _, _ = canonical_frames()
        with TemporaryDirectory() as temp_dir:
            paths = write_pair(Path(temp_dir), basins, streamnet)
            gpd.GeoDataFrame(geometry=[Point(0.0, 0.0)], crs="EPSG:4326").to_file(
                paths[0], layer="extra", driver="GPKG", engine="pyogrio", mode="a")
            with self.assertRaisesRegex(ValueError, r"(?=.*basins)(?=.*extra)"):
                load_tdx_geopackages(*paths)

    def test_rejects_invalid_dscontarea_values(self) -> None:
        for value in (None, True, 0.0, -1.0, float("nan"), float("inf")):
            with self.subTest(value=value):
                basins, streamnet, _, _ = canonical_frames()
                streamnet["DSContArea"] = streamnet["DSContArea"].astype(object)
                streamnet.at[1, "DSContArea"] = value
                offending_representation = "nan" if value is None else repr(value)
                with TemporaryDirectory() as temp_dir:
                    with self.assertRaisesRegex(
                        ValueError,
                        rf"(?=.*streamnet\.DSContArea)(?=.*{offending_representation})",
                    ):
                        load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))

    def test_rejects_empirical_dscontarea_scale_mismatch(self) -> None:
        basins, streamnet, _, _ = canonical_frames()
        streamnet["DSContArea"] *= 1000
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(
                ValueError,
                r"(?=.*DSContArea)(?=.*m2_relative_error)(?=.*km2_relative_error)(?=.*0\.05)",
            ):
                load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))

    def test_join_miss_never_uses_spatial_fallback(self) -> None:
        basins, streamnet, _, _ = canonical_frames()
        basins.loc[basins["streamID"] == 100, "streamID"] = 999
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(
                ValueError, r"basins\.streamID does not join to streamnet\.LINKNO: 999$"
            ):
                load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))

    def test_normalization_rejects_duplicate_ids(self) -> None:
        for layer in ("basins", "streamnet"):
            with self.subTest(layer=layer):
                basins, streamnet, _, _ = canonical_frames()
                if layer == "basins":
                    basins = gpd.GeoDataFrame(pd.concat([basins, basins.iloc[[1]]], ignore_index=True), crs=basins.crs)
                    pattern = r"duplicate unit.*100"
                else:
                    streamnet = gpd.GeoDataFrame(pd.concat([streamnet, streamnet.iloc[[1]]], ignore_index=True), crs=streamnet.crs)
                    pattern = r"duplicate LINKNO.*100"
                with TemporaryDirectory() as temp_dir:
                    with self.assertRaisesRegex(ValueError, pattern):
                        load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))


class HeaderCrosswalkTests(unittest.TestCase):
    def test_loads_vendored_crosswalk(self) -> None:
        crosswalk = load_header_crosswalk()

        self.assertEqual(len(crosswalk), 62)
        self.assertEqual(crosswalk["7020000010"], 71)
        self.assertTrue(all(isinstance(value, int) for value in crosswalk.values()))


class GlobalLinknoTests(unittest.TestCase):
    def test_adds_processing_basin_header(self) -> None:
        self.assertEqual(global_linkno(123_456, 71), 710_123_456)

    def test_preserves_minus_one_sentinel(self) -> None:
        self.assertEqual(global_linkno(-1, 71), -1)


class StreamnetModelTests(unittest.TestCase):
    def test_uses_single_coordinate_as_degenerate_polygon_bearing_root_outlet(
        self,
    ) -> None:
        basins = pd.DataFrame({"streamID": [100]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": [LineString([(4.0, 5.0), (4.0, 5.0)])],
            }
        )
        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )

        self.assertEqual(
            model.units,
            (StreamnetUnit(100, 710_000_100, 0, None, -1, -1, 0, 4.0, 5.0),),
        )
        self.assertEqual(model.edges, ())
        self.assertEqual(model.roots, (710_000_100,))
        self.assertEqual(
            model.diagnostics,
            StreamnetDiagnostics(
                polygon_bearing_link_count=1,
                polygonless_dropped_reach_count=0,
                degenerate_reach_count=1,
                degenerate_reach_native_linknos=(100,),
                degenerate_polygon_bearing_reach_count=1,
                degenerate_polygon_bearing_reach_native_linknos=(100,),
                degenerate_polygonless_reach_count=0,
                degenerate_polygonless_reach_native_linknos=(),
                root_count=1,
                contracted_edge_count=0,
                contracted_root_count=0,
                contracted_link_traversal_count=0,
                endpoint_coincidence_proven_link_count=0,
                predecessor_orientation_proven_root_count=0,
                trusted_orientation_isolated_root_count=0,
                trusted_orientation_isolated_root_native_linknos=(),
                trusted_orientation_polygon_bearing_isolated_root_count=0,
                trusted_orientation_polygon_bearing_isolated_root_native_linknos=(),
                orientation_tolerance=0.001,
            ),
        )

    def test_proves_healthy_reach_by_coincidence_with_degenerate_successor(
        self,
    ) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                    LineString([(1.0, 0.0), (1.0, 0.0)]),
                ],
            }
        )
        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )

        self.assertEqual(
            [(unit.linkno, unit.outlet_lon, unit.outlet_lat) for unit in model.units],
            [(100, 1.0, 0.0), (200, 1.0, 0.0)],
        )
        self.assertEqual(model.edges, ((710_000_100, 710_000_200),))
        self.assertEqual(model.roots, (710_000_200,))
        self.assertEqual(
            model.diagnostics,
            StreamnetDiagnostics(
                polygon_bearing_link_count=2,
                polygonless_dropped_reach_count=0,
                degenerate_reach_count=1,
                degenerate_reach_native_linknos=(200,),
                degenerate_polygon_bearing_reach_count=1,
                degenerate_polygon_bearing_reach_native_linknos=(200,),
                degenerate_polygonless_reach_count=0,
                degenerate_polygonless_reach_native_linknos=(),
                root_count=1,
                contracted_edge_count=0,
                contracted_root_count=0,
                contracted_link_traversal_count=0,
                endpoint_coincidence_proven_link_count=1,
                predecessor_orientation_proven_root_count=0,
                trusted_orientation_isolated_root_count=0,
                trusted_orientation_isolated_root_native_linknos=(),
                trusted_orientation_polygon_bearing_isolated_root_count=0,
                trusted_orientation_polygon_bearing_isolated_root_native_linknos=(),
                orientation_tolerance=0.001,
            ),
        )

    def test_contracts_through_polygonless_degenerate_chain(self) -> None:
        point = (-120.729444444445, 42.8208888888891)
        basins = pd.DataFrame({"streamID": [244107, 240000]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [244107, 242123, 240000],
                "DSLINKNO": [-1, 244107, 242123],
                "geometry": [
                    LineString([point, point]),
                    LineString([point, point]),
                    LineString([(-120.731444444445, 42.8208888888891), point]),
                ],
            }
        )
        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )

        self.assertEqual(
            model.units,
            (
                StreamnetUnit(
                    240000,
                    710_240_000,
                    0,
                    None,
                    244107,
                    710_244_107,
                    1,
                    -120.729444444445,
                    42.8208888888891,
                ),
                StreamnetUnit(
                    244107,
                    710_244_107,
                    0,
                    None,
                    -1,
                    -1,
                    0,
                    -120.729444444445,
                    42.8208888888891,
                ),
            ),
        )
        self.assertEqual(model.edges, ((710_240_000, 710_244_107),))
        self.assertEqual(model.roots, (710_244_107,))
        self.assertEqual(model.diagnostics.polygon_bearing_link_count, 2)
        self.assertEqual(model.diagnostics.polygonless_dropped_reach_count, 1)
        self.assertEqual(model.diagnostics.root_count, 1)
        self.assertEqual(model.diagnostics.contracted_edge_count, 1)
        self.assertEqual(model.diagnostics.contracted_root_count, 0)
        self.assertEqual(model.diagnostics.contracted_link_traversal_count, 1)
        self.assertEqual(model.diagnostics.degenerate_reach_count, 2)
        self.assertEqual(
            model.diagnostics.degenerate_reach_native_linknos, (242123, 244107)
        )
        self.assertEqual(model.diagnostics.degenerate_polygon_bearing_reach_count, 1)
        self.assertEqual(
            model.diagnostics.degenerate_polygon_bearing_reach_native_linknos,
            (244107,),
        )
        self.assertEqual(model.diagnostics.degenerate_polygonless_reach_count, 1)
        self.assertEqual(
            model.diagnostics.degenerate_polygonless_reach_native_linknos, (242123,)
        )
        self.assertEqual(model.diagnostics.endpoint_coincidence_proven_link_count, 1)
        self.assertEqual(model.diagnostics.predecessor_orientation_proven_root_count, 0)
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_count, 0)
        self.assertEqual(
            model.diagnostics.trusted_orientation_isolated_root_native_linknos, ()
        )
        self.assertEqual(
            model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_count,
            0,
        )
        self.assertEqual(
            model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_native_linknos,
            (),
        )
        self.assertEqual(model.diagnostics.orientation_tolerance, 0.001)

    def test_builds_deterministic_contracted_model(self) -> None:
        basins = pd.DataFrame(
            {
                "streamID": [300, 100, 200],
                "geometry": ["polygon-300", "polygon-100", "polygon-200"],
            }
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [201, 102, 300, 100, 200, 101, 301],
                "DSLINKNO": [-1, 200, -1, 101, 201, 102, 300],
                "geometry": [
                    LineString([(5.0, 0.0), (6.0, 0.0)]),
                    LineString([(3.0, 0.0), (4.0, 0.0)]),
                    LineString([(11.0, 0.0), (12.0, 0.0)]),
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                    LineString([(4.0, 0.0), (5.0, 0.0)]),
                    LineString([(1.0, 0.0), (3.0, 0.0)]),
                    LineString([(10.0, 0.0), (11.0, 0.0)]),
                ],
            }
        )
        basins_before = basins.copy(deep=True)
        streamnet_before = streamnet.copy(deep=True)

        with self.assertLogs("tdx-hydro", level="INFO") as captured:
            model = build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

        self.assertEqual(
            model.units,
            (
                StreamnetUnit(
                    100, 710_000_100, 0, None, 200, 710_000_200, 2, 1.0, 0.0
                ),
                StreamnetUnit(200, 710_000_200, 0, None, -1, -1, 1, 5.0, 0.0),
                StreamnetUnit(300, 710_000_300, 0, None, -1, -1, 0, 12.0, 0.0),
            ),
        )
        self.assertEqual(model.edges, ((710_000_100, 710_000_200),))
        self.assertEqual(model.roots, (710_000_200, 710_000_300))
        self.assertEqual(
            model.diagnostics,
            StreamnetDiagnostics(
                polygon_bearing_link_count=3,
                polygonless_dropped_reach_count=4,
                degenerate_reach_count=0,
                degenerate_reach_native_linknos=(),
                degenerate_polygon_bearing_reach_count=0,
                degenerate_polygon_bearing_reach_native_linknos=(),
                degenerate_polygonless_reach_count=0,
                degenerate_polygonless_reach_native_linknos=(),
                root_count=2,
                contracted_edge_count=1,
                contracted_root_count=1,
                contracted_link_traversal_count=3,
                endpoint_coincidence_proven_link_count=5,
                predecessor_orientation_proven_root_count=2,
                trusted_orientation_isolated_root_count=0,
                trusted_orientation_isolated_root_native_linknos=(),
                trusted_orientation_polygon_bearing_isolated_root_count=0,
                trusted_orientation_polygon_bearing_isolated_root_native_linknos=(),
                orientation_tolerance=0.001,
            ),
        )
        self.assertEqual(model.diagnostics.polygonless_dropped_reach_count, 4)
        self.assertIn(
            "streamnet_model polygon_bearing_links=3 degenerate_reaches=0 "
            "degenerate_reach_native_linknos=() degenerate_polygon_bearing_reaches=0 "
            "degenerate_polygon_bearing_reach_native_linknos=() "
            "degenerate_polygonless_reaches=0 "
            "degenerate_polygonless_reach_native_linknos=() roots=2 contracted_edges=1 "
            "contracted_roots=1 contracted_link_traversals=3 "
            "endpoint_coincidence_proven_links=5 predecessor_orientation_proven_roots=2 "
            "trusted_orientation_isolated_roots=0 "
            "trusted_orientation_isolated_root_native_linknos=() "
            "trusted_orientation_polygon_bearing_isolated_roots=0 "
            "trusted_orientation_polygon_bearing_isolated_root_native_linknos=() "
            "orientation_tolerance=0.001",
            "\n".join(captured.output),
        )
        pd.testing.assert_frame_equal(basins, basins_before)
        pd.testing.assert_frame_equal(streamnet, streamnet_before)

    def test_uses_tolerance_and_reversed_coordinate_order(self) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(1.0005, 0.0), (0.0, 0.0)]),
                    LineString([(1.0, 0.0), (2.0, 0.0)]),
                ],
            }
        )

        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )

        self.assertEqual(
            [(unit.outlet_lon, unit.outlet_lat) for unit in model.units],
            [(1.0005, 0.0), (2.0, 0.0)],
        )
        self.assertEqual(model.diagnostics.endpoint_coincidence_proven_link_count, 1)
        self.assertEqual(model.diagnostics.predecessor_orientation_proven_root_count, 1)
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_count, 0)
        self.assertEqual(model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_count, 0)
        self.assertEqual(model.diagnostics.orientation_tolerance, 0.001)


class StreamnetOrientationRejectionTests(unittest.TestCase):
    def test_rejects_non_coincident_successor(self) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                    LineString([(2.0, 0.0), (3.0, 0.0)]),
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*orientation)(?=.*100)(?=.*200)(?=.*non-coincident)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_both_current_endpoints_as_ambiguous(self) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*orientation)(?=.*100)(?=.*200)(?=.*ambiguous)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_one_endpoint_matching_two_distinct_successor_coordinates(
        self,
    ) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                    LineString([(1.0004, 0.0), (1.0008, 0.0)]),
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*orientation)(?=.*100)(?=.*200)(?=.*ambiguous)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_degenerate_reach_noncoincident_with_successor(self) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(1.0, 0.0), (1.0, 0.0)]),
                    LineString([(2.0, 0.0), (3.0, 0.0)]),
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*orientation)(?=.*100)(?=.*200)(?=.*non-coincident)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_uses_native_order_for_isolated_root_under_trust_assumption(self) -> None:
        basins = pd.DataFrame({"streamID": [100]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": [LineString([(0.0, 0.0), (1.0, 0.0)])],
            }
        )

        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )
        self.assertEqual((model.units[0].outlet_lon, model.units[0].outlet_lat), (1.0, 0.0))
        self.assertEqual(model.diagnostics.endpoint_coincidence_proven_link_count, 0)
        self.assertEqual(model.diagnostics.predecessor_orientation_proven_root_count, 0)
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_count, 1)
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_native_linknos, (100,))
        self.assertEqual(model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_count, 1)
        self.assertEqual(model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_native_linknos, (100,))

    def test_distinguishes_polygonless_isolated_root_diagnostic(self) -> None:
        basins = pd.DataFrame({"streamID": [100]})
        streamnet = pd.DataFrame(
            {"LINKNO": [100, 900], "DSLINKNO": [-1, -1], "geometry": [
                LineString([(0.0, 0.0), (1.0, 0.0)]),
                LineString([(10.0, 0.0), (11.0, 0.0)]),
            ]}
        )
        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_count, 2)
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_native_linknos, (100, 900))
        self.assertEqual(model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_count, 1)
        self.assertEqual(model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_native_linknos, (100,))
        self.assertEqual((model.units[0].outlet_lon, model.units[0].outlet_lat), (1.0, 0.0))

    def test_rejects_conflicting_root_predecessors(self) -> None:
        basins = pd.DataFrame({"streamID": [100]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 101, 102],
                "DSLINKNO": [-1, 100, 100],
                "geometry": [
                    LineString([(0.0, 0.0), (2.0, 0.0)]),
                    LineString([(-1.0, 0.0), (0.0, 0.0)]),
                    LineString([(3.0, 0.0), (2.0, 0.0)]),
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*orientation)(?=.*root)(?=.*100)(?=.*conflicting)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )


class EndpointToleranceContractTests(unittest.TestCase):
    def test_rejects_values_that_are_not_positive_and_finite(self) -> None:
        basins = pd.DataFrame({"streamID": [100]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": [LineString([(0.0, 0.0), (1.0, 0.0)])],
            }
        )

        for endpoint_tolerance in (
            0.0,
            -0.001,
            float("nan"),
            float("inf"),
            True,
            None,
        ):
            with self.subTest(endpoint_tolerance=endpoint_tolerance):
                with self.assertRaisesRegex(
                    ValueError, r"(?=.*endpoint_tolerance)(?=.*positive finite)"
                ):
                    build_streamnet_model(
                        basins,
                        streamnet,
                        header_number=71,
                        endpoint_tolerance=endpoint_tolerance,
                    )


class StreamnetInputContractTests(unittest.TestCase):
    def test_rejects_duplicate_unit_identity(self) -> None:
        basins = pd.DataFrame(
            {
                "streamID": [100, 100],
                "geometry": ["polygon-100-a", "polygon-100-b"],
            }
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": ["reach-100"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"duplicate unit.*100"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_bifurcation(self) -> None:
        basins = pd.DataFrame(
            {
                "streamID": [100, 200, 300],
                "geometry": ["polygon-100", "polygon-200", "polygon-300"],
            }
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 100, 200, 300],
                "DSLINKNO": [200, 300, -1, -1],
                "geometry": [
                    "reach-100-a",
                    "reach-100-b",
                    "reach-200",
                    "reach-300",
                ],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*bifurcation)(?=.*duplicate LINKNO)(?=.*100)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_identical_duplicate_linkno(self) -> None:
        basins = pd.DataFrame(
            {
                "streamID": [100, 200],
                "geometry": ["polygon-100", "polygon-200"],
            }
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 100, 200],
                "DSLINKNO": [200, 200, -1],
                "geometry": ["reach-100-a", "reach-100-b", "reach-200"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"duplicate LINKNO.*100"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_basin_join_miss(self) -> None:
        basins = pd.DataFrame(
            {
                "streamID": [100, 999],
                "geometry": ["polygon-100", "polygon-999"],
            }
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": ["reach-100"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"(?=.*streamID)(?=.*LINKNO)(?=.*999)"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_invalid_negative_downstream(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {"LINKNO": [100], "DSLINKNO": [-2], "geometry": ["reach-100"]}
        )

        with self.assertRaisesRegex(ValueError, r"(?=.*DSLINKNO)(?=.*-1)(?=.*-2)"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_non_integral_downstream(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [101.5],
                "geometry": ["reach-100"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"(?=.*DSLINKNO)(?=.*integer)(?=.*101.5)"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_requires_basin_streamid(self) -> None:
        basins = pd.DataFrame({"geometry": ["polygon-100"]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": ["reach-100"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"(?=.*basins)(?=.*streamID)"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_requires_streamnet_dslinkno(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {"LINKNO": [100], "geometry": ["reach-100"]}
        )

        with self.assertRaisesRegex(ValueError, r"(?=.*streamnet)(?=.*DSLINKNO)"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_empty_unit_selection(self) -> None:
        basins = pd.DataFrame(
            {
                "streamID": pd.Series([], dtype="int64"),
                "geometry": pd.Series([], dtype="object"),
            }
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": ["reach-100"],
            }
        )

        with self.assertRaisesRegex(ValueError, "no drainage units"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )


class StreamnetTopologyRejectionTests(unittest.TestCase):
    def test_rejects_direct_self_link(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [100],
                "geometry": ["reach-100"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"self-link.*100"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_cycle_through_polygonless_links(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 101, 102],
                "DSLINKNO": [101, 102, 101],
                "geometry": ["reach-100", "reach-101", "reach-102"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"(?=.*cycle)(?=.*101)(?=.*102)"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_missing_downstream_link(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 101],
                "DSLINKNO": [101, 999],
                "geometry": ["reach-100", "reach-101"],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*missing downstream)(?=.*101)(?=.*999)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

    def test_rejects_disconnected_malformed_row(self) -> None:
        basins = pd.DataFrame(
            {"streamID": [100], "geometry": ["polygon-100"]}
        )
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 900],
                "DSLINKNO": [-1, 900],
                "geometry": ["reach-100", "reach-900"],
            }
        )

        with self.assertRaisesRegex(ValueError, r"self-link.*900"):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )


class CoreHfxCompilationTests(unittest.TestCase):
    created_at = datetime(2026, 7, 21, 12, 34, 56, tzinfo=timezone.utc)
    basin_id = "7020000010"
    fabric_version = "synthetic-2026.07"

    def compile_fixture(
        self,
        directory: Path,
        out_dir: Path,
        *,
        isolated_roots: bool = False,
    ):
        directory.mkdir()
        basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
        if isolated_roots:
            streamnet["DSLINKNO"] = [-1, -1]
            streamnet["DSContArea"] = [
                area_200_m2 / 1_000_000,
                area_100_m2 / 1_000_000,
            ]
        source = load_tdx_geopackages(*write_pair(directory, basins, streamnet))
        model = build_streamnet_model(
            source.basins,
            source.streamnet,
            header_number=71,
            endpoint_tolerance=0.001,
        )
        result = compile_core_hfx(
            source,
            model,
            out_dir,
            processing_basin_id=self.basin_id,
            fabric_version=self.fabric_version,
            created_at=self.created_at,
        )
        return source, model, result, basins

    def test_compile_core_hfx_writes_deterministic_artifacts_and_preserves_diagnostics(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source, model, first, _ = self.compile_fixture(
                root / "source-a", root / "output-a"
            )
            _, _, second, _ = self.compile_fixture(
                root / "source-b", root / "output-b"
            )
            expected_names = {
                "catchments.parquet",
                "graph.parquet",
                "manifest.json",
                "aux",
            }
            self.assertEqual(
                {path.name for path in (root / "output-a").iterdir()},
                expected_names,
            )
            self.assertEqual(
                {path.name for path in (root / "output-b").iterdir()},
                expected_names,
            )
            expected_files = {
                "catchments.parquet",
                "graph.parquet",
                "manifest.json",
                "aux/snap_stems.parquet",
            }
            for output_name in ("output-a", "output-b"):
                output = root / output_name
                self.assertEqual(
                    {
                        path.relative_to(output).as_posix()
                        for path in output.rglob("*")
                        if path.is_file()
                    },
                    expected_files,
                )
            self.assertEqual(first.catchments_path, root / "output-a/catchments.parquet")
            self.assertEqual(first.graph_path, root / "output-a/graph.parquet")
            self.assertEqual(first.snap_path, root / "output-a/aux/snap_stems.parquet")
            self.assertEqual(first.manifest_path, root / "output-a/manifest.json")
            for name in expected_files:
                self.assertEqual(
                    (root / "output-a" / name).read_bytes(),
                    (root / "output-b" / name).read_bytes(),
                )

        self.assertIsInstance(first, CoreBuildResult)
        self.assertIs(first.diagnostics.ingestion, source.diagnostics)
        self.assertIs(first.diagnostics.streamnet, model.diagnostics)
        ingestion = first.diagnostics.ingestion
        self.assertEqual(ingestion.basins_clamp, LayerClampDiagnostics(0, ()))
        self.assertEqual(ingestion.streamnet_clamp, LayerClampDiagnostics(0, ()))
        self.assertEqual(ingestion.dscontarea.source_unit, "km2")
        self.assertEqual(ingestion.dscontarea.checked_polygon_bearing_link_count, 2)
        self.assertEqual(ingestion.dscontarea.km2_relative_error, 0.0)
        self.assertEqual(ingestion.dscontarea.selected_relative_error, 0.0)
        diagnostics = first.diagnostics.streamnet
        self.assertEqual(diagnostics.polygon_bearing_link_count, 2)
        self.assertEqual(diagnostics.degenerate_reach_count, 0)
        self.assertEqual(diagnostics.degenerate_reach_native_linknos, ())
        self.assertEqual(diagnostics.degenerate_polygon_bearing_reach_count, 0)
        self.assertEqual(
            diagnostics.degenerate_polygon_bearing_reach_native_linknos, ()
        )
        self.assertEqual(diagnostics.degenerate_polygonless_reach_count, 0)
        self.assertEqual(diagnostics.degenerate_polygonless_reach_native_linknos, ())
        self.assertEqual(diagnostics.root_count, 1)
        self.assertEqual(diagnostics.contracted_edge_count, 0)
        self.assertEqual(diagnostics.contracted_root_count, 0)
        self.assertEqual(diagnostics.contracted_link_traversal_count, 0)
        self.assertEqual(diagnostics.endpoint_coincidence_proven_link_count, 1)
        self.assertEqual(diagnostics.predecessor_orientation_proven_root_count, 1)
        self.assertEqual(diagnostics.trusted_orientation_isolated_root_count, 0)
        self.assertEqual(
            diagnostics.trusted_orientation_isolated_root_native_linknos, ()
        )
        self.assertEqual(
            diagnostics.trusted_orientation_polygon_bearing_isolated_root_count, 0
        )
        self.assertEqual(
            diagnostics.trusted_orientation_polygon_bearing_isolated_root_native_linknos,
            (),
        )
        self.assertEqual(diagnostics.orientation_tolerance, 0.001)

    def test_compile_core_hfx_writes_exact_schemas_values_and_manifest(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _, _, result, source_basins = self.compile_fixture(
                root / "source", root / "output"
            )
            catchments_file = pq.ParquetFile(result.catchments_path)
            graph_file = pq.ParquetFile(result.graph_path)
            snap_file = pq.ParquetFile(result.snap_path)
            catchments = catchments_file.read()
            graph = graph_file.read()
            snap = snap_file.read()

            expected_geo = {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "geometry_types": ["Polygon", "MultiPolygon"],
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
            bbox_type = pa.struct(
                [pa.field(name, pa.float32(), nullable=False) for name in BBOX_LEAF_NAMES]
            )
            expected_catchment_schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("level", pa.int16(), nullable=False),
                    pa.field("parent_id", pa.int64(), nullable=True),
                    pa.field("area_km2", pa.float32(), nullable=False),
                    pa.field("up_area_km2", pa.float32(), nullable=True),
                    pa.field("outlet_lon", pa.float64(), nullable=False),
                    pa.field("outlet_lat", pa.float64(), nullable=False),
                    pa.field("bbox", bbox_type, nullable=False),
                    pa.field("geometry", pa.binary(), nullable=False),
                ],
                metadata={b"geo": json.dumps(expected_geo).encode("utf-8")},
            )
            expected_graph_schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("level", pa.int16(), nullable=False),
                    pa.field(
                        "upstream_ids",
                        pa.list_(pa.field("item", pa.int64(), nullable=True)),
                        nullable=False,
                    ),
                    pa.field("bbox_minx", pa.float32(), nullable=False),
                    pa.field("bbox_miny", pa.float32(), nullable=False),
                    pa.field("bbox_maxx", pa.float32(), nullable=False),
                    pa.field("bbox_maxy", pa.float32(), nullable=False),
                ]
            )
            expected_snap_geo = {
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
            expected_snap_schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("unit_id", pa.int64(), nullable=False),
                    pa.field("weight", pa.float32(), nullable=False),
                    pa.field("stem_role", pa.string(), nullable=True),
                    pa.field("bbox", bbox_type, nullable=True),
                    pa.field("geometry", pa.binary(), nullable=False),
                ],
                metadata={b"geo": json.dumps(expected_snap_geo).encode("utf-8")},
            )
            self.assertEqual(catchments.schema, expected_catchment_schema)
            self.assertEqual(graph.schema, expected_graph_schema)
            self.assertEqual(snap.schema, expected_snap_schema)
            self.assertEqual(catchments_file.num_row_groups, 1)
            self.assertEqual(graph_file.num_row_groups, 1)
            self.assertEqual(snap_file.num_row_groups, 1)
            self.assertEqual(catchments["id"].to_pylist(), [710000100, 710000200])
            self.assertEqual(catchments["level"].to_pylist(), [0, 0])
            self.assertEqual(catchments["parent_id"].to_pylist(), [None, None])
            self.assertEqual(
                catchments["area_km2"].to_pylist(),
                [1.2309072017669678, 1.2309072017669678],
            )
            self.assertEqual(
                catchments["up_area_km2"].to_pylist(),
                [1.2309072017669678, 2.4618144035339355],
            )
            self.assertEqual(
                list(zip(catchments["outlet_lon"].to_pylist(), catchments["outlet_lat"].to_pylist())),
                [(0.01, 0.0), (0.02, 0.0)],
            )
            for actual, expected in zip(
                from_wkb(catchments["geometry"].to_pylist()),
                [source_basins.geometry.iloc[1], source_basins.geometry.iloc[0]],
                strict=True,
            ):
                self.assertTrue(actual.equals(expected))
            expected_bounds = [
                [0.0, 0.0, 0.009999999776482582, 0.009999999776482582],
                [0.009999999776482582, 0.0, 0.019999999552965164, 0.009999999776482582],
            ]
            actual_bounds = [
                [row[name] for name in BBOX_LEAF_NAMES]
                for row in catchments["bbox"].to_pylist()
            ]
            self.assertEqual(actual_bounds, expected_bounds)
            self.assertEqual(graph["id"].to_pylist(), [710000100, 710000200])
            self.assertEqual(graph["level"].to_pylist(), [0, 0])
            self.assertEqual(graph["upstream_ids"].to_pylist(), [[], [710000100]])
            self.assertEqual(
                list(
                    zip(
                        graph["bbox_minx"].to_pylist(),
                        graph["bbox_miny"].to_pylist(),
                        graph["bbox_maxx"].to_pylist(),
                        graph["bbox_maxy"].to_pylist(),
                    )
                ),
                [tuple(bounds) for bounds in expected_bounds],
            )
            self.assertEqual(snap["id"].to_pylist(), [1, 2])
            self.assertEqual(snap["unit_id"].to_pylist(), [710000100, 710000200])
            self.assertEqual(
                snap["weight"].to_pylist(),
                [1.2309072017669678, 2.4618144035339355],
            )
            self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
            self.assertEqual(
                snap["bbox"].to_pylist(),
                [
                    {
                        "xmin": 0.0,
                        "ymin": -9.999999747378752e-05,
                        "xmax": 0.009999999776482582,
                        "ymax": 9.999999747378752e-05,
                    },
                    {
                        "xmin": 0.009999999776482582,
                        "ymin": -9.999999747378752e-05,
                        "xmax": 0.019999999552965164,
                        "ymax": 9.999999747378752e-05,
                    },
                ],
            )
            expected_stems = [
                LineString([(0.0, 0.0), (0.01, 0.0)]),
                LineString([(0.01, 0.0), (0.02, 0.0)]),
            ]
            self.assertEqual(
                snap["geometry"].to_pylist(),
                [geometry.wkb for geometry in expected_stems],
            )
            stem_distances = gpd.GeoSeries(
                [
                    LineString([(0.01, 0.0), (0.02, 0.0)]),
                    LineString([(0.0, 0.0), (0.01, 0.0)]),
                ],
                crs=CRS,
            ).centroid.hilbert_distance(total_bounds=source_basins.geometry.total_bounds)
            self.assertEqual(stem_distances.tolist(), [4026531839, 268435455])
            distances = source_basins.geometry.centroid.hilbert_distance(
                total_bounds=source_basins.geometry.total_bounds
            )
            self.assertEqual(distances.tolist(), [3489660928, 805306368])
            for parquet_file, names in (
                (catchments_file, [f"bbox.{name}" for name in BBOX_LEAF_NAMES]),
                (graph_file, [f"bbox_{name}" for name in ("minx", "miny", "maxx", "maxy")]),
            ):
                parquet_schema = parquet_file.schema_arrow
                for name in names:
                    column_index = parquet_file.schema.names.index(name.split(".")[-1])
                    statistics = parquet_file.metadata.row_group(0).column(column_index).statistics
                    self.assertIsNotNone(statistics, name)
                    self.assertTrue(statistics.has_min_max, name)
                self.assertEqual(parquet_file.schema_arrow, parquet_schema)
            for name in [f"bbox.{leaf}" for leaf in BBOX_LEAF_NAMES]:
                column_index = snap_file.schema.names.index(name.split(".")[-1])
                statistics = snap_file.metadata.row_group(0).column(column_index).statistics
                self.assertIsNotNone(statistics, name)
                self.assertTrue(statistics.has_min_max, name)
            self.assertEqual(
                json.loads(catchments.schema.metadata[b"geo"].decode("utf-8")),
                expected_geo,
            )
            self.assertEqual(
                json.loads(snap.schema.metadata[b"geo"].decode("utf-8")),
                expected_snap_geo,
            )
            self.assertTrue(
                validate_geoparquet(
                    str(result.catchments_path), target_version="1.1"
                ).is_valid
            )
            self.assertTrue(
                validate_geoparquet(
                    str(result.snap_path), target_version="1.1"
                ).is_valid
            )
            manifest = json.loads(result.manifest_path.read_text())

        expected_manifest = {
            "format_version": "0.3.0",
            "fabric_name": "tdx_hydro",
            "fabric_version": "synthetic-2026.07",
            "crs": "EPSG:4326",
            "has_up_area": True,
            "topology": "tree",
            "region": "7020000010",
            "bbox": [0.0, 0.0, 0.019999999552965164, 0.009999999776482582],
            "unit_count": 2,
            "created_at": "2026-07-21T12:34:56+00:00",
            "adapter_version": "0.1.0",
            "auxiliary": [
                {
                    "schema": "hfx.aux.snap.v2",
                    "artifacts": {"snap": "aux/snap_stems.parquet"},
                    "metadata": {
                        "name": "stems",
                        "description": "Native TDX-Hydro LineString reaches for polygon-bearing level 0 drainage units.",
                        "references_levels": [0],
                        "weight_semantics": "Drainage-area weight equals inclusive DSContArea in km2; higher values indicate stronger drainage dominance.",
                    },
                }
            ],
        }
        self.assertEqual(manifest, expected_manifest)
        self.assertEqual(FORMAT_VERSION, "0.3.0")
        self.assertEqual(FABRIC_NAME, "tdx_hydro")
        self.assertEqual(CRS, "EPSG:4326")
        self.assertIs(HAS_UP_AREA, True)
        self.assertEqual(TOPOLOGY, "tree")
        self.assertEqual(ADAPTER_VERSION, "0.1.0")
        schema_path = Path(__file__).resolve().parents[2] / "schemas/manifest.schema.json"
        schema = json.loads(schema_path.read_text())
        errors = sorted(
            Draft202012Validator(
                schema, format_checker=FormatChecker()
            ).iter_errors(manifest),
            key=lambda error: list(error.path),
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            schema["required"],
            [
                "format_version",
                "fabric_name",
                "crs",
                "has_up_area",
                "topology",
                "bbox",
                "unit_count",
                "created_at",
                "adapter_version",
            ],
        )

    def test_compile_core_hfx_preserves_trusted_isolated_root_outlets(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            _, _, result, _ = self.compile_fixture(
                root / "source", root / "output", isolated_roots=True
            )
            catchments = pq.read_table(result.catchments_path)
            graph = pq.read_table(result.graph_path)
            snap = pq.read_table(result.snap_path)
        self.assertEqual(graph["upstream_ids"].to_pylist(), [[], []])
        self.assertEqual(
            list(zip(catchments["outlet_lon"].to_pylist(), catchments["outlet_lat"].to_pylist())),
            [(0.01, 0.0), (0.02, 0.0)],
        )
        self.assertEqual(snap["id"].to_pylist(), [1, 2])
        self.assertEqual(snap["unit_id"].to_pylist(), [710000100, 710000200])
        self.assertEqual(
            snap["weight"].to_pylist(),
            [1.2309072017669678, 1.2309072017669678],
        )
        self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
        self.assertEqual(
            snap["geometry"].to_pylist(),
            [
                LineString([(0.0, 0.0), (0.01, 0.0)]).wkb,
                LineString([(0.01, 0.0), (0.02, 0.0)]).wkb,
            ],
        )
        diagnostics = result.diagnostics.streamnet
        self.assertEqual(diagnostics.endpoint_coincidence_proven_link_count, 0)
        self.assertEqual(diagnostics.predecessor_orientation_proven_root_count, 0)
        self.assertEqual(diagnostics.trusted_orientation_isolated_root_count, 2)
        self.assertEqual(
            diagnostics.trusted_orientation_isolated_root_native_linknos, (100, 200)
        )
        self.assertEqual(
            diagnostics.trusted_orientation_polygon_bearing_isolated_root_count, 2
        )
        self.assertEqual(
            diagnostics.trusted_orientation_polygon_bearing_isolated_root_native_linknos,
            (100, 200),
        )

    def test_compile_core_hfx_requires_build_identity_and_aware_timestamp(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            basins, streamnet, _, _ = canonical_frames()
            source = load_tdx_geopackages(*write_pair(root, basins, streamnet))
            model = build_streamnet_model(
                source.basins,
                source.streamnet,
                header_number=71,
                endpoint_tolerance=0.001,
            )
            valid = {
                "processing_basin_id": self.basin_id,
                "fabric_version": self.fabric_version,
                "created_at": self.created_at,
            }
            for argument, value in (
                ("fabric_version", ""),
                ("fabric_version", "   "),
                ("processing_basin_id", ""),
                ("processing_basin_id", "not-digits"),
                ("created_at", datetime(2026, 7, 21)),
            ):
                invalid = valid | {argument: value}
                with self.subTest(argument=argument, value=value):
                    with self.assertRaisesRegex(ValueError, argument):
                        compile_core_hfx(source, model, root / "output", **invalid)


class BuildCliTests(unittest.TestCase):
    created_at = datetime(2026, 7, 21, 12, 34, 56, tzinfo=timezone.utc)
    basin_id = "7020000010"
    fabric_version = "synthetic-2026.07"

    def build_args(
        self, basins_path: Path, streamnet_path: Path, output: Path, report: Path
    ) -> list[str]:
        return [
            "build",
            "--basins", str(basins_path),
            "--streamnet", str(streamnet_path),
            "--out", str(output),
            "--report", str(report),
            "--processing-basin-id", self.basin_id,
            "--fabric-version", self.fabric_version,
        ]

    def expected_report(self, output: Path, *, isolated: bool = False) -> dict[str, object]:
        ingestion_area = (
            2461814.409986507 if isolated else 3692721.6149797607
        )
        raw_area = 2.4618144099865074 if isolated else 3.692721614979761
        streamnet = {
            "polygon_bearing_link_count": 2,
            "polygonless_dropped_reach_count": 0 if isolated else 1,
            "degenerate_reach_count": 0,
            "degenerate_reach_native_linknos": [],
            "degenerate_polygon_bearing_reach_count": 0,
            "degenerate_polygon_bearing_reach_native_linknos": [],
            "degenerate_polygonless_reach_count": 0,
            "degenerate_polygonless_reach_native_linknos": [],
            "root_count": 2 if isolated else 1,
            "contracted_edge_count": 0 if isolated else 1,
            "contracted_root_count": 0,
            "contracted_link_traversal_count": 0 if isolated else 1,
            "endpoint_coincidence_proven_link_count": 0 if isolated else 2,
            "predecessor_orientation_proven_root_count": 0 if isolated else 1,
            "trusted_orientation_isolated_root_count": 2 if isolated else 0,
            "trusted_orientation_isolated_root_native_linknos": [100, 200] if isolated else [],
            "trusted_orientation_polygon_bearing_isolated_root_count": 2 if isolated else 0,
            "trusted_orientation_polygon_bearing_isolated_root_native_linknos": [100, 200] if isolated else [],
            "orientation_tolerance": 0.001,
        }
        return {
            "build_identity": {
                "processing_basin_id": self.basin_id,
                "fabric_name": "tdx_hydro",
                "fabric_version": self.fabric_version,
                "created_at": "2026-07-21T12:34:56+00:00",
                "adapter_version": "0.1.0",
                "dataset_root": str(output.resolve()),
            },
            "diagnostics": {
                "ingestion": {
                    "basins_clamp": {"altered_vertex_count": 0, "altered_native_ids": []},
                    "streamnet_clamp": {"altered_vertex_count": 0, "altered_native_ids": []},
                    "dscontarea": {
                        "source_unit": "km2",
                        "checked_polygon_bearing_link_count": 2,
                        "geodesic_upstream_area_sum_m2": ingestion_area,
                        "dscontarea_sum_raw": raw_area,
                        "m2_relative_error": 0.999999,
                        "km2_relative_error": 0.0,
                        "selected_relative_error": 0.0,
                    },
                },
                "streamnet": streamnet,
            },
        }

    def assert_no_temporary_entries(self, *parents: Path) -> None:
        for parent in parents:
            self.assertFalse(
                any(".tmp-" in entry.name for entry in parent.iterdir()), parent
            )

    def test_build_cli_reports_degenerate_reaches(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            source_dir.mkdir()
            basins, streamnet, _, _ = canonical_frames()
            streamnet.loc[streamnet["LINKNO"] == 200, "geometry"] = LineString(
                [(0.01, 0.00), (0.01, 0.00)]
            )
            basins_path, streamnet_path = write_pair(source_dir, basins, streamnet)
            output = root / "output"
            report = root / "report.json"
            with patch("build_adapter._utc_now", return_value=self.created_at):
                with self.assertLogs("tdx-hydro", level="WARNING") as captured:
                    status = main(
                        self.build_args(
                            basins_path, streamnet_path, output, report
                        )
                    )

            self.assertEqual(status, 0)
            report_data = json.loads(report.read_text())
            diagnostics = report_data["diagnostics"]["streamnet"]
            self.assertEqual(
                diagnostics,
                {
                    "polygon_bearing_link_count": 2,
                    "polygonless_dropped_reach_count": 0,
                    "degenerate_reach_count": 1,
                    "degenerate_reach_native_linknos": [200],
                    "degenerate_polygon_bearing_reach_count": 1,
                    "degenerate_polygon_bearing_reach_native_linknos": [200],
                    "degenerate_polygonless_reach_count": 0,
                    "degenerate_polygonless_reach_native_linknos": [],
                    "root_count": 1,
                    "contracted_edge_count": 0,
                    "contracted_root_count": 0,
                    "contracted_link_traversal_count": 0,
                    "endpoint_coincidence_proven_link_count": 1,
                    "predecessor_orientation_proven_root_count": 0,
                    "trusted_orientation_isolated_root_count": 0,
                    "trusted_orientation_isolated_root_native_linknos": [],
                    "trusted_orientation_polygon_bearing_isolated_root_count": 0,
                    "trusted_orientation_polygon_bearing_isolated_root_native_linknos": [],
                    "orientation_tolerance": 0.001,
                },
            )
            snap = pq.read_table(output / "aux/snap_stems.parquet")
            row_index = snap["unit_id"].to_pylist().index(710000200)
            self.assertEqual(
                snap["geometry"][row_index].as_py(),
                LineString([(0.01, 0.00), (0.01, 0.00)]).wkb,
            )
            self.assertEqual(
                snap["bbox"][row_index].as_py(),
                {
                    "xmin": 0.00989999994635582,
                    "ymin": -9.999999747378752e-05,
                    "xmax": 0.010099999606609344,
                    "ymax": 9.999999747378752e-05,
                },
            )
            messages = [record.getMessage() for record in captured.records]
            self.assertEqual(
                sum(
                    "diagnostic=degenerate_reach_count count=1 native_ids=(200,)"
                    in message
                    for message in messages
                ),
                1,
            )
            self.assertEqual(
                sum(
                    "diagnostic=degenerate_polygon_bearing_reach_count count=1 "
                    "native_ids=(200,)" in message
                    for message in messages
                ),
                1,
            )
            self.assertFalse(
                any(
                    "diagnostic=degenerate_polygonless_reach_count" in message
                    for message in messages
                )
            )

    def test_build_cli_writes_dataset_and_exact_external_report(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            source_dir.mkdir()
            basins_path, streamnet_path = write_pair(source_dir, *build_cli_frames())
            output = root / "dataset" / "output"
            report = root / "reports" / "report.json"
            with patch("build_adapter._utc_now", return_value=self.created_at):
                with self.assertLogs("tdx-hydro", level="WARNING") as captured:
                    status = main(self.build_args(basins_path, streamnet_path, output, report))
            self.assertEqual(status, 0)
            self.assertEqual(
                {path.name for path in output.iterdir()},
                {"catchments.parquet", "graph.parquet", "manifest.json", "aux"},
            )
            self.assertEqual(
                {
                    path.relative_to(output).as_posix()
                    for path in output.rglob("*")
                    if path.is_file()
                },
                {
                    "catchments.parquet",
                    "graph.parquet",
                    "manifest.json",
                    "aux/snap_stems.parquet",
                },
            )
            self.assertFalse(report.is_relative_to(output))
            manifest = json.loads((output / "manifest.json").read_text())
            self.assertEqual(manifest, {
                "format_version": "0.3.0", "fabric_name": "tdx_hydro",
                "fabric_version": self.fabric_version, "crs": "EPSG:4326",
                "has_up_area": True, "topology": "tree", "region": self.basin_id,
                "bbox": [0.0, 0.0, 0.019999999552965164, 0.009999999776482582],
                "unit_count": 2, "created_at": "2026-07-21T12:34:56+00:00",
                "adapter_version": "0.1.0",
                "auxiliary": [{
                    "schema": "hfx.aux.snap.v2",
                    "artifacts": {"snap": "aux/snap_stems.parquet"},
                    "metadata": {
                        "name": "stems",
                        "description": "Native TDX-Hydro LineString reaches for polygon-bearing level 0 drainage units.",
                        "references_levels": [0],
                        "weight_semantics": "Drainage-area weight equals inclusive DSContArea in km2; higher values indicate stronger drainage dominance.",
                    },
                }],
            })
            snap = pq.read_table(output / "aux/snap_stems.parquet")
            self.assertEqual(snap["id"].to_pylist(), [1, 2])
            self.assertEqual(snap["unit_id"].to_pylist(), [710000100, 710000200])
            self.assertNotIn(710000150, snap["unit_id"].to_pylist())
            self.assertEqual(
                snap["weight"].to_pylist(),
                [1.2309072017669678, 2.4618144035339355],
            )
            self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
            self.assertEqual(
                snap["geometry"].to_pylist(),
                [
                    LineString([(0.0, 0.0), (0.01, 0.0)]).wkb,
                    LineString([(0.01, 0.0), (0.02, 0.0)]).wkb,
                ],
            )
            self.assertEqual(json.loads(report.read_text()), self.expected_report(output))
            self.assert_no_temporary_entries(output.parent, report.parent)
            messages = [record.getMessage() for record in captured.records]
            self.assertTrue(any("diagnostic=contracted_edge_count count=1" in message for message in messages))
            self.assertTrue(any("diagnostic=contracted_link_traversal_count count=1" in message for message in messages))
            self.assertTrue(any("diagnostic=polygonless_dropped_reach_count count=1" in message for message in messages))
            self.assertEqual(
                sum(
                    "diagnostic=polygonless_dropped_reach_count count=1" in message
                    for message in messages
                ),
                1,
            )
            self.assertFalse(any("contracted_root_count" in message for message in messages))
            self.assertFalse(any("degenerate_" in message for message in messages))
            self.assertFalse(any("trusted_orientation" in message for message in messages))
            self.assertFalse(any("trusted" in message and "proven" in message for message in messages))

    def test_build_cli_rejects_report_inside_dataset_before_compiling(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            basins_path, streamnet_path = write_pair(root, *build_cli_frames())
            for report_suffix in (Path("report.json"), Path(".")):
                output = root / f"output-{report_suffix.name or 'equal'}"
                report = output / report_suffix if report_suffix != Path(".") else output
                with self.subTest(report=report):
                    with patch("build_adapter.compile_core_hfx") as compiler:
                        with self.assertRaisesRegex(ValueError, "report path must be outside dataset root"):
                            main(self.build_args(basins_path, streamnet_path, output, report))
                    compiler.assert_not_called()
                    self.assertFalse(output.exists())
                    self.assertFalse(report.exists())

    def test_build_cli_rejects_nonempty_output_without_mutation(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source"
            source.mkdir()
            basins_path, streamnet_path = write_pair(source, *build_cli_frames())
            output = root / "output"
            output.mkdir()
            sentinel = output / "sentinel.txt"
            sentinel.write_text("keep me\n")
            report = root / "report.json"
            with self.assertRaisesRegex(ValueError, "output dataset root exists and is not empty"):
                main(self.build_args(basins_path, streamnet_path, output, report))
            self.assertEqual(list(output.iterdir()), [sentinel])
            self.assertEqual(sentinel.read_text(), "keep me\n")
            self.assertFalse(report.exists())
            self.assert_no_temporary_entries(root)

    def test_build_cli_rolls_back_partial_compile_failure(self) -> None:
        failures = (
            ("_write_graph", "induced graph write failure"),
            ("_write_snap_stems", "induced snap write failure"),
        )
        for writer, message in failures:
            for precreate_output in (False, True):
                with self.subTest(writer=writer, precreate_output=precreate_output), TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    source = root / "source"
                    source.mkdir()
                    basins_path, streamnet_path = write_pair(source, *build_cli_frames())
                    output = root / "output"
                    report = root / "reports" / "report.json"
                    if precreate_output:
                        output.mkdir()
                    with patch(
                        f"build_adapter.{writer}", side_effect=RuntimeError(message)
                    ):
                        with self.assertRaisesRegex(RuntimeError, f"^{message}$"):
                            main(self.build_args(basins_path, streamnet_path, output, report))
                    if precreate_output:
                        self.assertTrue(output.is_dir())
                        self.assertEqual(list(output.iterdir()), [])
                    else:
                        self.assertFalse(output.exists())
                    self.assertFalse(report.exists())
                    self.assert_no_temporary_entries(root, report.parent)

    def test_build_cli_reports_trusted_isolated_roots(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source"
            source.mkdir()
            basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
            streamnet["DSLINKNO"] = [-1, -1]
            streamnet["DSContArea"] = [area_200_m2 / 1_000_000, area_100_m2 / 1_000_000]
            basins_path, streamnet_path = write_pair(source, basins, streamnet)
            output, report = root / "output", root / "report.json"
            with patch("build_adapter._utc_now", return_value=self.created_at):
                with self.assertLogs("tdx-hydro", level="WARNING") as captured:
                    self.assertEqual(main(self.build_args(basins_path, streamnet_path, output, report)), 0)
            self.assertEqual(json.loads(report.read_text()), self.expected_report(output, isolated=True))
            snap = pq.read_table(output / "aux/snap_stems.parquet")
            self.assertEqual(snap["id"].to_pylist(), [1, 2])
            self.assertEqual(snap["unit_id"].to_pylist(), [710000100, 710000200])
            self.assertEqual(
                snap["weight"].to_pylist(),
                [1.2309072017669678, 1.2309072017669678],
            )
            self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
            self.assertEqual(
                snap["geometry"].to_pylist(),
                [
                    LineString([(0.0, 0.0), (0.01, 0.0)]).wkb,
                    LineString([(0.01, 0.0), (0.02, 0.0)]).wkb,
                ],
            )
            messages = [record.getMessage() for record in captured.records]
            self.assertTrue(any("diagnostic=trusted_orientation_isolated_root_count count=2" in message and "native_ids=(100, 200)" in message for message in messages))
            self.assertTrue(any("diagnostic=trusted_orientation_polygon_bearing_isolated_root_count count=2" in message and "native_ids=(100, 200)" in message for message in messages))
            self.assertFalse(any("trusted" in message and "proven" in message for message in messages))

    def test_validate_cli_runs_explicit_binary_and_all_dataset_layer_checks(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source"
            source.mkdir()
            basins_path, streamnet_path = write_pair(source, *build_cli_frames())
            output, report = root / "output", root / "report.json"
            with patch("build_adapter._utc_now", return_value=self.created_at):
                main(self.build_args(basins_path, streamnet_path, output, report))
            capture = root / "argv.json"
            double = root / "hfx-double"
            double.write_text(
                f"#!{sys.executable}\nimport json, sys\nfrom pathlib import Path\n"
                f"Path({str(capture)!r}).write_text(json.dumps(sys.argv[1:]))\n"
            )
            double.chmod(0o755)
            real_validator = build_adapter.validate_geoparquet
            with patch("build_adapter.validate_geoparquet", wraps=real_validator) as validator:
                self.assertEqual(main(["validate", str(output), "--hfx-binary", str(double)]), 0)
            self.assertEqual(json.loads(capture.read_text()), [str(output), "--strict", "--sample-pct", "100", "--format", "text"])
            self.assertEqual(validator.call_args_list, [
                unittest.mock.call(str(output / "catchments.parquet"), target_version="1.1"),
                unittest.mock.call(str(output / "graph.parquet"), target_version="1.1"),
                unittest.mock.call(str(output / "aux/snap_stems.parquet"), target_version="1.1"),
            ])
            failing = root / "hfx-failing"
            failing.write_text(f"#!{sys.executable}\nimport sys\nsys.exit(7)\n")
            failing.chmod(0o755)
            with patch("build_adapter.validate_geoparquet") as validator:
                with self.assertRaisesRegex(RuntimeError, "return code 7"):
                    main(["validate", str(output), "--hfx-binary", str(failing)])
            validator.assert_not_called()


if __name__ == "__main__":
    unittest.main()
