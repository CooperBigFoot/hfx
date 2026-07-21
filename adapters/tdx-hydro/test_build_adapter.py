import math
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import geopandas as gpd
import pandas as pd
import pyogrio
from pyproj import Geod
from shapely import get_coordinates
from shapely.geometry import LineString, MultiLineString, Point, Polygon

from build_adapter import (
    COORDINATE_DOMAIN_TOLERANCE_DEGREES,
    LayerClampDiagnostics,
    StreamnetDiagnostics,
    StreamnetUnit,
    build_streamnet_model,
    global_linkno,
    load_header_crosswalk,
    load_tdx_geopackages,
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


class GeoPackageIngestionTests(unittest.TestCase):
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
        self.assertIn(
            "streamnet_model polygon_bearing_links=3 roots=2 contracted_edges=1 "
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

    def test_rejects_one_endpoint_matching_both_successor_endpoints(self) -> None:
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


if __name__ == "__main__":
    unittest.main()
