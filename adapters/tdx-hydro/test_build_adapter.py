import unittest

import pandas as pd
from shapely.geometry import LineString

from build_adapter import (
    StreamnetDiagnostics,
    StreamnetUnit,
    build_streamnet_model,
    global_linkno,
    load_header_crosswalk,
)


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
                orientation_checked_link_count=7,
                root_orientation_checked_count=2,
                orientation_tolerance=0.001,
            ),
        )
        self.assertIn(
            "streamnet_model polygon_bearing_links=3 roots=2 contracted_edges=1 "
            "contracted_roots=1 contracted_link_traversals=3 "
            "orientation_checked_links=7 root_orientation_checks=2 "
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
        self.assertEqual(model.diagnostics.orientation_checked_link_count, 2)
        self.assertEqual(model.diagnostics.root_orientation_checked_count, 1)
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

    def test_rejects_unprovable_isolated_root(self) -> None:
        basins = pd.DataFrame({"streamID": [100]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100],
                "DSLINKNO": [-1],
                "geometry": [LineString([(0.0, 0.0), (1.0, 0.0)])],
            }
        )

        with self.assertRaisesRegex(
            ValueError, r"(?=.*orientation)(?=.*root)(?=.*100)(?=.*predecessor)"
        ):
            build_streamnet_model(
                basins, streamnet, header_number=71, endpoint_tolerance=0.001
            )

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
