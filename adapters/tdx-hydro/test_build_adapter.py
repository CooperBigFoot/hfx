import json
import math
import os
import subprocess
import sys
import unittest
from contextlib import chdir
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

HFX_BINARY = Path(os.environ["HFX_BINARY"]).expanduser().resolve()
if not HFX_BINARY.is_absolute() or not HFX_BINARY.is_file():
    raise RuntimeError(f"HFX binary is not a regular absolute path: {HFX_BINARY}")

ABS_SCHEMA_PATH = (
    Path(__file__).resolve().parents[2] / "schemas" / "manifest.schema.json"
).resolve()
if not ABS_SCHEMA_PATH.is_absolute() or not ABS_SCHEMA_PATH.is_file():
    raise RuntimeError(f"manifest schema is not a regular absolute path: {ABS_SCHEMA_PATH}")

MERGE_RUN_A = [
    (710_000_101, -170.0, -80.0, [], 1.0, 1.0),
    (710_000_102, -80.0, -60.0, [710_000_101], 2.0, 3.0),
    (710_000_103, 0.0, 0.0, [710_000_102], 3.0, 6.0),
]
MERGE_RUN_B = [
    (720_000_201, -120.0, -80.0, [], 4.0, 4.0),
    (720_000_202, -20.0, -40.0, [720_000_201], 5.0, 9.0),
    (720_000_203, 0.0, 0.0, [720_000_202], 6.0, 15.0),
    (720_000_204, -170.0, -20.0, [720_000_203], 7.0, 22.0),
]
PARTIAL_SNAP_A = [
    (91, 710_000_101, -170.0, -80.0, 1.0),
    (7, 710_000_102, -80.0, -60.0, 3.0),
    (400, 710_000_103, 0.0, 0.0, 6.0),
]
PARTIAL_SNAP_B = [
    (800, 720_000_201, -120.0, -80.0, 4.0),
    (3, 720_000_202, -20.0, -40.0, 9.0),
    (200, 720_000_203, 0.0, 0.0, 15.0),
    (1, 720_000_204, -170.0, -20.0, 22.0),
]


def merge_fixture_polygon(x: float, y: float) -> Polygon:
    return Polygon(
        [
            (x - 0.1, y - 0.1),
            (x + 0.1, y - 0.1),
            (x + 0.1, y + 0.1),
            (x - 0.1, y + 0.1),
            (x - 0.1, y - 0.1),
        ]
    )


def merge_fixture_schemas() -> tuple[pa.Schema, pa.Schema]:
    catchment_schema = pa.schema(
        [
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
    ).with_metadata(
        build_adapter.build_geo_metadata(["Polygon", "MultiPolygon"])
    )
    graph_schema = pa.schema(
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
    return catchment_schema, graph_schema


def write_merge_fixture(
    root: Path,
    rows: list[tuple[int, float, float, list[int], float, float]],
    *,
    row_group_size: int = 2,
    catchment_schema: pa.Schema | None = None,
    graph_schema: pa.Schema | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    root.mkdir(parents=True)
    expected_catchment_schema, expected_graph_schema = merge_fixture_schemas()
    catchment_schema = catchment_schema or expected_catchment_schema
    graph_schema = graph_schema or expected_graph_schema
    catchment_rows: list[dict[str, object]] = []
    graph_rows: list[dict[str, object]] = []
    for unit_id, x, y, upstream_ids, area_km2, up_area_km2 in rows:
        polygon = merge_fixture_polygon(x, y)
        minx, miny, maxx, maxy = (
            np.float32(value) for value in polygon.bounds
        )
        bbox = {
            "xmin": minx,
            "ymin": miny,
            "xmax": maxx,
            "ymax": maxy,
        }
        catchment_rows.append(
            {
                "id": unit_id,
                "level": 0,
                "parent_id": None,
                "area_km2": np.float32(area_km2),
                "up_area_km2": np.float32(up_area_km2),
                "outlet_lon": x,
                "outlet_lat": y,
                "bbox": bbox,
                "geometry": polygon.wkb,
            }
        )
        graph_rows.append(
            {
                "id": unit_id,
                "level": 0,
                "upstream_ids": upstream_ids,
                "bbox_minx": minx,
                "bbox_miny": miny,
                "bbox_maxx": maxx,
                "bbox_maxy": maxy,
            }
        )
    pq.write_table(
        pa.Table.from_pylist(catchment_rows, schema=catchment_schema),
        root / "catchments.parquet",
        row_group_size=row_group_size,
        compression="snappy",
        write_statistics=True,
    )
    pq.write_table(
        pa.Table.from_pylist(graph_rows, schema=graph_schema),
        root / "graph.parquet",
        row_group_size=row_group_size,
        compression="snappy",
        write_statistics=True,
    )
    return catchment_rows, graph_rows


def merge_hilbert_keys(wkb_values: list[bytes]) -> list[int]:
    geometry = gpd.GeoSeries.from_wkb(wkb_values, crs=CRS)
    return [
        int(value)
        for value in geometry.centroid.hilbert_distance(
            total_bounds=[-180, -90, 180, 90]
        )
    ]


def rewrite_merge_rows(
    path: Path,
    rows: list[dict[str, object]],
    *,
    row_group_size: int = 2,
) -> None:
    schema = pq.ParquetFile(path).schema_arrow
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema),
        path,
        row_group_size=row_group_size,
        compression="snappy",
        write_statistics=True,
    )


def write_assembly_fixture(
    root: Path,
    region: str,
    rows: list[tuple[int, float, float, list[int], float, float]],
    snap_rows: list[tuple[int, int, float, float, float]],
) -> None:
    catchments, _ = write_merge_fixture(root, rows)
    snap_schema = assembly_snap_schema()
    authored_snap_rows = []
    for source_id, unit_id, x, y, weight in snap_rows:
        geometry = LineString(
            [(x - 0.1, y - 0.1), (x + 0.1, y + 0.1)]
        )
        minx, miny, maxx, maxy = (
            np.float32(value) for value in geometry.bounds
        )
        authored_snap_rows.append(
            {
                "id": source_id,
                "unit_id": unit_id,
                "weight": np.float32(weight),
                "stem_role": None,
                "bbox": {
                    "xmin": minx,
                    "ymin": miny,
                    "xmax": maxx,
                    "ymax": maxy,
                },
                "geometry": geometry.wkb,
            }
        )
    aux = root / "aux"
    aux.mkdir()
    pq.write_table(
        pa.Table.from_pylist(authored_snap_rows, schema=snap_schema),
        aux / "snap_stems.parquet",
        row_group_size=2,
        compression="snappy",
        write_statistics=True,
    )
    bounds = [row["bbox"] for row in catchments]
    manifest = {
        "adapter_version": "0.1.0",
        "auxiliary": [
            {
                "schema": "hfx.aux.snap.v2",
                "artifacts": {"snap": "aux/snap_stems.parquet"},
                "metadata": {
                    "name": "stems",
                    "description": (
                        "Native TDX-Hydro LineString reaches for polygon-bearing "
                        "level 0 drainage units."
                    ),
                    "references_levels": [0],
                    "weight_semantics": (
                        "Drainage-area weight equals inclusive DSContArea in km2; "
                        "higher values indicate stronger drainage dominance."
                    ),
                },
            }
        ],
        "bbox": [
            float(np.float32(min(value["xmin"] for value in bounds))),
            float(np.float32(min(value["ymin"] for value in bounds))),
            float(np.float32(max(value["xmax"] for value in bounds))),
            float(np.float32(max(value["ymax"] for value in bounds))),
        ],
        "created_at": "2026-07-21T12:34:56+00:00",
        "crs": "EPSG:4326",
        "fabric_name": "tdx_hydro",
        "fabric_version": "synthetic-2026.07",
        "format_version": "0.3.0",
        "has_up_area": True,
        "region": region,
        "topology": "tree",
        "unit_count": len(rows),
    }
    (root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )


def assembly_snap_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox", build_adapter.bbox_struct_type(), nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(build_adapter.build_geo_metadata(["LineString"]))


def rewrite_snap_rows(
    input_root: Path,
    rows: list[dict[str, object]],
    *,
    schema: pa.Schema | None = None,
) -> None:
    pq.write_table(
        pa.Table.from_pylist(rows, schema=schema or assembly_snap_schema()),
        input_root / "aux" / "snap_stems.parquet",
        row_group_size=2,
        compression="snappy",
        write_statistics=True,
    )


def validate_assembled_manifest(path: Path) -> None:
    subprocess.run(
        [
            "uv",
            "run",
            "--with",
            "check-jsonschema",
            "check-jsonschema",
            "--schemafile",
            str(ABS_SCHEMA_PATH),
            str(path.resolve()),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def validate_with_release_hfx(dataset: Path) -> None:
    completed = subprocess.run(
        [
            str(HFX_BINARY),
            str(dataset.resolve()),
            "--strict",
            "--sample-pct",
            "100",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    message = f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    if completed.returncode != 0:
        raise AssertionError(message)
    if "0 error(s), 0 warning(s), 0 info(s)" not in completed.stdout:
        raise AssertionError(message)
    if "Result: VALID" not in completed.stdout:
        raise AssertionError(message)


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


def planetary_order_frames() -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    polygon_100 = Polygon([
        (-170.1, -80.1),
        (-169.9, -80.1),
        (-169.9, -79.9),
        (-170.1, -79.9),
        (-170.1, -80.1),
    ])
    polygon_200 = Polygon([
        (-120.1, -80.1),
        (-119.9, -80.1),
        (-119.9, -79.9),
        (-120.1, -79.9),
        (-120.1, -80.1),
    ])
    polygon_300 = Polygon([
        (-170.1, -20.1),
        (-169.9, -20.1),
        (-169.9, -19.9),
        (-170.1, -19.9),
        (-170.1, -20.1),
    ])
    polygons = [polygon_100, polygon_200, polygon_300]
    geod = Geod(ellps="WGS84")
    areas_km2 = [
        abs(geod.geometry_area_perimeter(polygon)[0]) / 1_000_000
        for polygon in polygons
    ]
    basins = gpd.GeoDataFrame(
        {"streamID": [100, 200, 300]},
        geometry=polygons,
        crs="EPSG:4326",
    )
    streamnet = gpd.GeoDataFrame(
        {
            "LINKNO": [100, 200, 300],
            "DSLINKNO": [-1, -1, -1],
            "DSContArea": areas_km2,
        },
        geometry=[
            LineString([(-170.05, -80.0), (-169.95, -80.0)]),
            LineString([(-120.05, -80.0), (-119.95, -80.0)]),
            LineString([(-170.05, -20.0), (-169.95, -20.0)]),
        ],
        crs="EPSG:4326",
    )
    return basins, streamnet


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
        self.assertEqual(diagnostics.signed_aggregate_relative_divergence, 0.0)
        self.assertEqual(diagnostics.absolute_aggregate_relative_divergence, 0.0)
        self.assertEqual(diagnostics.max_absolute_relative_divergence, 0.0)

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
        self.assertEqual(diagnostics.signed_aggregate_relative_divergence, 0.0)
        self.assertEqual(diagnostics.absolute_aggregate_relative_divergence, 0.0)
        self.assertEqual(diagnostics.max_absolute_relative_divergence, 0.0)
        self.assertEqual(
            source.streamnet["DSContArea_km2"].tolist(),
            [(area_100_m2 + area_200_m2) / 1_000_000, area_100_m2 / 1_000_000],
        )
        self.assertEqual(
            diagnostics.geodesic_upstream_area_sum_m2,
            math.fsum([expected_200_m2, expected_100_m2]),
        )

    def test_accepts_decisive_m2_unit_with_raster_vector_divergence(self) -> None:
        basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
        streamnet["DSContArea"] = [
            1.13 * (area_100_m2 + area_200_m2),
            1.13 * area_100_m2,
        ]
        with TemporaryDirectory() as temp_dir:
            source = load_tdx_geopackages(
                *write_pair(Path(temp_dir), basins, streamnet)
            )

        diagnostics = source.diagnostics.dscontarea
        self.assertEqual(diagnostics.source_unit, "m2")
        self.assertEqual(diagnostics.checked_polygon_bearing_link_count, 2)
        self.assertEqual(
            diagnostics.geodesic_upstream_area_sum_m2,
            3692721.6149797607,
        )
        self.assertEqual(diagnostics.dscontarea_sum_raw, 4172775.424927129)
        self.assertEqual(diagnostics.m2_relative_error, 0.1299999999999998)
        self.assertEqual(diagnostics.km2_relative_error, 1129998.9999999998)
        self.assertEqual(diagnostics.selected_relative_error, 0.1299999999999998)
        self.assertAlmostEqual(
            diagnostics.signed_aggregate_relative_divergence,
            0.1299999999999998,
            places=12,
        )
        self.assertAlmostEqual(
            diagnostics.absolute_aggregate_relative_divergence,
            0.1299999999999998,
            places=12,
        )
        self.assertAlmostEqual(
            diagnostics.max_absolute_relative_divergence,
            0.1299999999999998,
            places=12,
        )
        self.assertEqual(
            source.streamnet["DSContArea"].tolist(),
            [2781850.2832847526, 1390925.1416423763],
        )
        self.assertEqual(
            source.streamnet["DSContArea_km2"].tolist(),
            [2.7818502832847525, 1.3909251416423762],
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
        self.assertLess(
            abs(source.diagnostics.dscontarea.signed_aggregate_relative_divergence),
            1e-9,
        )
        self.assertLess(
            source.diagnostics.dscontarea.absolute_aggregate_relative_divergence,
            1e-9,
        )
        self.assertLess(
            source.diagnostics.dscontarea.max_absolute_relative_divergence,
            1e-9,
        )

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
        self.assertAlmostEqual(
            source.diagnostics.dscontarea.signed_aggregate_relative_divergence,
            0.0,
        )
        self.assertAlmostEqual(
            source.diagnostics.dscontarea.absolute_aggregate_relative_divergence,
            0.0,
        )
        self.assertAlmostEqual(
            source.diagnostics.dscontarea.max_absolute_relative_divergence,
            0.0,
        )

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

    def test_rejects_non_decisive_dscontarea_unit_candidates(self) -> None:
        basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
        tie_factor = 2.0 / 1_000_001.0
        streamnet["DSContArea"] = [
            tie_factor * (area_100_m2 + area_200_m2),
            tie_factor * area_100_m2,
        ]
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(
                ValueError,
                r"(?=.*DSContArea)(?=.*unit candidates are not decisive)"
                r"(?=.*unit_decisiveness_ratio)(?=.*minimum_ratio=1000\.0)",
            ):
                load_tdx_geopackages(*write_pair(Path(temp_dir), basins, streamnet))

    def test_rejects_gross_dscontarea_fabric_divergence(self) -> None:
        basins, streamnet, area_100_m2, area_200_m2 = canonical_frames()
        streamnet["DSContArea"] = [
            2.1 * (area_100_m2 + area_200_m2),
            2.1 * area_100_m2,
        ]
        with TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(
                ValueError,
                r"(?=.*DSContArea fabric divergence sanity check failed)"
                r"(?=.*source_unit='m2')(?=.*selected_relative_error=1\.1)"
                r"(?=.*sanity_ceiling=1\.0)",
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
                short_successor_resolved_reach_count=0,
                short_successor_resolved_reach_native_linknos=(),
                reach_side_near_degenerate_resolved_reach_count=0,
                reach_side_near_degenerate_resolved_reach_native_linknos=(),
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
                short_successor_resolved_reach_count=0,
                short_successor_resolved_reach_native_linknos=(),
                reach_side_near_degenerate_resolved_reach_count=0,
                reach_side_near_degenerate_resolved_reach_native_linknos=(),
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
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_native_linknos, ()
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
            (),
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
                short_successor_resolved_reach_count=0,
                short_successor_resolved_reach_native_linknos=(),
                reach_side_near_degenerate_resolved_reach_count=0,
                reach_side_near_degenerate_resolved_reach_native_linknos=(),
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
            "degenerate_polygonless_reach_native_linknos=() "
            "short_successor_resolved_reaches=0 "
            "short_successor_resolved_reach_native_linknos=() "
            "reach_side_near_degenerate_resolved_reaches=0 "
            "reach_side_near_degenerate_resolved_reach_native_linknos=() "
            "roots=2 contracted_edges=1 "
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
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_native_linknos, ()
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
            (),
        )
        self.assertEqual(model.diagnostics.trusted_orientation_isolated_root_count, 0)
        self.assertEqual(model.diagnostics.trusted_orientation_polygon_bearing_isolated_root_count, 0)
        self.assertEqual(model.diagnostics.orientation_tolerance, 0.001)

    def test_proves_reach_when_one_current_endpoint_matches_both_short_successor_endpoints(
        self,
    ) -> None:
        basins = pd.DataFrame({"streamID": [100, 200, 300]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200, 300],
                "DSLINKNO": [200, 300, -1],
                "geometry": [
                    LineString([(0.0, 0.0), (1.0, 0.0)]),
                    LineString([(0.9992, 0.0), (1.0008, 0.0)]),
                    LineString([(1.0008, 0.0), (2.0, 0.0)]),
                ],
            }
        )
        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )

        self.assertEqual(
            [(unit.linkno, unit.outlet_lon, unit.outlet_lat) for unit in model.units],
            [
                (100, 1.0, 0.0),
                (200, 1.0008, 0.0),
                (300, 2.0, 0.0),
            ],
        )
        self.assertEqual(
            model.diagnostics,
            StreamnetDiagnostics(
                polygon_bearing_link_count=3,
                polygonless_dropped_reach_count=0,
                degenerate_reach_count=0,
                degenerate_reach_native_linknos=(),
                degenerate_polygon_bearing_reach_count=0,
                degenerate_polygon_bearing_reach_native_linknos=(),
                degenerate_polygonless_reach_count=0,
                degenerate_polygonless_reach_native_linknos=(),
                short_successor_resolved_reach_count=1,
                short_successor_resolved_reach_native_linknos=(100,),
                reach_side_near_degenerate_resolved_reach_count=0,
                reach_side_near_degenerate_resolved_reach_native_linknos=(),
                root_count=1,
                contracted_edge_count=0,
                contracted_root_count=0,
                contracted_link_traversal_count=0,
                endpoint_coincidence_proven_link_count=2,
                predecessor_orientation_proven_root_count=1,
                trusted_orientation_isolated_root_count=0,
                trusted_orientation_isolated_root_native_linknos=(),
                trusted_orientation_polygon_bearing_isolated_root_count=0,
                trusted_orientation_polygon_bearing_isolated_root_native_linknos=(),
                orientation_tolerance=0.001,
            ),
        )

    def test_uses_native_order_for_reach_side_near_degenerate_reach(self) -> None:
        basins = pd.DataFrame({"streamID": [100, 200]})
        streamnet = pd.DataFrame(
            {
                "LINKNO": [100, 200],
                "DSLINKNO": [200, -1],
                "geometry": [
                    LineString([(0.9992, 0.0), (1.0008, 0.0)]),
                    LineString([(0.9992, 0.0), (1.0008, 0.0)]),
                ],
            }
        )
        model = build_streamnet_model(
            basins, streamnet, header_number=71, endpoint_tolerance=0.001
        )

        self.assertEqual(
            [(unit.linkno, unit.outlet_lon, unit.outlet_lat) for unit in model.units],
            [(100, 1.0008, 0.0), (200, 0.9992, 0.0)],
        )
        self.assertEqual(
            model.diagnostics,
            StreamnetDiagnostics(
                polygon_bearing_link_count=2,
                polygonless_dropped_reach_count=0,
                degenerate_reach_count=0,
                degenerate_reach_native_linknos=(),
                degenerate_polygon_bearing_reach_count=0,
                degenerate_polygon_bearing_reach_native_linknos=(),
                degenerate_polygonless_reach_count=0,
                degenerate_polygonless_reach_native_linknos=(),
                short_successor_resolved_reach_count=0,
                short_successor_resolved_reach_native_linknos=(),
                reach_side_near_degenerate_resolved_reach_count=1,
                reach_side_near_degenerate_resolved_reach_native_linknos=(100,),
                root_count=1,
                contracted_edge_count=0,
                contracted_root_count=0,
                contracted_link_traversal_count=0,
                endpoint_coincidence_proven_link_count=0,
                predecessor_orientation_proven_root_count=1,
                trusted_orientation_isolated_root_count=0,
                trusted_orientation_isolated_root_native_linknos=(),
                trusted_orientation_polygon_bearing_isolated_root_count=0,
                trusted_orientation_polygon_bearing_isolated_root_native_linknos=(),
                orientation_tolerance=0.001,
            ),
        )


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
            ValueError,
            r"(?=.*orientation)(?=.*100)(?=.*200)(?=.*reach-side)(?=.*ambiguous)",
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
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_native_linknos, ()
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
            (),
        )
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
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.short_successor_resolved_reach_native_linknos, ()
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_count, 0
        )
        self.assertEqual(
            model.diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
            (),
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


class AssemblyTests(unittest.TestCase):
    def test_partial_coverage_assembly(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            write_assembly_fixture(
                first, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            write_assembly_fixture(
                second, "7020014250", MERGE_RUN_B, PARTIAL_SNAP_B
            )

            result = build_adapter.assemble_hfx(
                [first, second],
                root / "assembled",
                created_at=datetime(2026, 7, 23, 12, 34, 56, tzinfo=timezone.utc),
                input_batch_size=2,
                row_group_min=2,
                row_group_max=3,
            )
            manifest = json.loads(result.manifest_path.read_text())
            self.assertEqual(manifest["region"], "tdx-hydro-partial-afd4ffb0b736")
            self.assertEqual(
                manifest["bbox"],
                [
                    -170.10000610351562,
                    -80.0999984741211,
                    0.10000000149011612,
                    0.10000000149011612,
                ],
            )
            self.assertEqual(manifest["unit_count"], 7)
            self.assertEqual(
                manifest["created_at"], "2026-07-23T12:34:56+00:00"
            )
            catchment_ids = pq.read_table(
                result.catchments_path, columns=["id"]
            )["id"].to_pylist()
            graph = pq.read_table(result.graph_path).to_pydict()
            self.assertEqual(catchment_ids, graph["id"])
            self.assertEqual(
                graph["upstream_ids"],
                [
                    [],
                    [],
                    [710_000_101],
                    [720_000_201],
                    [710_000_102],
                    [720_000_202],
                    [720_000_203],
                ],
            )
            snap = pq.read_table(result.snap_path).to_pydict()
            self.assertEqual(snap["id"], list(range(1, 8)))
            self.assertEqual(
                snap["unit_id"],
                [
                    710_000_101,
                    720_000_201,
                    710_000_102,
                    720_000_202,
                    710_000_103,
                    720_000_203,
                    720_000_204,
                ],
            )
            self.assertEqual(set(snap["unit_id"]), set(catchment_ids))
            self.assertTrue(result.notice_path.read_bytes() == (Path(__file__).parent / "NOTICE").read_bytes())
            self.assertTrue(result.citation_path.read_bytes() == (Path(__file__).parent / "CITATION.txt").read_bytes())
            for path in (result.notice_path, result.citation_path):
                text = path.read_text()
                self.assertIn("TDX-Hydro", text)
                self.assertIn("National Geospatial-Intelligence Agency", text)
            self.assertTrue(
                pq.ParquetFile(result.snap_path).schema_arrow.equals(
                    build_adapter._snap_merge_schema(), check_metadata=True
                )
            )
            self.assertTrue(validate_geoparquet(str(result.snap_path), target_version="1.1").is_valid)
            validate_assembled_manifest(result.manifest_path)
            reversed_result = build_adapter.assemble_hfx(
                [second, first],
                root / "reversed",
                created_at=datetime(2026, 7, 23, 12, 34, 56, tzinfo=timezone.utc),
                input_batch_size=2,
                row_group_min=2,
                row_group_max=3,
            )
            validate_assembled_manifest(reversed_result.manifest_path)
            for name in (
                "catchments.parquet",
                "graph.parquet",
                "aux/snap_stems.parquet",
                "manifest.json",
            ):
                self.assertEqual(
                    (root / "assembled" / name).read_bytes(),
                    (root / "reversed" / name).read_bytes(),
                )

    def test_complete_coverage_assembly(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            inputs = []
            for index, (region, header) in enumerate(
                sorted(load_header_crosswalk().items()), start=1
            ):
                dataset = root / region
                unit_id = header * 10_000_000 + 1
                write_assembly_fixture(
                    dataset,
                    region,
                    [(unit_id, 0.0, 0.0, [], 1.0, 1.0)],
                    [(9_000 + index, unit_id, 0.0, 0.0, 1.0)],
                )
                inputs.append(dataset)
            result = build_adapter.assemble_hfx(
                inputs,
                root / "assembled",
                created_at=datetime(2026, 7, 23, 12, 34, 56, tzinfo=timezone.utc),
            )
            manifest = json.loads(result.manifest_path.read_text())
            self.assertNotIn("region", manifest)
            self.assertEqual(manifest["bbox"], [-180, -90, 180, 90])
            self.assertEqual(manifest["unit_count"], 62)
            self.assertEqual(
                pq.read_table(result.snap_path, columns=["id"])["id"].to_pylist(),
                list(range(1, 63)),
            )
            self.assertLessEqual(
                result.snap_metrics.peak_buffered_rows,
                result.snap_metrics.buffer_row_ceiling,
            )
            self.assertEqual(result.snap_metrics.emitted_rows, 62)
            validate_assembled_manifest(result.manifest_path)
            validate_with_release_hfx(root / "assembled")

    def test_rejects_dangling_duplicate_and_count_mismatches(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "dataset"
            write_assembly_fixture(
                dataset, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            snap_path = dataset / "aux" / "snap_stems.parquet"
            table = pq.read_table(snap_path)
            rows = table.to_pylist()
            rows[0]["unit_id"] = 999
            pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), snap_path)
            with self.assertRaisesRegex(ValueError, "dangling snap unit_id"):
                build_adapter.assemble_hfx(
                    [dataset], root / "out", created_at=datetime.now(timezone.utc)
                )

            write_assembly_fixture(
                root / "duplicate", "7020014250", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            duplicate_path = root / "duplicate" / "aux" / "snap_stems.parquet"
            duplicate_table = pq.read_table(duplicate_path)
            duplicate_rows = duplicate_table.to_pylist()
            duplicate_rows[1]["unit_id"] = duplicate_rows[0]["unit_id"]
            pq.write_table(
                pa.Table.from_pylist(duplicate_rows, schema=duplicate_table.schema),
                duplicate_path,
            )
            with self.assertRaisesRegex(ValueError, "duplicate snap unit_id"):
                build_adapter.assemble_hfx(
                    [root / "duplicate"],
                    root / "out2",
                    created_at=datetime.now(timezone.utc),
                )

            count_root = root / "count"
            write_assembly_fixture(
                count_root, "7020021430", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            manifest_path = count_root / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["unit_count"] = 2
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "row counts differ"):
                build_adapter.assemble_hfx(
                    [count_root],
                    root / "out3",
                    created_at=datetime.now(timezone.utc),
                )

    def test_rejects_manifest_identity_regions_bbox_and_naive_complete(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            write_assembly_fixture(
                first, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            manifest_path = first / "manifest.json"
            original = json.loads(manifest_path.read_text())
            mutations = [
                ("fabric_name", "other", "fabric_name"),
                ("fabric_version", "", "fabric_version"),
                ("region", "999", "region"),
                ("bbox", [float("inf"), 0.0, 1.0, 1.0], "bbox"),
                ("auxiliary", [], "auxiliary"),
            ]
            for key, value, message in mutations:
                manifest = dict(original)
                manifest[key] = value
                manifest_path.write_text(json.dumps(manifest))
                with self.assertRaisesRegex(ValueError, message):
                    build_adapter.assemble_hfx(
                        [first],
                        root / f"out-{key}",
                        created_at=datetime.now(timezone.utc),
                    )
            manifest_path.write_text(json.dumps(original))
            with self.assertRaisesRegex(ValueError, "unique"):
                build_adapter.assemble_hfx(
                    [first, first],
                    root / "duplicate-root",
                    created_at=datetime.now(timezone.utc),
                )
            with self.assertRaisesRegex(ValueError, "timezone-aware"):
                build_adapter.assemble_hfx(
                    [first], root / "naive", created_at=datetime.now()
                )

    def test_attribution_and_manifest_publication_are_atomic(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "dataset"
            write_assembly_fixture(
                dataset, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            duplicate = root / "duplicate"
            write_assembly_fixture(
                duplicate, "7020014250", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            output = root / "merge-failure"
            output.mkdir()
            with self.assertRaisesRegex(
                ValueError, "incompatible duplicate merge key"
            ):
                build_adapter.assemble_hfx(
                    [dataset, duplicate],
                    output,
                    created_at=datetime.now(timezone.utc),
                    input_batch_size=1,
                    row_group_min=1,
                    row_group_max=1,
                )
            self.assertEqual(list(output.iterdir()), [])
            BuildCliTests.assert_no_temporary_entries(self, output.parent)

            real_replace = build_adapter.os.replace

            def fail_citation(source: Path, destination: Path) -> None:
                if Path(destination).name == "CITATION.txt":
                    raise OSError("injected citation replacement failure")
                real_replace(source, destination)

            output = root / "attribution-failure"
            output.mkdir()
            with patch.object(
                build_adapter.os, "replace", side_effect=fail_citation
            ):
                with self.assertRaisesRegex(OSError, "injected citation"):
                    build_adapter.assemble_hfx(
                        [dataset],
                        output,
                        created_at=datetime.now(timezone.utc),
                    )
            self.assertEqual(list(output.iterdir()), [])
            BuildCliTests.assert_no_temporary_entries(self, output.parent)

            def fail_manifest(source: Path, destination: Path) -> None:
                if Path(destination).name == "manifest.json":
                    raise OSError("injected manifest replacement failure")
                real_replace(source, destination)

            output = root / "manifest-failure"
            output.mkdir()
            with patch.object(
                build_adapter.os, "replace", side_effect=fail_manifest
            ):
                with self.assertRaisesRegex(OSError, "injected manifest"):
                    build_adapter.assemble_hfx(
                        [dataset],
                        output,
                        created_at=datetime.now(timezone.utc),
                    )
            self.assertEqual(list(output.iterdir()), [])
            BuildCliTests.assert_no_temporary_entries(self, output.parent)

    def test_rejects_missing_empty_and_empty_geometry_snap_inputs(self) -> None:
        cases = ("missing", "empty", "empty-geometry")
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for case in cases:
                with self.subTest(case=case):
                    input_root = root / f"input-{case}"
                    write_assembly_fixture(
                        input_root,
                        "7020000010",
                        MERGE_RUN_A,
                        PARTIAL_SNAP_A,
                    )
                    snap_path = input_root / "aux" / "snap_stems.parquet"
                    if case == "missing":
                        snap_path.unlink()
                        message = (
                            f"{input_root.resolve()}: required regular "
                            "aux/snap_stems.parquet is missing"
                        )
                    elif case == "empty":
                        rewrite_snap_rows(input_root, [])
                        message = (
                            f"{input_root.resolve()}: input snap file must be nonempty"
                        )
                    else:
                        rows = pq.read_table(snap_path).to_pylist()
                        rows[0]["geometry"] = LineString().wkb
                        rewrite_snap_rows(input_root, rows)
                        message = (
                            f"{input_root.resolve()}: invalid snap geometry at row 0"
                        )
                    with self.assertRaises(ValueError) as raised:
                        build_adapter.assemble_hfx(
                            [input_root],
                            root / f"output-{case}",
                            created_at=datetime.now(timezone.utc),
                        )
                    self.assertIn(message, str(raised.exception))
                    if case == "empty-geometry":
                        self.assertNotIn(
                            "Hilbert distance cannot be computed",
                            str(raised.exception),
                        )

    def test_rejects_missing_attribution_phrases(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = root / "dataset"
            write_assembly_fixture(
                dataset, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            original_read_bytes = Path.read_bytes
            notice = (Path(__file__).resolve().parent / "NOTICE").resolve()

            for phrase in (
                "TDX-Hydro",
                "National Geospatial-Intelligence Agency",
            ):
                def altered_read_bytes(path: Path, removed: str = phrase) -> bytes:
                    content = original_read_bytes(path)
                    if path.resolve() == notice:
                        return content.replace(removed.encode(), b"removed")
                    return content

                with patch.object(Path, "read_bytes", altered_read_bytes):
                    with self.assertRaisesRegex(ValueError, "missing required phrase"):
                        build_adapter.assemble_hfx(
                            [dataset],
                            root / f"missing-{phrase.split()[0]}",
                            created_at=datetime.now(timezone.utc),
                        )


class AssemblyCliTests(unittest.TestCase):
    def assert_rejected(
        self,
        input_roots: list[Path],
        output: Path,
        message: str,
    ) -> None:
        output.mkdir()
        arguments = ["assemble"]
        for input_root in input_roots:
            arguments.extend(["--input", str(input_root)])
        arguments.extend(["--out", str(output)])
        with self.assertRaises(ValueError) as raised:
            main(arguments)
        self.assertIn(message, str(raised.exception))
        self.assertEqual(list(output.iterdir()), [])
        BuildCliTests.assert_no_temporary_entries(self, output.parent)

    def test_assemble_cli_publishes_and_validates_partial_dataset(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_assembly_fixture(
                root / "first", "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            write_assembly_fixture(
                root / "second", "7020014250", MERGE_RUN_B, PARTIAL_SNAP_B
            )
            with chdir(root), patch.object(
                build_adapter,
                "_utc_now",
                return_value=datetime(
                    2026, 7, 23, 12, 34, 56, tzinfo=timezone.utc
                ),
            ):
                return_code = main(
                    [
                        "assemble",
                        "--input",
                        "first",
                        "--input",
                        "second",
                        "--out",
                        "assembled",
                    ]
                )
            dataset = root / "assembled"
            self.assertEqual(return_code, 0)
            expected_ids = [
                710_000_101,
                720_000_201,
                710_000_102,
                720_000_202,
                710_000_103,
                720_000_203,
                720_000_204,
            ]
            catchments = pq.read_table(dataset / "catchments.parquet")
            graph = pq.read_table(dataset / "graph.parquet")
            snap = pq.read_table(dataset / "aux" / "snap_stems.parquet")
            self.assertEqual(catchments["id"].to_pylist(), expected_ids)
            self.assertEqual(graph["id"].to_pylist(), expected_ids)
            self.assertEqual(
                graph["upstream_ids"].to_pylist(),
                [
                    [],
                    [],
                    [710_000_101],
                    [720_000_201],
                    [710_000_102],
                    [720_000_202],
                    [720_000_203],
                ],
            )
            self.assertEqual(snap["id"].to_pylist(), list(range(1, 8)))
            self.assertEqual(snap["unit_id"].to_pylist(), expected_ids)
            self.assertEqual(set(snap["unit_id"].to_pylist()), set(expected_ids))
            manifest = json.loads((dataset / "manifest.json").read_text())
            self.assertEqual(
                manifest,
                {
                    "adapter_version": "0.1.0",
                    "auxiliary": [
                        {
                            "schema": "hfx.aux.snap.v2",
                            "artifacts": {"snap": "aux/snap_stems.parquet"},
                            "metadata": {
                                "name": "stems",
                                "description": (
                                    "Native TDX-Hydro LineString reaches for "
                                    "polygon-bearing level 0 drainage units."
                                ),
                                "references_levels": [0],
                                "weight_semantics": (
                                    "Drainage-area weight equals inclusive "
                                    "DSContArea in km2; higher values indicate "
                                    "stronger drainage dominance."
                                ),
                            },
                        }
                    ],
                    "bbox": [
                        -170.10000610351562,
                        -80.0999984741211,
                        0.10000000149011612,
                        0.10000000149011612,
                    ],
                    "created_at": "2026-07-23T12:34:56+00:00",
                    "crs": "EPSG:4326",
                    "fabric_name": "tdx_hydro",
                    "fabric_version": "synthetic-2026.07",
                    "format_version": "0.3.0",
                    "has_up_area": True,
                    "region": "tdx-hydro-partial-afd4ffb0b736",
                    "topology": "tree",
                    "unit_count": 7,
                },
            )
            source_root = Path(__file__).parent
            for name in ("NOTICE", "CITATION.txt"):
                self.assertEqual(
                    (dataset / name).read_bytes(),
                    (source_root / name).read_bytes(),
                )
                text = (dataset / name).read_text()
                self.assertIn("TDX-Hydro", text)
                self.assertIn("National Geospatial-Intelligence Agency", text)
            catchment_schema, graph_write_schema = merge_fixture_schemas()
            graph_read_schema = pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("level", pa.int16(), nullable=False),
                    pa.field(
                        "upstream_ids",
                        pa.list_(
                            pa.field("element", pa.int64(), nullable=True)
                        ),
                        nullable=False,
                    ),
                    pa.field("bbox_minx", pa.float32(), nullable=False),
                    pa.field("bbox_miny", pa.float32(), nullable=False),
                    pa.field("bbox_maxx", pa.float32(), nullable=False),
                    pa.field("bbox_maxy", pa.float32(), nullable=False),
                ]
            )
            self.assertTrue(
                catchments.schema.equals(catchment_schema, check_metadata=True)
            )
            self.assertTrue(
                snap.schema.equals(assembly_snap_schema(), check_metadata=True)
            )
            self.assertTrue(
                graph.schema.equals(graph_read_schema, check_metadata=True)
            )
            self.assertIsNone(graph.schema.metadata)
            self.assertIsNone(graph_write_schema.metadata)
            self.assertTrue(
                validate_geoparquet(
                    str(dataset / "catchments.parquet"), target_version="1.1"
                ).is_valid
            )
            self.assertTrue(
                validate_geoparquet(
                    str(dataset / "aux" / "snap_stems.parquet"),
                    target_version="1.1",
                ).is_valid
            )
            validate_assembled_manifest(dataset / "manifest.json")
            validate_with_release_hfx(dataset)

    def test_assemble_cli_rejects_invalid_inputs(self) -> None:
        cases = [
            ("incompatible snap schema", "incompatible snap schema"),
            ("non-monotonic snap input", "non-monotonic snap input"),
            ("empty snap input file", "input snap file must be nonempty"),
            ("invalid weight", "invalid snap weight at row 0"),
            ("non-LineString geometry", "invalid snap geometry at row 0"),
            ("empty geometry", "invalid snap geometry at row 0"),
            ("invalid stem_role", "invalid snap stem_role at row 0"),
            ("invalid bbox", "invalid snap bbox at row 0"),
        ]
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            for index, (case, suffix) in enumerate(cases):
                with self.subTest(case=case):
                    input_root = root / f"input-{index}"
                    write_assembly_fixture(
                        input_root,
                        "7020000010",
                        MERGE_RUN_A,
                        PARTIAL_SNAP_A,
                    )
                    snap_path = input_root / "aux" / "snap_stems.parquet"
                    rows = pq.read_table(snap_path).to_pylist()
                    schema = assembly_snap_schema()
                    if case == "incompatible snap schema":
                        schema = pa.schema(list(schema))
                    elif case == "non-monotonic snap input":
                        rows = [rows[1], rows[0], rows[2]]
                    elif case == "empty snap input file":
                        rows = []
                    elif case == "invalid weight":
                        rows[0]["weight"] = np.float32(-1.0)
                    elif case == "non-LineString geometry":
                        rows[0]["geometry"] = Point(0.0, 0.0).wkb
                    elif case == "empty geometry":
                        rows[0]["geometry"] = LineString().wkb
                    elif case == "invalid stem_role":
                        rows[0]["stem_role"] = "invalid"
                    else:
                        rows[0]["bbox"] = {
                            "xmin": np.float32(1.0),
                            "ymin": np.float32(0.0),
                            "xmax": np.float32(0.0),
                            "ymax": np.float32(1.0),
                        }
                    rewrite_snap_rows(input_root, rows, schema=schema)
                    if case in {
                        "incompatible snap schema",
                        "empty snap input file",
                    }:
                        message = f"{input_root.resolve()}: {suffix}"
                    else:
                        message = suffix
                    self.assert_rejected(
                        [input_root], root / f"output-{index}", message
                    )

    def test_assemble_cli_rejects_duplicate_region_and_missing_snap(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            second = root / "second"
            write_assembly_fixture(
                first,
                "7020000010",
                [(710_000_101, -170.0, -80.0, [], 1.0, 1.0)],
                [(91, 710_000_101, -170.0, -80.0, 1.0)],
            )
            write_assembly_fixture(
                second,
                "7020000010",
                [(710_000_201, -120.0, -80.0, [], 1.0, 1.0)],
                [(92, 710_000_201, -120.0, -80.0, 1.0)],
            )
            self.assert_rejected(
                [first, second],
                root / "duplicate-output",
                "duplicate manifest region 7020000010",
            )
            missing = root / "missing-snap"
            write_assembly_fixture(
                missing, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            (missing / "aux" / "snap_stems.parquet").unlink()
            self.assert_rejected(
                [missing],
                root / "missing-output",
                "required regular aux/snap_stems.parquet is missing",
            )

    def test_assemble_cli_rejects_missing_output_parent(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_root = root / "input"
            write_assembly_fixture(
                input_root, "7020000010", MERGE_RUN_A, PARTIAL_SNAP_A
            )
            missing_parent = root / "missing"
            output = missing_parent / "assembled"
            with self.assertRaises(ValueError) as raised:
                main(
                    [
                        "assemble",
                        "--input",
                        str(input_root),
                        "--out",
                        str(output),
                    ]
                )
            self.assertIn(
                f"{missing_parent.resolve()}: output parent must be an existing directory",
                str(raised.exception),
            )
            self.assertFalse(missing_parent.exists())
            self.assertFalse(output.exists())
            BuildCliTests.assert_no_temporary_entries(self, root)


class CoreMergeTests(unittest.TestCase):
    def assert_merge_output_scan(
        self,
        result: build_adapter.CoreMergeResult,
        expected_upstream_by_id: dict[int, list[int]],
    ) -> None:
        catchment_file = pq.ParquetFile(result.catchments_path)
        graph_file = pq.ParquetFile(result.graph_path)
        previous_key = None
        row_count = 0
        for catchment_batch, graph_batch in zip(
            catchment_file.iter_batches(batch_size=2),
            graph_file.iter_batches(batch_size=2),
            strict=True,
        ):
            catchment_ids = catchment_batch.column("id").to_pylist()
            graph_ids = graph_batch.column("id").to_pylist()
            self.assertEqual(catchment_ids, graph_ids)
            hilbert_keys = merge_hilbert_keys(
                catchment_batch.column("geometry").to_pylist()
            )
            upstream_lists = graph_batch.column("upstream_ids").to_pylist()
            for hilbert, unit_id, upstream_ids in zip(
                hilbert_keys,
                catchment_ids,
                upstream_lists,
                strict=True,
            ):
                current_key = hilbert, unit_id
                if previous_key is not None:
                    self.assertLessEqual(previous_key, current_key)
                previous_key = current_key
                self.assertEqual(
                    upstream_ids, expected_upstream_by_id[unit_id]
                )
                row_count += 1
        self.assertEqual(row_count, result.metrics.total_input_rows)

    def test_merge_catchments_and_graph_interleaves_sorted_runs_in_lockstep(
        self,
    ) -> None:
        expected_ids = [
            710_000_101,
            720_000_201,
            710_000_102,
            720_000_202,
            710_000_103,
            720_000_203,
            720_000_204,
        ]
        expected_upstream_ids = [
            [],
            [],
            [710_000_101],
            [720_000_201],
            [710_000_102],
            [720_000_202],
            [720_000_203],
        ]
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_a_catchments, run_a_graph = write_merge_fixture(
                root / "run-a", MERGE_RUN_A
            )
            run_b_catchments, run_b_graph = write_merge_fixture(
                root / "run-b", MERGE_RUN_B
            )
            keys_a = merge_hilbert_keys(
                [row["geometry"] for row in run_a_catchments]
            )
            keys_b = merge_hilbert_keys(
                [row["geometry"] for row in run_b_catchments]
            )
            self.assertEqual(keys_a, [7_054_384, 517_598_622, 715_827_882])
            self.assertEqual(
                keys_b,
                [238_609_294, 622_261_220, 715_827_882, 1_008_396_555],
            )
            concatenated_ids = [
                row["id"]
                for row in run_a_catchments + run_b_catchments
            ]
            self.assertEqual(
                concatenated_ids,
                [
                    710_000_101,
                    710_000_102,
                    710_000_103,
                    720_000_201,
                    720_000_202,
                    720_000_203,
                    720_000_204,
                ],
            )
            self.assertNotEqual(concatenated_ids, expected_ids)

            output = root / "output"
            result = build_adapter.merge_catchments_and_graph(
                [root / "run-a", root / "run-b"],
                output,
                input_batch_size=2,
                row_group_min=3,
                row_group_max=4,
            )
            catchment_file = pq.ParquetFile(result.catchments_path)
            graph_file = pq.ParquetFile(result.graph_path)
            actual_catchments = catchment_file.read().to_pylist()
            actual_graph = graph_file.read().to_pylist()
            self.assertEqual(
                [row["id"] for row in actual_catchments], expected_ids
            )
            self.assertEqual([row["id"] for row in actual_graph], expected_ids)
            self.assertEqual(
                [row["upstream_ids"] for row in actual_graph],
                expected_upstream_ids,
            )
            source_catchments = {
                row["id"]: row
                for row in run_a_catchments + run_b_catchments
            }
            source_graph = {
                row["id"]: row for row in run_a_graph + run_b_graph
            }
            for catchment_row, graph_row in zip(
                actual_catchments, actual_graph, strict=True
            ):
                self.assertEqual(
                    catchment_row, source_catchments[catchment_row["id"]]
                )
                self.assertEqual(graph_row, source_graph[graph_row["id"]])
            self.assertEqual(
                [
                    catchment_file.metadata.row_group(index).num_rows
                    for index in range(catchment_file.num_row_groups)
                ],
                [4, 3],
            )
            self.assertEqual(
                [
                    graph_file.metadata.row_group(index).num_rows
                    for index in range(graph_file.num_row_groups)
                ],
                [4, 3],
            )

            previous_key = None
            scanned_ids = []
            catchment_batches = catchment_file.iter_batches(batch_size=2)
            graph_batches = graph_file.iter_batches(batch_size=2)
            for catchment_batch, graph_batch in zip(
                catchment_batches, graph_batches, strict=True
            ):
                catchment_ids = catchment_batch.column("id").to_pylist()
                graph_ids = graph_batch.column("id").to_pylist()
                self.assertEqual(graph_ids, catchment_ids)
                keys = merge_hilbert_keys(
                    catchment_batch.column("geometry").to_pylist()
                )
                for key, unit_id in zip(keys, catchment_ids, strict=True):
                    current_key = (key, unit_id)
                    if previous_key is not None:
                        self.assertLessEqual(previous_key, current_key)
                    previous_key = current_key
                scanned_ids.extend(catchment_ids)
            self.assertEqual(scanned_ids, expected_ids)
            build_adapter.assert_geoparquet_valid(result.catchments_path)
            self.assertNotIn(b"geo", graph_file.schema_arrow.metadata or {})

            reverse_output = root / "reverse-output"
            reverse_result = build_adapter.merge_catchments_and_graph(
                [root / "run-b", root / "run-a"],
                reverse_output,
                input_batch_size=2,
                row_group_min=3,
                row_group_max=4,
            )
            self.assertEqual(
                result.catchments_path.read_bytes(),
                reverse_result.catchments_path.read_bytes(),
            )
            self.assertEqual(
                result.graph_path.read_bytes(),
                reverse_result.graph_path.read_bytes(),
            )

    def test_merge_catchments_and_graph_buffer_ceiling_is_row_count_independent(
        self,
    ) -> None:
        observed_metrics = []
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for rows_per_run in (2, 25):
                run_a = [
                    (
                        710_000_000 + native_id,
                        0.0,
                        0.0,
                        (
                            []
                            if native_id == 1
                            else [710_000_000 + native_id - 1]
                        ),
                        float(native_id),
                        float(native_id),
                    )
                    for native_id in range(1, rows_per_run + 1)
                ]
                run_b = [
                    (
                        720_000_000 + native_id,
                        0.0,
                        0.0,
                        (
                            []
                            if native_id == 1
                            else [720_000_000 + native_id - 1]
                        ),
                        float(native_id),
                        float(native_id),
                    )
                    for native_id in range(1, rows_per_run + 1)
                ]
                case_root = root / f"rows-{rows_per_run}"
                _, graph_a = write_merge_fixture(case_root / "run-a", run_a)
                _, graph_b = write_merge_fixture(case_root / "run-b", run_b)
                expected_upstream = {
                    row["id"]: row["upstream_ids"]
                    for row in graph_a + graph_b
                }
                result = build_adapter.merge_catchments_and_graph(
                    [case_root / "run-a", case_root / "run-b"],
                    case_root / "output",
                    input_batch_size=2,
                    row_group_min=3,
                    row_group_max=4,
                )
                metrics = result.metrics
                observed_metrics.append(metrics)
                self.assertEqual(metrics.input_count, 2)
                self.assertEqual(metrics.total_input_rows, 2 * rows_per_run)
                self.assertEqual(metrics.emitted_rows, 2 * rows_per_run)
                self.assertEqual(metrics.input_batch_size, 2)
                self.assertEqual(metrics.row_group_min, 3)
                self.assertEqual(metrics.row_group_max, 4)
                self.assertEqual(metrics.buffer_row_pair_ceiling, 8)
                self.assertLessEqual(metrics.peak_input_buffer_row_pairs, 4)
                self.assertLessEqual(metrics.peak_output_buffer_row_pairs, 4)
                self.assertLessEqual(metrics.peak_heap_entries, 2)
                self.assertLessEqual(metrics.peak_buffered_row_pairs, 8)
                self.assert_merge_output_scan(result, expected_upstream)
            self.assertEqual(
                observed_metrics[0].buffer_row_pair_ceiling,
                observed_metrics[1].buffer_row_pair_ceiling,
            )

    def test_merge_rejects_invalid_paths_and_size_arguments(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with self.assertRaisesRegex(ValueError, "at least one input"):
                build_adapter.merge_catchments_and_graph([], root / "output")

            valid = root / "valid"
            write_merge_fixture(valid, MERGE_RUN_A)
            missing_catchments = root / "missing-catchments"
            write_merge_fixture(missing_catchments, MERGE_RUN_A)
            (missing_catchments / "catchments.parquet").unlink()
            missing_graph = root / "missing-graph"
            write_merge_fixture(missing_graph, MERGE_RUN_A)
            (missing_graph / "graph.parquet").unlink()
            for missing_root, filename in (
                (missing_catchments, "catchments.parquet"),
                (missing_graph, "graph.parquet"),
            ):
                with self.subTest(filename=filename):
                    with self.assertRaises(ValueError) as raised:
                        build_adapter.merge_catchments_and_graph(
                            [missing_root], root / f"output-{filename}"
                        )
                    self.assertIn(str(missing_root.resolve()), str(raised.exception))
                    self.assertIn(filename, str(raised.exception))

            with self.assertRaisesRegex(ValueError, "unique after resolution"):
                build_adapter.merge_catchments_and_graph(
                    [valid, valid / ".." / "valid"], root / "duplicate-output"
                )
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph([valid], valid)
            self.assertIn(str(valid.resolve()), str(raised.exception))
            self.assertIn("aliases output", str(raised.exception))

            invalid_arguments = (
                {"input_batch_size": 0},
                {"input_batch_size": -1},
                {"row_group_min": 0},
                {"row_group_min": -1},
                {"row_group_min": 4, "row_group_max": 3},
                {"row_group_max": 0},
            )
            for arguments in invalid_arguments:
                with self.subTest(arguments=arguments):
                    with self.assertRaises(ValueError):
                        build_adapter.merge_catchments_and_graph(
                            [valid],
                            root / f"invalid-{len(arguments)}",
                            **arguments,
                        )

    def test_merge_rejects_row_count_and_exact_schema_mismatches(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            count_root = root / "count"
            _, graph_rows = write_merge_fixture(count_root, MERGE_RUN_A)
            rewrite_merge_rows(
                count_root / "graph.parquet", graph_rows[:-1]
            )
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph(
                    [count_root], root / "count-output"
                )
            self.assertIn(str(count_root.resolve()), str(raised.exception))
            self.assertIn("row counts differ", str(raised.exception))

            catchment_schema, graph_schema = merge_fixture_schemas()
            wrong_catchment_schema = catchment_schema.remove_metadata()
            wrong_catchment_roots = [
                root / "wrong-catchment-a",
                root / "wrong-catchment-b",
            ]
            for wrong_root in wrong_catchment_roots:
                write_merge_fixture(
                    wrong_root,
                    MERGE_RUN_A,
                    catchment_schema=wrong_catchment_schema,
                )
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph(
                    wrong_catchment_roots, root / "wrong-catchment-output"
                )
            self.assertIn(
                str(wrong_catchment_roots[0].resolve()),
                str(raised.exception),
            )
            self.assertIn("catchment schema", str(raised.exception))

            wrong_graph_root = root / "wrong-graph"
            write_merge_fixture(
                wrong_graph_root,
                MERGE_RUN_A,
                graph_schema=graph_schema.with_metadata({b"invalid": b"schema"}),
            )
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph(
                    [wrong_graph_root], root / "wrong-graph-output"
                )
            self.assertIn(str(wrong_graph_root.resolve()), str(raised.exception))
            self.assertIn("graph schema", str(raised.exception))

    def test_merge_rejects_paired_row_value_mismatches(self) -> None:
        mutations = {
            "id": lambda rows: rows[0].update(id=rows[0]["id"] + 1),
            "level": lambda rows: rows[0].update(level=1),
            "bbox": lambda rows: rows[0].update(
                bbox_minx=np.float32(rows[0]["bbox_minx"] + 1.0)
            ),
        }
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for condition, mutate in mutations.items():
                with self.subTest(condition=condition):
                    case_root = root / condition
                    _, graph_rows = write_merge_fixture(
                        case_root, MERGE_RUN_A
                    )
                    mutate(graph_rows)
                    rewrite_merge_rows(
                        case_root / "graph.parquet", graph_rows
                    )
                    with self.assertRaises(ValueError) as raised:
                        build_adapter.merge_catchments_and_graph(
                            [case_root], root / f"{condition}-output"
                        )
                    message = str(raised.exception)
                    self.assertIn(str(case_root.resolve()), message)
                    self.assertIn(condition, message)

    def test_merge_rejects_decrease_within_input_batch_without_repair(
        self,
    ) -> None:
        bad_rows = [MERGE_RUN_A[1], MERGE_RUN_A[0], MERGE_RUN_A[2]]
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_root = root / "within-batch"
            write_merge_fixture(input_root, bad_rows)
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph(
                    [input_root],
                    root / "output",
                    input_batch_size=2,
                    row_group_min=3,
                    row_group_max=4,
                )
            message = str(raised.exception)
            self.assertIn(str(input_root.resolve()), message)
            self.assertIn("(517598622, 710000102)", message)
            self.assertIn("(7054384, 710000101)", message)
            self.assertEqual(
                pq.read_table(input_root / "catchments.parquet")
                .column("id")
                .to_pylist(),
                [710_000_102, 710_000_101, 710_000_103],
            )

    def test_merge_rejects_decrease_across_input_batch_boundary_without_repair(
        self,
    ) -> None:
        bad_rows = [MERGE_RUN_A[0], MERGE_RUN_A[2], MERGE_RUN_A[1]]
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_root = root / "across-batch"
            write_merge_fixture(input_root, bad_rows)
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph(
                    [input_root],
                    root / "output",
                    input_batch_size=2,
                    row_group_min=3,
                    row_group_max=4,
                )
            message = str(raised.exception)
            self.assertIn(str(input_root.resolve()), message)
            self.assertIn("(715827882, 710000103)", message)
            self.assertIn("(517598622, 710000102)", message)
            self.assertEqual(
                pq.read_table(input_root / "catchments.parquet")
                .column("id")
                .to_pylist(),
                [710_000_101, 710_000_103, 710_000_102],
            )

    def test_merge_rejects_identical_hilbert_and_id_across_runs(self) -> None:
        duplicate = [(710_000_101, -170.0, -80.0, [], 9.0, 9.0)]
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_a = root / "run-a"
            run_b = root / "run-b"
            write_merge_fixture(run_a, [MERGE_RUN_A[0]])
            write_merge_fixture(run_b, duplicate)
            with self.assertRaises(ValueError) as raised:
                build_adapter.merge_catchments_and_graph(
                    [run_a, run_b],
                    root / "output",
                    input_batch_size=2,
                    row_group_min=3,
                    row_group_max=4,
                )
            self.assertIn("duplicate merge key", str(raised.exception))
            self.assertIn("(7054384, 710000101)", str(raised.exception))


class CoreHfxCompilationTests(unittest.TestCase):
    created_at = datetime(2026, 7, 21, 12, 34, 56, tzinfo=timezone.utc)
    basin_id = "7020000010"
    fabric_version = "synthetic-2026.07"

    def test_compile_core_hfx_uses_dataset_global_hilbert_order(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            source_dir.mkdir()
            basins, streamnet = planetary_order_frames()
            source = load_tdx_geopackages(
                *write_pair(source_dir, basins, streamnet)
            )
            model = build_streamnet_model(
                source.basins,
                source.streamnet,
                header_number=71,
                endpoint_tolerance=0.001,
            )
            result = compile_core_hfx(
                source,
                model,
                root / "output",
                processing_basin_id=self.basin_id,
                fabric_version=self.fabric_version,
                created_at=self.created_at,
            )
            catchment_ids = pq.read_table(
                result.catchments_path, columns=["id"]
            )["id"].to_pylist()
            snap_unit_ids = pq.read_table(
                result.snap_path, columns=["unit_id"]
            )["unit_id"].to_pylist()

        local_catchment_distances = (
            source.basins.geometry.centroid.hilbert_distance(
                total_bounds=source.basins.geometry.total_bounds
            )
        )
        local_stem_distances = (
            source.streamnet.geometry.centroid.hilbert_distance(
                total_bounds=source.basins.geometry.total_bounds
            )
        )
        world_catchment_distances = (
            source.basins.geometry.centroid.hilbert_distance(
                total_bounds=[-180, -90, 180, 90]
            )
        )
        world_stem_distances = (
            source.streamnet.geometry.centroid.hilbert_distance(
                total_bounds=[-180, -90, 180, 90]
            )
        )
        self.assertEqual(
            local_catchment_distances.tolist(),
            [54876, 4294912416, 1431666952],
        )
        self.assertEqual(
            local_stem_distances.tolist(),
            [54876, 4294912416, 1431666952],
        )
        self.assertEqual(
            world_catchment_distances.tolist(),
            [7054384, 238609294, 1008396555],
        )
        self.assertEqual(
            world_stem_distances.tolist(),
            [7054384, 238609294, 1008396555],
        )
        self.assertEqual(
            (catchment_ids, snap_unit_ids),
            (
                [710000100, 710000200, 710000300],
                [710000100, 710000200, 710000300],
            ),
        )

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
        self.assertEqual(
            ingestion.dscontarea.signed_aggregate_relative_divergence,
            0.0,
        )
        self.assertEqual(
            ingestion.dscontarea.absolute_aggregate_relative_divergence,
            0.0,
        )
        self.assertEqual(ingestion.dscontarea.max_absolute_relative_divergence, 0.0)
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
        self.assertEqual(diagnostics.short_successor_resolved_reach_count, 0)
        self.assertEqual(
            diagnostics.short_successor_resolved_reach_native_linknos, ()
        )
        self.assertEqual(
            diagnostics.reach_side_near_degenerate_resolved_reach_count, 0
        )
        self.assertEqual(
            diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
            (),
        )
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
            self.assertEqual(snap["unit_id"].to_pylist(), [710000200, 710000100])
            self.assertEqual(
                snap["weight"].to_pylist(),
                [2.4618144035339355, 1.2309072017669678],
            )
            self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
            self.assertEqual(
                snap["bbox"].to_pylist(),
                [
                    {
                        "xmin": 0.009999999776482582,
                        "ymin": -9.999999747378752e-05,
                        "xmax": 0.019999999552965164,
                        "ymax": 9.999999747378752e-05,
                    },
                    {
                        "xmin": 0.0,
                        "ymin": -9.999999747378752e-05,
                        "xmax": 0.009999999776482582,
                        "ymax": 9.999999747378752e-05,
                    },
                ],
            )
            expected_stems = [
                LineString([(0.01, 0.0), (0.02, 0.0)]),
                LineString([(0.0, 0.0), (0.01, 0.0)]),
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
            ).centroid.hilbert_distance(total_bounds=[-180, -90, 180, 90])
            self.assertEqual(stem_distances.tolist(), [3579139411, 3579139413])
            distances = source_basins.geometry.centroid.hilbert_distance(
                total_bounds=[-180, -90, 180, 90]
            )
            self.assertEqual(distances.tolist(), [2147483655, 2147483649])
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
        self.assertEqual(snap["unit_id"].to_pylist(), [710000200, 710000100])
        self.assertEqual(
            snap["weight"].to_pylist(),
            [1.2309072017669678, 1.2309072017669678],
        )
        self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
        self.assertEqual(
            snap["geometry"].to_pylist(),
            [
                LineString([(0.01, 0.0), (0.02, 0.0)]).wkb,
                LineString([(0.0, 0.0), (0.01, 0.0)]).wkb,
            ],
        )
        diagnostics = result.diagnostics.streamnet
        self.assertEqual(diagnostics.endpoint_coincidence_proven_link_count, 0)
        self.assertEqual(diagnostics.predecessor_orientation_proven_root_count, 0)
        self.assertEqual(diagnostics.short_successor_resolved_reach_count, 0)
        self.assertEqual(
            diagnostics.short_successor_resolved_reach_native_linknos, ()
        )
        self.assertEqual(
            diagnostics.reach_side_near_degenerate_resolved_reach_count, 0
        )
        self.assertEqual(
            diagnostics.reach_side_near_degenerate_resolved_reach_native_linknos,
            (),
        )
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
            "short_successor_resolved_reach_count": 0,
            "short_successor_resolved_reach_native_linknos": [],
            "reach_side_near_degenerate_resolved_reach_count": 0,
            "reach_side_near_degenerate_resolved_reach_native_linknos": [],
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
                        "signed_aggregate_relative_divergence": 0.0,
                        "absolute_aggregate_relative_divergence": 0.0,
                        "max_absolute_relative_divergence": 0.0,
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
                    "short_successor_resolved_reach_count": 0,
                    "short_successor_resolved_reach_native_linknos": [],
                    "reach_side_near_degenerate_resolved_reach_count": 0,
                    "reach_side_near_degenerate_resolved_reach_native_linknos": [],
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
            self.assertFalse(
                any(
                    "diagnostic=short_successor_resolved_reach_count" in message
                    for message in messages
                )
            )
            self.assertFalse(
                any(
                    "diagnostic=reach_side_near_degenerate_resolved_reach_count"
                    in message
                    for message in messages
                )
            )

    def test_build_cli_reports_short_successor_and_near_degenerate_root_resolution(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_dir = root / "source"
            source_dir.mkdir()
            basins, streamnet, _, _ = canonical_frames()
            streamnet.loc[streamnet["LINKNO"] == 100, "geometry"] = LineString(
                [(0.0, 0.0), (0.01, 0.0)]
            )
            streamnet.loc[streamnet["LINKNO"] == 200, "geometry"] = LineString(
                [(0.0092, 0.0), (0.0108, 0.0)]
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
            self.assertEqual(
                report_data["diagnostics"]["streamnet"],
                {
                    "polygon_bearing_link_count": 2,
                    "polygonless_dropped_reach_count": 0,
                    "degenerate_reach_count": 0,
                    "degenerate_reach_native_linknos": [],
                    "degenerate_polygon_bearing_reach_count": 0,
                    "degenerate_polygon_bearing_reach_native_linknos": [],
                    "degenerate_polygonless_reach_count": 0,
                    "degenerate_polygonless_reach_native_linknos": [],
                    "short_successor_resolved_reach_count": 1,
                    "short_successor_resolved_reach_native_linknos": [100],
                    "reach_side_near_degenerate_resolved_reach_count": 1,
                    "reach_side_near_degenerate_resolved_reach_native_linknos": [200],
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
            catchments = pq.read_table(output / "catchments.parquet")
            outlets = {
                linkno: (lon, lat)
                for linkno, lon, lat in zip(
                    catchments["id"].to_pylist(),
                    catchments["outlet_lon"].to_pylist(),
                    catchments["outlet_lat"].to_pylist(),
                    strict=True,
                )
            }
            self.assertEqual(outlets[710000100], (0.01, 0.0))
            self.assertEqual(outlets[710000200], (0.0108, 0.0))
            messages = [record.getMessage() for record in captured.records]
            self.assertEqual(
                sum(
                    "diagnostic=short_successor_resolved_reach_count count=1 "
                    "native_ids=(100,)" in message
                    for message in messages
                ),
                1,
            )
            self.assertEqual(
                sum(
                    "diagnostic=reach_side_near_degenerate_resolved_reach_count "
                    "count=1 native_ids=(200,)" in message
                    for message in messages
                ),
                1,
            )
            self.assertFalse(
                any("diagnostic=degenerate_reach_count" in message for message in messages)
            )
            self.assertFalse(
                any(
                    "diagnostic=trusted_orientation_isolated_root_count" in message
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
            self.assertEqual(snap["unit_id"].to_pylist(), [710000200, 710000100])
            self.assertNotIn(710000150, snap["unit_id"].to_pylist())
            self.assertEqual(
                snap["weight"].to_pylist(),
                [2.4618144035339355, 1.2309072017669678],
            )
            self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
            self.assertEqual(
                snap["geometry"].to_pylist(),
                [
                    LineString([(0.01, 0.0), (0.02, 0.0)]).wkb,
                    LineString([(0.0, 0.0), (0.01, 0.0)]).wkb,
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
            self.assertFalse(
                any(
                    "short_successor_resolved_reach_count" in message
                    for message in messages
                )
            )
            self.assertFalse(
                any(
                    "reach_side_near_degenerate_resolved_reach_count" in message
                    for message in messages
                )
            )
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
            self.assertEqual(snap["unit_id"].to_pylist(), [710000200, 710000100])
            self.assertEqual(
                snap["weight"].to_pylist(),
                [1.2309072017669678, 1.2309072017669678],
            )
            self.assertEqual(snap["stem_role"].to_pylist(), [None, None])
            self.assertEqual(
                snap["geometry"].to_pylist(),
                [
                    LineString([(0.01, 0.0), (0.02, 0.0)]).wkb,
                    LineString([(0.0, 0.0), (0.01, 0.0)]).wkb,
                ],
            )
            messages = [record.getMessage() for record in captured.records]
            self.assertTrue(any("diagnostic=trusted_orientation_isolated_root_count count=2" in message and "native_ids=(100, 200)" in message for message in messages))
            self.assertTrue(any("diagnostic=trusted_orientation_polygon_bearing_isolated_root_count count=2" in message and "native_ids=(100, 200)" in message for message in messages))
            self.assertFalse(
                any(
                    "short_successor_resolved_reach_count" in message
                    for message in messages
                )
            )
            self.assertFalse(
                any(
                    "reach_side_near_degenerate_resolved_reach_count" in message
                    for message in messages
                )
            )
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
