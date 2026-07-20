# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "numpy>=1.26.0,<3.0.0",
#   "rasterio>=1.4.0,<2.0.0",
#   "shapely>=2.0.0",
#   "pyarrow>=12.0.0,<23.0.0",
# ]
# ///
"""Generate HFX v0.3.0 conformance fixtures.

Run with:
    uv run conformance/generate_fixtures.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import shapely.wkb
from rasterio.transform import from_origin
from shapely.geometry import Point, box

CREATED_AT = "2026-01-01T00:00:00Z"
ADAPTER_VERSION = "conformance-fixture-v2"
FABRIC_NAME = "conformance-tiny"

SCRIPT_DIR = Path(__file__).parent

BBOX_LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")

PREBUILT_D8_FIXTURES = (
    "valid/tiny-with-aux-d8",
    "valid/tiny-with-two-aux-d8",
    "valid/tiny-with-two-aux-d8-tiled",
    "invalid/tiny-with-aux-d8-no-overlap",
    "invalid/tiny-with-bad-second-aux-d8",
)


BASE_ROWS = [
    # id, level, parent_id, minx, miny, maxx, maxy, outlet_lon, outlet_lat
    (1, 0, None, 0.0, 0.0, 2.0, 2.0, 1.0, 0.0),
    (2, 1, 1, 0.0, 1.0, 1.0, 2.0, 0.5, 1.0),
    (3, 1, 1, 1.0, 1.0, 2.0, 2.0, 1.5, 1.0),
    (4, 1, 1, 0.0, 0.0, 1.0, 1.0, 0.5, 0.0),
    (5, 1, 1, 1.0, 0.0, 2.0, 1.0, 1.5, 0.0),
]

VALID_UPSTREAM = {
    1: [],
    2: [],
    3: [],
    4: [2, 3],
    5: [4],
}

DANGLING_UPSTREAM = {
    1: [],
    2: [],
    3: [],
    4: [2, 999],
    5: [4],
}


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def wkb_rect(minx: float, miny: float, maxx: float, maxy: float) -> bytes:
    return shapely.wkb.dumps(box(minx, miny, maxx, maxy), byte_order=1)


def bbox_struct_type() -> pa.DataType:
    """Struct type for the GeoParquet 1.1 covering `bbox` column (non-nullable leaves)."""
    return pa.struct(
        [pa.field(name, pa.float32(), nullable=False) for name in BBOX_LEAF_NAMES]
    )


def build_bbox_struct(
    minx: list[float],
    miny: list[float],
    maxx: list[float],
    maxy: list[float],
) -> pa.StructArray:
    """Build the `bbox` struct array so its 4 float32 leaves carry row-group stats.

    Uses pa.StructArray.from_arrays (NOT the pa.array([{...}]) list-of-dicts
    anti-pattern) so the Parquet writer records min/max on bbox.xmin / ymin /
    xmax / ymax. This is the GATE-1 (s04) proven writer pattern, mirrored from
    conformance/scripts/bbox_struct.py.
    """
    return pa.StructArray.from_arrays(
        [
            pa.array(minx, type=pa.float32()),
            pa.array(miny, type=pa.float32()),
            pa.array(maxx, type=pa.float32()),
            pa.array(maxy, type=pa.float32()),
        ],
        fields=[pa.field(name, pa.float32(), nullable=False) for name in BBOX_LEAF_NAMES],
    )


def _geo_metadata(geometry_types: list[str]) -> dict:
    """GeoParquet 1.1 covering metadata declaring the `bbox` struct leaves.

    Covering lives at the locked literal path
    geo.columns.geometry.covering.bbox.{xmin,ymin,xmax,ymax}; each entry points
    at the matching struct leaf as ["bbox", "<leaf>"].
    """
    return {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
                "covering": {"bbox": {name: ["bbox", name] for name in BBOX_LEAF_NAMES}},
            }
        },
    }


def write_catchments(
    out_dir: Path,
    rows=BASE_ROWS,
    *,
    up_area_values: list[float | None] | None = None,
    write_statistics: bool = True,
    write_covering: bool = True,
) -> None:
    if up_area_values is None:
        up_area_values = [None for _ in rows]
    if len(up_area_values) != len(rows):
        raise ValueError("up_area_values length must match rows length")

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox", bbox_struct_type(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
            pa.field("source_id", pa.string(), nullable=True),
            pa.field("level_label", pa.string(), nullable=True),
        ]
    )
    if write_covering:
        schema = schema.with_metadata(
            {b"geo": json.dumps(_geo_metadata(["Polygon", "MultiPolygon"])).encode("utf-8")}
        )

    bbox = build_bbox_struct(
        [r[3] for r in rows],
        [r[4] for r in rows],
        [r[5] for r in rows],
        [r[6] for r in rows],
    )
    table = pa.table(
        {
            "id": pa.array([r[0] for r in rows], type=pa.int64()),
            "level": pa.array([r[1] for r in rows], type=pa.int16()),
            "parent_id": pa.array([r[2] for r in rows], type=pa.int64()),
            "area_km2": pa.array([4.0 if r[0] == 1 else 1.0 for r in rows], type=pa.float32()),
            "up_area_km2": pa.array(up_area_values, type=pa.float32()),
            "outlet_lon": pa.array([r[7] for r in rows], type=pa.float64()),
            "outlet_lat": pa.array([r[8] for r in rows], type=pa.float64()),
            "bbox": bbox,
            "geometry": pa.array([wkb_rect(r[3], r[4], r[5], r[6]) for r in rows], type=pa.binary()),
            "source_id": pa.array([f"src-{r[0]}" for r in rows], type=pa.string()),
            "level_label": pa.array(["coarse" if r[1] == 0 else "fine" for r in rows], type=pa.string()),
        },
        schema=schema,
    )

    with pq.ParquetWriter(out_dir / "catchments.parquet", schema, write_statistics=write_statistics) as writer:
        writer.write_table(table)


def migrate_prebuilt_d8_fixture(out_dir: Path) -> None:
    """Migrate a pre-built D8-raster fixture to format v0.3.0 and D8 v2 in place.

    These fixtures ship hand-authored GeoTIFF rasters under aux/d8/ (committed
    binaries with no generator) plus a hand-written README, so they MUST NOT be
    reset_dir()'d. Only catchments.parquet (flat bbox -> struct covering) and
    manifest declarations are rewritten; raster bytes, graph.parquet, README.md,
    fabric_name, auxiliary artifact entry order, and artifact paths are preserved.
    """
    write_catchments(out_dir)
    manifest_path = out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["format_version"] = "0.3.0"
    for entry in manifest.get("auxiliary", []):
        if entry.get("schema") in {
            "hfx.aux.d8_raster.v1",
            "hfx.aux.d8_raster.v2",
        }:
            entry["schema"] = "hfx.aux.d8_raster.v2"
            entry["metadata"] = {
                "crs": "EPSG:4326",
                "flow_dir_encoding": "esri",
                "flow_acc_units": "cells",
            }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def migrate_prebuilt_d8_fixtures() -> None:
    for rel in PREBUILT_D8_FIXTURES:
        migrate_prebuilt_d8_fixture(SCRIPT_DIR / rel)


def write_graph(
    out_dir: Path,
    upstream: dict[int, list[int]],
    rows=BASE_ROWS,
    *,
    include_bbox: bool = True,
    write_statistics: bool = True,
) -> None:
    ids = [r[0] for r in rows]
    row_by_id = {r[0]: r for r in rows}
    fields = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("level", pa.int16(), nullable=False),
        pa.field(
            "upstream_ids",
            pa.list_(pa.field("item", pa.int64(), nullable=True)),
            nullable=False,
        ),
    ]
    columns = {
        "id": pa.array(ids, type=pa.int64()),
        "level": pa.array([row_by_id[i][1] for i in ids], type=pa.int16()),
        "upstream_ids": pa.array(
            [upstream[i] for i in ids],
            type=pa.list_(pa.field("item", pa.int64(), nullable=True)),
        ),
    }
    if include_bbox:
        fields.extend(
            [
                pa.field("bbox_minx", pa.float32(), nullable=False),
                pa.field("bbox_miny", pa.float32(), nullable=False),
                pa.field("bbox_maxx", pa.float32(), nullable=False),
                pa.field("bbox_maxy", pa.float32(), nullable=False),
            ]
        )
        columns.update(
            {
                "bbox_minx": pa.array([row_by_id[i][3] for i in ids], type=pa.float32()),
                "bbox_miny": pa.array([row_by_id[i][4] for i in ids], type=pa.float32()),
                "bbox_maxx": pa.array([row_by_id[i][5] for i in ids], type=pa.float32()),
                "bbox_maxy": pa.array([row_by_id[i][6] for i in ids], type=pa.float32()),
            }
        )
    schema = pa.schema(fields)
    table = pa.table(columns, schema=schema)
    with pq.ParquetWriter(out_dir / "graph.parquet", schema, write_statistics=write_statistics) as writer:
        writer.write_table(table)


def point_wkb(x: float, y: float) -> bytes:
    return shapely.wkb.dumps(Point(x, y), byte_order=1)


def write_snap(
    out_dir: Path,
    rel_path: str,
    rows: list[tuple[int, int, float, str | None, float, float, bytes | None]],
) -> None:
    path = out_dir / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox", bbox_struct_type(), nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(
        {b"geo": json.dumps(_geo_metadata(["Point", "LineString"])).encode("utf-8")}
    )
    bbox = build_bbox_struct(
        [r[4] for r in rows],
        [r[5] for r in rows],
        [r[4] for r in rows],
        [r[5] for r in rows],
    )
    table = pa.table(
        {
            "id": pa.array([r[0] for r in rows], type=pa.int64()),
            "unit_id": pa.array([r[1] for r in rows], type=pa.int64()),
            "weight": pa.array([r[2] for r in rows], type=pa.float32()),
            "stem_role": pa.array([r[3] for r in rows], type=pa.string()),
            "bbox": bbox,
            "geometry": pa.array([r[6] if r[6] is not None else point_wkb(r[4], r[5]) for r in rows], type=pa.binary()),
        },
        schema=schema,
    )
    with pq.ParquetWriter(path, schema, write_statistics=True) as writer:
        writer.write_table(table)


def snap_aux(name: str, rel_path: str, levels: list[int]) -> dict:
    return {
        "schema": "hfx.aux.snap.v2",
        "artifacts": {"snap": rel_path},
        "metadata": {
            "name": name,
            "description": f"{name} conformance snap features",
            "references_levels": levels,
            "weight_semantics": "Higher values indicate stronger drainage dominance.",
        },
    }


def write_manifest(
    out_dir: Path,
    *,
    fabric_name: str = FABRIC_NAME,
    crs: str = "EPSG:4326",
    version: str = "0.3.0",
    has_up_area: bool = False,
    auxiliary: list[dict] | None = None,
) -> None:
    manifest = {
        "format_version": version,
        "fabric_name": fabric_name,
        "crs": crs,
        "has_up_area": has_up_area,
        "topology": "tree",
        "bbox": [0.0, 0.0, 2.0, 2.0],
        "unit_count": 5,
        "created_at": CREATED_AT,
        "adapter_version": ADAPTER_VERSION,
    }
    if auxiliary is not None:
        manifest["auxiliary"] = auxiliary
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def write_readme(out_dir: Path, title: str, expected: str) -> None:
    (out_dir / "README.md").write_text(f"# {title}\n\nExpected diagnostic: `{expected}`.\n")


def generate_valid_tiny() -> None:
    out = SCRIPT_DIR / "valid" / "tiny"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out)
    write_readme(out, "Valid tiny v0.3.0 fixture", "none")


def write_projected_d8_cogs(out_dir: Path) -> None:
    raster_dir = out_dir / "aux" / "d8" / "projected"
    raster_dir.mkdir(parents=True)

    rows, cols = np.indices((256, 256))
    flow_dir = ((rows + cols) % 8 + 1).astype(np.int8)
    flow_dir[0, :] *= -1
    flow_dir[128, 128] = 0
    flow_dir[255, 255] = -128

    flow_acc = (rows * 256 + cols + 1).astype(np.int32)
    flow_acc[255, 255] = -2147483648

    common = {
        "mode": "w",
        "driver": "COG",
        "width": 256,
        "height": 256,
        "count": 1,
        "crs": "EPSG:8857",
        "transform": from_origin(0.0, 256000.0, 1000.0, 1000.0),
        "compress": "DEFLATE",
        "predictor": 2,
        "blocksize": 256,
        "overviews": "NONE",
    }
    with rasterio.open(
        raster_dir / "flow_dir.tif", dtype=np.int8, nodata=-128, **common
    ) as dataset:
        dataset.write(flow_dir, 1)
    with rasterio.open(
        raster_dir / "flow_acc.tif",
        dtype=np.int32,
        nodata=-2147483648,
        **common,
    ) as dataset:
        dataset.write(flow_acc, 1)


def generate_valid_projected_grass_d8() -> None:
    out = SCRIPT_DIR / "valid" / "tiny-with-aux-d8-projected-grass"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(
        out,
        fabric_name="conformance-tiny-projected-grass",
        auxiliary=[
            {
                "schema": "hfx.aux.d8_raster.v2",
                "artifacts": {
                    "flow_dir": "aux/d8/projected/flow_dir.tif",
                    "flow_acc": "aux/d8/projected/flow_acc.tif",
                },
                "metadata": {
                    "crs": "EPSG:8857",
                    "flow_dir_encoding": "grass",
                    "flow_acc_units": "km2",
                },
            }
        ],
    )
    write_projected_d8_cogs(out)
    (out / "README.md").write_text(
        "# Valid tiny v0.3.0 fixture with projected GRASS D8 auxiliary rasters\n"
        "\n"
        "This fixture contains core catchment and graph data in EPSG:4326.\n"
        "Its D8 auxiliary pair uses an EPSG:8857 grid, GRASS flow-direction encoding, and integer square-kilometer flow accumulation.\n"
        "Expected diagnostic: `none`.\n"
    )


def generate_valid_grit_two_level() -> None:
    out = SCRIPT_DIR / "valid" / "grit-two-level"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out)
    write_readme(out, "Valid GRIT-style two-level fixture", "none")


def generate_valid_grit_two_snap() -> None:
    out = SCRIPT_DIR / "valid" / "grit-two-snap"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_snap(
        out,
        "snap/segment_stems.parquet",
        [(1, 1, 10.0, "mainstem", 1.0, 0.0, None)],
    )
    write_snap(
        out,
        "snap/reach_stems.parquet",
        [
            (1, 2, 7.0, "tributary", 0.5, 1.0, None),
            (2, 3, 7.0, "distributary", 1.5, 1.0, None),
            (3, 4, 8.0, "mainstem", 0.5, 0.0, None),
            (4, 5, 9.0, "mainstem", 1.5, 0.0, None),
        ],
    )
    write_manifest(
        out,
        auxiliary=[
            snap_aux("segment-stems", "snap/segment_stems.parquet", [0]),
            snap_aux("reach-stems", "snap/reach_stems.parquet", [1]),
        ],
    )
    write_readme(out, "Valid GRIT-style two-level fixture with two snap auxiliaries", "none")


def generate_valid_up_area_partial_nulls() -> None:
    out = SCRIPT_DIR / "valid" / "up-area-partial-nulls"
    reset_dir(out)
    write_catchments(out, up_area_values=[4.0, 1.0, None, 3.0, 4.0])
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out, has_up_area=True)
    write_readme(out, "Valid partial upstream area fixture", "none")


def generate_invalid_dangling() -> None:
    out = SCRIPT_DIR / "invalid" / "dangling-upstream-ref"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, DANGLING_UPSTREAM)
    write_manifest(out)
    write_readme(out, "Dangling upstream reference", "referential.upstream_not_in_catchments")


def generate_invalid_crs() -> None:
    out = SCRIPT_DIR / "invalid" / "crs-mismatch"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out, crs="EPSG:32632")
    write_readme(out, "CRS mismatch", "manifest.crs")


def generate_invalid_parent_cycle() -> None:
    out = SCRIPT_DIR / "invalid" / "parent-cycle"
    reset_dir(out)
    rows = [
        (1, 0, None, 0.0, 0.0, 2.0, 2.0, 1.0, 0.0),
        (2, 1, 3, 0.0, 1.0, 1.0, 2.0, 0.5, 1.0),
        (3, 1, 2, 1.0, 1.0, 2.0, 2.0, 1.5, 1.0),
        BASE_ROWS[3],
        BASE_ROWS[4],
    ]
    write_catchments(out, rows)
    write_graph(out, VALID_UPSTREAM, rows)
    write_manifest(out)
    write_readme(out, "Parent cycle", "parent.cycle_detected")


def generate_invalid_parent_level() -> None:
    out = SCRIPT_DIR / "invalid" / "parent-level-not-coarser"
    reset_dir(out)
    rows = [
        BASE_ROWS[0],
        (2, 1, 3, 0.0, 1.0, 1.0, 2.0, 0.5, 1.0),
        BASE_ROWS[2],
        BASE_ROWS[3],
        BASE_ROWS[4],
    ]
    write_catchments(out, rows)
    write_graph(out, VALID_UPSTREAM, rows)
    write_manifest(out)
    write_readme(out, "Parent level not coarser", "parent.level_not_coarser")


def generate_legacy_format_version() -> None:
    out = SCRIPT_DIR / "invalid" / "legacy-format-version"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out, version="0.1")
    write_readme(out, "Legacy v0.1 manifest", "manifest.unsupported_format_version")


def generate_legacy_graph_arrow() -> None:
    out = SCRIPT_DIR / "invalid" / "legacy-graph-arrow"
    reset_dir(out)
    write_catchments(out)
    (out / "graph.arrow").write_bytes(b"legacy graph placeholder\n")
    write_manifest(out)
    write_readme(out, "Legacy graph.arrow file", "graph.legacy_arrow_format")


def generate_invalid_graph_missing_bbox_cols() -> None:
    out = SCRIPT_DIR / "invalid" / "graph-missing-bbox-cols"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM, include_bbox=False)
    write_manifest(out)
    write_readme(out, "Graph missing bbox columns", "schema.missing_column")


def generate_invalid_graph_bbox_stats_missing() -> None:
    out = SCRIPT_DIR / "invalid" / "graph-bbox-stats-missing"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM, write_statistics=False)
    write_manifest(out)
    write_readme(out, "Graph bbox statistics missing", "schema.graph.bbox_stats_missing")


def generate_invalid_covering_metadata_missing() -> None:
    out = SCRIPT_DIR / "invalid" / "covering-metadata-missing"
    reset_dir(out)
    write_catchments(out, write_covering=False)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out)
    write_readme(out, "Catchments covering metadata missing", "schema.catchments.covering_missing")


def generate_invalid_leaf_stats_missing() -> None:
    out = SCRIPT_DIR / "invalid" / "leaf-stats-missing"
    reset_dir(out)
    write_catchments(out, write_statistics=False)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out)
    write_readme(out, "Catchments bbox leaf statistics missing", "schema.catchments.bbox_stats_missing")


def generate_invalid_catchments_multi_level_unsorted() -> None:
    out = SCRIPT_DIR / "invalid" / "catchments-multi-level-unsorted"
    reset_dir(out)
    rows = [BASE_ROWS[1], BASE_ROWS[0], *BASE_ROWS[2:]]
    write_catchments(out, rows)
    write_graph(out, VALID_UPSTREAM, rows)
    write_manifest(out)
    write_readme(out, "Catchments multi-level unsorted", "ordering.catchments.level_unsorted")


def generate_invalid_graph_level_unsorted() -> None:
    out = SCRIPT_DIR / "invalid" / "graph-level-unsorted"
    reset_dir(out)
    graph_rows = [BASE_ROWS[1], BASE_ROWS[0], *BASE_ROWS[2:]]
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM, graph_rows)
    write_manifest(out)
    write_readme(out, "Graph level unsorted", "ordering.graph.level_unsorted")


def generate_invalid_legacy_core_snap() -> None:
    out = SCRIPT_DIR / "invalid" / "legacy-core-snap"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_snap(out, "snap.parquet", [(1, 1, 1.0, "mainstem", 1.0, 0.0, None)])
    write_manifest(out)
    write_readme(out, "Legacy core snap", "file_presence.legacy_snap_parquet")


def generate_invalid_aux_snap_bad_stem_role() -> None:
    out = SCRIPT_DIR / "invalid" / "aux-snap-bad-stem-role"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_snap(out, "snap/bad.parquet", [(1, 1, 1.0, "primary", 1.0, 0.0, None)])
    write_manifest(out, auxiliary=[snap_aux("bad-stems", "snap/bad.parquet", [0])])
    write_readme(out, "Aux snap bad stem role", "aux.snap.stem_role_invalid")


def generate_invalid_aux_snap_level_not_declared() -> None:
    out = SCRIPT_DIR / "invalid" / "aux-snap-level-not-declared"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_snap(out, "snap/reach.parquet", [(1, 2, 1.0, "tributary", 0.5, 1.0, None)])
    write_manifest(out, auxiliary=[snap_aux("reach-stems", "snap/reach.parquet", [0])])
    write_readme(out, "Aux snap level not declared", "aux.snap.level_not_declared")


def generate_invalid_aux_snap_duplicate_name() -> None:
    out = SCRIPT_DIR / "invalid" / "aux-snap-duplicate-name"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_snap(out, "snap/one.parquet", [(1, 1, 1.0, "mainstem", 1.0, 0.0, None)])
    write_snap(out, "snap/two.parquet", [(1, 1, 1.0, "mainstem", 1.0, 0.0, None)])
    write_manifest(
        out,
        auxiliary=[
            snap_aux("duplicate-stems", "snap/one.parquet", [0]),
            snap_aux("duplicate-stems", "snap/two.parquet", [0]),
        ],
    )
    write_readme(out, "Aux snap duplicate name", "aux.snap.duplicate_name")


def generate_invalid_aux_snap_bad_geometry() -> None:
    out = SCRIPT_DIR / "invalid" / "aux-snap-bad-geometry"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    bad_geometry = wkb_rect(0.0, 0.0, 1.0, 1.0)
    write_snap(out, "snap/bad_geometry.parquet", [(1, 1, 1.0, "mainstem", 1.0, 0.0, bad_geometry)])
    write_manifest(out, auxiliary=[snap_aux("bad-geometry", "snap/bad_geometry.parquet", [0])])
    write_readme(out, "Aux snap bad geometry", "geometry.snap_wrong_type")


def generate_invalid_aux_snap_weight_negative() -> None:
    out = SCRIPT_DIR / "invalid" / "aux-snap-weight-negative"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_snap(out, "snap/negative_weight.parquet", [(1, 1, -1.0, "mainstem", 1.0, 0.0, None)])
    write_manifest(out, auxiliary=[snap_aux("negative-weight", "snap/negative_weight.parquet", [0])])
    write_readme(out, "Aux snap weight negative", "aux.snap.weight_invalid")


def generate_invalid_v02_format_version() -> None:
    out = SCRIPT_DIR / "invalid" / "v02-format-version"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out, version="0.2")
    write_readme(out, "v0.2 manifest", "manifest.unsupported_format_version")


def generate_invalid_up_area_empty_but_claimed() -> None:
    out = SCRIPT_DIR / "invalid" / "up-area-empty-but-claimed"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out, has_up_area=True)
    write_readme(out, "Upstream area empty but claimed", "values.up_area_empty")


def generate_invalid_up_area_unexpected() -> None:
    out = SCRIPT_DIR / "invalid" / "up-area-unexpected"
    reset_dir(out)
    write_catchments(out, up_area_values=[None, None, 2.0, None, None])
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out, has_up_area=False)
    write_readme(out, "Unexpected upstream area values", "values.up_area_unexpected")


def main() -> None:
    generate_valid_tiny()
    generate_valid_grit_two_level()
    generate_valid_grit_two_snap()
    generate_valid_up_area_partial_nulls()
    generate_valid_projected_grass_d8()
    generate_invalid_dangling()
    generate_invalid_crs()
    generate_invalid_parent_cycle()
    generate_invalid_parent_level()
    generate_legacy_format_version()
    generate_legacy_graph_arrow()
    generate_invalid_graph_missing_bbox_cols()
    generate_invalid_graph_bbox_stats_missing()
    generate_invalid_covering_metadata_missing()
    generate_invalid_leaf_stats_missing()
    generate_invalid_catchments_multi_level_unsorted()
    generate_invalid_graph_level_unsorted()
    generate_invalid_legacy_core_snap()
    generate_invalid_aux_snap_bad_stem_role()
    generate_invalid_aux_snap_level_not_declared()
    generate_invalid_aux_snap_duplicate_name()
    generate_invalid_aux_snap_bad_geometry()
    generate_invalid_aux_snap_weight_negative()
    generate_invalid_v02_format_version()
    generate_invalid_up_area_empty_but_claimed()
    generate_invalid_up_area_unexpected()
    migrate_prebuilt_d8_fixtures()
    print("[generate_fixtures] Wrote HFX v0.3.0 conformance fixtures")


if __name__ == "__main__":
    main()
