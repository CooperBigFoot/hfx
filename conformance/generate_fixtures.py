# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "shapely>=2.0.0",
#   "pyarrow>=12.0.0,<23.0.0",
# ]
# ///
"""Generate HFX v0.2 conformance fixtures.

Run with:
    uv run conformance/generate_fixtures.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely.wkb
from shapely.geometry import box

CREATED_AT = "2026-01-01T00:00:00Z"
ADAPTER_VERSION = "conformance-fixture-v2"
FABRIC_NAME = "conformance-tiny"

SCRIPT_DIR = Path(__file__).parent


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


def write_catchments(out_dir: Path, rows=BASE_ROWS) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
            pa.field("source_id", pa.string(), nullable=True),
            pa.field("level_label", pa.string(), nullable=True),
        ]
    )

    table = pa.table(
        {
            "id": pa.array([r[0] for r in rows], type=pa.int64()),
            "level": pa.array([r[1] for r in rows], type=pa.int16()),
            "parent_id": pa.array([r[2] for r in rows], type=pa.int64()),
            "area_km2": pa.array([4.0 if r[0] == 1 else 1.0 for r in rows], type=pa.float32()),
            "up_area_km2": pa.array([None for _ in rows], type=pa.float32()),
            "outlet_lon": pa.array([r[7] for r in rows], type=pa.float64()),
            "outlet_lat": pa.array([r[8] for r in rows], type=pa.float64()),
            "bbox_minx": pa.array([r[3] for r in rows], type=pa.float32()),
            "bbox_miny": pa.array([r[4] for r in rows], type=pa.float32()),
            "bbox_maxx": pa.array([r[5] for r in rows], type=pa.float32()),
            "bbox_maxy": pa.array([r[6] for r in rows], type=pa.float32()),
            "geometry": pa.array([wkb_rect(r[3], r[4], r[5], r[6]) for r in rows], type=pa.binary()),
            "source_id": pa.array([f"src-{r[0]}" for r in rows], type=pa.string()),
            "level_label": pa.array(["coarse" if r[1] == 0 else "fine" for r in rows], type=pa.string()),
        },
        schema=schema,
    )

    with pq.ParquetWriter(out_dir / "catchments.parquet", schema, write_statistics=True) as writer:
        writer.write_table(table)


def write_graph(out_dir: Path, upstream: dict[int, list[int]]) -> None:
    ids = [r[0] for r in BASE_ROWS]
    levels_by_id = {r[0]: r[1] for r in BASE_ROWS}
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("item", pa.int64(), nullable=True)),
                nullable=False,
            ),
        ]
    )
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "level": pa.array([levels_by_id[i] for i in ids], type=pa.int16()),
            "upstream_ids": pa.array(
                [upstream[i] for i in ids],
                type=pa.list_(pa.field("item", pa.int64(), nullable=True)),
            ),
        },
        schema=schema,
    )
    with pq.ParquetWriter(out_dir / "graph.parquet", schema, write_statistics=True) as writer:
        writer.write_table(table)


def write_manifest(
    out_dir: Path,
    *,
    crs: str = "EPSG:4326",
    version: str = "0.2",
    auxiliary: list[dict] | None = None,
) -> None:
    manifest = {
        "format_version": version,
        "fabric_name": FABRIC_NAME,
        "crs": crs,
        "has_up_area": False,
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


def write_placeholder_rasters(out_dir: Path) -> None:
    (out_dir / "flow_dir.tif").write_bytes(b"placeholder flow_dir for skip-raster conformance\n")
    (out_dir / "flow_acc.tif").write_bytes(b"placeholder flow_acc for skip-raster conformance\n")


def generate_valid_tiny() -> None:
    out = SCRIPT_DIR / "valid" / "tiny"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_manifest(out)
    write_readme(out, "Valid tiny v0.2 fixture", "none")


def generate_valid_tiny_with_aux_d8() -> None:
    out = SCRIPT_DIR / "valid" / "tiny-with-aux-d8"
    reset_dir(out)
    write_catchments(out)
    write_graph(out, VALID_UPSTREAM)
    write_placeholder_rasters(out)
    write_manifest(
        out,
        auxiliary=[
            {
                "schema": "hfx.aux.d8_raster.v1",
                "artifacts": {
                    "flow_dir": "flow_dir.tif",
                    "flow_acc": "flow_acc.tif",
                },
                "metadata": {
                    "flow_dir_encoding": "esri",
                },
            }
        ],
    )
    write_readme(out, "Valid tiny v0.2 fixture with D8 auxiliary rasters", "none")


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
    write_graph(out, VALID_UPSTREAM)
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
    write_graph(out, VALID_UPSTREAM)
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


def main() -> None:
    generate_valid_tiny()
    generate_valid_tiny_with_aux_d8()
    generate_invalid_dangling()
    generate_invalid_crs()
    generate_invalid_parent_cycle()
    generate_invalid_parent_level()
    generate_legacy_format_version()
    generate_legacy_graph_arrow()
    print("[generate_fixtures] Wrote HFX v0.2 conformance fixtures")


if __name__ == "__main__":
    main()
