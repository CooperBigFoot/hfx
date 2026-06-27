# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "shapely>=2.0.0",
#   "pyarrow>=12.0.0,<23.0.0",
# ]
# ///
"""GATE-1 prototype helpers (milestone alden-feedback, step s04).

Self-contained spike artifact. Does NOT modify the canonical conformance
fixtures or generate_fixtures.py (those stay flat-bbox until s06). The single
purpose is to prove PyArrow emits Parquet row-group statistics on the LEAF
fields of a struct `bbox` column {xmin, ymin, xmax, ymax} via
pa.StructArray.from_arrays.

Reused by:
  - verify_struct_leaf_stats.py (this step, s04)
  - s06 will lift build_bbox_struct_array() into generate_fixtures.py's
    write_catchments / write_snap.

Run as a builder:
    uv run conformance/scripts/bbox_struct.py [OUTPUT.parquet]
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import shapely.wkb
from shapely.geometry import box

# Canonical covering leaf names locked for the s05 spec shape:
# geo.columns.<geom>.covering.bbox.{xmin,ymin,xmax,ymax}
BBOX_LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")


def bbox_struct_type(*, nullable_leaves: bool = False) -> pa.DataType:
    """Return the struct DataType for a GeoParquet 1.1 covering bbox column."""
    return pa.struct(
        [pa.field(name, pa.float32(), nullable=nullable_leaves) for name in BBOX_LEAF_NAMES]
    )


def build_bbox_struct_array(
    minx: list[float],
    miny: list[float],
    maxx: list[float],
    maxy: list[float],
) -> pa.StructArray:
    """Build a struct `bbox` array whose 4 float32 leaves carry row-group stats.

    Uses pa.StructArray.from_arrays (NOT the pa.array([{...}]) list-of-dicts
    anti-pattern, which does not reliably propagate leaf statistics) so the
    Parquet writer records min/max on bbox.xmin / ymin / xmax / ymax.
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


# (id, level, parent_id, minx, miny, maxx, maxy, outlet_lon, outlet_lat)
_PROTO_ROWS = [
    (1, 0, None, 0.0, 0.0, 2.0, 2.0, 1.0, 0.0),
    (2, 1, 1, 0.0, 1.0, 1.0, 2.0, 0.5, 1.0),
    (3, 1, 1, 1.0, 1.0, 2.0, 2.0, 1.5, 1.0),
    (4, 1, 1, 0.0, 0.0, 1.0, 1.0, 0.5, 0.0),
    (5, 1, 1, 1.0, 0.0, 2.0, 1.0, 1.5, 0.0),
]


def write_prototype_struct_catchments(out_path: Path) -> Path:
    """Write a throwaway struct-bbox catchments Parquet mirroring the s06 shape.

    SCRATCH prototype only: it is NOT a canonical fixture and MUST NOT be written
    under conformance/valid/ or conformance/invalid/. Non-bbox columns mirror the
    flat write_catchments so the file is a faithful preview of the s06 layout.
    """
    rows = _PROTO_ROWS
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
    bbox = build_bbox_struct_array(
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
            "up_area_km2": pa.array([None for _ in rows], type=pa.float32()),
            "outlet_lon": pa.array([r[7] for r in rows], type=pa.float64()),
            "outlet_lat": pa.array([r[8] for r in rows], type=pa.float64()),
            "bbox": bbox,
            "geometry": pa.array(
                [shapely.wkb.dumps(box(r[3], r[4], r[5], r[6]), byte_order=1) for r in rows],
                type=pa.binary(),
            ),
            "source_id": pa.array([f"src-{r[0]}" for r in rows], type=pa.string()),
            "level_label": pa.array(
                ["coarse" if r[1] == 0 else "fine" for r in rows], type=pa.string()
            ),
        },
        schema=schema,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pq.ParquetWriter(out_path, schema, write_statistics=True) as writer:
        writer.write_table(table, row_group_size=2)
    return out_path


def main(argv: list[str]) -> int:
    if len(argv) > 1:
        out = Path(argv[1])
    else:
        out = Path(tempfile.mkdtemp(prefix="hfx-bbox-proto-")) / "catchments.parquet"
    written = write_prototype_struct_catchments(out)
    print(f"[bbox_struct] wrote prototype struct-bbox catchments: {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
