# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "shapely>=2.0.0",
#   "pyarrow>=12.0.0,<23.0.0",
# ]
# ///
"""Generate HFX conformance fixtures.

Run with:
    uv run conformance/generate_fixtures.py

Regenerates ALL fixtures under conformance/valid/ and conformance/invalid/.
Idempotent: existing files are deleted before rewriting.
"""

from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import shapely.wkb
from shapely.geometry import box

# ---------------------------------------------------------------------------
# Frozen constants for determinism
# ---------------------------------------------------------------------------

CREATED_AT = "2026-01-01T00:00:00Z"
ADAPTER_VERSION = "conformance-fixture-v1"
FABRIC_NAME = "conformance-tiny"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
VALID_TINY = SCRIPT_DIR / "valid" / "tiny"
INVALID_DANGLING = SCRIPT_DIR / "invalid" / "dangling-upstream-ref"
INVALID_CRS = SCRIPT_DIR / "invalid" / "crs-mismatch"


# ---------------------------------------------------------------------------
# WKB helpers
# ---------------------------------------------------------------------------

def make_wkb_polygon(minx: float, miny: float, maxx: float, maxy: float) -> bytes:
    """Return little-endian WKB for a unit rectangle polygon (type 3)."""
    poly = box(minx, miny, maxx, maxy)
    return shapely.wkb.dumps(poly, byte_order=1)


# ---------------------------------------------------------------------------
# Catchments / graph data (shared across fixtures)
# ---------------------------------------------------------------------------

# Five-atom Y-tree topology (IDs 1..5), hand-ordered by id.
CATCHMENT_ROWS = [
    # (id, minx, miny, maxx, maxy)
    (1, 0.0, 1.0, 1.0, 2.0),
    (2, 1.0, 1.0, 2.0, 2.0),
    (3, 0.5, 0.0, 1.5, 1.0),
    (4, 0.5, -1.0, 1.5, 0.0),
    (5, 0.5, -2.0, 1.5, -1.0),
]

# upstream_ids for each atom in the valid topology.
VALID_UPSTREAM = {
    1: [],
    2: [],
    3: [1, 2],
    4: [3],
    5: [4],
}

# upstream_ids with dangling ref: id=3 references 999 (not in catchments).
DANGLING_UPSTREAM = {
    1: [],
    2: [],
    3: [1, 2, 999],
    4: [3],
    5: [4],
}


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------

def write_catchments_parquet(out_dir: Path) -> None:
    """Write catchments.parquet for the 5-atom tiny fixture."""
    ids = [r[0] for r in CATCHMENT_ROWS]
    bbox_minx = [r[1] for r in CATCHMENT_ROWS]
    bbox_miny = [r[2] for r in CATCHMENT_ROWS]
    bbox_maxx = [r[3] for r in CATCHMENT_ROWS]
    bbox_maxy = [r[4] for r in CATCHMENT_ROWS]
    areas = [1.0] * len(CATCHMENT_ROWS)
    up_areas = [None] * len(CATCHMENT_ROWS)
    geometries = [
        make_wkb_polygon(r[1], r[2], r[3], r[4]) for r in CATCHMENT_ROWS
    ]

    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("area_km2", pa.float32(), nullable=False),
        pa.field("up_area_km2", pa.float32(), nullable=True),
        pa.field("bbox_minx", pa.float32(), nullable=False),
        pa.field("bbox_miny", pa.float32(), nullable=False),
        pa.field("bbox_maxx", pa.float32(), nullable=False),
        pa.field("bbox_maxy", pa.float32(), nullable=False),
        pa.field("geometry", pa.binary(), nullable=False),
    ])

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "area_km2": pa.array(areas, type=pa.float32()),
            "up_area_km2": pa.array(up_areas, type=pa.float32()),
            "bbox_minx": pa.array(bbox_minx, type=pa.float32()),
            "bbox_miny": pa.array(bbox_miny, type=pa.float32()),
            "bbox_maxx": pa.array(bbox_maxx, type=pa.float32()),
            "bbox_maxy": pa.array(bbox_maxy, type=pa.float32()),
            "geometry": pa.array(geometries, type=pa.binary()),
        },
        schema=schema,
    )

    out_path = out_dir / "catchments.parquet"
    with pq.ParquetWriter(out_path, schema, compression=None, write_statistics=True) as writer:
        writer.write_table(table)


def write_graph_arrow(out_dir: Path, upstream_map: dict[int, list[int]]) -> None:
    """Write graph.arrow for the given upstream adjacency map."""
    ids_ordered = sorted(upstream_map.keys())
    upstream_lists = [upstream_map[i] for i in ids_ordered]

    id_array = pa.array(ids_ordered, type=pa.int64())
    upstream_array = pa.array(
        upstream_lists,
        type=pa.list_(pa.field("item", pa.int64(), nullable=True)),
    )

    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field(
            "upstream_ids",
            pa.list_(pa.field("item", pa.int64(), nullable=True)),
            nullable=False,
        ),
    ])

    batch = pa.record_batch(
        {"id": id_array, "upstream_ids": upstream_array},
        schema=schema,
    )

    out_path = out_dir / "graph.arrow"
    with pa_ipc.new_file(str(out_path), schema) as writer:
        writer.write_batch(batch)


def write_manifest(out_dir: Path, crs: str = "EPSG:4326") -> None:
    """Write manifest.json for the 5-atom tiny fixture."""
    manifest = {
        "format_version": "0.1",
        "fabric_name": FABRIC_NAME,
        "crs": crs,
        "has_up_area": False,
        "has_rasters": False,
        "has_snap": False,
        "terminal_sink_id": 0,
        "topology": "tree",
        "bbox": [-180.0, -90.0, 180.0, 90.0],
        "atom_count": 5,
        "created_at": CREATED_AT,
        "adapter_version": ADAPTER_VERSION,
    }
    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# SHA-256 helpers
# ---------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def print_sha_matrix(label: str, out_dir: Path) -> None:
    print(f"\n--- SHA-256 for {label} ---")
    for name in ("catchments.parquet", "graph.arrow", "manifest.json"):
        p = out_dir / name
        if p.exists():
            print(f"  {name}: {sha256_file(p)}")
        else:
            print(f"  {name}: MISSING")


# ---------------------------------------------------------------------------
# Per-fixture generators
# ---------------------------------------------------------------------------

def generate_valid_tiny() -> None:
    out_dir = VALID_TINY
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in out_dir.glob("*.parquet"):
        f.unlink()
    for f in out_dir.glob("*.arrow"):
        f.unlink()
    for f in out_dir.glob("manifest.json"):
        f.unlink()

    write_catchments_parquet(out_dir)
    write_graph_arrow(out_dir, VALID_UPSTREAM)
    write_manifest(out_dir, crs="EPSG:4326")
    print(f"[generate_fixtures] Wrote valid/tiny/ ({out_dir})")
    print_sha_matrix("valid/tiny", out_dir)


def generate_invalid_dangling() -> None:
    out_dir = INVALID_DANGLING
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in out_dir.glob("*.parquet"):
        f.unlink()
    for f in out_dir.glob("*.arrow"):
        f.unlink()
    for f in out_dir.glob("manifest.json"):
        f.unlink()

    write_catchments_parquet(out_dir)
    write_graph_arrow(out_dir, DANGLING_UPSTREAM)
    write_manifest(out_dir, crs="EPSG:4326")
    print(f"[generate_fixtures] Wrote invalid/dangling-upstream-ref/ ({out_dir})")
    print_sha_matrix("invalid/dangling-upstream-ref", out_dir)


def generate_invalid_crs() -> None:
    out_dir = INVALID_CRS
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in out_dir.glob("*.parquet"):
        f.unlink()
    for f in out_dir.glob("*.arrow"):
        f.unlink()
    for f in out_dir.glob("manifest.json"):
        f.unlink()

    write_catchments_parquet(out_dir)
    write_graph_arrow(out_dir, VALID_UPSTREAM)
    write_manifest(out_dir, crs="EPSG:32632")
    print(f"[generate_fixtures] Wrote invalid/crs-mismatch/ ({out_dir})")
    print_sha_matrix("invalid/crs-mismatch", out_dir)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    generate_valid_tiny()
    generate_invalid_dangling()
    generate_invalid_crs()
    print("\n[generate_fixtures] Done. 3 fixture directories written.")
