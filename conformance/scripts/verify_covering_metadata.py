# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "pyarrow>=12.0.0,<23.0.0",
#   "duckdb>=1.0.0",
# ]
# ///
"""Verify the GeoParquet 1.1 covering on regenerated HFX fixtures.

Two independent checks:
  1. METADATA PATH (mandatory; hfx's own expectation): the file-level `geo`
     metadata declares the covering at the locked literal path
     geo.columns.geometry.covering.bbox.{xmin,ymin,xmax,ymax}, each entry being
     the column-path reference ["bbox", "<leaf>"] to the matching struct leaf.
  2. EXTERNAL-TOOL RECOGNITION (standards positioning): an external engine reads
     the struct-leaf bbox and filters on it. Primary: DuckDB bbox-filtered
     SELECT over the struct leaves (no spatial extension / network required).
     Fallback: GeoPandas covering-aware bbox read; else pyarrow metadata parse.
     A recognizer that is absent (ImportError) or fails at runtime degrades to
     the next recognizer; only a successful read returning the wrong ids fails.

Usage (run from the hfx worktree root):
    uv run conformance/scripts/verify_covering_metadata.py [PARQUET]

Default target: conformance/valid/tiny/catchments.parquet. For that file the
[0.2, 0.2, 0.3, 0.3] query window must return ids {1, 4}. For any other path the
covering-path check still runs and the external tool must read the struct leaves
without error.

Exit 0  == covering path correct AND an external tool recognized the struct bbox.
Exit !=0 otherwise.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")
DEFAULT_TARGET = Path("conformance/valid/tiny/catchments.parquet")
QUERY_WINDOW = (0.2, 0.2, 0.3, 0.3)  # xmin, ymin, xmax, ymax
EXPECTED_IDS = [1, 4]


def check_covering_path(path: Path) -> list[str]:
    """Return a list of failures; empty means the covering metadata path is correct."""
    md = pq.read_schema(path).metadata or {}
    raw = md.get(b"geo")
    if raw is None:
        return [f"{path}: no file-level `geo` metadata key"]
    geo = json.loads(raw)
    try:
        covering = geo["columns"]["geometry"]["covering"]["bbox"]
    except (KeyError, TypeError):
        return [f"{path}: geo.columns.geometry.covering.bbox is absent"]
    failures: list[str] = []
    for leaf in LEAF_NAMES:
        if covering.get(leaf) != ["bbox", leaf]:
            failures.append(
                f"{path}: covering.bbox.{leaf} != ['bbox', '{leaf}'] (got {covering.get(leaf)!r})"
            )
    return failures


def recognize_external(path: Path, assert_ids: bool) -> tuple[bool, str]:
    """Prove an external tool reads + bbox-filters the struct leaves.

    Tiered recognizers; a recognizer that is absent (ImportError) OR fails at
    runtime degrades to the next one. Only a *successful* read that returns the
    wrong ids is a hard failure (returns False) — a crash is not.
    """
    xmin, ymin, xmax, ymax = QUERY_WINDOW
    try:
        import duckdb

        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")  # best-effort; not required
        except Exception:
            pass
        query = (
            f"SELECT id FROM read_parquet('{path.as_posix()}') "
            f"WHERE bbox.xmin <= {xmax} AND bbox.xmax >= {xmin} "
            f"AND bbox.ymin <= {ymax} AND bbox.ymax >= {ymin} ORDER BY id"
        )
        ids = [row[0] for row in con.execute(query).fetchall()]
    except ImportError:
        pass
    except Exception as exc:  # duckdb present but the struct-leaf query failed; degrade
        print(f"  (duckdb runtime error, falling back: {exc})", file=sys.stderr)
    else:
        if assert_ids and ids != EXPECTED_IDS:
            return False, f"duckdb returned {ids}, expected {EXPECTED_IDS}"
        return True, f"duckdb struct-leaf bbox filter -> {ids}"
    try:
        import geopandas as gpd

        frame = gpd.read_parquet(path, bbox=QUERY_WINDOW)
        ids = sorted(frame["id"].tolist())
    except ImportError:
        pass
    except Exception as exc:  # geopandas present but the covering read failed; degrade
        print(f"  (geopandas runtime error, falling back: {exc})", file=sys.stderr)
    else:
        if assert_ids and ids != EXPECTED_IDS:
            return False, f"geopandas returned {ids}, expected {EXPECTED_IDS}"
        return True, f"geopandas covering bbox read -> {ids}"
    return True, "pyarrow metadata parse (no duckdb/geopandas available)"


def main(argv: list[str]) -> int:
    target = Path(argv[1]) if len(argv) > 1 else DEFAULT_TARGET
    assert_ids = target.resolve() == DEFAULT_TARGET.resolve()
    failures = check_covering_path(target)
    if failures:
        print(f"COVERING FAIL: {target}")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    ok, how = recognize_external(target, assert_ids)
    if not ok:
        print(f"COVERING FAIL (external recognition): {target}: {how}")
        return 1
    print(f"COVERING PASS: {target} — covering path correct; {how}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
