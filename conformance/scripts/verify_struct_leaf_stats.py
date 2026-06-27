# /// script
# requires-python = ">=3.11,<3.14"
# dependencies = [
#   "shapely>=2.0.0",
#   "pyarrow>=12.0.0,<23.0.0",
# ]
# ///
"""GATE-1 verifier: assert per-row-group min/max stats on bbox struct leaves.

Usage:
    uv run conformance/scripts/verify_struct_leaf_stats.py [PARQUET]

If PARQUET is omitted, a throwaway prototype is built via bbox_struct and
verified, so the gate is runnable with zero setup. Reused unchanged by s06
against the regenerated canonical fixtures, e.g.:

    uv run conformance/scripts/verify_struct_leaf_stats.py \
        conformance/valid/tiny/catchments.parquet

Exit 0  == every row group carries has_min_max stats on
           bbox.xmin / bbox.ymin / bbox.xmax / bbox.ymax.
Exit !=0 otherwise.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

REQUIRED_LEAF_PATHS = ("bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax")


def verify(parquet_path: Path) -> list[str]:
    """Return a list of human-readable failures; empty means the gate passes."""
    failures: list[str] = []
    md = pq.ParquetFile(parquet_path).metadata
    if md.num_row_groups == 0:
        return [f"{parquet_path}: file has zero row groups"]
    for rg in range(md.num_row_groups):
        row_group = md.row_group(rg)
        by_path = {
            row_group.column(ci).path_in_schema: row_group.column(ci)
            for ci in range(md.num_columns)
        }
        for leaf in REQUIRED_LEAF_PATHS:
            col = by_path.get(leaf)
            if col is None:
                failures.append(f"row group {rg}: leaf column '{leaf}' is absent")
                continue
            if not col.is_stats_set:
                failures.append(f"row group {rg}: leaf '{leaf}' has no statistics block")
                continue
            stats = col.statistics
            if stats is None or not stats.has_min_max:
                failures.append(f"row group {rg}: leaf '{leaf}' missing has_min_max")
    return failures


def main(argv: list[str]) -> int:
    if len(argv) > 1:
        target = Path(argv[1])
    else:
        # Bulletproof sibling import regardless of cwd / uv launch dir.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from bbox_struct import write_prototype_struct_catchments

        target = Path(tempfile.mkdtemp(prefix="hfx-bbox-proto-")) / "catchments.parquet"
        write_prototype_struct_catchments(target)
        print(f"[verify] no path given; built prototype at {target}")

    failures = verify(target)
    if failures:
        print(f"GATE 1 FAIL: {target}")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(
        f"GATE 1 PASS: {target} — bbox.{{xmin,ymin,xmax,ymax}} carry "
        f"has_min_max stats in every row group"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
