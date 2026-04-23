#!/usr/bin/env python3
"""Merge N per-basin HFX datasets into one global HFX dataset.

Memory budget (N=61 full run):
  pre-read 61 basin catchments   ~3.7 GB
  sort-index arrays               ~82 MB
  graph concat                    ~55 MB
  pyarrow writer buffers         ~100 MB
  Target                        < 4.5 GB

Usage::

    python merge_basins.py \\
        --input-root /path/to/per-basin \\
        --output-root /tmp/merit-global \\
        [--input-basins merit-hfx-pfaf27,merit-hfx-pfaf28] \\
        [--rasters-ready] \\
        [--force] \\
        [--dry-run] \\
        [--skip-validate] \\
        [--log-level DEBUG]
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import shapely
from geopandas import GeoSeries

sys.path.insert(0, str(Path(__file__).parent))
from build_adapter import (
    ADAPTER_VERSION,
    FLOW_DIR_ENCODING,
    MANIFEST_BBOX_EPSILON,
    ROW_GROUP_MAX,
    ROW_GROUP_MIN,
    assert_geoparquet_valid,
    balanced_row_group_bounds,
    build_geo_metadata,
    outward_bbox,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("merit-hfx-merge")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class MergeError(RuntimeError):
    """Raised by merge_basins stages. Carries structured context."""


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PerBasinInput:
    """Holds resolved paths and metadata for one per-basin HFX dataset."""

    dir: Path
    manifest: dict
    catchments_path: Path
    snap_path: Path
    graph_path: Path
    atom_count: int
    bbox: tuple[float, float, float, float]


# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------

def stage_1_discover_inputs(input_root: Path, basin_names: list[str] | None) -> list[PerBasinInput]:
    """Glob input root for per-basin HFX directories and load their manifests."""
    if basin_names:
        dirs = [input_root / name for name in basin_names]
    else:
        dirs = sorted(input_root.glob("merit-hfx-pfaf*"))
        # Back-compat: also scan parent of input_root for stray pfaf dirs.
        parent_extras = sorted(input_root.parent.glob("merit-hfx-pfaf*"))
        seen = {d.resolve() for d in dirs}
        for d in parent_extras:
            if d.resolve() not in seen:
                dirs.append(d)
        dirs = sorted(set(dirs), key=lambda p: p.name)

    if not dirs:
        raise MergeError(f"no per-basin directories found under {input_root}")

    results: list[PerBasinInput] = []
    for d in dirs:
        d = Path(d)
        if not d.is_dir():
            raise MergeError(f"input basin directory does not exist: {d}")
        manifest_path = d / "manifest.json"
        catchments_path = d / "catchments.parquet"
        snap_path = d / "snap.parquet"
        graph_path = d / "graph.arrow"
        for p in (manifest_path, catchments_path, snap_path, graph_path):
            if not p.exists():
                raise MergeError(f"missing required file: {p}")
        manifest = json.loads(manifest_path.read_text())
        atom_count = int(manifest["atom_count"])
        bbox_list = manifest["bbox"]
        bbox = (float(bbox_list[0]), float(bbox_list[1]), float(bbox_list[2]), float(bbox_list[3]))
        results.append(PerBasinInput(
            dir=d,
            manifest=manifest,
            catchments_path=catchments_path,
            snap_path=snap_path,
            graph_path=graph_path,
            atom_count=atom_count,
            bbox=bbox,
        ))

    logger.info("stage_1_discover_inputs: found %d basin(s)", len(results))
    return results


def stage_2_validate_inputs(basins: list[PerBasinInput]) -> int:
    """Cross-validate basin manifests for consensus fields; return global atom count."""
    consensus_fields = ("format_version", "crs", "flow_dir_encoding", "topology",
                        "terminal_sink_id", "adapter_version")
    reference = basins[0].manifest
    for field in consensus_fields:
        ref_val = reference.get(field)
        for b in basins[1:]:
            val = b.manifest.get(field)
            if val != ref_val:
                raise MergeError(
                    f"consensus mismatch on '{field}': "
                    f"{basins[0].dir.name}={ref_val!r} vs {b.dir.name}={val!r}"
                )

    for b in basins:
        if not b.manifest.get("has_snap"):
            raise MergeError(f"basin {b.dir.name} has has_snap != True")
        if not b.manifest.get("has_up_area"):
            raise MergeError(f"basin {b.dir.name} has has_up_area != True")
        meta = pq.read_metadata(b.catchments_path)
        if meta.num_rows != b.atom_count:
            raise MergeError(
                f"basin {b.dir.name}: manifest.atom_count={b.atom_count} "
                f"but catchments.parquet has {meta.num_rows} rows"
            )

    global_count = sum(b.atom_count for b in basins)
    logger.info("stage_2_validate_inputs: %d basins ok, global_atom_count=%d", len(basins), global_count)
    return global_count


def _build_hilbert_sort_index(
    basins: list[PerBasinInput],
    parquet_attr: str,
) -> np.ndarray:
    """Return argsort index over (hilbert, basin_idx, row_idx) for a parquet layer."""
    basin_indices: list[np.ndarray] = []
    row_indices: list[np.ndarray] = []
    hilbert_values: list[np.ndarray] = []

    for b_idx, basin in enumerate(basins):
        path = getattr(basin, parquet_attr)
        tbl = pq.read_table(path, columns=["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"])
        cx = (np.array(tbl["bbox_minx"], dtype="float64") + np.array(tbl["bbox_maxx"], dtype="float64")) / 2.0
        cy = (np.array(tbl["bbox_miny"], dtype="float64") + np.array(tbl["bbox_maxy"], dtype="float64")) / 2.0

        pts = shapely.points(cx, cy)
        h = GeoSeries(pts).hilbert_distance(
            total_bounds=(-180.0, -90.0, 180.0, 90.0)
        )
        n = len(tbl)
        basin_indices.append(np.full(n, b_idx, dtype="int32"))
        row_indices.append(np.arange(n, dtype="int32"))
        hilbert_values.append(h.to_numpy(dtype="int64"))

    all_basin = np.concatenate(basin_indices)
    all_row = np.concatenate(row_indices)
    all_hilbert = np.concatenate(hilbert_values)

    # Structured sort: (hilbert, basin_idx, row_idx) for full determinism.
    sort_keys = np.lexsort((all_row, all_basin, all_hilbert))
    stacked = np.column_stack([all_basin[sort_keys], all_row[sort_keys]]).astype("int32")
    return stacked  # shape (N, 2): col0=basin_idx, col1=row_idx


def _stream_write_parquet(
    tables: list[pa.Table],
    sort_index: np.ndarray,
    out_path: Path,
    total_rows: int,
    kind: str,
) -> None:
    """Write a merged parquet file from pre-loaded tables in global Hilbert order."""
    schema = tables[0].schema
    row_groups = balanced_row_group_bounds(total_rows, ROW_GROUP_MIN, ROW_GROUP_MAX)
    logger.info("writing %d rows in %d row groups -> %s", total_rows, len(row_groups), out_path)

    with pq.ParquetWriter(out_path, schema=schema, compression="snappy", write_statistics=True) as writer:
        for start, stop in row_groups:
            slice_idx = sort_index[start:stop]  # shape (slice_size, 2): col0=basin_idx, col1=row_idx
            basin_ids = slice_idx[:, 0]
            row_ids = slice_idx[:, 1]

            # Gather one sub-table per basin, tracking where each row lands in
            # the concatenated result so we can restore the original order.
            parts: list[pa.Table] = []
            part_positions: list[np.ndarray] = []
            for b_idx in np.unique(basin_ids):
                mask = basin_ids == b_idx
                rows_for_basin = row_ids[mask]
                parts.append(tables[int(b_idx)].take(pa.array(rows_for_basin)))
                part_positions.append(np.where(mask)[0])

            # pa.concat_tables is zero-copy with matching schemas.
            concat_tbl = pa.concat_tables(parts)
            # concat_positions[i] = position in the final sorted slice that
            # concat row i must occupy.  argsort inverts that mapping.
            concat_positions = np.concatenate(part_positions)
            perm = np.argsort(concat_positions, kind="stable")
            sorted_tbl = concat_tbl.take(pa.array(perm))

            writer.write_table(sorted_tbl)

    assert_geoparquet_valid(out_path, kind=kind)
    logger.info("%s validated ok", out_path.name)


def stage_3_build_global_sort_index_catchments(basins: list[PerBasinInput]) -> np.ndarray:
    """Build global Hilbert sort index for catchments (bbox-center proxy)."""
    idx = _build_hilbert_sort_index(basins, "catchments_path")
    logger.info("stage_3_build_global_sort_index_catchments: %d total rows indexed", len(idx))
    return idx


def stage_4_stream_write_catchments(
    basins: list[PerBasinInput],
    sort_index: np.ndarray,
    out_dir: Path,
    global_atom_count: int,
) -> None:
    """Write merged catchments.parquet in global Hilbert order."""
    logger.info("stage_4_stream_write_catchments: reading all %d basin catchment tables", len(basins))
    tables = [pq.read_table(b.catchments_path) for b in basins]
    _stream_write_parquet(tables, sort_index, out_dir / "catchments.parquet", global_atom_count, "catchments")


def stage_5_build_global_sort_index_snap(basins: list[PerBasinInput]) -> np.ndarray:
    """Build global Hilbert sort index for snap (bbox-center proxy)."""
    idx = _build_hilbert_sort_index(basins, "snap_path")
    logger.info("stage_5_build_global_sort_index_snap: %d total rows indexed", len(idx))
    return idx


def stage_6_stream_write_snap(
    basins: list[PerBasinInput],
    sort_index: np.ndarray,
    out_dir: Path,
    global_atom_count: int,
) -> None:
    """Write merged snap.parquet in global Hilbert order."""
    logger.info("stage_6_stream_write_snap: reading all %d basin snap tables", len(basins))
    tables = [pq.read_table(b.snap_path) for b in basins]
    _stream_write_parquet(tables, sort_index, out_dir / "snap.parquet", global_atom_count, "snap")


def stage_7_concat_graph(basins: list[PerBasinInput], out_dir: Path) -> None:
    """Concatenate per-basin graph.arrow files, checking for id collisions."""
    tables: list[pa.Table] = []
    seen_ids: set[int] = set()
    collision_samples: list[int] = []

    for basin in basins:
        with pa_ipc.open_file(basin.graph_path) as f:
            tbl = f.read_all()
        basin_ids = tbl.column("id").to_pylist()
        for id_val in basin_ids:
            if id_val in seen_ids:
                if len(collision_samples) < 10:
                    collision_samples.append(id_val)
            else:
                seen_ids.add(id_val)
        if collision_samples:
            raise MergeError(
                f"graph id collisions across basins, first offenders: {collision_samples}"
            )
        tables.append(tbl)

    merged = pa.concat_tables(tables).sort_by("id")
    schema = merged.schema
    out_path = out_dir / "graph.arrow"
    with pa.OSFile(str(out_path), "wb") as sink:
        with pa_ipc.new_file(sink, schema) as writer:
            writer.write(merged)
    logger.info("stage_7_concat_graph: wrote %d graph nodes -> %s", len(merged), out_path)


def stage_8_write_manifest(
    basins: list[PerBasinInput],
    out_dir: Path,
    global_atom_count: int,
    rasters_ready: bool,
) -> None:
    """Write merged manifest.json omitting 'region' (global dataset pattern)."""
    reference = basins[0].manifest

    # Envelope of all per-basin bboxes.
    all_minx = [b.bbox[0] for b in basins]
    all_miny = [b.bbox[1] for b in basins]
    all_maxx = [b.bbox[2] for b in basins]
    all_maxy = [b.bbox[3] for b in basins]
    raw_bbox = (min(all_minx), min(all_miny), max(all_maxx), max(all_maxy))
    padded = outward_bbox(raw_bbox, pad=MANIFEST_BBOX_EPSILON)
    # Clamp to geographic limits.
    clamped_bbox = [
        max(-180.0, padded[0]),
        max(-90.0, padded[1]),
        min(180.0, padded[2]),
        min(90.0, padded[3]),
    ]

    # Verify final parquet row count matches expected atom_count.
    actual_rows = pq.read_metadata(out_dir / "catchments.parquet").num_rows
    if actual_rows != global_atom_count:
        raise MergeError(
            f"atom_count mismatch: manifest expects {global_atom_count} "
            f"but catchments.parquet has {actual_rows} rows"
        )

    manifest = {
        "format_version": reference["format_version"],
        "fabric_name": "merit_basins",
        "crs": reference["crs"],
        "has_up_area": True,
        "has_rasters": rasters_ready,
        "has_snap": True,
        "flow_dir_encoding": reference["flow_dir_encoding"],
        "terminal_sink_id": reference["terminal_sink_id"],
        "topology": reference["topology"],
        "bbox": clamped_bbox,
        "atom_count": global_atom_count,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adapter_version": reference["adapter_version"],
    }
    # 'region' intentionally omitted — global dataset per HFX spec §Deployment Patterns.

    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    logger.info(
        "stage_8_write_manifest: wrote %s fabric_name=%s atom_count=%d",
        out_path, manifest["fabric_name"], global_atom_count,
    )


def stage_9_validate(out_dir: Path, rasters_ready: bool, skip_validate: bool) -> None:
    """Run GeoParquet and optional hfx-validator checks on the merged output."""
    assert_geoparquet_valid(out_dir / "catchments.parquet", kind="catchments")
    logger.info("stage_9_validate: catchments.parquet passed GeoParquet 1.1 validation")
    assert_geoparquet_valid(out_dir / "snap.parquet", kind="snap")
    logger.info("stage_9_validate: snap.parquet passed GeoParquet 1.1 validation")

    if skip_validate:
        logger.info("stage_9_validate: --skip-validate set; skipping hfx CLI validation")
        return

    hfx_bin = shutil.which("hfx")
    if hfx_bin is None:
        logger.warning(
            "stage_9_validate: 'hfx' not found on PATH; "
            "skipping hfx CLI validation (install hfx-validator to enable)"
        )
        return

    cmd = [hfx_bin, str(out_dir), "--strict", "--sample-pct", "100", "--format", "text"]
    if not rasters_ready:
        cmd.append("--skip-rasters")

    logger.info("stage_9_validate: running %s", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise MergeError(f"hfx validator exited with code {result.returncode}")
    logger.info("stage_9_validate: hfx validator passed")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge N per-basin HFX datasets into one global HFX dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("/Users/nicolaslazaro/Desktop/merit-hfx/per-basin"),
        metavar="PATH",
        help="root directory containing per-basin HFX directories",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        required=True,
        metavar="PATH",
        help="output root; writes into <output-root>/hfx/",
    )
    parser.add_argument(
        "--input-basins",
        type=str,
        default=None,
        metavar="LIST",
        help="comma-separated basin directory names to merge (default: glob merit-hfx-pfaf*)",
    )
    parser.add_argument(
        "--rasters-ready",
        action="store_true",
        help="set manifest has_rasters=true (rasters must be provided separately)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing output artifacts",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="discover and validate inputs only; do not write any output",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="skip hfx CLI validation step",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="logging verbosity",
    )
    return parser.parse_args()


def main() -> int:
    """Orchestrate the merge pipeline."""
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    try:
        basin_names = [n.strip() for n in args.input_basins.split(",")] if args.input_basins else None
        basins = stage_1_discover_inputs(args.input_root, basin_names)
        global_atom_count = stage_2_validate_inputs(basins)

        if args.dry_run:
            logger.info("--dry-run: inputs valid; %d basins, %d total atoms. Exiting.", len(basins), global_atom_count)
            return 0

        hfx_dir = args.output_root / "hfx"
        if hfx_dir.exists() and not args.force:
            raise MergeError(
                f"output directory already exists: {hfx_dir}. "
                "Use --force to overwrite."
            )
        hfx_dir.mkdir(parents=True, exist_ok=True)

        sort_idx_catchments = stage_3_build_global_sort_index_catchments(basins)
        stage_4_stream_write_catchments(basins, sort_idx_catchments, hfx_dir, global_atom_count)

        sort_idx_snap = stage_5_build_global_sort_index_snap(basins)
        stage_6_stream_write_snap(basins, sort_idx_snap, hfx_dir, global_atom_count)

        stage_7_concat_graph(basins, hfx_dir)
        stage_8_write_manifest(basins, hfx_dir, global_atom_count, args.rasters_ready)
        stage_9_validate(hfx_dir, args.rasters_ready, args.skip_validate)

        logger.info("merge complete: output at %s", hfx_dir)
        return 0

    except MergeError as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
