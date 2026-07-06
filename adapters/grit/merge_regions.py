#!/usr/bin/env python3
"""Merge per-region GRIT HFX datasets into one global GRIT HFX dataset."""

from __future__ import annotations

import argparse
import json
import logging
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
from build_adapter import (  # noqa: E402
    ADAPTER_VERSION,
    FABRIC_NAME,
    FABRIC_VERSION,
    HAS_RASTERS,
    HAS_SNAP,
    HAS_UP_AREA,
    REGION_CODES,
    ROW_GROUP_MAX,
    ROW_GROUP_MIN,
    TOPOLOGY,
    assert_geoparquet_valid,
    balanced_row_group_bounds,
    outward_bbox,
)


logger = logging.getLogger("grit-hfx-merge")

DEFAULT_INPUTS_ROOT = Path("/Users/nicolaslazaro/Desktop/grit-hfx/per-region")
OWNED_OUTPUT_FILES = (
    "catchments.parquet",
    "snap.parquet",
    "graph.arrow",
    "manifest.json",
)
GLOBAL_TOTAL_BOUNDS = (-180.0, -90.0, 180.0, 90.0)
GLOBAL_BBOX = [-180.0, -90.0, 180.0, 90.0]


class MergeError(RuntimeError):
    """Raised by merge_regions stages. Carries structured context."""


@dataclass(frozen=True)
class PerRegionInput:
    """Holds resolved paths and metadata for one per-region GRIT HFX dataset."""

    dir: Path
    region_code: str | None
    manifest: dict
    catchments_path: Path
    snap_path: Path
    graph_path: Path
    atom_count: int
    bbox: tuple[float, float, float, float]


def _parse_region_selector(selector: str) -> str:
    """Convert a region code or directory name into a per-region directory name."""
    value = selector.strip()
    if not value:
        raise MergeError("empty region selector in comma-separated list")
    if value.startswith("grit-hfx-"):
        return value
    upper = value.upper()
    if upper in REGION_CODES:
        return f"grit-hfx-{upper.lower()}"
    return value


def _region_code_from_dir(path: Path) -> str | None:
    """Return the GRIT region code encoded in a canonical per-region directory name."""
    prefix = "grit-hfx-"
    if not path.name.startswith(prefix):
        return None
    code = path.name[len(prefix):].upper()
    return code if code in REGION_CODES else None


def stage_1_discover_inputs(input_root: Path, region_selectors: list[str] | None) -> list[PerRegionInput]:
    """Glob input root for per-region HFX directories and load their manifests."""
    if region_selectors:
        dirs = [input_root / _parse_region_selector(selector) for selector in region_selectors]
    else:
        dirs = sorted(input_root.glob("grit-hfx-*"))

    if not dirs:
        raise MergeError(f"no per-region directories found under {input_root}")

    results: list[PerRegionInput] = []
    for d in dirs:
        d = Path(d)
        if not d.is_dir():
            raise MergeError(f"input region directory does not exist: {d}")

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
        results.append(
            PerRegionInput(
                dir=d,
                region_code=_region_code_from_dir(d),
                manifest=manifest,
                catchments_path=catchments_path,
                snap_path=snap_path,
                graph_path=graph_path,
                atom_count=atom_count,
                bbox=bbox,
            )
        )

    results.sort(key=lambda r: r.dir.name)
    logger.info("stage_1_discover_inputs: found %d region(s)", len(results))
    return results


def _validate_consensus(regions: list[PerRegionInput], field: str) -> None:
    reference = regions[0].manifest
    ref_val = reference.get(field)
    for region in regions[1:]:
        val = region.manifest.get(field)
        if val != ref_val:
            raise MergeError(
                f"consensus mismatch on '{field}': "
                f"{regions[0].dir.name}={ref_val!r} vs {region.dir.name}={val!r}"
            )


def stage_2_validate_inputs(regions: list[PerRegionInput]) -> int:
    """Cross-validate region manifests for consensus fields; return global atom count."""
    consensus_fields = (
        "format_version",
        "fabric_name",
        "fabric_version",
        "crs",
        "topology",
        "terminal_sink_id",
        "adapter_version",
    )
    for field in consensus_fields:
        _validate_consensus(regions, field)

    for region in regions:
        manifest = region.manifest
        if manifest.get("fabric_name") != FABRIC_NAME:
            raise MergeError(f"region {region.dir.name} has fabric_name != {FABRIC_NAME!r}")
        if manifest.get("fabric_version", FABRIC_VERSION) != FABRIC_VERSION:
            raise MergeError(f"region {region.dir.name} has fabric_version != {FABRIC_VERSION!r}")
        if manifest.get("topology") != TOPOLOGY:
            raise MergeError(f"region {region.dir.name} has topology != {TOPOLOGY!r}")
        if manifest.get("has_snap") is not HAS_SNAP:
            raise MergeError(f"region {region.dir.name} has has_snap != True")
        if manifest.get("has_rasters") is not HAS_RASTERS:
            raise MergeError(f"region {region.dir.name} has has_rasters != False")
        if manifest.get("has_up_area") is not HAS_UP_AREA:
            raise MergeError(f"region {region.dir.name} has has_up_area != False")

        meta = pq.read_metadata(region.catchments_path)
        if meta.num_rows != region.atom_count:
            raise MergeError(
                f"region {region.dir.name}: manifest.atom_count={region.atom_count} "
                f"but catchments.parquet has {meta.num_rows} rows"
            )

    global_count = sum(region.atom_count for region in regions)
    logger.info(
        "stage_2_validate_inputs: %d region(s) ok, global_atom_count=%d",
        len(regions),
        global_count,
    )
    return global_count


def _build_hilbert_sort_index(
    regions: list[PerRegionInput],
    parquet_attr: str,
) -> np.ndarray:
    """Return argsort index over (hilbert, region_idx, row_idx) for a parquet layer."""
    region_indices: list[np.ndarray] = []
    row_indices: list[np.ndarray] = []
    hilbert_values: list[np.ndarray] = []

    for region_idx, region in enumerate(regions):
        path = getattr(region, parquet_attr)
        tbl = pq.read_table(path, columns=["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"])
        cx = (np.array(tbl["bbox_minx"], dtype="float64") + np.array(tbl["bbox_maxx"], dtype="float64")) / 2.0
        cy = (np.array(tbl["bbox_miny"], dtype="float64") + np.array(tbl["bbox_maxy"], dtype="float64")) / 2.0

        pts = shapely.points(cx, cy)
        h = GeoSeries(pts).hilbert_distance(total_bounds=GLOBAL_TOTAL_BOUNDS)
        n = len(tbl)
        region_indices.append(np.full(n, region_idx, dtype="int32"))
        row_indices.append(np.arange(n, dtype="int32"))
        hilbert_values.append(h.to_numpy(dtype="int64"))

    all_region = np.concatenate(region_indices)
    all_row = np.concatenate(row_indices)
    all_hilbert = np.concatenate(hilbert_values)

    sort_keys = np.lexsort((all_row, all_region, all_hilbert))
    return np.column_stack([all_region[sort_keys], all_row[sort_keys]]).astype("int32")


def _stream_write_parquet(
    tables: list[pa.Table],
    sort_index: np.ndarray,
    out_path: Path,
    total_rows: int,
) -> None:
    """Write a merged parquet file from pre-loaded tables in global Hilbert order."""
    schema = tables[0].schema
    columns_by_table = [
        [table.column(col_idx) for col_idx in range(len(schema))]
        for table in tables
    ]
    row_groups = balanced_row_group_bounds(total_rows, ROW_GROUP_MIN, ROW_GROUP_MAX)
    logger.info("writing %d rows in %d row groups -> %s", total_rows, len(row_groups), out_path)

    with pq.ParquetWriter(out_path, schema=schema, compression="snappy", write_statistics=True) as writer:
        for start, stop in row_groups:
            slice_idx = sort_index[start:stop]
            arrays: list[pa.Array] = []

            for col_idx, field in enumerate(schema):
                region_columns = [columns[col_idx] for columns in columns_by_table]
                values = [
                    region_columns[int(region_idx)][int(row_idx)].as_py()
                    for region_idx, row_idx in slice_idx
                ]
                arrays.append(pa.array(values, type=field.type))

            writer.write_table(pa.Table.from_arrays(arrays, schema=schema))

    assert_geoparquet_valid(out_path)
    logger.info("%s validated ok", out_path.name)


def stage_3_build_global_sort_index_catchments(regions: list[PerRegionInput]) -> np.ndarray:
    """Build global Hilbert sort index for catchments using bbox-center proxies."""
    idx = _build_hilbert_sort_index(regions, "catchments_path")
    logger.info("stage_3_build_global_sort_index_catchments: %d total rows indexed", len(idx))
    return idx


def stage_4_stream_write_catchments(
    regions: list[PerRegionInput],
    sort_index: np.ndarray,
    out_dir: Path,
    global_atom_count: int,
) -> None:
    """Write merged catchments.parquet in global Hilbert order."""
    logger.info("stage_4_stream_write_catchments: reading all %d region catchment tables", len(regions))
    tables = [pq.read_table(region.catchments_path) for region in regions]
    _stream_write_parquet(tables, sort_index, out_dir / "catchments.parquet", global_atom_count)


def stage_5_build_global_sort_index_snap(regions: list[PerRegionInput]) -> np.ndarray:
    """Build global Hilbert sort index for snap using bbox-center proxies."""
    idx = _build_hilbert_sort_index(regions, "snap_path")
    logger.info("stage_5_build_global_sort_index_snap: %d total rows indexed", len(idx))
    return idx


def stage_6_stream_write_snap(
    regions: list[PerRegionInput],
    sort_index: np.ndarray,
    out_dir: Path,
    global_atom_count: int,
) -> None:
    """Write merged snap.parquet in global Hilbert order."""
    logger.info("stage_6_stream_write_snap: reading all %d region snap tables", len(regions))
    tables = [pq.read_table(region.snap_path) for region in regions]
    _stream_write_parquet(tables, sort_index, out_dir / "snap.parquet", global_atom_count)


def stage_7_concat_graph(regions: list[PerRegionInput], out_dir: Path) -> None:
    """Concatenate per-region graph.arrow files, checking for id collisions."""
    tables: list[pa.Table] = []
    seen_ids: set[int] = set()
    collision_samples: list[int] = []

    for region in regions:
        with pa_ipc.open_file(region.graph_path) as f:
            tbl = f.read_all()
        region_ids = tbl.column("id").to_pylist()
        for id_val in region_ids:
            if id_val in seen_ids:
                if len(collision_samples) < 10:
                    collision_samples.append(id_val)
            else:
                seen_ids.add(id_val)
        if collision_samples:
            raise MergeError(
                f"graph id collisions across regions, first offenders: {collision_samples}"
            )
        tables.append(tbl)

    merged = pa.concat_tables(tables).sort_by("id")
    out_path = out_dir / "graph.arrow"
    with pa.OSFile(str(out_path), "wb") as sink:
        with pa_ipc.new_file(sink, merged.schema) as writer:
            writer.write(merged)
    logger.info("stage_7_concat_graph: wrote %d graph nodes -> %s", len(merged), out_path)


def _manifest_bbox(regions: list[PerRegionInput]) -> list[float]:
    """Return the global manifest bbox according to full-vs-partial region coverage."""
    present_codes = {region.region_code for region in regions if region.region_code is not None}
    if present_codes == set(REGION_CODES):
        return GLOBAL_BBOX

    raw_bbox = (
        min(region.bbox[0] for region in regions),
        min(region.bbox[1] for region in regions),
        max(region.bbox[2] for region in regions),
        max(region.bbox[3] for region in regions),
    )
    padded = outward_bbox(raw_bbox)
    return [
        max(-180.0, padded[0]),
        max(-90.0, padded[1]),
        min(180.0, padded[2]),
        min(90.0, padded[3]),
    ]


def stage_8_write_manifest(
    regions: list[PerRegionInput],
    out_dir: Path,
    global_atom_count: int,
) -> None:
    """Write merged manifest.json omitting 'region' for the global dataset."""
    reference = regions[0].manifest
    fabric_version = reference.get("fabric_version", FABRIC_VERSION)

    actual_rows = pq.read_metadata(out_dir / "catchments.parquet").num_rows
    if actual_rows != global_atom_count:
        raise MergeError(
            f"atom_count mismatch: manifest expects {global_atom_count} "
            f"but catchments.parquet has {actual_rows} rows"
        )

    manifest = {
        "format_version": reference["format_version"],
        "fabric_name": FABRIC_NAME,
        "fabric_version": fabric_version,
        "crs": reference["crs"],
        "has_up_area": False,
        "has_rasters": False,
        "has_snap": True,
        "terminal_sink_id": reference["terminal_sink_id"],
        "topology": TOPOLOGY,
        "bbox": _manifest_bbox(regions),
        "atom_count": global_atom_count,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adapter_version": reference.get("adapter_version", ADAPTER_VERSION),
    }

    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    logger.info(
        "stage_8_write_manifest: wrote %s fabric_name=%s atom_count=%d",
        out_path,
        manifest["fabric_name"],
        global_atom_count,
    )


def stage_9_validate(out_dir: Path, skip_validate: bool) -> None:
    """Run GeoParquet and optional hfx-cli checks on the merged output."""
    assert_geoparquet_valid(out_dir / "catchments.parquet")
    logger.info("stage_9_validate: catchments.parquet passed GeoParquet 1.1 validation")
    assert_geoparquet_valid(out_dir / "snap.parquet")
    logger.info("stage_9_validate: snap.parquet passed GeoParquet 1.1 validation")

    if skip_validate:
        logger.info("stage_9_validate: --skip-validate set; skipping hfx CLI validation")
        return

    hfx_bin = shutil.which("hfx")
    if hfx_bin is None:
        logger.warning(
            "stage_9_validate: 'hfx' not found on PATH; "
            "skipping hfx CLI validation (install hfx-cli to enable)"
        )
        return

    cmd = [
        hfx_bin,
        str(out_dir),
        "--strict",
        "--sample-pct",
        "100",
        "--format",
        "text",
        "--skip-rasters",
    ]
    logger.info("stage_9_validate: running %s", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise MergeError(f"hfx validator exited with code {result.returncode}")
    logger.info("stage_9_validate: hfx validator passed")


def _prepare_output_dir(out_dir: Path, force: bool) -> None:
    """Create the output directory and remove only owned artifacts when forced."""
    if out_dir.exists() and not out_dir.is_dir():
        raise MergeError(f"output path exists and is not a directory: {out_dir}")

    if out_dir.exists() and any(out_dir.iterdir()) and not force:
        raise MergeError(
            f"output directory already exists and is not empty: {out_dir}. "
            "Use --force to overwrite owned output files."
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    if not force:
        return

    for name in OWNED_OUTPUT_FILES:
        path = out_dir / name
        if path.exists():
            if not path.is_file():
                raise MergeError(f"owned output path exists but is not a file: {path}")
            path.unlink()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge per-region GRIT HFX datasets into one global HFX dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--inputs-root",
        "--input-root",
        dest="inputs_root",
        type=Path,
        default=DEFAULT_INPUTS_ROOT,
        metavar="PATH",
        help="root directory containing per-region GRIT HFX directories",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        metavar="PATH",
        help="output dataset directory; writes final HFX files directly here",
    )
    parser.add_argument(
        "--input-regions",
        "--regions",
        dest="input_regions",
        type=str,
        default=None,
        metavar="LIST",
        help="comma-separated region codes or directory names to merge",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing owned output artifacts",
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
        region_selectors = (
            [region.strip() for region in args.input_regions.split(",")]
            if args.input_regions
            else None
        )
        regions = stage_1_discover_inputs(args.inputs_root, region_selectors)
        global_atom_count = stage_2_validate_inputs(regions)

        if args.dry_run:
            logger.info(
                "--dry-run: inputs valid; %d region(s), %d total atoms. Exiting.",
                len(regions),
                global_atom_count,
            )
            return 0

        _prepare_output_dir(args.output, args.force)

        sort_idx_catchments = stage_3_build_global_sort_index_catchments(regions)
        stage_4_stream_write_catchments(regions, sort_idx_catchments, args.output, global_atom_count)

        sort_idx_snap = stage_5_build_global_sort_index_snap(regions)
        stage_6_stream_write_snap(regions, sort_idx_snap, args.output, global_atom_count)

        stage_7_concat_graph(regions, args.output)
        stage_8_write_manifest(regions, args.output, global_atom_count)
        stage_9_validate(args.output, args.skip_validate)

        logger.info("merge complete: output at %s", args.output)
        return 0

    except (MergeError, RuntimeError) as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
