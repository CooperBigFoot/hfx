#!/usr/bin/env python3
"""Verify MERIT-Basins cross-basin invariants before HFX v0.2.1 compilation."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyogrio
import rasterio
from rasterio.errors import RasterioIOError


VALID_PFAF_CODES: tuple[int, ...] = (
    11, 12, 13, 14, 15, 16, 17, 18,
    21, 22, 23, 24, 25, 26, 27, 28, 29,
    31, 32, 33, 34, 35, 36,
    41, 42, 43, 44, 45, 46, 47, 48, 49,
    51, 52, 53, 54, 55, 56, 57,
    61, 62, 63, 64, 65, 66, 67,
    71, 72, 73, 74, 75, 76, 77, 78,
    81, 82, 83, 84, 85, 86,
    91,
)
EXCLUDED_PFAF_CODES = frozenset({35})
DEFAULT_SELECTED_PFAF_CODES = tuple(
    code for code in VALID_PFAF_CODES if code not in EXCLUDED_PFAF_CODES
)
assert len(DEFAULT_SELECTED_PFAF_CODES) == 60

DEFAULT_ROOT = Path("/Users/nicolaslazaro/Desktop/merit-hfx-v2")
DEFAULT_MERIT_BASINS_ROOT = Path("~/data/merit_basins/pfaf_level_02").expanduser()
DEFAULT_RASTERS_ROOT = Path("~/data/merit_hydro_rasters").expanduser()
GOOGLE_DRIVE_SOURCE = "GoogleDrive:MERIT-Hydro_v07_Basins_v01_bugfix1/pfaf_level_02/"
VECTOR_EXTENSIONS = ("shp", "shx", "dbf", "cpg")
REPORT_SCHEMA_VERSION = 1
SAMPLE_LIMIT = 10
MERIT_PIXEL_DEGREES = 1.0 / 1200.0

logger = logging.getLogger("merit-v2-preflight")


class PreflightError(RuntimeError):
    """Raised when MERIT v2 preflight cannot complete."""


@dataclass(frozen=True)
class BasinPaths:
    """Resolved Phase 0 inputs for one Pfaf-L2 basin."""

    pfaf: int
    catchments: Path
    rivers: Path
    flow_dir: Path
    flow_acc: Path


def parse_pfaf_codes(raw: str | None) -> list[int]:
    """Parse a comma-separated Pfaf code list."""
    if raw is None or raw.strip() == "":
        return list(DEFAULT_SELECTED_PFAF_CODES)
    if raw.strip().lower() == "all":
        return list(DEFAULT_SELECTED_PFAF_CODES)

    codes = [int(part.strip()) for part in raw.split(",") if part.strip()]
    unknown = [code for code in codes if code not in VALID_PFAF_CODES]
    if unknown:
        raise PreflightError(
            f"unknown Pfaf code(s): {unknown}; expected one of {list(VALID_PFAF_CODES)}"
        )
    selected_excluded = [code for code in codes if code in EXCLUDED_PFAF_CODES]
    if selected_excluded:
        raise PreflightError(
            f"excluded Pfaf code(s) requested: {selected_excluded}; "
            "pfaf-35 is deliberately outside the v2 build set"
        )
    return codes


def ensure_dir(path: Path) -> None:
    """Create a directory and its parents."""
    path.mkdir(parents=True, exist_ok=True)


def configure_logging(level: str) -> None:
    """Configure structured-enough CLI logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _basins_dir(root: Path) -> Path:
    candidate = root / "pfaf_level_02"
    return candidate if candidate.is_dir() else root


def _single_match(root: Path, pattern: str) -> Path | None:
    matches = sorted(path for path in root.glob(pattern) if path.stat().st_size > 0)
    if len(matches) == 1:
        return matches[0]
    return None


def vector_files_present(pfaf: int, merit_basins_root: Path) -> bool:
    """Return True when all expected cat/riv shapefile sidecars are present."""
    root = _basins_dir(merit_basins_root)
    for prefix in ("cat", "riv"):
        for ext in VECTOR_EXTENSIONS:
            pattern = f"{prefix}_pfaf_{pfaf:02d}_*.{ext}"
            if _single_match(root, pattern) is None:
                return False
    return True


def tif_valid(path: Path) -> bool:
    """Return True when a raster exists and opens successfully with rasterio."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with rasterio.open(path) as src:
            return bool(src.width > 0 and src.height > 0 and src.count >= 1)
    except RasterioIOError:
        return False


def download_vectors(pfaf: int, merit_basins_root: Path) -> None:
    """Download missing MERIT-Basins vector sidecars via rclone."""
    ensure_dir(merit_basins_root)
    include = f"*pfaf_{pfaf:02d}_*"
    result = subprocess.run(
        [
            "rclone",
            "copy",
            "--drive-shared-with-me",
            GOOGLE_DRIVE_SOURCE,
            str(merit_basins_root),
            "--include",
            include,
        ],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    if result.returncode != 0:
        raise PreflightError(
            f"rclone failed for pfaf-{pfaf:02d}: {result.stderr.strip()}"
        )


def download_raster(url: str, dest: Path) -> None:
    """Download one MERIT Hydro raster with curl."""
    ensure_dir(dest.parent)
    result = subprocess.run(
        [
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--max-time",
            "1800",
            "-o",
            str(dest),
            url,
        ],
        capture_output=True,
        text=True,
        timeout=1860,
    )
    if result.returncode != 0:
        if dest.exists():
            dest.unlink(missing_ok=True)
        raise PreflightError(f"curl failed for {url}: {result.stderr.strip()}")


def ensure_inputs(
    pfaf: int,
    merit_basins_root: Path,
    rasters_root: Path,
    download_missing: bool,
) -> None:
    """Fetch missing preflight inputs when requested."""
    if not vector_files_present(pfaf, merit_basins_root):
        if not download_missing:
            raise PreflightError(f"missing vector sidecars for pfaf-{pfaf:02d}")
        logger.info("download vectors pfaf-%02d", pfaf)
        download_vectors(pfaf, merit_basins_root)

    flow_dir = rasters_root / "flow_dir_basins" / f"flowdir{pfaf:02d}.tif"
    flow_acc = rasters_root / "accum_basins" / f"accum{pfaf:02d}.tif"
    if not tif_valid(flow_dir):
        if not download_missing:
            raise PreflightError(f"missing or invalid flow_dir raster for pfaf-{pfaf:02d}")
        logger.info("download flow_dir pfaf-%02d", pfaf)
        download_raster(
            f"https://mghydro.com/watersheds/rasters/flow_dir_basins/flowdir{pfaf:02d}.tif",
            flow_dir,
        )
    if not tif_valid(flow_acc):
        if not download_missing:
            raise PreflightError(f"missing or invalid flow_acc raster for pfaf-{pfaf:02d}")
        logger.info("download flow_acc pfaf-%02d", pfaf)
        download_raster(
            f"https://mghydro.com/watersheds/rasters/accum_basins/accum{pfaf:02d}.tif",
            flow_acc,
        )


def resolve_basin_paths(
    pfaf: int,
    merit_basins_root: Path,
    rasters_root: Path,
) -> BasinPaths:
    """Resolve the four Phase 0 file paths for one basin."""
    root = _basins_dir(merit_basins_root)
    cat_path = _single_match(root, f"cat_pfaf_{pfaf:02d}_*.shp")
    riv_path = _single_match(root, f"riv_pfaf_{pfaf:02d}_*.shp")
    if cat_path is None:
        raise PreflightError(f"expected exactly one cat_pfaf_{pfaf:02d}_*.shp under {root}")
    if riv_path is None:
        raise PreflightError(f"expected exactly one riv_pfaf_{pfaf:02d}_*.shp under {root}")
    return BasinPaths(
        pfaf=pfaf,
        catchments=cat_path,
        rivers=riv_path,
        flow_dir=rasters_root / "flow_dir_basins" / f"flowdir{pfaf:02d}.tif",
        flow_acc=rasters_root / "accum_basins" / f"accum{pfaf:02d}.tif",
    )


def schema_signature(path: Path) -> list[dict[str, str]]:
    """Return comparable shapefile field names and dtypes."""
    info = pyogrio.read_info(path)
    fields = info.get("fields")
    dtypes = info.get("dtypes")
    if fields is None or dtypes is None:
        raise PreflightError(f"pyogrio did not return fields/dtypes for {path}")
    return [
        {"name": str(name), "dtype": str(dtype)}
        for name, dtype in zip(fields, dtypes, strict=True)
    ]


def read_ids(path: Path) -> set[int]:
    """Read the COMID column as a Python int set."""
    frame = pyogrio.read_dataframe(
        path,
        columns=["COMID"],
        read_geometry=False,
        use_arrow=True,
    )
    return {int(value) for value in frame["COMID"].dropna().tolist()}


def read_river_rows(path: Path) -> list[tuple[int, int]]:
    """Read COMID and NextDownID rows from a river shapefile."""
    frame = pyogrio.read_dataframe(
        path,
        columns=["COMID", "NextDownID"],
        read_geometry=False,
        use_arrow=True,
    )
    return [
        (int(comid), int(next_down))
        for comid, next_down in zip(frame["COMID"], frame["NextDownID"], strict=True)
    ]


def shapefile_bounds(path: Path) -> tuple[float, float, float, float] | None:
    """Read shapefile total bounds from metadata."""
    info = pyogrio.read_info(path)
    bounds = info.get("total_bounds")
    if bounds is None:
        bounds = info.get("bounds")
    if bounds is None:
        return None
    if len(bounds) != 4:
        raise PreflightError(f"unexpected bounds metadata for {path}: {bounds}")
    return tuple(float(value) for value in bounds)


def raster_summary(path: Path) -> dict[str, Any]:
    """Read CRS, bounds, dtype, and shape from a raster."""
    with rasterio.open(path) as src:
        crs = src.crs
        epsg = crs.to_epsg() if crs is not None else None
        return {
            "path": str(path),
            "crs": str(crs) if crs is not None else None,
            "epsg": epsg,
            "bounds": [float(src.bounds.left), float(src.bounds.bottom), float(src.bounds.right), float(src.bounds.top)],
            "dtype": str(src.dtypes[0]),
            "width": int(src.width),
            "height": int(src.height),
            "nodata": None if src.nodata is None else float(src.nodata),
        }


def bounds_within_world(bounds: tuple[float, float, float, float] | list[float]) -> bool:
    """Return True when bounds are ordered and inside the WGS84 world extent."""
    minx, miny, maxx, maxy = [float(value) for value in bounds]
    return (
        minx <= maxx
        and miny <= maxy
        and -180.0 <= minx <= 180.0
        and -180.0 <= maxx <= 180.0
        and -90.0 <= miny <= 90.0
        and -90.0 <= maxy <= 90.0
    )


def raster_bounds_within_world(
    bounds: tuple[float, float, float, float] | list[float],
    tolerance: float = MERIT_PIXEL_DEGREES,
) -> bool:
    """Return True when raster bounds are inside WGS84 extent within one MERIT pixel."""
    minx, miny, maxx, maxy = [float(value) for value in bounds]
    return (
        minx <= maxx
        and miny <= maxy
        and -180.0 - tolerance <= minx <= 180.0 + tolerance
        and -180.0 - tolerance <= maxx <= 180.0 + tolerance
        and -90.0 - tolerance <= miny <= 90.0 + tolerance
        and -90.0 - tolerance <= maxy <= 90.0 + tolerance
    )


def add_sample(samples: dict[str, list[int]], key: str, value: int) -> None:
    """Append a bounded integer sample under key."""
    bucket = samples.setdefault(key, [])
    if len(bucket) < SAMPLE_LIMIT:
        bucket.append(int(value))


def build_text_report(report: dict[str, Any]) -> str:
    """Render the human-readable Phase 0 report."""
    lines = [
        "MERIT v2 cross-basin preflight report",
        f"generated_at: {report['generated_at']}",
        f"schema_version: {report['schema_version']}",
        f"scanned_basins: {', '.join(f'pfaf-{code:02d}' for code in report['scanned_basins'])}",
        f"excluded_basins: {', '.join(f'pfaf-{code:02d}' for code in report['excluded_basins'])}",
        f"total_unique_ids: {report['total_unique_ids']}",
        f"max_id_value: {report['max_id_value']}",
        f"id_collisions: {report['summary']['id_collision_total']}",
        f"cross_basin_next_down_refs: {report['summary']['cross_basin_next_down_ref_total']}",
        f"schema_drift: {report['summary']['schema_drift_total']}",
        f"raster_issues: {report['summary']['raster_issue_total']}",
        f"cat_riv_set_drift_worst_fraction: {report['cat_riv_set_drift_worst_fraction']:.6f}",
        f"antimeridian_basins: {report['antimeridian_basins']}",
        f"result: {report['result']}",
        "",
        "Per-basin rows:",
    ]
    for item in report["per_basin"]:
        lines.append(
            f"- pfaf-{item['pfaf']:02d}: cat={item['catchment_id_count']} "
            f"riv={item['river_id_count']} "
            f"cat_only={item['cat_only_count']} riv_only={item['riv_only_count']} "
            f"foreign_next_down={item['foreign_next_down_count']}"
        )

    if report["id_collisions"]:
        lines.extend(["", "ID collisions:"])
        for item in report["id_collisions"]:
            lines.append(
                f"- COMID {item['comid']}: basins={item['basins']} "
                f"occurrences={item['occurrences']}"
            )

    if report["cross_basin_next_down_refs"]:
        lines.extend(["", "Cross-basin NextDownID references:"])
        for item in report["cross_basin_next_down_refs"]:
            lines.append(
                f"- pfaf-{item['source_pfaf']:02d} -> pfaf-{item['target_pfaf']:02d}: "
                f"count={item['count']} sample_next_down_ids={item['sample_next_down_ids']}"
            )

    if report["schema_drift"]:
        lines.extend(["", "Schema drift:"])
        for item in report["schema_drift"]:
            lines.append(
                f"- pfaf-{item['pfaf']:02d} {item['kind']}: "
                f"expected={item['expected']} actual={item['actual']}"
            )

    if report["raster_issues"]:
        lines.extend(["", "Raster issues:"])
        for item in report["raster_issues"]:
            lines.append(f"- pfaf-{item['pfaf']:02d} {item['kind']}: {item['message']}")

    return "\n".join(lines) + "\n"


def write_reports(root: Path, report: dict[str, Any]) -> tuple[Path, Path]:
    """Write JSON and text reports under root/preflight."""
    preflight_dir = root / "preflight"
    ensure_dir(preflight_dir)
    json_path = preflight_dir / "cross_basin_report.json"
    txt_path = preflight_dir / "cross_basin_report.txt"

    with json_path.open("w", encoding="utf-8") as dst:
        json.dump(report, dst, indent=2, sort_keys=True)
        dst.write("\n")
    txt_path.write_text(build_text_report(report), encoding="utf-8")
    return json_path, txt_path


def run_preflight(
    pfaf_codes: list[int],
    merit_basins_root: Path,
    rasters_root: Path,
    root: Path,
    download_missing: bool,
) -> tuple[dict[str, Any], Path, Path]:
    """Run the complete Phase 0 preflight and write reports."""
    paths: list[BasinPaths] = []
    for pfaf in pfaf_codes:
        logger.info("prepare pfaf-%02d", pfaf)
        ensure_inputs(pfaf, merit_basins_root, rasters_root, download_missing)
        paths.append(resolve_basin_paths(pfaf, merit_basins_root, rasters_root))

    reference = next((item for item in paths if item.pfaf == 27), paths[0])
    reference_cat_schema = schema_signature(reference.catchments)
    reference_riv_schema = schema_signature(reference.rivers)

    global_lookup: dict[int, int] = {}
    occurrence_counts: Counter[int] = Counter()
    occurrence_basins: dict[int, set[int]] = defaultdict(set)
    basin_local_ids: dict[int, set[int]] = {}
    basin_river_rows: dict[int, list[tuple[int, int]]] = {}
    schema_drift: list[dict[str, Any]] = []
    raster_issues: list[dict[str, Any]] = []
    per_basin: list[dict[str, Any]] = []
    antimeridian_basins: list[int] = []
    max_id_value: int | None = None

    for basin in paths:
        logger.info("scan shapefiles pfaf-%02d", basin.pfaf)
        cat_schema = schema_signature(basin.catchments)
        riv_schema = schema_signature(basin.rivers)
        if cat_schema != reference_cat_schema:
            schema_drift.append(
                {
                    "pfaf": basin.pfaf,
                    "kind": "catchments",
                    "expected": reference_cat_schema,
                    "actual": cat_schema,
                }
            )
        if riv_schema != reference_riv_schema:
            schema_drift.append(
                {
                    "pfaf": basin.pfaf,
                    "kind": "rivers",
                    "expected": reference_riv_schema,
                    "actual": riv_schema,
                }
            )

        cat_ids = read_ids(basin.catchments)
        river_rows = read_river_rows(basin.rivers)
        riv_ids = {comid for comid, _next_down in river_rows}
        local_ids = cat_ids | riv_ids
        basin_local_ids[basin.pfaf] = local_ids
        basin_river_rows[basin.pfaf] = river_rows

        for comid in local_ids:
            occurrence_counts[comid] += 1
            occurrence_basins[comid].add(basin.pfaf)
            global_lookup.setdefault(comid, basin.pfaf)

        if local_ids:
            local_max = max(local_ids)
            max_id_value = local_max if max_id_value is None else max(max_id_value, local_max)

        bounds = shapefile_bounds(basin.catchments)
        if bounds is not None:
            if not bounds_within_world(bounds):
                raster_issues.append(
                    {
                        "pfaf": basin.pfaf,
                        "kind": "catchment_bounds",
                        "message": f"catchment bounds outside WGS84 world extent: {list(bounds)}",
                    }
                )
            if bounds[0] < -180.0 or bounds[2] > 180.0:
                antimeridian_basins.append(basin.pfaf)

        for kind, raster_path in (("flow_dir", basin.flow_dir), ("flow_acc", basin.flow_acc)):
            try:
                summary = raster_summary(raster_path)
            except RasterioIOError as exc:
                raster_issues.append(
                    {"pfaf": basin.pfaf, "kind": kind, "message": f"cannot open raster: {exc}"}
                )
                continue
            if summary["epsg"] != 4326:
                raster_issues.append(
                    {
                        "pfaf": basin.pfaf,
                        "kind": kind,
                        "message": f"expected EPSG:4326, got {summary['crs']}",
                    }
                )
            if not raster_bounds_within_world(summary["bounds"]):
                raster_issues.append(
                    {
                        "pfaf": basin.pfaf,
                        "kind": kind,
                        "message": (
                            "raster bounds outside WGS84 world extent plus one "
                            f"MERIT pixel ({MERIT_PIXEL_DEGREES} deg): {summary['bounds']}"
                        ),
                    }
                )

        cat_only = cat_ids - riv_ids
        riv_only = riv_ids - cat_ids
        denominator = max(len(cat_ids), len(riv_ids), 1)
        drift_fraction = max(len(cat_only), len(riv_only)) / denominator
        per_basin.append(
            {
                "pfaf": basin.pfaf,
                "catchment_id_count": len(cat_ids),
                "river_id_count": len(riv_ids),
                "cat_only_count": len(cat_only),
                "riv_only_count": len(riv_only),
                "cat_only_sample": sorted(cat_only)[:SAMPLE_LIMIT],
                "riv_only_sample": sorted(riv_only)[:SAMPLE_LIMIT],
                "cat_riv_set_drift_fraction": float(drift_fraction),
                "foreign_next_down_count": 0,
            }
        )

    collision_items = [
        {
            "comid": int(comid),
            "basins": sorted(occurrence_basins[comid]),
            "occurrences": int(count),
        }
        for comid, count in occurrence_counts.items()
        if count > 1
    ]
    collision_items.sort(key=lambda item: item["comid"])

    cross_counts: Counter[str] = Counter()
    cross_samples: dict[str, list[int]] = {}
    unresolved_next_down: Counter[int] = Counter()
    foreign_count_by_basin: Counter[int] = Counter()
    for pfaf, rows in basin_river_rows.items():
        local_ids = basin_local_ids[pfaf]
        for _comid, next_down in rows:
            if next_down <= 0 or next_down in local_ids:
                continue
            target_pfaf = global_lookup.get(next_down)
            if target_pfaf is None:
                unresolved_next_down[next_down] += 1
                continue
            if target_pfaf == pfaf:
                continue
            key = f"{pfaf}->{target_pfaf}"
            cross_counts[key] += 1
            add_sample(cross_samples, key, next_down)
            foreign_count_by_basin[pfaf] += 1

    for item in per_basin:
        item["foreign_next_down_count"] = int(foreign_count_by_basin[item["pfaf"]])

    cross_items = []
    for key, count in sorted(cross_counts.items()):
        source, target = key.split("->", maxsplit=1)
        cross_items.append(
            {
                "source_pfaf": int(source),
                "target_pfaf": int(target),
                "count": int(count),
                "sample_next_down_ids": cross_samples.get(key, []),
            }
        )

    unresolved_items = {
        str(comid): int(count)
        for comid, count in unresolved_next_down.most_common(SAMPLE_LIMIT)
    }
    cat_riv_worst = max(
        (item["cat_riv_set_drift_fraction"] for item in per_basin),
        default=0.0,
    )
    hard_failure = bool(collision_items or cross_items or schema_drift)
    operational_failure = bool(raster_issues)

    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scanned_basins": pfaf_codes,
        "excluded_basins": sorted(EXCLUDED_PFAF_CODES),
        "reference_schema_basin": reference.pfaf,
        "reference_schema": {
            "catchments": reference_cat_schema,
            "rivers": reference_riv_schema,
        },
        "total_unique_ids": len(global_lookup),
        "max_id_value": max_id_value,
        "id_collisions": collision_items[:SAMPLE_LIMIT],
        "id_collision_count": len(collision_items),
        "cross_basin_next_down_refs": cross_items,
        "unresolved_next_down_refs": unresolved_items,
        "schema_drift": schema_drift,
        "raster_issues": raster_issues,
        "cat_riv_set_drift_worst_fraction": float(cat_riv_worst),
        "antimeridian_basins": sorted(set(antimeridian_basins)),
        "per_basin": sorted(per_basin, key=lambda item: item["pfaf"]),
        "summary": {
            "id_collision_total": len(collision_items),
            "cross_basin_next_down_ref_total": int(cross_counts.total()),
            "unresolved_next_down_ref_total": int(unresolved_next_down.total()),
            "schema_drift_total": len(schema_drift),
            "raster_issue_total": len(raster_issues),
        },
        "result": "FAIL" if hard_failure or operational_failure else "PASS",
        "hard_failures_present": hard_failure,
        "operational_failures_present": operational_failure,
    }
    json_path, txt_path = write_reports(root, report)
    return report, json_path, txt_path


def build_arg_parser() -> argparse.ArgumentParser:
    """Build CLI parser."""
    parser = argparse.ArgumentParser(
        description="Verify MERIT-Basins COMID, NextDownID, schema, and raster preflight invariants."
    )
    parser.add_argument(
        "--merit-basins",
        type=Path,
        default=Path(os.environ.get("HFX_MERIT_BASINS_ROOT", DEFAULT_MERIT_BASINS_ROOT)),
        help=f"Directory containing cat/riv shapefiles. Defaults to {DEFAULT_MERIT_BASINS_ROOT}.",
    )
    parser.add_argument(
        "--rasters",
        type=Path,
        default=Path(os.environ.get("HFX_MERIT_RASTERS_ROOT", DEFAULT_RASTERS_ROOT)),
        help=f"Directory containing flow_dir_basins/ and accum_basins/. Defaults to {DEFAULT_RASTERS_ROOT}.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("HFX_MERIT_V2_ROOT", DEFAULT_ROOT)),
        help=f"Working root for preflight reports. Defaults to {DEFAULT_ROOT}.",
    )
    parser.add_argument(
        "--pfaf-codes",
        help="Comma-separated Pfaf-L2 codes. Defaults to all 60 selected v2 basins.",
    )
    parser.add_argument(
        "--download-missing",
        action="store_true",
        help="Download missing vectors with rclone and rasters with curl before scanning.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level. Defaults to INFO.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""
    args = build_arg_parser().parse_args(argv)
    configure_logging(args.log_level)

    if args.download_missing:
        missing_tools = [
            name for name in ("rclone", "curl") if shutil.which(name) is None
        ]
        if missing_tools:
            raise PreflightError(f"missing required download tools: {missing_tools}")

    pfaf_codes = parse_pfaf_codes(args.pfaf_codes)
    report, json_path, txt_path = run_preflight(
        pfaf_codes=pfaf_codes,
        merit_basins_root=args.merit_basins.expanduser(),
        rasters_root=args.rasters.expanduser(),
        root=args.root.expanduser(),
        download_missing=args.download_missing,
    )

    print()
    print("MERIT v2 cross-basin preflight")
    print(f"  scanned basins: {len(report['scanned_basins'])}")
    print(f"  total unique ids: {report['total_unique_ids']}")
    print(f"  id collisions: {report['summary']['id_collision_total']}")
    print(f"  cross-basin NextDownID refs: {report['summary']['cross_basin_next_down_ref_total']}")
    print(f"  schema drift: {report['summary']['schema_drift_total']}")
    print(f"  raster issues: {report['summary']['raster_issue_total']}")
    print("  reports:")
    print(f"    {json_path}")
    print(f"    {txt_path}")
    print(f"  result: {report['result']}")
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        raise SystemExit(1)
