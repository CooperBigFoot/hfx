#!/usr/bin/env python3
"""Verify cross-region GRIT segment identifiers before HFX compilation."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pyogrio

from build_adapter import (
    DEFAULT_ROOT,
    REGION_CODES,
    REGION_INPUTS,
    SEGMENTS_LAYER,
    _extract_inner_gpkg,
    _extract_member,
    _parse_csv_int_lists,
)


DEFAULT_OUTER_ARCHIVE = Path("/Users/nicolaslazaro/Desktop/grit-hfx/17435232.zip")
WRAPPED_SAMPLE_LIMIT = 10


def log(message: str) -> None:
    print(f"[grit-cross-region] {message}", flush=True)


def parse_regions(raw: str | None) -> list[str]:
    if raw is None or raw.strip() == "":
        return list(REGION_CODES)

    regions = [part.strip().upper() for part in raw.split(",") if part.strip()]
    unknown = [region for region in regions if region not in REGION_CODES]
    if unknown:
        raise ValueError(
            f"unknown region code(s): {', '.join(unknown)}; "
            f"expected one of {', '.join(REGION_CODES)}"
        )
    return regions


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def extract_segment_gpkg(root: Path, outer_archive: Path, region_code: str) -> Path:
    """Extract only the regional segment GPKG needed for preflight checks."""
    preflight_dir = root / "preflight" / region_code
    ensure_dir(preflight_dir)
    member_name = REGION_INPUTS[region_code]["segments"]
    inner_zip_path = preflight_dir / member_name
    _extract_member(outer_archive, member_name, inner_zip_path)
    return _extract_inner_gpkg(inner_zip_path)


def read_segment_columns(gpkg_path: Path) -> pl.DataFrame:
    df = pyogrio.read_dataframe(
        gpkg_path,
        layer=SEGMENTS_LAYER,
        columns=["global_id", "upstream_line_ids"],
        read_geometry=False,
        use_arrow=True,
    )
    return pl.from_pandas(df).with_columns(
        pl.col("global_id").cast(pl.Int64),
        pl.col("upstream_line_ids").cast(pl.Utf8),
    )


def layer_bounds(gpkg_path: Path) -> tuple[float, float, float, float] | None:
    info = pyogrio.read_info(gpkg_path, layer=SEGMENTS_LAYER)
    bounds = info.get("total_bounds")
    if bounds is None:
        bounds = info.get("bounds")
    if bounds is None:
        return None
    if len(bounds) != 4:
        raise ValueError(f"unexpected bounds metadata for {gpkg_path}: {bounds}")
    return tuple(float(value) for value in bounds)


def wrapped_segment_summary(gpkg_path: Path) -> dict[str, Any]:
    """Count segments with feature bounds outside the longitude domain."""
    df = pyogrio.read_dataframe(
        gpkg_path,
        layer=SEGMENTS_LAYER,
        columns=["global_id"],
        use_arrow=True,
    )
    total_count = int(len(df))
    bounds = df.geometry.bounds
    wrapped_mask = (bounds["minx"] < -180.0) | (bounds["maxx"] > 180.0)
    wrapped_count = int(wrapped_mask.sum())

    sample_ids: list[int] = []
    if "global_id" in df.columns and wrapped_count:
        sample_ids = [
            int(value)
            for value in df.loc[wrapped_mask, "global_id"]
            .dropna()
            .head(WRAPPED_SAMPLE_LIMIT)
            .tolist()
        ]

    return {
        "wrapped_segment_count": wrapped_count,
        "wrapped_segment_fraction": (
            float(wrapped_count / total_count) if total_count else 0.0
        ),
        "wrapped_segment_sample_global_ids": sample_ids,
    }


def empty_matrix() -> dict[str, dict[str, int]]:
    return {
        source: {target: 0 for target in REGION_CODES}
        for source in REGION_CODES
    }


def counter_samples(counter: Counter[int], limit: int = 10) -> list[int]:
    return [int(value) for value, _count in counter.most_common(limit)]


def pair_key(region_a: str, region_b: str) -> str:
    return f"{region_a}->{region_b}"


def normalise_samples(samples: dict[str, list[int]]) -> dict[str, list[int]]:
    return {key: values[:10] for key, values in sorted(samples.items())}


def add_collision_sample(samples: dict[str, list[int]], key: str, global_id: int) -> None:
    bucket = samples.setdefault(key, [])
    if len(bucket) < 10:
        bucket.append(int(global_id))


def resolve_foreign_refs(
    foreign_refs_by_region: dict[str, Counter[int]],
    global_lookup: dict[int, str],
) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, Any]], int]:
    matrix = empty_matrix()
    unresolved: dict[str, dict[str, Any]] = {}
    resolved_total = 0

    for source_region in REGION_CODES:
        refs = foreign_refs_by_region.get(source_region, Counter())
        unresolved_counter: Counter[int] = Counter()
        for ref_id, count in refs.items():
            target_region = global_lookup.get(ref_id)
            if target_region is None:
                unresolved_counter[ref_id] += count
                continue
            matrix[source_region][target_region] += int(count)
            resolved_total += int(count)

        if unresolved_counter:
            unresolved[source_region] = {
                "count": int(unresolved_counter.total()),
                "sample_ids": counter_samples(unresolved_counter),
            }
        else:
            unresolved[source_region] = {"count": 0, "sample_ids": []}

    return matrix, unresolved, resolved_total


def build_reports(
    root: Path,
    report: dict[str, Any],
    bounds_note: str,
    resolved_foreign_ref_total: int,
    collision_total: int,
    unresolved_total: int,
) -> None:
    preflight_dir = root / "preflight"
    ensure_dir(preflight_dir)
    json_path = preflight_dir / "cross_region_report.json"
    txt_path = preflight_dir / "cross_region_report.txt"

    with json_path.open("w", encoding="utf-8") as dst:
        json.dump(report, dst, indent=2, sort_keys=True)
        dst.write("\n")

    lines = [
        "GRIT cross-region preflight report",
        f"generated_at: {report['generated_at']}",
        f"scanned_regions: {', '.join(report['scanned_regions'])}",
        f"total_unique_ids: {report['total_unique_ids']}",
        f"max_id_value: {report['max_id_value']}",
        f"id_collisions: {collision_total}",
        f"resolved_cross_region_foreign_refs: {resolved_foreign_ref_total}",
        f"unresolved_foreign_refs: {unresolved_total}",
        "",
        "Foreign reference matrix (source -> target):",
    ]

    header = "source " + " ".join(f"{region:>10}" for region in REGION_CODES)
    lines.append(header)
    for source_region in REGION_CODES:
        row = report["foreign_ref_matrix"][source_region]
        lines.append(
            f"{source_region:>6} "
            + " ".join(f"{row[target_region]:>10}" for target_region in REGION_CODES)
        )

    if report["id_collision_pairs"]:
        lines.extend(["", "ID collision pairs:"])
        for item in report["id_collision_pairs"]:
            lines.append(
                f"- {item['region_a']} -> {item['region_b']}: "
                f"{item['count']} sample_ids={item['sample_ids']}"
            )

    if any(item["count"] for item in report["unresolved_foreign_refs"].values()):
        lines.extend(["", "Unresolved foreign refs:"])
        for region, item in report["unresolved_foreign_refs"].items():
            if item["count"]:
                lines.append(
                    f"- {region}: {item['count']} sample_ids={item['sample_ids']}"
                )

    if report["antimeridian_regions"]:
        lines.extend(["", "Antimeridian regions:"])
        for item in report["antimeridian_regions"]:
            lines.append(
                f"- {item['region']}: bounds={item['bounds']} "
                f"wrapped_segment_count={item['wrapped_segment_count']} "
                f"wrapped_segment_fraction={item['wrapped_segment_fraction']:.6f} "
                f"sample_global_ids={item['wrapped_segment_sample_global_ids']}"
            )

    lines.extend(["", bounds_note])

    with txt_path.open("w", encoding="utf-8") as dst:
        dst.write("\n".join(lines))
        dst.write("\n")


def print_summary(
    report: dict[str, Any],
    report_json: Path,
    report_txt: Path,
    collision_total: int,
    resolved_foreign_ref_total: int,
    unresolved_total: int,
    exit_code: int,
) -> None:
    print()
    print("GRIT cross-region preflight")
    print(f"  scanned regions: {', '.join(report['scanned_regions'])}")
    print(f"  total unique ids: {report['total_unique_ids']}")
    print(f"  max id value: {report['max_id_value']}")
    print(f"  id collisions: {collision_total}")
    print(f"  resolved cross-region refs: {resolved_foreign_ref_total}")
    print(f"  unresolved foreign refs: {unresolved_total}")
    print(f"  antimeridian regions: {len(report['antimeridian_regions'])}")
    print("  reports:")
    print(f"    {report_json}")
    print(f"    {report_txt}")
    print(f"  result: {'PASS' if exit_code == 0 else 'FAIL'}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify GRIT segment IDs and cross-region upstream references."
    )
    parser.add_argument(
        "--outer-archive",
        type=Path,
        default=Path(os.environ.get("GRIT_OUTER_ARCHIVE", DEFAULT_OUTER_ARCHIVE)),
        help=(
            "GRIT outer archive path. Defaults to GRIT_OUTER_ARCHIVE, then "
            f"{DEFAULT_OUTER_ARCHIVE}."
        ),
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help=f"Working root for preflight extraction and reports. Defaults to {DEFAULT_ROOT}.",
    )
    parser.add_argument(
        "--regions",
        help=(
            "Comma-separated region codes for smoke/dev checks. "
            f"Defaults to all regions: {','.join(REGION_CODES)}."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    regions = parse_regions(args.regions)
    root = args.root.expanduser()
    outer_archive = args.outer_archive.expanduser()

    if not outer_archive.exists():
        raise FileNotFoundError(f"outer archive missing: {outer_archive}")

    global_lookup: dict[int, str] = {}
    foreign_refs_by_region: dict[str, Counter[int]] = defaultdict(Counter)
    collision_counts: Counter[str] = Counter()
    collision_samples: dict[str, list[int]] = {}
    antimeridian_regions: list[dict[str, Any]] = []
    max_id_value: int | None = None

    for region_code in regions:
        log(f"scan {region_code}")
        segment_gpkg = extract_segment_gpkg(root, outer_archive, region_code)

        bounds = layer_bounds(segment_gpkg)
        if bounds is not None:
            minx, _miny, maxx, _maxy = bounds
            if minx < -180.0 or maxx > 180.0:
                wrapped_summary = wrapped_segment_summary(segment_gpkg)
                antimeridian_regions.append(
                    {
                        "region": region_code,
                        "bounds": list(bounds),
                        **wrapped_summary,
                    }
                )

        segments = read_segment_columns(segment_gpkg)
        local_ids = set(int(value) for value in segments["global_id"].to_list())
        if local_ids:
            local_max = max(local_ids)
            max_id_value = local_max if max_id_value is None else max(max_id_value, local_max)

        for global_id in segments["global_id"].to_list():
            gid = int(global_id)
            existing_region = global_lookup.get(gid)
            if existing_region is None:
                global_lookup[gid] = region_code
                continue
            key = pair_key(existing_region, region_code)
            collision_counts[key] += 1
            add_collision_sample(collision_samples, key, gid)

        upstream_lists = _parse_csv_int_lists(segments["upstream_line_ids"])
        foreign_counter = foreign_refs_by_region[region_code]
        for upstream_ids in upstream_lists:
            for upstream_id in upstream_ids:
                if upstream_id not in local_ids:
                    foreign_counter[int(upstream_id)] += 1

        log(
            f"{region_code}: rows={segments.height} local_ids={len(local_ids)} "
            f"foreign_refs={foreign_counter.total()}"
        )

    foreign_ref_matrix, unresolved_foreign_refs, resolved_foreign_ref_total = (
        resolve_foreign_refs(foreign_refs_by_region, global_lookup)
    )
    unresolved_total = sum(item["count"] for item in unresolved_foreign_refs.values())

    id_collision_pairs = []
    for key, count in sorted(collision_counts.items()):
        region_a, region_b = key.split("->", maxsplit=1)
        id_collision_pairs.append(
            {
                "region_a": region_a,
                "region_b": region_b,
                "count": int(count),
                "sample_ids": normalise_samples(collision_samples).get(key, []),
            }
        )

    generated_at = datetime.now(timezone.utc).isoformat()
    preflight_dir = root / "preflight"
    report_json = preflight_dir / "cross_region_report.json"
    report_txt = preflight_dir / "cross_region_report.txt"
    bounds_note = (
        "Antimeridian bounds are read from layer metadata with pyogrio.read_info. "
        "Segment geometries are read only for metadata-flagged regions to count "
        "per-feature bounds outside [-180, 180]."
    )
    report: dict[str, Any] = {
        "scanned_regions": regions,
        "id_collision_pairs": id_collision_pairs,
        "total_unique_ids": len(global_lookup),
        "max_id_value": max_id_value,
        "foreign_ref_matrix": foreign_ref_matrix,
        "unresolved_foreign_refs": unresolved_foreign_refs,
        "antimeridian_regions": antimeridian_regions,
        "generated_at": generated_at,
    }

    collision_total = int(collision_counts.total())
    build_reports(
        root=root,
        report=report,
        bounds_note=bounds_note,
        resolved_foreign_ref_total=resolved_foreign_ref_total,
        collision_total=collision_total,
        unresolved_total=unresolved_total,
    )

    exit_code = 0
    if collision_total or resolved_foreign_ref_total or unresolved_total:
        exit_code = 1
    print_summary(
        report=report,
        report_json=report_json,
        report_txt=report_txt,
        collision_total=collision_total,
        resolved_foreign_ref_total=resolved_foreign_ref_total,
        unresolved_total=unresolved_total,
        exit_code=exit_code,
    )
    return exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        raise SystemExit(1)
