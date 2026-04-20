#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

import geopandas as gpd
import polars as pl
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import pyogrio
from geoparquet_io.core.validate import validate_geoparquet


DEFAULT_ROOT = Path("/tmp/grit-hfx-eu")


def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    """Build GeoParquet 1.1 ``geo`` metadata for embedding into an Arrow schema.

    No ``crs`` key is written — GeoParquet 1.1 defaults to OGC:CRS84 when absent,
    which is semantically equivalent to EPSG:4326 for lon/lat data.  Writing a
    plain string ``"EPSG:4326"`` would violate the spec (requires PROJJSON dict or
    null), so we omit the key entirely.
    """
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
            },
        },
    }
    return {b"geo": json.dumps(geo).encode("utf-8")}


def assert_geoparquet_valid(out_path: Path) -> None:
    """Assert that ``out_path`` passes GeoParquet 1.1 validation.

    Raises ``RuntimeError`` listing every failed check if validation fails.
    """
    result = validate_geoparquet(str(out_path), target_version="1.1")
    if not result.is_valid:
        failures = [c for c in result.checks if c.status.value == "failed"]
        raise RuntimeError(
            f"GeoParquet 1.1 validation failed for {out_path}: "
            + "; ".join(f"{c.name}: {c.message}" for c in failures)
        )

EU_INPUTS = {
    "segments": "GRITv1.0_segments_EU_EPSG4326.gpkg.zip",
    "segment_catchments": "GRITv1.0_segment_catchments_EU_EPSG4326.gpkg.zip",
    "reaches": "GRITv1.0_reaches_EU_EPSG4326.gpkg.zip",
}

SEGMENTS_LAYER = "lines"
SEGMENT_CATCHMENTS_LAYER = "segment_catchments__1"
REACHES_LAYER = "lines"

EXPECTED_SEGMENT_COUNT = 150_325
EXPECTED_REACH_COUNT = 1_922_187
ROW_GROUP_SIZE = 4_096
REACH_CHUNK_SIZE = 50_000
SNAP_BBOX_EPSILON = 1e-4
MANIFEST_BBOX_EPSILON = 1e-4


def log(message: str) -> None:
    print(f"[grit-hfx] {message}", flush=True)


def vsi_zip(path: Path) -> str:
    return f"/vsizip/{path}"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def balanced_row_group_bounds(
    total_rows: int,
    min_size: int = 4_096,
    max_size: int = 8_192,
) -> list[tuple[int, int]]:
    if total_rows <= 0:
        return []

    min_groups = math.ceil(total_rows / max_size)
    max_groups = max(1, total_rows // min_size)
    group_count = max_groups
    while group_count >= min_groups:
        base = total_rows // group_count
        remainder = total_rows % group_count
        if min_size <= base <= max_size and base + (1 if remainder else 0) <= max_size:
            bounds: list[tuple[int, int]] = []
            start = 0
            for index in range(group_count):
                size = base + (1 if index < remainder else 0)
                stop = start + size
                bounds.append((start, stop))
                start = stop
            return bounds
        group_count -= 1

    return [(0, total_rows)]


def outward_bbox(bounds: tuple[float, float, float, float]) -> list[float]:
    minx, miny, maxx, maxy = bounds
    return [
        float(minx) - MANIFEST_BBOX_EPSILON,
        float(miny) - MANIFEST_BBOX_EPSILON,
        float(maxx) + MANIFEST_BBOX_EPSILON,
        float(maxy) + MANIFEST_BBOX_EPSILON,
    ]


def inflate_degenerate_bounds(bounds, epsilon: float = SNAP_BBOX_EPSILON):
    bounds = bounds.copy()
    same_x = bounds["minx"] >= bounds["maxx"]
    same_y = bounds["miny"] >= bounds["maxy"]
    bounds.loc[same_x, "minx"] = bounds.loc[same_x, "minx"] - epsilon
    bounds.loc[same_x, "maxx"] = bounds.loc[same_x, "maxx"] + epsilon
    bounds.loc[same_y, "miny"] = bounds.loc[same_y, "miny"] - epsilon
    bounds.loc[same_y, "maxy"] = bounds.loc[same_y, "maxy"] + epsilon
    return bounds


def extract_member(outer_zip: Path, member_name: str, out_path: Path) -> None:
    ensure_dir(out_path.parent)
    with ZipFile(outer_zip) as archive:
        info = archive.getinfo(member_name)
        if out_path.exists() and out_path.stat().st_size == info.file_size:
            log(f"reuse extracted {out_path.name}")
            return
        log(f"extract {member_name} -> {out_path}")
        with archive.open(info) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)


def extract_inputs(root: Path, outer_archive: Path) -> dict[str, Path]:
    input_dir = root / "input"
    ensure_dir(input_dir)
    outputs: dict[str, Path] = {}
    for key, member_name in EU_INPUTS.items():
        out_path = input_dir / member_name
        extract_member(outer_archive, member_name, out_path)
        outputs[key] = extract_inner_gpkg(out_path)
    return outputs


def extract_inner_gpkg(inner_zip_path: Path) -> Path:
    with ZipFile(inner_zip_path) as archive:
        gpkg_names = [name for name in archive.namelist() if name.endswith(".gpkg")]
        if len(gpkg_names) != 1:
            raise ValueError(f"expected exactly one .gpkg in {inner_zip_path}, found {gpkg_names}")
        member_name = gpkg_names[0]
        out_path = inner_zip_path.with_suffix("")
        if out_path.exists():
            return out_path
        log(f"inflate {inner_zip_path.name} -> {out_path.name}")
        with archive.open(member_name) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        return out_path


def read_segments_table(segment_zip: Path) -> pl.DataFrame:
    df = pyogrio.read_dataframe(
        segment_zip,
        layer=SEGMENTS_LAYER,
        columns=[
            "global_id",
            "upstream_line_ids",
            "downstream_line_ids",
            "is_mainstem",
            "drainage_area_out",
        ],
        read_geometry=False,
        use_arrow=True,
    )
    return (
        pl.from_pandas(df)
        .with_columns(
            pl.col("global_id").cast(pl.Int64),
            pl.col("upstream_line_ids").cast(pl.Utf8),
            pl.col("downstream_line_ids").cast(pl.Utf8),
            pl.col("is_mainstem").cast(pl.Int64),
            pl.col("drainage_area_out").cast(pl.Float64),
        )
        .sort("global_id")
    )


def parse_csv_int_lists(values: pl.Series) -> list[list[int]]:
    parsed: list[list[int]] = []
    for raw in values.fill_null("").to_list():
        if raw == "":
            parsed.append([])
            continue
        parsed.append([int(part.strip()) for part in raw.split(",") if part.strip()])
    return parsed


def build_catchments(
    segment_catchments_zip: Path,
    out_dir: Path,
) -> tuple[list[int], tuple[float, float, float, float]]:
    log("read segment catchments")
    gdf = pyogrio.read_dataframe(
        segment_catchments_zip,
        layer=SEGMENT_CATCHMENTS_LAYER,
        columns=["global_id", "area"],
        read_geometry=True,
        use_arrow=True,
    )

    row_count = len(gdf)
    if row_count != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} Europe segment catchments, found {row_count}"
        )

    total_bounds = tuple(float(value) for value in gdf.total_bounds)
    centroids = gdf.geometry.centroid
    gdf["hilbert_index"] = centroids.hilbert_distance(total_bounds=gdf.total_bounds)
    gdf = gdf.sort_values(["hilbert_index", "global_id"], kind="mergesort").reset_index(drop=True)
    log(f"segment catchments sorted rows={row_count}")

    ids = [int(value) for value in gdf["global_id"].tolist()]

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )
    # Attach GeoParquet 1.1 metadata so downstream consumers recognise this file
    # as GeoParquet rather than plain Parquet with a binary geometry column.
    schema = schema.with_metadata(
        build_geo_metadata(["Polygon", "MultiPolygon"])
    )
    out_path = out_dir / "catchments.parquet"
    with pq.ParquetWriter(
        out_path,
        schema=schema,
        compression=None,
        write_statistics=True,
    ) as writer:
        for start, stop in balanced_row_group_bounds(row_count):
            chunk = gdf.iloc[start:stop]
            bounds = chunk.geometry.bounds
            geometry_wkb = chunk.geometry.to_wkb(hex=False)
            table = pa.Table.from_arrays(
                [
                    pa.array(chunk["global_id"].tolist(), type=pa.int64()),
                    pa.array(chunk["area"].tolist(), type=pa.float32()),
                    pa.array([None] * len(chunk), type=pa.float32()),
                    pa.array(bounds["minx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(bounds["miny"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(bounds["maxx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(bounds["maxy"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(geometry_wkb.tolist(), type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table)
            if start == 0 or stop == row_count or (start // ROW_GROUP_SIZE) % 10 == 0:
                log(f"catchments chunk rows={stop - start} written={stop}/{row_count}")

    del gdf
    del centroids
    gc.collect()
    log(f"wrote {out_path}")
    assert_geoparquet_valid(out_path)
    log("catchments.parquet passed GeoParquet 1.1 validation")
    return ids, total_bounds


def build_graph(
    segments: pl.DataFrame,
    catchment_ids: list[int],
    out_dir: Path,
) -> None:
    log("build graph.arrow")
    graph_df = (
        pl.DataFrame({"id": catchment_ids})
        .join(
            segments.select(["global_id", "upstream_line_ids"]).rename({"global_id": "id"}),
            on="id",
            how="left",
            validate="1:1",
        )
        .with_columns(pl.col("upstream_line_ids").fill_null(""))
    )

    missing_upstream_rows = graph_df.filter(pl.col("upstream_line_ids").is_null()).height
    if missing_upstream_rows:
        raise ValueError(f"graph build lost {missing_upstream_rows} segment rows")

    graph_ids = set(graph_df["id"].to_list())
    catchment_id_set = set(catchment_ids)
    if graph_ids != catchment_id_set:
        missing = sorted(catchment_id_set - graph_ids)[:10]
        extra = sorted(graph_ids - catchment_id_set)[:10]
        raise ValueError(f"graph/catchment id mismatch missing={missing} extra={extra}")

    upstream_lists = parse_csv_int_lists(graph_df["upstream_line_ids"])
    list_type = pa.list_(pa.field("item", pa.int64(), nullable=True))
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("upstream_ids", list_type, nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(graph_df["id"].to_list(), type=pa.int64()),
            pa.array(upstream_lists, type=list_type),
        ],
        schema=schema,
    )

    out_path = out_dir / "graph.arrow"
    with pa.OSFile(str(out_path), "wb") as sink:
        with pa_ipc.new_file(sink, schema) as writer:
            writer.write(table)
    log(f"wrote {out_path}")


def build_snap(
    segments_gpkg: Path,
    catchment_id_set: set[int],
    out_dir: Path,
) -> int:
    log("build snap.parquet from segment lines")
    out_path = out_dir / "snap.parquet"
    gdf = pyogrio.read_dataframe(
        segments_gpkg,
        layer=SEGMENTS_LAYER,
        columns=["global_id", "drainage_area_out", "is_mainstem"],
        read_geometry=True,
        use_arrow=True,
    )
    row_count = len(gdf)
    if row_count != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} segment snap rows, found {row_count}"
        )

    ids = [int(value) for value in gdf["global_id"].tolist()]
    unknown = sorted(set(ids) - catchment_id_set)
    if unknown:
        raise ValueError(f"segment snap ids missing from catchments: {unknown[:10]}")

    bounds = gdf.geometry.bounds
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("catchment_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("is_mainstem", pa.bool_(), nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    )
    # Attach GeoParquet 1.1 metadata. GRIT segment geometries are LineStrings
    # (or MultiLineString where segments are split across dateline or similar).
    schema = schema.with_metadata(
        build_geo_metadata(["LineString", "MultiLineString"])
    )
    with pq.ParquetWriter(
        out_path,
        schema=schema,
        compression=None,
        write_statistics=True,
    ) as writer:
        for start, stop in balanced_row_group_bounds(row_count):
            chunk = gdf.iloc[start:stop]
            chunk_bounds = inflate_degenerate_bounds(chunk.geometry.bounds)
            table = pa.Table.from_arrays(
                [
                    pa.array(chunk["global_id"].tolist(), type=pa.int64()),
                    pa.array(chunk["global_id"].tolist(), type=pa.int64()),
                    pa.array(chunk["drainage_area_out"].astype("float32").tolist(), type=pa.float32()),
                    pa.array((chunk["is_mainstem"] == 1).tolist(), type=pa.bool_()),
                    pa.array(chunk_bounds["minx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk_bounds["miny"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk_bounds["maxx"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk_bounds["maxy"].astype("float32").tolist(), type=pa.float32()),
                    pa.array(chunk.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table)
            if start == 0 or stop == row_count or (start // ROW_GROUP_SIZE) % 10 == 0:
                log(f"snap chunk rows={stop - start} written={stop}/{row_count}")

    del gdf
    gc.collect()
    log(f"wrote {out_path}")
    assert_geoparquet_valid(out_path)
    log("snap.parquet passed GeoParquet 1.1 validation")
    return row_count


def build_manifest(
    out_dir: Path,
    bbox: tuple[float, float, float, float],
    atom_count: int,
    has_snap: bool,
) -> None:
    manifest = {
        "format_version": "0.1",
        "fabric_name": "grit",
        "fabric_version": "v1.0",
        "crs": "EPSG:4326",
        "has_up_area": False,
        "has_rasters": False,
        "has_snap": has_snap,
        "terminal_sink_id": 0,
        "topology": "dag",
        "region": "europe",
        "bbox": outward_bbox(bbox),
        "atom_count": atom_count,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adapter_version": "grit-eu-scratch-2026-04-13",
    }
    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2) + "\n")
    log(f"wrote {out_path}")


def build_dataset(root: Path, outer_archive: Path) -> None:
    ensure_dir(root / "hfx")
    inputs = extract_inputs(root, outer_archive)
    segments = read_segments_table(inputs["segments"])
    if segments.height != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} Europe segment rows, found {segments.height}"
        )

    catchment_ids, bbox = build_catchments(inputs["segment_catchments"], root / "hfx")
    if len(catchment_ids) != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} catchment ids, found {len(catchment_ids)}"
        )

    build_graph(segments, catchment_ids, root / "hfx")
    catchment_id_set = set(catchment_ids)
    snap_rows = build_snap(inputs["segments"], catchment_id_set, root / "hfx")
    if snap_rows != EXPECTED_SEGMENT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_SEGMENT_COUNT} snap rows, found {snap_rows}"
        )
    build_manifest(root / "hfx", bbox, len(catchment_ids), has_snap=True)


def validate_dataset(root: Path, strict: bool, sample_pct: float) -> int:
    dataset_path = root / "hfx"
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset path missing: {dataset_path}")

    cmd = [
        "cargo",
        "run",
        "-p",
        "hfx-validator",
        "--",
        str(dataset_path),
        "--format",
        "text",
        "--sample-pct",
        str(sample_pct),
    ]
    if strict:
        cmd.append("--strict")
    log("run validator (text)")
    text = subprocess.run(cmd, cwd=Path(__file__).resolve().parents[2], check=False)

    json_cmd = [
        "cargo",
        "run",
        "-p",
        "hfx-validator",
        "--",
        str(dataset_path),
        "--format",
        "json",
        "--sample-pct",
        str(sample_pct),
    ]
    if strict:
        json_cmd.append("--strict")
    log("run validator (json)")
    json_run = subprocess.run(
        json_cmd,
        cwd=Path(__file__).resolve().parents[2],
        check=False,
        capture_output=True,
        text=True,
    )
    (root / "validator-report.json").write_text(json_run.stdout)
    if json_run.stderr:
        (root / "validator-report.stderr").write_text(json_run.stderr)

    return max(text.returncode, json_run.returncode)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scratch GRIT Europe -> HFX wrangler.")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="working directory for extracted inputs and generated HFX output",
    )
    parser.add_argument(
        "--outer-archive",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "path to the outer GRIT archive zip (e.g. 17435232.zip). "
            "Falls back to the GRIT_OUTER_ARCHIVE environment variable. "
            "Required for the 'extract' and 'build' subcommands."
        ),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("extract", help="extract the Europe inputs from the outer GRIT archive")
    subparsers.add_parser("build", help="build the scratch HFX dataset into <root>/hfx")

    validate = subparsers.add_parser("validate", help="run the existing Rust validator")
    validate.add_argument("--strict", action="store_true", default=True)
    validate.add_argument("--sample-pct", type=float, default=100.0)

    return parser.parse_args()


def resolve_outer_archive(args: argparse.Namespace) -> Path | None:
    if args.outer_archive is not None:
        return args.outer_archive.resolve()
    env_val = os.environ.get("GRIT_OUTER_ARCHIVE")
    if env_val:
        return Path(env_val).resolve()
    return None


def require_outer_archive(args: argparse.Namespace) -> Path:
    archive = resolve_outer_archive(args)
    if archive is None:
        raise SystemExit(
            "error: outer archive path is required for this subcommand.\n"
            "  Provide it via --outer-archive PATH\n"
            "  or set the GRIT_OUTER_ARCHIVE environment variable."
        )
    return archive


def main() -> int:
    args = parse_args()
    root = args.root.resolve()

    if args.command == "extract":
        extract_inputs(root, require_outer_archive(args))
        return 0
    if args.command == "build":
        build_dataset(root, require_outer_archive(args))
        return 0
    if args.command == "validate":
        return validate_dataset(root, strict=args.strict, sample_pct=args.sample_pct)
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
