#!/usr/bin/env python3
"""Rehearse a real TDX-Hydro compile under explicit RSS and scratch ceilings."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import psutil
import pyarrow.parquet as pq
import pyogrio
from pyproj import Geod
from shapely.geometry import LineString, Polygon


CI_BASIN_ROWS = 350_000
CI_STREAMNET_ROWS = 350_001
CI_RSS_CEILING_BYTES = 1_073_741_824
CI_SCRATCH_CEILING_BYTES = 2_000_000_000
CHILD_TIMEOUT_SECONDS = 180
GENERATION_BATCH_SIZE = 4_096


def _write_sources(root: Path, basin_rows: int, streamnet_rows: int) -> tuple[Path, Path]:
    basins_path = root / "basins.gpkg"
    streamnet_path = root / "streamnet.gpkg"
    polygon = Polygon([(7.0, 46.0), (7.0001, 46.0), (7.0001, 46.0001), (7.0, 46.0001), (7.0, 46.0)])
    line = LineString([(7.0, 46.0), (7.0001, 46.0001)])
    area_m2 = abs(float(Geod(ellps="WGS84").geometry_area_perimeter(polygon)[0]))
    for start in range(0, basin_rows, GENERATION_BATCH_SIZE):
        size = min(GENERATION_BATCH_SIZE, basin_rows - start)
        ids = np.arange(
            basin_rows - start,
            basin_rows - start - size,
            -1,
            dtype=np.int64,
        )
        frame = gpd.GeoDataFrame(
            {"streamID": ids},
            geometry=[polygon] * size,
            crs="EPSG:4326",
        )
        pyogrio.write_dataframe(
            frame,
            basins_path,
            driver="GPKG",
            layer="basins",
            append=start != 0,
        )
    for start in range(0, streamnet_rows, GENERATION_BATCH_SIZE):
        size = min(GENERATION_BATCH_SIZE, streamnet_rows - start)
        ids = np.arange(
            streamnet_rows - start,
            streamnet_rows - start - size,
            -1,
            dtype=np.int64,
        )
        frame = gpd.GeoDataFrame(
            {
                "LINKNO": ids,
                "DSLINKNO": np.full(size, -1, dtype=np.int64),
                "DSContArea": np.full(size, area_m2, dtype=np.float64),
            },
            geometry=[line] * size,
            crs="EPSG:4326",
        )
        pyogrio.write_dataframe(
            frame,
            streamnet_path,
            driver="GPKG",
            layer="streamnet",
            append=start != 0,
        )
    return basins_path, streamnet_path


def _tree_rss(process: psutil.Process) -> int:
    processes = [process]
    try:
        processes.extend(process.children(recursive=True))
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
        pass
    total = 0
    for member in processes:
        try:
            total += int(member.memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            continue
    return total


def _terminate_tree(process: psutil.Process) -> None:
    try:
        descendants = process.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
        descendants = []
    for member in descendants:
        try:
            member.terminate()
        except psutil.NoSuchProcess:
            pass
    try:
        process.terminate()
    except psutil.NoSuchProcess:
        pass
    _, alive = psutil.wait_procs(descendants + [process], timeout=3)
    for member in alive:
        try:
            member.kill()
        except psutil.NoSuchProcess:
            pass


def run_rehearsal(
    *,
    basin_rows: int,
    streamnet_rows: int,
    rss_ceiling_bytes: int,
    scratch_ceiling_bytes: int,
    child_timeout_seconds: int,
    adapter_path: Path | None = None,
) -> dict[str, object]:
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="tdx-compile-rehearsal-") as temporary:
        root = Path(temporary)
        basins, streamnet = _write_sources(root, basin_rows, streamnet_rows)
        output = root / "dataset"
        report = root / "report.json"
        command = [
            sys.executable,
            str(adapter_path or Path(__file__).with_name("build_adapter.py")),
            "build",
            "--basins",
            str(basins),
            "--streamnet",
            str(streamnet),
            "--out",
            str(output),
            "--report",
            str(report),
            "--processing-basin-id",
            "7020000010",
            "--fabric-version",
            "synthetic-compile-rehearsal",
        ]
        child_stderr_path = root / "child.stderr"
        child_stderr = child_stderr_path.open("w", encoding="utf-8")
        child = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=child_stderr,
            text=True,
        )
        child_started = time.monotonic()
        monitored = psutil.Process(child.pid)
        peak = 0
        while child.poll() is None:
            peak = max(peak, _tree_rss(monitored))
            elapsed = time.monotonic() - child_started
            scratch = sum(
                path.stat().st_size
                for path in root.rglob("*")
                if path.is_file() and path not in (basins, streamnet)
            )
            if peak > rss_ceiling_bytes:
                _terminate_tree(monitored)
                child_stderr.close()
                raise RuntimeError(
                    f"RSS ceiling exceeded: observed={peak} ceiling={rss_ceiling_bytes}"
                )
            if scratch > scratch_ceiling_bytes:
                _terminate_tree(monitored)
                child_stderr.close()
                raise RuntimeError(
                    f"scratch ceiling exceeded: observed={scratch} ceiling={scratch_ceiling_bytes}"
                )
            if elapsed > child_timeout_seconds:
                _terminate_tree(monitored)
                child_stderr.close()
                stderr = child_stderr_path.read_text()
                raise RuntimeError(
                    f"child timeout exceeded: elapsed={elapsed:.3f} "
                    f"timeout={child_timeout_seconds} stderr={stderr.strip()}"
                )
            time.sleep(0.05)
        child_stderr.close()
        stderr = child_stderr_path.read_text()
        if child.returncode != 0:
            raise RuntimeError(
                f"child build failed: returncode={child.returncode} stderr={stderr.strip()}"
            )
        hfx_binary = os.environ.get("HFX_BINARY")
        if hfx_binary is None:
            raise RuntimeError("HFX_BINARY must name the validator executable")
        validation = subprocess.run(
            [
                sys.executable,
                str(adapter_path or Path(__file__).with_name("build_adapter.py")),
                "validate",
                str(output),
                "--hfx-binary",
                hfx_binary,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if validation.returncode != 0:
            raise RuntimeError(
                "dataset validation failed: "
                f"returncode={validation.returncode} stderr={validation.stderr.strip()}"
            )
        catchments = pq.ParquetFile(output / "catchments.parquet")
        graph = pq.ParquetFile(output / "graph.parquet")
        snap = pq.ParquetFile(output / "aux" / "snap_stems.parquet")
        catchment_groups = [
            catchments.metadata.row_group(index).num_rows
            for index in range(catchments.num_row_groups)
        ]
        graph_groups = [
            graph.metadata.row_group(index).num_rows
            for index in range(graph.num_row_groups)
        ]
        if catchment_groups != graph_groups or sum(catchment_groups) != basin_rows:
            raise RuntimeError("catchment/graph row groups are not in lockstep")
        previous_id = 0
        for batch in catchments.iter_batches(columns=["id"], batch_size=4_096):
            values = batch.column(0).to_numpy(zero_copy_only=False)
            if len(values) and (
                int(values[0]) <= previous_id or np.any(values[1:] <= values[:-1])
            ):
                raise RuntimeError("catchments are not in Hilbert/id order")
            if len(values):
                previous_id = int(values[-1])
        expected_snap_id = 1
        for batch in snap.iter_batches(columns=["id"], batch_size=4_096):
            values = batch.column(0).to_numpy(zero_copy_only=False)
            expected = np.arange(
                expected_snap_id,
                expected_snap_id + len(values),
                dtype="int64",
            )
            if not np.array_equal(values, expected):
                raise RuntimeError("snap IDs are not sequential")
            expected_snap_id += len(values)
        authored = json.loads(report.read_text())
        memory = authored.get("diagnostics", {}).get("memory", {})
        phases = memory.get("phases", {})
        largest_phase, largest_values = max(
            phases.items(),
            key=lambda item: item[1]["max_intra_phase_increase_bytes"],
        )
        return {
            "adapter_high_water_rss_bytes": memory.get("high_water_rss_bytes", peak),
            "adapter_observed_peak_rss_bytes": memory.get("observed_peak_rss_bytes", peak),
            "basins_coordinate_count": basin_rows * 5,
            "basins_input_bytes": basins.stat().st_size,
            "basins_rows": basin_rows,
            "child_timeout_seconds": child_timeout_seconds,
            "largest_allocating_phase": largest_phase,
            "largest_phase_allocation_delta_bytes": largest_values[
                "allocation_delta_bytes"
            ],
            "parent_observed_tree_peak_rss_bytes": peak,
            "rss_ceiling_bytes": rss_ceiling_bytes,
            "scratch_ceiling_bytes": scratch_ceiling_bytes,
            "scratch_high_water_bytes": memory["scratch_high_water_bytes"],
            "streamnet_coordinate_count": streamnet_rows * 2,
            "streamnet_input_bytes": streamnet.stat().st_size,
            "streamnet_rows": streamnet_rows,
            "wall_time_seconds": time.monotonic() - started,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--basin-rows", type=int, default=CI_BASIN_ROWS)
    parser.add_argument("--streamnet-rows", type=int, default=CI_STREAMNET_ROWS)
    parser.add_argument("--rss-ceiling-bytes", type=int, default=CI_RSS_CEILING_BYTES)
    parser.add_argument(
        "--scratch-ceiling-bytes", type=int, default=CI_SCRATCH_CEILING_BYTES
    )
    parser.add_argument(
        "--child-timeout-seconds", type=int, default=CHILD_TIMEOUT_SECONDS
    )
    parser.add_argument("--adapter-path", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_rehearsal(
            basin_rows=args.basin_rows,
            streamnet_rows=args.streamnet_rows,
            rss_ceiling_bytes=args.rss_ceiling_bytes,
            scratch_ceiling_bytes=args.scratch_ceiling_bytes,
            child_timeout_seconds=args.child_timeout_seconds,
            adapter_path=args.adapter_path,
        )
    except Exception as exc:
        print(f"compile rehearsal failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
