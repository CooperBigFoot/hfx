#!/usr/bin/env python3
"""Batch orchestrator for building GRIT regional HFX datasets."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple


REGIONS = ("AF", "AS", "EU", "NA", "SA", "SI", "SP")

ADAPTER_DIR = Path(__file__).parent.resolve()
ADAPTER_SCRIPT = ADAPTER_DIR / "build_adapter.py"

DEFAULT_ROOT = Path("/Users/nicolaslazaro/Desktop/grit-hfx")
DEFAULT_PARALLELISM = int(os.environ.get("HFX_GRIT_PARALLELISM", "2"))
DEFAULT_TIMEOUT_SEC = 21_600
MIN_FREE_BYTES = 200 * 1024**3

REQUIRED_HFX_FILES = (
    "catchments.parquet",
    "graph.arrow",
    "manifest.json",
    "snap.parquet",
)

logger = logging.getLogger("grit-hfx-batch")


class RegionResult(NamedTuple):
    """Per-region outcome recorded in summary.json."""

    region: str
    extract_ok: bool
    extract_seconds: float
    build_ok: bool
    build_seconds: float
    validate_ok: bool
    validate_seconds: float
    peak_rss_mb: float
    atom_count: int
    output_dir: str
    exit_code: int
    stderr_tail: str


def _run_id() -> str:
    """Generate a UTC timestamp run ID."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log_dir(root: Path, run_id: str) -> Path:
    """Return the batch log directory for *run_id*."""
    return root / "batch_logs" / run_id


def _setup_logging(root: Path, run_id: str, log_level: str) -> None:
    """Configure stderr and orchestrator file logging."""
    log_dir = _log_dir(root, run_id)
    log_dir.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, log_level.upper(), logging.INFO)
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(level)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(logging.Formatter(fmt))
    stderr_handler.setLevel(level)
    root_logger.addHandler(stderr_handler)

    file_handler = logging.FileHandler(log_dir / "orchestrator.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt))
    file_handler.setLevel(level)
    root_logger.addHandler(file_handler)


def _check_tool(name: str) -> bool:
    """Return whether *name* is available on PATH."""
    return shutil.which(name) is not None


def _precondition_check(root: Path) -> bool:
    """Check required tools and disk space for heavy build/run work."""
    missing = [tool for tool in ("uv", "gdalinfo") if not _check_tool(tool)]
    if missing:
        logger.error("Required tools not found on PATH: %s", ", ".join(missing))
        return False

    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    free_gb = usage.free / 1024**3
    if usage.free < MIN_FREE_BYTES:
        logger.error(
            "Insufficient disk space at %s: %.1f GB free, need >= 200 GB",
            root,
            free_gb,
        )
        return False
    logger.info("Disk check OK at %s: %.1f GB free", root, free_gb)
    return True


def _outer_archive(args: argparse.Namespace) -> Path:
    """Resolve the outer archive from CLI, env, then <root>/17435232.zip."""
    if args.outer_archive is not None:
        return args.outer_archive.expanduser().resolve()
    env_value = os.environ.get("GRIT_OUTER_ARCHIVE")
    if env_value:
        return Path(env_value).expanduser().resolve()
    return args.root.expanduser().resolve() / "17435232.zip"


def _output_dir_for(root: Path, region: str) -> Path:
    """Return the final HFX output directory for a GRIT region."""
    return root / "per-region" / f"grit-hfx-{region.lower()}"


def _read_manifest(output_dir: Path) -> dict:
    """Read a manifest, returning an empty dict on parse or IO failure."""
    try:
        with (output_dir / "manifest.json").open(encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}


def _is_complete(region: str, root: Path) -> tuple[bool, str]:
    """Return whether the region has the exact required HFX files and manifest."""
    output_dir = _output_dir_for(root, region)
    if not all((output_dir / filename).exists() for filename in REQUIRED_HFX_FILES):
        return False, ""

    manifest = _read_manifest(output_dir)
    if manifest.get("fabric_name") != "grit":
        return False, ""

    return True, str(output_dir)


def _resolve_regions(spec: str, root: Path) -> list[str]:
    """Expand a region selector into concrete region codes."""
    normalized = spec.strip()
    if normalized == "all":
        return list(REGIONS)
    if normalized == "missing":
        return [region for region in REGIONS if not _is_complete(region, root)[0]]

    regions: list[str] = []
    for token in normalized.split(","):
        region = token.strip().upper()
        if not region:
            continue
        if region not in REGIONS:
            logger.error(
                "Invalid GRIT region %r. Valid regions: %s",
                token,
                ", ".join(REGIONS),
            )
            raise SystemExit(2)
        regions.append(region)
    return sorted(set(regions))


def _parse_peak_rss_mb(time_output: str) -> float:
    """Parse BSD /usr/bin/time -l peak RSS bytes into MiB."""
    match = re.search(r"(\d+)\s+maximum resident set size", time_output)
    if match:
        return int(match.group(1)) / 1_048_576
    return 0.0


def _stderr_tail(stderr: str, lines: int = 40) -> str:
    """Return the last stderr lines for summary diagnostics."""
    return "\n".join(stderr.splitlines()[-lines:])


def _append_command_log(
    log_file: Path,
    phase: str,
    cmd: list[str],
    stdout: str,
    stderr: str,
    exit_code: int,
    elapsed: float,
) -> None:
    """Append a command transcript to a region log."""
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(f"\n=== {phase.upper()} COMMAND ===\n")
        fh.write(" ".join(cmd) + "\n")
        fh.write(f"exit_code={exit_code} elapsed_seconds={elapsed:.1f}\n")
        fh.write(f"\n=== {phase.upper()} STDOUT ===\n")
        fh.write(stdout)
        if stdout and not stdout.endswith("\n"):
            fh.write("\n")
        fh.write(f"\n=== {phase.upper()} STDERR ===\n")
        fh.write(stderr)
        if stderr and not stderr.endswith("\n"):
            fh.write("\n")


def _run_command(
    cmd: list[str],
    log_file: Path,
    phase: str,
    timeout_sec: int,
) -> tuple[bool, float, int, str]:
    """Run a subprocess, append its transcript, and return outcome details."""
    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            stdout, stderr = proc.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                stdout, stderr = proc.communicate(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
            stderr = (stderr or "") + f"\nTimeout after {timeout_sec}s"
    except OSError as exc:
        elapsed = time.monotonic() - t0
        stderr = str(exc)
        _append_command_log(log_file, phase, cmd, "", stderr, -1, elapsed)
        return False, elapsed, -1, _stderr_tail(stderr)

    elapsed = time.monotonic() - t0
    exit_code = proc.returncode if proc.returncode is not None else -1
    _append_command_log(log_file, phase, cmd, stdout, stderr, exit_code, elapsed)
    return exit_code == 0, elapsed, exit_code, _stderr_tail(stderr)


def _extract_command(
    region: str,
    root: Path,
    archive: Path,
    time_file: Path,
) -> list[str]:
    """Build the timed adapter extract command."""
    return [
        "/usr/bin/time",
        "-l",
        "-o",
        str(time_file),
        "uv",
        "run",
        "--directory",
        str(ADAPTER_DIR),
        "python",
        ADAPTER_SCRIPT.name,
        "--region",
        region,
        "--root",
        str(root),
        "--outer-archive",
        str(archive),
        "extract",
    ]


def _build_command(
    region: str,
    root: Path,
    archive: Path,
    time_file: Path,
) -> list[str]:
    """Build the timed adapter build command."""
    return [
        "/usr/bin/time",
        "-l",
        "-o",
        str(time_file),
        "uv",
        "run",
        "--directory",
        str(ADAPTER_DIR),
        "python",
        ADAPTER_SCRIPT.name,
        "--region",
        region,
        "--root",
        str(root),
        "--outer-archive",
        str(archive),
        "build",
    ]


def _validate_command(region: str, root: Path) -> list[str]:
    """Build the strict adapter validation command."""
    return [
        "uv",
        "run",
        "--directory",
        str(ADAPTER_DIR),
        "python",
        ADAPTER_SCRIPT.name,
        "--region",
        region,
        "--root",
        str(root),
        "validate",
        "--strict",
        "--sample-pct",
        "100",
    ]


def _peak_rss_from_file(time_file: Path) -> float:
    """Read peak RSS from a /usr/bin/time output file if present."""
    try:
        return _parse_peak_rss_mb(time_file.read_text(encoding="utf-8"))
    except OSError:
        return 0.0


def _dry_run_result(
    region: str,
    root: Path,
    archive: Path,
    log_dir: Path,
    action: str,
) -> RegionResult:
    """Emit dry-run commands for one region and return a successful placeholder."""
    time_file = log_dir / f"{region}.time.txt"
    extract_time_file = log_dir / f"{region}.extract.time.txt"
    log_file = log_dir / f"{region}.log"

    commands: list[tuple[str, list[str]]] = []
    if action in {"extract", "run"}:
        extract_phase_time_file = time_file if action == "extract" else extract_time_file
        commands.append(
            ("extract", _extract_command(region, root, archive, extract_phase_time_file))
        )
    if action in {"build", "run"}:
        commands.append(("build", _build_command(region, root, archive, time_file)))
        commands.append(("validate", _validate_command(region, root)))

    with log_file.open("w", encoding="utf-8") as fh:
        for phase, cmd in commands:
            fh.write(f"DRY-RUN {phase}: {' '.join(cmd)}\n")

    for phase, cmd in commands:
        logger.info("DRY-RUN %s %s: %s", region, phase, " ".join(cmd))

    phase_ok = action != "extract"
    return RegionResult(
        region=region,
        extract_ok=True,
        extract_seconds=0.0,
        build_ok=phase_ok,
        build_seconds=0.0,
        validate_ok=phase_ok,
        validate_seconds=0.0,
        peak_rss_mb=0.0,
        atom_count=0,
        output_dir=str(_output_dir_for(root, region)),
        exit_code=0,
        stderr_tail="",
    )


def _region_result(
    region: str,
    root: Path,
    extract_ok: bool,
    extract_seconds: float,
    build_ok: bool,
    build_seconds: float,
    validate_ok: bool,
    validate_seconds: float,
    peak_rss_mb: float,
    exit_code: int,
    stderr_tail: str,
) -> RegionResult:
    """Create a result enriched from the region manifest."""
    output_dir = _output_dir_for(root, region)
    manifest = _read_manifest(output_dir)
    atom_count = int(manifest.get("atom_count", 0) or 0)
    return RegionResult(
        region=region,
        extract_ok=extract_ok,
        extract_seconds=extract_seconds,
        build_ok=build_ok,
        build_seconds=build_seconds,
        validate_ok=validate_ok,
        validate_seconds=validate_seconds,
        peak_rss_mb=peak_rss_mb,
        atom_count=atom_count,
        output_dir=str(output_dir),
        exit_code=exit_code,
        stderr_tail=stderr_tail,
    )


def _worker(
    region: str,
    root: Path,
    archive: Path,
    log_dir: Path,
    action: str,
    force: bool,
    dry_run: bool,
    timeout_sec: int,
) -> RegionResult:
    """Run one region's extract/build/validate workflow."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)s [grit-{region}] %(message)s",
        stream=sys.stderr,
        force=True,
    )

    if dry_run:
        return _dry_run_result(region, root, archive, log_dir, action)

    time_file = log_dir / f"{region}.time.txt"
    extract_time_file = log_dir / f"{region}.extract.time.txt"
    log_file = log_dir / f"{region}.log"
    log_file.write_text("", encoding="utf-8")

    if action in {"build", "run"}:
        complete, complete_path = _is_complete(region, root)
        if complete and not force:
            logger.info("%s: already complete at %s; skipping", region, complete_path)
            return _region_result(
                region,
                root,
                extract_ok=True,
                extract_seconds=0.0,
                build_ok=True,
                build_seconds=0.0,
                validate_ok=True,
                validate_seconds=0.0,
                peak_rss_mb=0.0,
                exit_code=0,
                stderr_tail="",
            )

    extract_ok = action == "build"
    extract_seconds = 0.0
    build_ok = action == "extract"
    build_seconds = 0.0
    validate_ok = action == "extract"
    validate_seconds = 0.0
    peak_rss_mb = 0.0

    if action in {"extract", "run"}:
        extract_phase_time_file = time_file if action == "extract" else extract_time_file
        cmd = _extract_command(region, root, archive, extract_phase_time_file)
        extract_ok, extract_seconds, exit_code, tail = _run_command(
            cmd,
            log_file,
            "extract",
            timeout_sec,
        )
        if not extract_ok:
            peak_rss_mb = _peak_rss_from_file(extract_phase_time_file)
            return _region_result(
                region,
                root,
                extract_ok,
                extract_seconds,
                build_ok=False,
                build_seconds=0.0,
                validate_ok=False,
                validate_seconds=0.0,
                peak_rss_mb=peak_rss_mb,
                exit_code=exit_code,
                stderr_tail=tail,
            )

    if action in {"build", "run"}:
        cmd = _build_command(region, root, archive, time_file)
        build_ok, build_seconds, exit_code, tail = _run_command(
            cmd,
            log_file,
            "build",
            timeout_sec,
        )
        peak_rss_mb = _peak_rss_from_file(time_file)
        if not build_ok:
            return _region_result(
                region,
                root,
                extract_ok,
                extract_seconds,
                build_ok,
                build_seconds,
                validate_ok=False,
                validate_seconds=0.0,
                peak_rss_mb=peak_rss_mb,
                exit_code=exit_code,
                stderr_tail=tail,
            )

        complete, _ = _is_complete(region, root)
        if not complete:
            return _region_result(
                region,
                root,
                extract_ok,
                extract_seconds,
                build_ok=False,
                build_seconds=build_seconds,
                validate_ok=False,
                validate_seconds=0.0,
                peak_rss_mb=peak_rss_mb,
                exit_code=1,
                stderr_tail="build exited 0 but required HFX outputs are incomplete",
            )

        cmd = _validate_command(region, root)
        validate_ok, validate_seconds, exit_code, tail = _run_command(
            cmd,
            log_file,
            "validate",
            timeout_sec,
        )
        if not validate_ok:
            return _region_result(
                region,
                root,
                extract_ok,
                extract_seconds,
                build_ok,
                build_seconds,
                validate_ok,
                validate_seconds,
                peak_rss_mb,
                exit_code,
                tail,
            )

    return _region_result(
        region,
        root,
        extract_ok,
        extract_seconds,
        build_ok,
        build_seconds,
        validate_ok,
        validate_seconds,
        peak_rss_mb,
        exit_code=0,
        stderr_tail="",
    )


def _write_summary(results: list[RegionResult], log_dir: Path) -> None:
    """Write machine and human readable summaries."""
    json_path = log_dir / "summary.json"
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump([result._asdict() for result in results], fh, indent=2)
        fh.write("\n")

    txt_path = log_dir / "summary.txt"
    headers = (
        "region",
        "extract",
        "ext_s",
        "build",
        "bld_s",
        "valid",
        "val_s",
        "rss_mb",
        "atoms",
        "exit",
    )
    widths = (6, 7, 8, 6, 8, 6, 8, 8, 10, 5)
    sep = "-" * 92
    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write("GRIT regional HFX batch summary\n")
        fh.write(sep + "\n")
        fh.write("  ".join(h.ljust(w) for h, w in zip(headers, widths)) + "\n")
        fh.write(sep + "\n")
        for result in sorted(results, key=lambda item: item.region):
            row = (
                result.region,
                str(result.extract_ok),
                f"{result.extract_seconds:.1f}",
                str(result.build_ok),
                f"{result.build_seconds:.1f}",
                str(result.validate_ok),
                f"{result.validate_seconds:.1f}",
                f"{result.peak_rss_mb:.0f}",
                str(result.atom_count),
                str(result.exit_code),
            )
            fh.write("  ".join(v.ljust(w) for v, w in zip(row, widths)) + "\n")
        fh.write(sep + "\n")
        succeeded = sum(1 for result in results if _result_succeeded(result))
        fh.write(f"Total: {succeeded}/{len(results)} region(s) succeeded\n")

    logger.info("Summary written to %s and %s", json_path, txt_path)


def _result_succeeded(result: RegionResult) -> bool:
    """Return whether the result represents a successful requested workflow."""
    return result.exit_code == 0


def cmd_list(args: argparse.Namespace) -> int:
    """List region completion status."""
    root = args.root.expanduser().resolve()
    for region in _resolve_regions(args.regions, root):
        complete, path = _is_complete(region, root)
        status = f"COMPLETE  {path}" if complete else "missing"
        logger.info("%s  %s", region, status)
    return 0


def _check_archive_for_work(args: argparse.Namespace, archive: Path) -> bool:
    """Validate archive presence for real adapter work."""
    if args.dry_run:
        return True
    if not archive.exists():
        logger.error(
            "Outer archive missing: %s. Pass --outer-archive or set GRIT_OUTER_ARCHIVE.",
            archive,
        )
        return False
    return True


def _load_retry_regions(root: Path, run_id: str) -> list[str] | None:
    """Load failed regions from a previous summary."""
    summary_path = _log_dir(root, run_id) / "summary.json"
    if not summary_path.exists():
        logger.error("--retry-failed requested but summary does not exist: %s", summary_path)
        return None
    try:
        with summary_path.open(encoding="utf-8") as fh:
            previous = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Could not read prior summary %s: %s", summary_path, exc)
        return None
    regions = [
        str(row["region"]).upper()
        for row in previous
        if int(row.get("exit_code", 1)) != 0
    ]
    return [region for region in regions if region in REGIONS]


def _run_parallel(
    args: argparse.Namespace,
    regions: list[str],
    root: Path,
    archive: Path,
    log_dir: Path,
) -> tuple[list[RegionResult], bool]:
    """Run selected regions with bounded parallelism and SIGINT drain."""
    results: list[RegionResult] = []
    interrupted = False
    pending_regions = list(regions)
    active: dict[concurrent.futures.Future, str] = {}

    def make_failure(region: str, exc: BaseException) -> RegionResult:
        return _region_result(
            region,
            root,
            extract_ok=False,
            extract_seconds=0.0,
            build_ok=False,
            build_seconds=0.0,
            validate_ok=False,
            validate_seconds=0.0,
            peak_rss_mb=0.0,
            exit_code=-1,
            stderr_tail=str(exc),
        )

    def handle_sigint(sig, frame):
        nonlocal interrupted
        interrupted = True
        logger.warning("SIGINT received; stopping new submissions and draining workers")

    old_handler = signal.signal(signal.SIGINT, handle_sigint)
    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.parallelism
        ) as executor:
            while pending_regions and len(active) < args.parallelism:
                region = pending_regions.pop(0)
                future = executor.submit(
                    _worker,
                    region,
                    root,
                    archive,
                    log_dir,
                    args.command,
                    args.force,
                    args.dry_run,
                    args.per_region_timeout_sec,
                )
                active[future] = region

            while active:
                done, _ = concurrent.futures.wait(
                    active,
                    timeout=1,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    region = active.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        logger.error("%s: worker raised exception: %s", region, exc)
                        result = make_failure(region, exc)
                    results.append(result)
                    logger.info(
                        "%s complete: extract=%s build=%s validate=%s exit=%d",
                        region,
                        result.extract_ok,
                        result.build_ok,
                        result.validate_ok,
                        result.exit_code,
                    )

                while (
                    pending_regions
                    and not interrupted
                    and len(active) < args.parallelism
                ):
                    region = pending_regions.pop(0)
                    future = executor.submit(
                        _worker,
                        region,
                        root,
                        archive,
                        log_dir,
                        args.command,
                        args.force,
                        args.dry_run,
                        args.per_region_timeout_sec,
                    )
                    active[future] = region
    finally:
        signal.signal(signal.SIGINT, old_handler)

    if interrupted and pending_regions:
        logger.warning("Skipped %d unsubmitted region(s): %s", len(pending_regions), pending_regions)
    return results, interrupted


def cmd_extract_build_run(args: argparse.Namespace) -> int:
    """Run extract/build/run workflows."""
    root = args.root.expanduser().resolve()
    archive = _outer_archive(args)
    log_dir = _log_dir(root, args.run_id)
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.command in {"build", "run"} and not args.dry_run:
        if not _precondition_check(root):
            return 2

    if not _check_archive_for_work(args, archive):
        return 2

    if args.retry_failed:
        retry_regions = _load_retry_regions(root, args.run_id)
        if retry_regions is None:
            return 2
        regions = retry_regions
    else:
        regions = _resolve_regions(args.regions, root)

    if not regions:
        logger.info("No regions to process.")
        return 0

    logger.info(
        "Starting %s for %d region(s): %s | parallelism=%d | run_id=%s",
        args.command,
        len(regions),
        ",".join(regions),
        args.parallelism,
        args.run_id,
    )

    results, interrupted = _run_parallel(args, regions, root, archive, log_dir)
    _write_summary(results, log_dir)

    failed = [result.region for result in results if not _result_succeeded(result)]
    if failed:
        logger.error("Batch failed for %d region(s): %s", len(failed), failed)
        return 130 if interrupted else 1
    if interrupted:
        return 130
    logger.info("Batch complete: all %d submitted region(s) succeeded", len(results))
    return 0


def _shared_flags(parser: argparse.ArgumentParser, include_retry: bool = False) -> None:
    """Attach common flags to subcommands."""
    parser.add_argument(
        "--regions",
        default="all",
        metavar="REGIONS",
        help="Regions to process: all, missing, or comma-separated codes such as EU,SA.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help=f"GRIT working/output root (default: {DEFAULT_ROOT}).",
    )
    parser.add_argument(
        "--outer-archive",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Outer GRIT archive. Defaults to GRIT_OUTER_ARCHIVE, then "
            "<root>/17435232.zip."
        ),
    )
    parser.add_argument(
        "-j",
        "--parallelism",
        type=int,
        default=DEFAULT_PARALLELISM,
        metavar="N",
        help=f"Parallel workers (default: {DEFAULT_PARALLELISM}). Env: HFX_GRIT_PARALLELISM.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run build even when final HFX outputs are already complete.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without running extract, build, or validation.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        metavar="ID",
        help="Run identifier for batch_logs (default: UTC timestamp).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--per-region-timeout-sec",
        type=int,
        default=DEFAULT_TIMEOUT_SEC,
        metavar="SEC",
        help=f"Timeout per adapter phase in seconds (default: {DEFAULT_TIMEOUT_SEC}).",
    )
    if include_retry:
        parser.add_argument(
            "--retry-failed",
            action="store_true",
            help="Retry only failed regions from summary.json for --run-id.",
        )
    else:
        parser.set_defaults(retry_failed=False)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_all_regions.py",
        description="GRIT regional HFX batch orchestrator.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List region completion status.")
    list_parser.add_argument(
        "--regions",
        default="all",
        metavar="REGIONS",
        help="Regions to list: all, missing, or comma-separated codes.",
    )
    list_parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help=f"GRIT working/output root (default: {DEFAULT_ROOT}).",
    )
    list_parser.add_argument(
        "--run-id",
        default=None,
        metavar="ID",
        help="Run identifier for batch_logs (default: UTC timestamp).",
    )
    list_parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    extract_parser = subparsers.add_parser("extract", help="Extract regional inputs.")
    _shared_flags(extract_parser)

    build_parser = subparsers.add_parser(
        "build",
        help="Build and strictly validate regional HFX datasets.",
    )
    _shared_flags(build_parser)

    run_parser = subparsers.add_parser(
        "run",
        help="Extract, build, and strictly validate regional HFX datasets.",
    )
    _shared_flags(run_parser, include_retry=True)

    args = parser.parse_args()
    if hasattr(args, "run_id") and args.run_id is None:
        if getattr(args, "retry_failed", False):
            parser.error("--retry-failed requires --run-id")
        args.run_id = _run_id()
    if hasattr(args, "parallelism") and args.parallelism < 1:
        parser.error("--parallelism must be >= 1")
    return args


def main() -> int:
    """Dispatch subcommands."""
    args = _parse_args()
    root = args.root.expanduser().resolve()
    _setup_logging(root, args.run_id, args.log_level)

    if args.command == "list":
        return cmd_list(args)
    if args.command in {"extract", "build", "run"}:
        return cmd_extract_build_run(args)
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
