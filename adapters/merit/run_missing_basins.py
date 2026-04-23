#!/usr/bin/env python3
"""MERIT-Basins batch orchestrator — Phase 2, step 1.

Downloads any missing raw inputs (vectors via rclone, rasters via curl),
then runs ``build_adapter.py build`` for each Pfaf-L2 basin with bounded
parallelism (``ProcessPoolExecutor``).  Captures runtime and RSS telemetry
and emits a machine-readable ``summary.json`` plus a human-readable
``summary.txt`` and per-basin ``.log`` files under
``adapters/merit/batch_logs/<run_id>/``.

Runtime estimates (single machine, --parallelism 3)
----------------------------------------------------
  Per-basin wall-clock: 5–15 min typical, 30–90 min for Amazon-class basins.
  Total batch wall-clock: 3–15 h.
  Raw-data disk (vectors + rasters): ~4 GB.
  Output HFX datasets: up to 30 GB total.
  The orchestrator aborts before any work if < 5 GB free on the output
  filesystem.

Subcommands
-----------
  list        Print all 61 Pfaf-L2 codes with completion status.
  download    Download raw inputs only (no build).
  build       Build HFX datasets only (assume inputs are present).
  run         Download then build in one pass (default workflow).

Environment variables
---------------------
  HFX_MERIT_OUTPUT_ROOT    Override --output-root default.
  HFX_MERIT_PARALLELISM    Override --parallelism default.

Exit codes
----------
  0   All targeted basins succeeded.
  1   At least one basin failed (full run completes before exit).
  2   Preconditions failed before any work started (missing tools, disk full,
      bad argument).
  130 Interrupted by Ctrl-C (SIGINT).
"""

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

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

VALID_PFAF_CODES: tuple[int, ...] = (
    11, 12, 13, 14, 15, 16, 17,
    21, 22, 23, 24, 25, 26, 27, 28,
    31, 32, 33, 34, 35, 36,
    41, 42, 43, 44, 45, 46, 47, 48, 49,
    51, 52, 53, 54, 55, 56, 57,
    61, 62, 63, 64, 65, 66, 67,
    71, 72, 73, 74, 75, 76, 77, 78,
    81, 82, 83, 84, 85, 86, 87, 88,
    91,
)
# Codes 18 and 29 are absent from the MERIT-Basins Pfaf-L2 distribution
# (those are endorheic sub-basins with no outlet to the ocean, not published
# in the Lin et al. 2019 pfaf_level_02 shapefile set).
assert len(VALID_PFAF_CODES) == 61, f"Expected 61 codes, got {len(VALID_PFAF_CODES)}"

# Adapter location — this file lives next to build_adapter.py.
ADAPTER_DIR = Path(__file__).parent.resolve()
ADAPTER_SCRIPT = ADAPTER_DIR / "build_adapter.py"

# Six expected output files for a valid completed basin.
EXPECTED_HFX_FILES = frozenset({
    "catchments.parquet",
    "graph.arrow",
    "manifest.json",
    "snap.parquet",
    "flow_dir.tif",
    "flow_acc.tif",
})

# Legacy output root — checked as a fallback for the pfaf-27 special case.
LEGACY_OUTPUT_ROOT = Path("/Users/nicolaslazaro/Desktop/merit-hfx")

# Default paths.
DEFAULT_MERIT_BASINS_ROOT = Path("~/data/merit_basins/pfaf_level_02").expanduser()
DEFAULT_RASTERS_ROOT = Path("~/data/merit_hydro_rasters").expanduser()
DEFAULT_OUTPUT_ROOT = Path(
    os.environ.get("HFX_MERIT_OUTPUT_ROOT",
                   "/Users/nicolaslazaro/Desktop/merit-hfx/per-basin")
)
DEFAULT_PARALLELISM = int(os.environ.get("HFX_MERIT_PARALLELISM", "3"))
DEFAULT_CALIBRATION_PFAF = 42
DEFAULT_TIMEOUT_SEC = 10_800  # 3 h

# Minimum free disk before starting.
MIN_FREE_BYTES = 5 * 1024 ** 3  # 5 GB

# Vector file extensions required for each prefix (cat + riv).
VECTOR_EXTENSIONS = ("shp", "shx", "dbf", "cpg")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("merit-hfx-batch")


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

class BasinResult(NamedTuple):
    """Per-basin outcome recorded in summary.json."""

    pfaf: int
    download_ok: bool
    download_seconds: float
    build_ok: bool
    build_seconds: float
    peak_rss_mb: float
    exit_code: int
    atom_count: int
    fabric_name: str
    output_dir: str
    stderr_tail: str


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _run_id() -> str:
    """Generate a UTC-timestamp run ID."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log_dir(run_id: str) -> Path:
    """Return the batch_logs directory for a given run ID."""
    return ADAPTER_DIR / "batch_logs" / run_id


def _setup_logging(log_level: str, run_id: str) -> None:
    """Configure root logging to stderr and a per-run file."""
    log_dir = _log_dir(run_id)
    log_dir.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, log_level.upper(), logging.INFO)
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"

    handler_stderr = logging.StreamHandler(sys.stderr)
    handler_stderr.setFormatter(logging.Formatter(fmt))
    handler_stderr.setLevel(level)

    handler_file = logging.FileHandler(log_dir / "orchestrator.log", encoding="utf-8")
    handler_file.setFormatter(logging.Formatter(fmt))
    handler_file.setLevel(level)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler_stderr)
    root.addHandler(handler_file)


def _check_tool(name: str) -> bool:
    """Return True if *name* is on PATH."""
    return shutil.which(name) is not None


def _precondition_check() -> bool:
    """Verify required external tools exist. Return False and log on missing."""
    required = ["uv", "rclone", "gdalinfo", "curl"]
    missing = [t for t in required if not _check_tool(t)]
    if missing:
        logger.error(
            "Required tools not found on PATH: %s. Install them and retry.",
            ", ".join(missing),
        )
        return False
    return True


def _check_disk_space(path: Path) -> bool:
    """Return True if *path*'s filesystem has >= 5 GB free."""
    path.mkdir(parents=True, exist_ok=True)
    stat = shutil.disk_usage(path)
    free_gb = stat.free / 1024 ** 3
    if stat.free < MIN_FREE_BYTES:
        logger.error(
            "Insufficient disk space: %.1f GB free at %s (need >= 5 GB)",
            free_gb,
            path,
        )
        return False
    logger.info("Disk check OK: %.1f GB free at %s", free_gb, path)
    return True


def _output_dir_for(pfaf: int, output_root: Path) -> Path:
    """Return the canonical output directory for a given pfaf code."""
    return output_root / f"merit-hfx-pfaf{pfaf:02d}"


def _is_complete(pfaf: int, output_root: Path) -> tuple[bool, str]:
    """Return (True, output_dir_str) if the basin already has a valid manifest.

    Checks both the canonical location under *output_root* and the legacy
    root ``/Users/nicolaslazaro/Desktop/merit-hfx/merit-hfx-pfaf<NN>/``.
    Emits a warning when the legacy path is the only valid copy.
    """
    canonical = _output_dir_for(pfaf, output_root)
    legacy = LEGACY_OUTPUT_ROOT / f"merit-hfx-pfaf{pfaf:02d}"

    def _check_dir(d: Path) -> bool:
        manifest_path = d / "manifest.json"
        if not manifest_path.exists():
            return False
        try:
            with manifest_path.open() as fh:
                manifest = json.load(fh)
        except (json.JSONDecodeError, OSError):
            return False
        expected_fabric = f"merit_basins_pfaf{pfaf:02d}"
        if manifest.get("fabric_name") != expected_fabric:
            return False
        for fname in EXPECTED_HFX_FILES:
            if not (d / fname).exists():
                return False
        return True

    if _check_dir(canonical):
        return True, str(canonical)

    if _check_dir(legacy):
        logger.warning(
            "pfaf-%02d is complete at legacy path %s; "
            "recommend rehoming to %s (operator step, not done automatically)",
            pfaf,
            legacy,
            canonical,
        )
        return True, str(legacy)

    return False, ""


def _resolve_pfaf_codes(spec: str, output_root: Path) -> list[int]:
    """Expand ``--pfaf-codes`` value into a concrete list of ints."""
    if spec == "all":
        return list(VALID_PFAF_CODES)
    if spec == "missing":
        return [p for p in VALID_PFAF_CODES
                if not _is_complete(p, output_root)[0]]
    # Comma-separated list.
    codes = []
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            code = int(token)
        except ValueError:
            logger.error("Invalid pfaf code %r — must be an integer.", token)
            raise SystemExit(2)
        if code not in VALID_PFAF_CODES:
            logger.error(
                "Pfaf code %d is not a valid Pfaf-L2 code. Valid: %s",
                code,
                ", ".join(str(c) for c in VALID_PFAF_CODES),
            )
            raise SystemExit(2)
        codes.append(code)
    return sorted(set(codes))


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _vectors_present(pfaf: int, merit_basins_root: Path) -> bool:
    """Return True if all expected vector files for *pfaf* exist and are non-zero."""
    for prefix in ("cat", "riv"):
        for ext in VECTOR_EXTENSIONS:
            pattern = f"{prefix}_pfaf_{pfaf:02d}_*.{ext}"
            matches = list(merit_basins_root.glob(pattern))
            if not matches or all(f.stat().st_size == 0 for f in matches):
                return False
    return True


def _tif_valid(path: Path, timeout_sec: int = 30) -> bool:
    """Return True if *path* exists and passes a ``gdalinfo -json`` sanity check."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        result = subprocess.run(
            ["gdalinfo", "-json", str(path)],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        logger.warning("gdalinfo timed out for %s", path)
        return False
    if result.returncode != 0:
        return False
    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError:
        return False
    driver = info.get("driverShortName", "")
    bands = info.get("bands", [])
    size = info.get("size", [])
    return driver == "GTiff" and len(bands) > 0 and len(size) == 2


def _download_tif(url: str, dest: Path, log_file: Path, retries: int = 1) -> bool:
    """Download a TIF via curl and validate. Return True on success."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--silent",
                    "--show-error",
                    "--max-time",
                    "600",
                    "-o",
                    str(dest),
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=660,
            )
        except subprocess.TimeoutExpired:
            logger.warning("curl timed out for %s (attempt %d)", url, attempt + 1)
            with log_file.open("a") as fh:
                fh.write(f"curl timeout for {url} (attempt {attempt + 1})\n")
            continue
        if result.returncode != 0:
            msg = result.stderr.strip()
            logger.warning("curl failed for %s (attempt %d): %s", url, attempt + 1, msg)
            with log_file.open("a") as fh:
                fh.write(f"curl error {result.returncode} for {url}: {msg}\n")
            if dest.exists():
                dest.unlink(missing_ok=True)
            continue
        if _tif_valid(dest):
            return True
        # Invalid TIF — delete and retry.
        logger.warning("TIF failed gdalinfo check after download: %s", dest)
        dest.unlink(missing_ok=True)

    logger.error("Failed to download valid TIF from %s after %d attempt(s)", url, retries + 1)
    return False


def _download_vectors(pfaf: int, merit_basins_root: Path, log_file: Path) -> bool:
    """Download MERIT-Basins vector files via rclone. Return True on success."""
    merit_basins_root.mkdir(parents=True, exist_ok=True)
    for attempt in range(2):
        try:
            result = subprocess.run(
                [
                    "rclone",
                    "copy",
                    "--drive-shared-with-me",
                    "GoogleDrive:MERIT-Hydro_v07_Basins_v01_bugfix1/pfaf_level_02/",
                    str(merit_basins_root) + "/",
                    "--include",
                    f"*pfaf_{pfaf:02d}_*",
                ],
                capture_output=True,
                text=True,
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            logger.warning("rclone timed out for pfaf-%02d (attempt %d)", pfaf, attempt + 1)
            with log_file.open("a") as fh:
                fh.write(f"rclone timeout for pfaf-{pfaf:02d} (attempt {attempt + 1})\n")
            continue
        if result.returncode == 0:
            return True
        msg = (result.stdout + result.stderr).strip()
        logger.warning(
            "rclone failed for pfaf-%02d (attempt %d): %s", pfaf, attempt + 1, msg
        )
        with log_file.open("a") as fh:
            fh.write(f"rclone error {result.returncode} for pfaf-{pfaf:02d}: {msg}\n")

    logger.error("rclone failed for pfaf-%02d after 2 attempts", pfaf)
    return False


def download_basin(
    pfaf: int,
    merit_basins_root: Path,
    rasters_root: Path,
    log_file: Path,
    skip_downloads: bool = False,
) -> tuple[bool, float]:
    """Download all raw inputs for *pfaf*.  Return (download_ok, elapsed_sec)."""
    if skip_downloads:
        logger.info("pfaf-%02d: --skip-downloads set; skipping download phase", pfaf)
        return True, 0.0

    t0 = time.monotonic()

    # 1. Vectors.
    if _vectors_present(pfaf, merit_basins_root):
        logger.info("pfaf-%02d: vectors already present, skipping rclone", pfaf)
    else:
        logger.info("pfaf-%02d: downloading vectors via rclone", pfaf)
        ok = _download_vectors(pfaf, merit_basins_root, log_file)
        if not ok:
            return False, time.monotonic() - t0

    # 2. flow_dir TIF.
    flow_dir_path = rasters_root / "flow_dir_basins" / f"flowdir{pfaf:02d}.tif"
    if _tif_valid(flow_dir_path):
        logger.info("pfaf-%02d: flowdir TIF already valid, skipping curl", pfaf)
    else:
        url = f"https://mghydro.com/watersheds/rasters/flow_dir_basins/flowdir{pfaf:02d}.tif"
        logger.info("pfaf-%02d: downloading flowdir TIF", pfaf)
        if not _download_tif(url, flow_dir_path, log_file):
            return False, time.monotonic() - t0

    # 3. flow_acc TIF.
    flow_acc_path = rasters_root / "accum_basins" / f"accum{pfaf:02d}.tif"
    if _tif_valid(flow_acc_path):
        logger.info("pfaf-%02d: accum TIF already valid, skipping curl", pfaf)
    else:
        url = f"https://mghydro.com/watersheds/rasters/accum_basins/accum{pfaf:02d}.tif"
        logger.info("pfaf-%02d: downloading accum TIF", pfaf)
        if not _download_tif(url, flow_acc_path, log_file):
            return False, time.monotonic() - t0

    elapsed = time.monotonic() - t0
    logger.info("pfaf-%02d: download phase complete in %.1f s", pfaf, elapsed)
    return True, elapsed


# ---------------------------------------------------------------------------
# Build helpers
# ---------------------------------------------------------------------------

def _parse_peak_rss_mb(time_output: str) -> float:
    """Parse BSD ``/usr/bin/time -l`` output for peak RSS (bytes → MB)."""
    # macOS BSD time: "NNNN  maximum resident set size"
    m = re.search(r"(\d+)\s+maximum resident set size", time_output)
    if m:
        return int(m.group(1)) / 1_048_576
    return 0.0


def _read_manifest(hfx_dir: Path) -> dict:
    """Read manifest.json from *hfx_dir*. Return empty dict on any failure."""
    manifest_path = hfx_dir / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        with manifest_path.open() as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}


def _build_command(
    pfaf: int,
    merit_basins_root: Path,
    rasters_root: Path,
    build_dir: Path,
    time_file: Path,
) -> list[str]:
    """Assemble the full build command list."""
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
        "build_adapter.py",
        "build",
        "--merit-basins",
        str(merit_basins_root),
        "--rasters",
        str(rasters_root),
        "--pfaf",
        str(pfaf),
        "--out",
        str(build_dir),
    ]


def build_basin(
    pfaf: int,
    merit_basins_root: Path,
    rasters_root: Path,
    output_root: Path,
    log_dir: Path,
    force: bool,
    dry_run: bool,
    timeout_sec: int,
) -> tuple[bool, float, float, int, int, str, str]:
    """Build one basin.  Return (build_ok, elapsed_sec, peak_rss_mb, exit_code,
    atom_count, fabric_name, stderr_tail)."""
    out_dir = _output_dir_for(pfaf, output_root)
    build_dir = output_root / f"merit-hfx-pfaf{pfaf:02d}" / f"_build_{pfaf:02d}"
    time_file = log_dir / f"pfaf{pfaf:02d}.time.txt"
    log_file = log_dir / f"pfaf{pfaf:02d}.log"
    hfx_build_dir = build_dir / "hfx"

    cmd = _build_command(pfaf, merit_basins_root, rasters_root, build_dir, time_file)

    if dry_run:
        logger.info("DRY-RUN pfaf-%02d: %s", pfaf, " ".join(cmd))
        return True, 0.0, 0.0, 0, 0, f"merit_basins_pfaf{pfaf:02d}", ""

    # Guard: do not overwrite a completed basin unless --force.
    done, done_path = _is_complete(pfaf, output_root)
    if done and not force:
        logger.info(
            "pfaf-%02d: already complete at %s; skipping (pass --force to rebuild)",
            pfaf,
            done_path,
        )
        manifest = _read_manifest(Path(done_path))
        return True, 0.0, 0.0, 0, manifest.get("atom_count", 0), manifest.get("fabric_name", ""), ""

    # Create output directory.
    build_dir.mkdir(parents=True, exist_ok=True)

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
            logger.warning(
                "pfaf-%02d: adapter timed out after %d s; sending SIGTERM",
                pfaf,
                timeout_sec,
            )
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                logger.warning("pfaf-%02d: SIGTERM ignored; sending SIGKILL", pfaf)
                proc.kill()
                proc.wait()
            stdout, stderr = "", f"Timeout after {timeout_sec}s"
    except OSError as exc:
        logger.error("pfaf-%02d: failed to start adapter process: %s", pfaf, exc)
        return False, time.monotonic() - t0, 0.0, -1, 0, "", str(exc)[-40:]

    elapsed = time.monotonic() - t0
    exit_code = proc.returncode if proc.returncode is not None else -1

    # Write log.
    with log_file.open("w", encoding="utf-8") as fh:
        fh.write("=== STDOUT ===\n")
        fh.write(stdout)
        fh.write("\n=== STDERR ===\n")
        fh.write(stderr)

    # Parse time file.
    peak_rss_mb = 0.0
    if time_file.exists():
        try:
            peak_rss_mb = _parse_peak_rss_mb(time_file.read_text())
        except OSError:
            pass

    stderr_tail = "\n".join(stderr.splitlines()[-40:])

    if exit_code != 0:
        logger.error(
            "pfaf-%02d: adapter exited %d after %.1f s",
            pfaf,
            exit_code,
            elapsed,
        )
        return False, elapsed, peak_rss_mb, exit_code, 0, "", stderr_tail

    # Adapter succeeded — move files from _build_NN/hfx/ up to merit-hfx-pfafNN/.
    out_dir.mkdir(parents=True, exist_ok=True)
    if hfx_build_dir.exists():
        for fpath in hfx_build_dir.iterdir():
            dest = out_dir / fpath.name
            shutil.move(str(fpath), str(dest))
        try:
            hfx_build_dir.rmdir()
            build_dir.rmdir()
        except OSError as exc:
            logger.warning("pfaf-%02d: could not clean up build dirs: %s", pfaf, exc)
    else:
        logger.warning(
            "pfaf-%02d: adapter exit 0 but %s does not exist", pfaf, hfx_build_dir
        )

    # Verify manifest present.
    manifest = _read_manifest(out_dir)
    if not manifest:
        logger.error(
            "pfaf-%02d: adapter exited 0 but manifest.json missing or invalid at %s",
            pfaf,
            out_dir,
        )
        return False, elapsed, peak_rss_mb, exit_code, 0, "", stderr_tail

    atom_count = manifest.get("atom_count", 0)
    fabric_name = manifest.get("fabric_name", f"merit_basins_pfaf{pfaf:02d}")
    logger.info(
        "pfaf-%02d: build OK in %.1f s | atoms=%d rss=%.1f MB",
        pfaf,
        elapsed,
        atom_count,
        peak_rss_mb,
    )
    return True, elapsed, peak_rss_mb, exit_code, atom_count, fabric_name, ""


# ---------------------------------------------------------------------------
# Worker function (runs in subprocess pool)
# ---------------------------------------------------------------------------

def _worker(
    pfaf: int,
    merit_basins_root: Path,
    rasters_root: Path,
    output_root: Path,
    log_dir: Path,
    force: bool,
    dry_run: bool,
    skip_downloads: bool,
    timeout_sec: int,
) -> BasinResult:
    """Full per-basin workflow: download then build."""
    # Each subprocess needs logging configured independently.
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s %(levelname)s [pfaf-{pfaf:02d}] %(message)s",
        stream=sys.stderr,
    )

    dl_log = log_dir / "downloads.log"

    # Download phase.
    download_ok, download_sec = download_basin(
        pfaf,
        merit_basins_root,
        rasters_root,
        dl_log,
        skip_downloads=skip_downloads,
    )

    if not download_ok:
        return BasinResult(
            pfaf=pfaf,
            download_ok=False,
            download_seconds=download_sec,
            build_ok=False,
            build_seconds=0.0,
            peak_rss_mb=0.0,
            exit_code=-1,
            atom_count=0,
            fabric_name="",
            output_dir=str(_output_dir_for(pfaf, output_root)),
            stderr_tail="download failed",
        )

    # Build phase.
    build_ok, build_sec, peak_rss, exit_code, atom_count, fabric_name, stderr_tail = (
        build_basin(
            pfaf,
            merit_basins_root,
            rasters_root,
            output_root,
            log_dir,
            force=force,
            dry_run=dry_run,
            timeout_sec=timeout_sec,
        )
    )

    return BasinResult(
        pfaf=pfaf,
        download_ok=download_ok,
        download_seconds=download_sec,
        build_ok=build_ok,
        build_seconds=build_sec,
        peak_rss_mb=peak_rss,
        exit_code=exit_code,
        atom_count=atom_count,
        fabric_name=fabric_name,
        output_dir=str(_output_dir_for(pfaf, output_root)),
        stderr_tail=stderr_tail,
    )


# ---------------------------------------------------------------------------
# Summary writers
# ---------------------------------------------------------------------------

def _write_summary(results: list[BasinResult], log_dir: Path) -> None:
    """Write summary.json and summary.txt to *log_dir*."""
    # JSON.
    json_path = log_dir / "summary.json"
    payload = [r._asdict() for r in results]
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("Summary written to %s", json_path)

    # Human-readable table.
    txt_path = log_dir / "summary.txt"
    col_w = (6, 8, 10, 7, 10, 10, 6, 10, 6, 8)
    headers = (
        "pfaf",
        "dl_ok",
        "dl_sec",
        "bld_ok",
        "bld_sec",
        "rss_mb",
        "exit",
        "atoms",
        "fabric",
        "outdir",
    )
    sep = "-" * 90

    with txt_path.open("w", encoding="utf-8") as fh:
        fh.write("MERIT-Basins batch build summary\n")
        fh.write(sep + "\n")
        hdr = "  ".join(h.ljust(w) for h, w in zip(headers, col_w))
        fh.write(hdr + "\n")
        fh.write(sep + "\n")
        for r in sorted(results, key=lambda x: x.pfaf):
            row = (
                str(r.pfaf),
                str(r.download_ok),
                f"{r.download_seconds:.1f}",
                str(r.build_ok),
                f"{r.build_seconds:.1f}",
                f"{r.peak_rss_mb:.0f}",
                str(r.exit_code),
                str(r.atom_count),
                (r.fabric_name or "")[:8],
                Path(r.output_dir).name[:8],
            )
            line = "  ".join(v.ljust(w) for v, w in zip(row, col_w))
            fh.write(line + "\n")
        fh.write(sep + "\n")
        ok_count = sum(1 for r in results if r.build_ok)
        fh.write(f"Total: {ok_count}/{len(results)} basins succeeded\n")

    logger.info("Human summary written to %s", txt_path)


# ---------------------------------------------------------------------------
# Subcommand implementations
# ---------------------------------------------------------------------------

def cmd_list(args: argparse.Namespace) -> int:
    """List all 61 pfaf codes with completion status."""
    output_root = args.output_root.expanduser().resolve()
    for pfaf in VALID_PFAF_CODES:
        done, loc = _is_complete(pfaf, output_root)
        status = f"COMPLETE  {loc}" if done else "missing"
        logger.info("pfaf-%02d  %s", pfaf, status)
    return 0


def cmd_download(args: argparse.Namespace) -> int:
    """Download raw inputs for the selected basins (no build)."""
    output_root = args.output_root.expanduser().resolve()
    codes = _resolve_pfaf_codes(args.pfaf_codes, output_root)
    merit_basins_root = args.merit_basins_root.expanduser().resolve()
    rasters_root = args.rasters_root.expanduser().resolve()
    run_id = args.run_id
    log_dir = _log_dir(run_id)
    log_dir.mkdir(parents=True, exist_ok=True)
    dl_log = log_dir / "downloads.log"

    results = []
    for pfaf in codes:
        ok, secs = download_basin(pfaf, merit_basins_root, rasters_root, dl_log,
                                  skip_downloads=args.skip_downloads)
        results.append(BasinResult(
            pfaf=pfaf,
            download_ok=ok,
            download_seconds=secs,
            build_ok=False,
            build_seconds=0.0,
            peak_rss_mb=0.0,
            exit_code=0 if ok else -1,
            atom_count=0,
            fabric_name="",
            output_dir=str(_output_dir_for(pfaf, output_root)),
            stderr_tail="",
        ))

    _write_summary(results, log_dir)
    failed = [r for r in results if not r.download_ok]
    if failed:
        logger.error("Download failed for %d basin(s): %s",
                     len(failed), [r.pfaf for r in failed])
        return 1
    return 0


def cmd_build(args: argparse.Namespace) -> int:
    """Build HFX datasets for the selected basins (assumes inputs present)."""
    output_root = args.output_root.expanduser().resolve()
    codes = _resolve_pfaf_codes(args.pfaf_codes, output_root)
    merit_basins_root = args.merit_basins_root.expanduser().resolve()
    rasters_root = args.rasters_root.expanduser().resolve()
    run_id = args.run_id
    log_dir = _log_dir(run_id)
    log_dir.mkdir(parents=True, exist_ok=True)

    if not args.dry_run and not _check_disk_space(output_root):
        return 2

    results: list[BasinResult] = []
    for pfaf in codes:
        ok, secs, rss, exit_code, atoms, fabric, tail = build_basin(
            pfaf,
            merit_basins_root,
            rasters_root,
            output_root,
            log_dir,
            force=args.force,
            dry_run=args.dry_run,
            timeout_sec=args.per_basin_timeout_sec,
        )
        results.append(BasinResult(
            pfaf=pfaf,
            download_ok=True,
            download_seconds=0.0,
            build_ok=ok,
            build_seconds=secs,
            peak_rss_mb=rss,
            exit_code=exit_code,
            atom_count=atoms,
            fabric_name=fabric,
            output_dir=str(_output_dir_for(pfaf, output_root)),
            stderr_tail=tail,
        ))

    _write_summary(results, log_dir)
    failed = [r for r in results if not r.build_ok]
    if failed:
        logger.error("Build failed for %d basin(s): %s",
                     len(failed), [r.pfaf for r in failed])
        return 1
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Download and build all targeted basins with bounded parallelism."""
    output_root = args.output_root.expanduser().resolve()
    merit_basins_root = args.merit_basins_root.expanduser().resolve()
    rasters_root = args.rasters_root.expanduser().resolve()
    run_id = args.run_id
    log_dir = _log_dir(run_id)
    log_dir.mkdir(parents=True, exist_ok=True)

    # ---- Preconditions ----
    if not args.dry_run:
        if not _precondition_check():
            return 2
        if not _check_disk_space(output_root):
            return 2

    # ---- Calibration phase ----
    if getattr(args, "calibrate", False):
        cal_pfaf = args.calibration_pfaf
        logger.info("Calibration run on pfaf-%02d", cal_pfaf)
        cal_result = _worker(
            cal_pfaf,
            merit_basins_root,
            rasters_root,
            output_root,
            log_dir,
            force=args.force,
            dry_run=args.dry_run,
            skip_downloads=args.skip_downloads,
            timeout_sec=args.per_basin_timeout_sec,
        )
        logger.info(
            "Calibration pfaf-%02d: wall=%.1f s peak_rss=%.0f MB atoms=%d",
            cal_pfaf,
            cal_result.build_seconds,
            cal_result.peak_rss_mb,
            cal_result.atom_count,
        )
        if not getattr(args, "calibrate_auto_continue", False):
            logger.info(
                "Calibration complete. Pass --calibrate-auto-continue to proceed "
                "with the full batch automatically."
            )
            return 0

    # ---- Resolve codes ----
    if getattr(args, "retry_failed", False):
        # Reload summary from this run_id and pick failed codes.
        summary_path = log_dir / "summary.json"
        if summary_path.exists():
            with summary_path.open() as fh:
                prev = json.load(fh)
            codes = [r["pfaf"] for r in prev if not r["build_ok"]]
            logger.info("--retry-failed: re-running %d failed basins", len(codes))
        else:
            codes = _resolve_pfaf_codes(args.pfaf_codes, output_root)
    else:
        codes = _resolve_pfaf_codes(args.pfaf_codes, output_root)

    if not codes:
        logger.info("No basins to process — all targeted codes are already complete.")
        return 0

    parallelism = args.parallelism

    logger.info(
        "Starting batch: %d basin(s) | parallelism=%d | run_id=%s",
        len(codes),
        parallelism,
        run_id,
    )

    if args.dry_run:
        # Emit the exact commands that would run, then exit.
        for pfaf in codes:
            build_dir = output_root / f"merit-hfx-pfaf{pfaf:02d}" / f"_build_{pfaf:02d}"
            time_file = log_dir / f"pfaf{pfaf:02d}.time.txt"
            cmd = _build_command(
                pfaf, merit_basins_root, rasters_root, build_dir, time_file
            )
            logger.info("DRY-RUN pfaf-%02d: %s", pfaf, " ".join(cmd))
        return 0

    results: list[BasinResult] = []
    any_failed = False

    # Ctrl-C handler: graceful shutdown.
    _shutdown_requested = False

    def _handle_sigint(sig, frame):
        nonlocal _shutdown_requested
        _shutdown_requested = True
        logger.warning("SIGINT received — stopping submission, waiting for running workers")

    old_handler = signal.signal(signal.SIGINT, _handle_sigint)

    with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as executor:
        futures: dict[concurrent.futures.Future, int] = {}

        for pfaf in codes:
            if _shutdown_requested:
                break
            fut = executor.submit(
                _worker,
                pfaf,
                merit_basins_root,
                rasters_root,
                output_root,
                log_dir,
                args.force,
                False,  # dry_run already handled above
                args.skip_downloads,
                args.per_basin_timeout_sec,
            )
            futures[fut] = pfaf

        for fut in concurrent.futures.as_completed(futures):
            pfaf = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:  # noqa: BLE001
                logger.error("pfaf-%02d: worker raised exception: %s", pfaf, exc)
                result = BasinResult(
                    pfaf=pfaf,
                    download_ok=False,
                    download_seconds=0.0,
                    build_ok=False,
                    build_seconds=0.0,
                    peak_rss_mb=0.0,
                    exit_code=-1,
                    atom_count=0,
                    fabric_name="",
                    output_dir=str(_output_dir_for(pfaf, output_root)),
                    stderr_tail=str(exc),
                )
            results.append(result)
            if not result.build_ok:
                any_failed = True

    signal.signal(signal.SIGINT, old_handler)

    _write_summary(results, log_dir)

    failed = [r for r in results if not r.build_ok]
    if failed:
        logger.error(
            "Batch complete: %d/%d failed: %s",
            len(failed),
            len(results),
            [r.pfaf for r in failed],
        )
        return 130 if _shutdown_requested else 1

    logger.info("Batch complete: all %d basin(s) succeeded.", len(results))
    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _shared_flags(parser: argparse.ArgumentParser) -> None:
    """Attach flags shared by download / build / run subcommands."""
    parser.add_argument(
        "--pfaf-codes",
        default="all",
        metavar="CODES",
        help=(
            "Pfaf-L2 codes to process: 'all', 'missing', or comma-separated list "
            "(e.g. '11,42,91'). Default: all."
        ),
    )
    parser.add_argument(
        "--merit-basins-root",
        default=DEFAULT_MERIT_BASINS_ROOT,
        type=Path,
        metavar="PATH",
        help=f"MERIT-Basins vectors root (default: {DEFAULT_MERIT_BASINS_ROOT})",
    )
    parser.add_argument(
        "--rasters-root",
        default=DEFAULT_RASTERS_ROOT,
        type=Path,
        metavar="PATH",
        help=f"MERIT Hydro rasters root (default: {DEFAULT_RASTERS_ROOT})",
    )
    parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        type=Path,
        metavar="PATH",
        help=f"HFX output root (default: {DEFAULT_OUTPUT_ROOT}). "
             "Env: HFX_MERIT_OUTPUT_ROOT.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing completed output directories.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands that would run without touching disk or network.",
    )
    parser.add_argument(
        "--skip-downloads",
        action="store_true",
        help="Skip the download phase entirely.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        metavar="ID",
        help="Run identifier for log directory (default: UTC timestamp).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    parser.add_argument(
        "--per-basin-timeout-sec",
        default=DEFAULT_TIMEOUT_SEC,
        type=int,
        metavar="SEC",
        help=f"Per-basin adapter timeout in seconds (default: {DEFAULT_TIMEOUT_SEC}).",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_missing_basins.py",
        description=(
            "MERIT-Basins batch orchestrator. Downloads missing raw inputs and "
            "builds HFX datasets for each Pfafstetter Level-2 basin."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list
    list_parser = subparsers.add_parser(
        "list",
        help="List all 61 Pfaf-L2 codes with completion status.",
    )
    list_parser.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        type=Path,
        metavar="PATH",
        help=f"HFX output root to check (default: {DEFAULT_OUTPUT_ROOT}).",
    )

    # download
    dl_parser = subparsers.add_parser(
        "download",
        help="Download raw inputs only (no build).",
    )
    _shared_flags(dl_parser)

    # build
    build_parser = subparsers.add_parser(
        "build",
        help="Build HFX datasets only (raw inputs assumed present).",
    )
    _shared_flags(build_parser)
    build_parser.add_argument(
        "--parallelism",
        "-j",
        default=DEFAULT_PARALLELISM,
        type=int,
        metavar="N",
        help=f"Parallel workers (default: {DEFAULT_PARALLELISM}). Env: HFX_MERIT_PARALLELISM.",
    )

    # run
    run_parser = subparsers.add_parser(
        "run",
        help="Download then build all targeted basins (full workflow).",
    )
    _shared_flags(run_parser)
    run_parser.add_argument(
        "--parallelism",
        "-j",
        default=DEFAULT_PARALLELISM,
        type=int,
        metavar="N",
        help=f"Parallel workers (default: {DEFAULT_PARALLELISM}). Env: HFX_MERIT_PARALLELISM.",
    )
    run_parser.add_argument(
        "--calibrate",
        action="store_true",
        help=(
            "Run a single calibration basin before the batch to estimate wall-clock, "
            "peak RSS, and projected total time."
        ),
    )
    run_parser.add_argument(
        "--calibrate-auto-continue",
        action="store_true",
        help="After calibration, continue with the full batch without pausing.",
    )
    run_parser.add_argument(
        "--calibration-pfaf",
        default=DEFAULT_CALIBRATION_PFAF,
        type=int,
        metavar="NN",
        help=f"Pfaf code to use for calibration (default: {DEFAULT_CALIBRATION_PFAF}).",
    )
    run_parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry only basins that failed in a previous run (reads summary.json for --run-id).",
    )

    args = parser.parse_args()

    # Inject a run_id for subcommands that need one.
    if hasattr(args, "run_id") and args.run_id is None:
        args.run_id = _run_id()

    return args


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """Dispatch subcommands."""
    args = _parse_args()

    # Top-level --log-level on the root parser.
    run_id = getattr(args, "run_id", _run_id())
    _setup_logging(args.log_level, run_id)

    if args.command == "list":
        return cmd_list(args)
    if args.command == "download":
        return cmd_download(args)
    if args.command == "build":
        return cmd_build(args)
    if args.command == "run":
        return cmd_run(args)

    raise AssertionError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
