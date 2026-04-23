#!/usr/bin/env python3
"""Build global MERIT Hydro raster mosaics: flow_dir.tif and flow_acc.tif.

Purpose
-------
Assembles all MERIT Hydro 5-degree tiles into two planet-wide Cloud-Optimized
GeoTIFFs (COGs):

  <output-dir>/flow_dir.tif  — uint8, ESRI D8 encoding, NoData=255
  <output-dir>/flow_acc.tif  — float32, upstream area, NoData=-1.0

This script operates entirely in the raster domain. It does NOT touch
catchments.parquet, graph.arrow, snap.parquet, or manifest.json — those belong
to build_adapter.py / merge_basins.py. Once the two TIFs land, re-run
merge_basins.py with ``--rasters-ready --force`` to flip ``has_rasters=true`` in
the manifest.

Disk / RAM / wall-time budget
------------------------------
  Source tiles : ~5 GB dir + ~18 GB upa ≈ 23 GB
  Output       : ~28 GB
  Intermediate : up to ~10 GB transient (uint8 remap step)
  Peak disk    : 50–60 GB
  RAM          : GDAL_CACHEMAX 4096 MB + ~1–2 GB Python ≈ 6 GB
  Wall time    : dir 10–30 min, upa 30–90 min (single-machine, SSD)

Downloads
---------
Pre-download 5-degree MERIT Hydro tiles before running this script.
See WORKFLOW.md §6.1 for registration, download, and unpack instructions.

Source tiles are expected at::

  ~/data/merit_hydro_5deg/
    dir/  n00e005_dir.tif  ...
    upa/  n00e005_upa.tif  ...

Tile filename format: ``{lat}{lon}_{layer}.tif``
  lat : ``n<NN>`` or ``s<NN>``
  lon : ``e<NNN>`` or ``w<NNN>``
  layer : ``dir`` or ``upa``

If the source directory is missing, empty, or lacks ``*_dir.tif`` / ``*_upa.tif``
files, this script exits with code 2 and points at WORKFLOW.md §6.1.
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import rasterio
from rio_cogeo.cogeo import cog_validate

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("merit-global-rasters")

# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

PIXEL_SIZE = 0.000833333333333  # 3 arc-seconds in decimal degrees
TILE_SIZE_PX = 6000
TILE_SIZE_DEG = 5.0
FLOW_DIR_NODATA_OUT = 255
FLOW_ACC_NODATA_OUT = -1.0
COG_BLOCKSIZE = 512
GRID_TOL = 1e-7

# Expected fractional pixel alignment origin (0-based, so the first cell
# centre in a 5-degree tile that starts at lon=-180, lat=90 would be at
# 0.9995833... because each pixel is 0.000833... degrees wide and the
# tile corner is placed at the grid origin).
EXPECTED_FRAC_ORIGIN = 0.9995833333333  # = 1 - PIXEL_SIZE/2 ... approx
GRID_ALIGN_TOL = 1e-6

# MERIT int8 nodata value
MERIT_FLOWDIR_INT8_NODATA = -9

# WORKFLOW.md section to point operators at when source is missing.
WORKFLOW_SECTION = "WORKFLOW.md §6.1"

# ---------------------------------------------------------------------------
# Pre-flight helpers
# ---------------------------------------------------------------------------


def _check_gdal_tools() -> None:
    """Verify required GDAL CLI tools and Python imports are available.

    Exits 2 if anything is missing, with actionable error messages.
    """
    required_tools = [
        "gdalinfo",
        "gdalbuildvrt",
        "gdal_translate",
        "gdal_calc.py",
    ]
    missing_tools = [t for t in required_tools if shutil.which(t) is None]
    if missing_tools:
        logger.error(
            "Missing GDAL CLI tools: %s. Install GDAL 3.5+ and ensure the "
            "binaries are on PATH (e.g. `conda install -c conda-forge gdal`).",
            ", ".join(missing_tools),
        )
        raise SystemExit(2)

    # Verify rio_cogeo import is available (already imported at top, but be
    # explicit for the user-facing error message).
    try:
        import rio_cogeo.cogeo  # noqa: F401
    except ImportError as exc:
        logger.error(
            "rio_cogeo not importable: %s. Run `uv sync` inside adapters/merit/.",
            exc,
        )
        raise SystemExit(2)

    logger.debug("GDAL tools and Python imports OK")


# ---------------------------------------------------------------------------
# Tile enumeration
# ---------------------------------------------------------------------------


def _enumerate_tiles(
    source_dir: Path,
    *,
    smoke: bool = False,
) -> tuple[list[Path], list[Path]]:
    """Enumerate and match dir/upa tile pairs under ``source_dir``.

    Returns ``(dir_tiles, upa_tiles)`` sorted lists of matched Path objects.
    Exits 2 if either layer is missing or if the tile sets do not match.

    When ``smoke=True`` only the first 4 tiles per layer are returned so the
    alignment/validation steps run quickly without downloading all tiles.
    """
    dir_root = source_dir / "dir"
    upa_root = source_dir / "upa"

    for sub in (dir_root, upa_root):
        if not sub.is_dir():
            logger.error(
                "Source subdirectory missing: %s. "
                "Download and unpack MERIT Hydro tiles per %s.",
                sub,
                WORKFLOW_SECTION,
            )
            raise SystemExit(2)

    dir_tiles = sorted(dir_root.glob("*_dir.tif"))
    upa_tiles = sorted(upa_root.glob("*_upa.tif"))

    if not dir_tiles:
        logger.error(
            "No *_dir.tif tiles found under %s. See %s.",
            dir_root,
            WORKFLOW_SECTION,
        )
        raise SystemExit(2)

    if not upa_tiles:
        logger.error(
            "No *_upa.tif tiles found under %s. See %s.",
            upa_root,
            WORKFLOW_SECTION,
        )
        raise SystemExit(2)

    # Compare stem sets (strip _dir / _upa suffix to get the coordinate prefix).
    dir_stems = {p.stem.removesuffix("_dir") for p in dir_tiles}
    upa_stems = {p.stem.removesuffix("_upa") for p in upa_tiles}

    dir_only = sorted(dir_stems - upa_stems)
    upa_only = sorted(upa_stems - dir_stems)
    if dir_only or upa_only:
        logger.error(
            "Tile set mismatch between dir/ and upa/. "
            "dir-only (first 10): %s. upa-only (first 10): %s.",
            dir_only[:10],
            upa_only[:10],
        )
        raise SystemExit(2)

    logger.info(
        "Enumerated %d dir tiles and %d upa tiles",
        len(dir_tiles),
        len(upa_tiles),
    )

    if smoke:
        logger.info("--smoke-test: limiting to first 4 tiles per layer")
        dir_tiles = dir_tiles[:4]
        upa_tiles = upa_tiles[:4]

    return dir_tiles, upa_tiles


# ---------------------------------------------------------------------------
# Tile filename parser
# ---------------------------------------------------------------------------

_TILE_RE = re.compile(
    r"^(?P<lat_hem>[ns])(?P<lat>\d{2})(?P<lon_hem>[ew])(?P<lon>\d{3})_(?:dir|upa)$",
    re.IGNORECASE,
)


def _parse_tile_coords(path: Path) -> tuple[float, float]:
    """Parse the lower-left (lon, lat) corner from a MERIT tile filename stem.

    Returns ``(lon, lat)`` in decimal degrees.
    Exits 2 if the filename does not match the expected pattern.
    """
    stem = path.stem  # e.g. "n30w120_dir"
    m = _TILE_RE.match(stem)
    if not m:
        logger.error(
            "Cannot parse tile coordinates from filename: %s. "
            "Expected format like n30w120_dir.tif.",
            path.name,
        )
        raise SystemExit(2)

    lat = float(m.group("lat"))
    if m.group("lat_hem").lower() == "s":
        lat = -lat

    lon = float(m.group("lon"))
    if m.group("lon_hem").lower() == "w":
        lon = -lon

    return lon, lat


# ---------------------------------------------------------------------------
# Single-tile validation
# ---------------------------------------------------------------------------


def _validate_tile(path: Path, layer: str) -> None:
    """Validate a single MERIT Hydro 5-degree tile.

    Checks: dimensions, CRS, pixel size, transform alignment, dtype, and
    minimum file size. Exits 2 with a descriptive message on any failure.

    ``layer`` is either ``"dir"`` or ``"upa"`` and governs the dtype check.
    """
    if path.stat().st_size < 1024:
        logger.error("Tile too small (likely truncated): %s", path)
        raise SystemExit(2)

    lon, lat = _parse_tile_coords(path)

    try:
        with rasterio.open(path) as src:
            # Dimensions
            if src.width != TILE_SIZE_PX or src.height != TILE_SIZE_PX:
                logger.error(
                    "Tile %s has unexpected size %dx%d (expected %dx%d).",
                    path.name,
                    src.width,
                    src.height,
                    TILE_SIZE_PX,
                    TILE_SIZE_PX,
                )
                raise SystemExit(2)

            # CRS
            epsg = src.crs.to_epsg() if src.crs else None
            if epsg != 4326:
                logger.error(
                    "Tile %s must be EPSG:4326, got %s (epsg=%s).",
                    path.name,
                    src.crs,
                    epsg,
                )
                raise SystemExit(2)

            t = src.transform
            # Pixel size
            if abs(t.a - PIXEL_SIZE) > GRID_TOL:
                logger.error(
                    "Tile %s x-pixel size %.12f != expected %.12f (diff %.2e).",
                    path.name,
                    t.a,
                    PIXEL_SIZE,
                    abs(t.a - PIXEL_SIZE),
                )
                raise SystemExit(2)
            if abs(t.e + PIXEL_SIZE) > GRID_TOL:
                logger.error(
                    "Tile %s y-pixel size %.12f != expected -%.12f (diff %.2e).",
                    path.name,
                    t.e,
                    PIXEL_SIZE,
                    abs(t.e + PIXEL_SIZE),
                )
                raise SystemExit(2)

            # Transform origin: upper-left corner = (lon, lat+5)
            if abs(t.c - lon) > GRID_TOL:
                logger.error(
                    "Tile %s x-origin %.9f != expected %.9f (diff %.2e).",
                    path.name,
                    t.c,
                    lon,
                    abs(t.c - lon),
                )
                raise SystemExit(2)
            expected_top = lat + TILE_SIZE_DEG
            if abs(t.f - expected_top) > GRID_TOL:
                logger.error(
                    "Tile %s y-origin %.9f != expected %.9f (diff %.2e).",
                    path.name,
                    t.f,
                    expected_top,
                    abs(t.f - expected_top),
                )
                raise SystemExit(2)

            # dtype
            if layer == "dir" and src.dtypes[0] not in ("int8",):
                logger.error(
                    "Tile %s (dir) dtype is %s, expected int8.",
                    path.name,
                    src.dtypes[0],
                )
                raise SystemExit(2)
            if layer == "upa" and src.dtypes[0] not in ("float32",):
                logger.error(
                    "Tile %s (upa) dtype is %s, expected float32.",
                    path.name,
                    src.dtypes[0],
                )
                raise SystemExit(2)

    except rasterio.errors.RasterioIOError as exc:
        logger.error("Cannot open tile %s: %s", path, exc)
        raise SystemExit(2)


def _validate_tiles(tiles: list[Path], layer: str) -> None:
    """Validate all tiles for a given layer, logging progress every 50 tiles."""
    total = len(tiles)
    for idx, tile in enumerate(tiles, start=1):
        if idx == 1 or idx % 50 == 0 or idx == total:
            logger.info("Validating %s tile %d/%d: %s", layer, idx, total, tile.name)
        _validate_tile(tile, layer)
    logger.info("All %d %s tiles passed validation", total, layer)


# ---------------------------------------------------------------------------
# Grid alignment check
# ---------------------------------------------------------------------------


def _check_grid_alignment(tiles_subset: list[Path]) -> None:
    """Verify that a sample of tiles share the expected fractional pixel alignment.

    MERIT Hydro 5-degree tiles must align to a global 3-arc-second grid.
    The expected fractional remainder when dividing the tile origin lon/lat by
    PIXEL_SIZE must be consistent across all tiles and match the pfaf-27 reference.

    Exits 2 with a pixel-alignment message on failure.
    """
    if not tiles_subset:
        return

    ref_frac_x: float | None = None
    ref_frac_y: float | None = None

    for path in tiles_subset:
        try:
            with rasterio.open(path) as src:
                t = src.transform
                # Fractional offset of the tile origin in pixel units.
                frac_x = (t.c / PIXEL_SIZE) % 1.0
                frac_y = (t.f / PIXEL_SIZE) % 1.0
        except rasterio.errors.RasterioIOError as exc:
            logger.error("Cannot open tile %s for alignment check: %s", path, exc)
            raise SystemExit(2)

        if ref_frac_x is None:
            ref_frac_x = frac_x
            ref_frac_y = frac_y
            logger.debug(
                "Grid alignment reference: frac_x=%.9f frac_y=%.9f (from %s)",
                frac_x,
                frac_y,
                path.name,
            )
            continue

        if abs(frac_x - ref_frac_x) > GRID_ALIGN_TOL or abs(frac_y - ref_frac_y) > GRID_ALIGN_TOL:
            logger.error(
                "Grid alignment mismatch: tile %s has fractional offset "
                "(%.9f, %.9f) but reference is (%.9f, %.9f). "
                "This suggests a wrong tile version; redownload per %s.",
                path.name,
                frac_x,
                frac_y,
                ref_frac_x,
                ref_frac_y,
                WORKFLOW_SECTION,
            )
            raise SystemExit(2)

    logger.info(
        "Grid alignment check passed for %d tiles (frac_x=%.9f frac_y=%.9f)",
        len(tiles_subset),
        ref_frac_x,
        ref_frac_y,
    )


# ---------------------------------------------------------------------------
# VRT builder
# ---------------------------------------------------------------------------


def _run_gdal(argv: list[str], *, cachemax: int) -> subprocess.CompletedProcess:
    """Run a GDAL CLI command, logging at INFO and propagating failures.

    Exits 3 if the subprocess returns a non-zero exit code, attaching the
    captured stderr tail to the log for diagnosis.
    """
    logger.info("GDAL: %s", " ".join(str(a) for a in argv))
    try:
        result = subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
        )
        if result.stdout.strip():
            logger.debug("GDAL stdout: %s", result.stdout[-2000:])
        return result
    except subprocess.CalledProcessError as exc:
        stderr_tail = (exc.stderr or "")[-3000:]
        logger.error(
            "GDAL command failed (exit %d): %s\nstderr (tail):\n%s",
            exc.returncode,
            " ".join(str(a) for a in argv),
            stderr_tail,
        )
        raise SystemExit(3)


def _probe_upa_nodata(upa_tile: Path) -> float:
    """Return the NoData value stored in a upa tile, falling back to -1.0."""
    try:
        with rasterio.open(upa_tile) as src:
            nd = src.nodata
            if nd is not None:
                return float(nd)
    except rasterio.errors.RasterioIOError as exc:
        logger.warning("Could not probe upa nodata from %s: %s", upa_tile, exc)
    logger.debug("upa tile has no declared nodata; using -1.0 as fallback")
    return -1.0


def _build_vrt(
    input_paths: list[Path],
    vrt_path: Path,
    nodata: float | int,
    *,
    cachemax: int = 4096,
) -> Path:
    """Build a GDAL VRT mosaic from ``input_paths`` at ``vrt_path``.

    For >= 500 inputs, uses a file list to avoid OS argument length limits.
    Post-build verifies the VRT is readable via gdalinfo.

    Returns ``vrt_path``.
    """
    vrt_path.parent.mkdir(parents=True, exist_ok=True)

    base_argv = [
        "gdalbuildvrt",
        "-resolution", "highest",
        "-vrtnodata", str(nodata),
        str(vrt_path),
    ]

    if len(input_paths) >= 500:
        input_list_path = vrt_path.with_suffix(".txt")
        input_list_path.write_text("\n".join(str(p) for p in input_paths) + "\n")
        argv = base_argv + ["-input_file_list", str(input_list_path)]
        logger.info(
            "Using -input_file_list (%d paths) -> %s",
            len(input_paths),
            input_list_path,
        )
    else:
        argv = base_argv + [str(p) for p in input_paths]

    _run_gdal(argv, cachemax=cachemax)

    # Post-build sanity check via gdalinfo.
    info_result = subprocess.run(
        ["gdalinfo", str(vrt_path)],
        capture_output=True,
        text=True,
    )
    if info_result.returncode != 0:
        logger.error(
            "gdalinfo on freshly-built VRT failed: %s\n%s",
            vrt_path,
            info_result.stderr[-1000:],
        )
        raise SystemExit(3)
    logger.info("VRT built and verified: %s", vrt_path)
    return vrt_path


# ---------------------------------------------------------------------------
# COG translation
# ---------------------------------------------------------------------------


def _translate_flow_dir(
    vrt: Path,
    out: Path,
    tmp: Path,
    *,
    cachemax: int,
    overviews: bool,
) -> None:
    """Transcode the flow-direction VRT into an HFX-conformant global COG.

    Three-stage process:
      1. gdal_translate: reinterpret int8 source as Byte (247 for NoData).
      2. gdal_calc.py: remap 247 -> 255 (HFX NoData), preserving 0 as a valid
         D8 sink indicator.
      3. gdal_translate: write COG with DEFLATE/PREDICTOR=2 compression.
    """
    step1 = tmp / "flow_dir_step1.tif"
    step2 = tmp / "flow_dir_step2.tif"

    logger.info("flow_dir step 1: reinterpret int8 -> Byte (247 becomes nodata candidate)")
    _run_gdal(
        [
            "gdal_translate",
            "-ot", "Byte",
            "-a_nodata", "255",
            "--config", "GDAL_CACHEMAX", str(cachemax),
            str(vrt),
            str(step1),
        ],
        cachemax=cachemax,
    )

    logger.info("flow_dir step 2: remap 247 -> 255 via gdal_calc.py")
    _run_gdal(
        [
            "gdal_calc.py",
            "-A", str(step1),
            "--calc", "where(A==247, 255, A)",
            "--NoDataValue=255",
            "--type=Byte",
            "--outfile", str(step2),
            "--overwrite",
        ],
        cachemax=cachemax,
    )

    overview_mode = "AUTO" if overviews else "NONE"
    logger.info("flow_dir step 3: write COG (overviews=%s) -> %s", overview_mode, out)
    _run_gdal(
        [
            "gdal_translate",
            "-of", "COG",
            "-co", f"BLOCKSIZE={COG_BLOCKSIZE}",
            "-co", "COMPRESS=DEFLATE",
            "-co", "PREDICTOR=2",
            "-co", "BIGTIFF=YES",
            "-co", "NUM_THREADS=ALL_CPUS",
            "-co", f"OVERVIEWS={overview_mode}",
            "--config", "GDAL_CACHEMAX", str(cachemax),
            str(step2),
            str(out),
        ],
        cachemax=cachemax,
    )

    logger.info("flow_dir COG written: %s", out)


def _translate_flow_acc(
    vrt: Path,
    out: Path,
    tmp: Path,
    *,
    src_nodata: float,
    cachemax: int,
    overviews: bool,
) -> None:
    """Transcode the flow-accumulation VRT into an HFX-conformant global COG.

    If the source NoData is not already -1.0, a gdal_calc.py remapping step is
    inserted before the final COG translation. DEFLATE + PREDICTOR=3 (floating
    point predictor) is used for compression.
    """
    overview_mode = "AUTO" if overviews else "NONE"

    if abs(src_nodata - FLOW_ACC_NODATA_OUT) > 1e-9:
        step1 = tmp / "flow_acc_step1.tif"
        logger.info(
            "flow_acc step 1: remap NoData %.6f -> -1.0 via gdal_calc.py",
            src_nodata,
        )
        _run_gdal(
            [
                "gdal_calc.py",
                "-A", str(vrt),
                "--calc", f"where(A=={src_nodata}, -1.0, A)",
                "--NoDataValue=-1.0",
                "--type=Float32",
                "--outfile", str(step1),
                "--overwrite",
            ],
            cachemax=cachemax,
        )
        cog_src = step1
    else:
        logger.info("flow_acc: source NoData already -1.0; skipping remap step")
        cog_src = vrt

    logger.info("flow_acc: write COG (overviews=%s) -> %s", overview_mode, out)
    _run_gdal(
        [
            "gdal_translate",
            "-of", "COG",
            "-ot", "Float32",
            "-a_nodata", str(FLOW_ACC_NODATA_OUT),
            "-co", f"BLOCKSIZE={COG_BLOCKSIZE}",
            "-co", "COMPRESS=DEFLATE",
            "-co", "PREDICTOR=3",
            "-co", "BIGTIFF=YES",
            "-co", "NUM_THREADS=ALL_CPUS",
            "-co", f"OVERVIEWS={overview_mode}",
            "--config", "GDAL_CACHEMAX", str(cachemax),
            str(cog_src),
            str(out),
        ],
        cachemax=cachemax,
    )

    logger.info("flow_acc COG written: %s", out)


# ---------------------------------------------------------------------------
# COG validation
# ---------------------------------------------------------------------------


def _validate_cog(path: Path, *, skip_overviews: bool = False) -> None:
    """Validate that ``path`` is a well-formed COG.

    Checks:
    - ``rio_cogeo.cog_validate`` is valid (``is_valid == True``).
    - ``gdalinfo`` output contains ``LAYOUT=COG`` in Image Structure Metadata.
    - At least 4 overview levels exist (unless ``skip_overviews=True``).

    Exits 4 on validation failure with the validator's error list.
    """
    is_valid, errors, warnings = cog_validate(str(path))
    if not is_valid:
        logger.error(
            "COG validation failed for %s:\n  errors: %s\n  warnings: %s",
            path,
            errors,
            warnings,
        )
        raise SystemExit(4)

    if warnings:
        logger.warning("COG warnings for %s: %s", path.name, warnings)

    # Verify LAYOUT=COG in gdalinfo output.
    info_result = subprocess.run(
        ["gdalinfo", str(path)],
        capture_output=True,
        text=True,
    )
    if "LAYOUT=COG" not in info_result.stdout:
        logger.error(
            "gdalinfo for %s does not show LAYOUT=COG in Image Structure "
            "Metadata. Output fragment:\n%s",
            path.name,
            info_result.stdout[:2000],
        )
        raise SystemExit(4)

    if not skip_overviews:
        overview_count = info_result.stdout.count("Overview ")
        if overview_count < 4:
            logger.error(
                "COG %s has only %d overview levels (expected >= 4). "
                "Re-run without --skip-overviews or check GDAL version.",
                path.name,
                overview_count,
            )
            raise SystemExit(4)
        logger.debug("%s: %d overview levels found", path.name, overview_count)

    logger.info("COG validated: %s", path)


# ---------------------------------------------------------------------------
# Sanity check against reference
# ---------------------------------------------------------------------------


def _sanity_check_against_reference(out_dir: Path, reference_dir: Path) -> None:
    """Compare the global COGs against a reference pfaf-27 region.

    Opens the global COGs and reads the sub-window matching the reference
    raster's bounds. Expects bit-exact agreement for flow_dir and
    numpy.allclose agreement for flow_acc (same MERIT source data).

    Logs a warning and returns if ``reference_dir`` does not exist.
    Exits 5 on mismatch with diff count, max delta, and a suggested
    pixel-offset diagnosis.
    """
    if not reference_dir.exists():
        logger.warning(
            "Reference directory does not exist: %s — skipping sanity check.",
            reference_dir,
        )
        return

    layers = [
        ("flow_dir", FLOW_DIR_NODATA_OUT, "dir"),
        ("flow_acc", FLOW_ACC_NODATA_OUT, "acc"),
    ]

    for layer_name, nodata, _short in layers:
        global_path = out_dir / f"{layer_name}.tif"
        ref_path = reference_dir / f"{layer_name}.tif"

        if not global_path.exists():
            logger.warning("Global %s not found at %s; skipping reference check.", layer_name, global_path)
            continue
        if not ref_path.exists():
            logger.warning("Reference %s not found at %s; skipping.", layer_name, ref_path)
            continue

        logger.info("Sanity-checking %s against reference %s", global_path.name, ref_path)

        try:
            with rasterio.open(ref_path) as ref_src:
                ref_bounds = ref_src.bounds
                ref_data = ref_src.read(1)
                ref_nodata = ref_src.nodata

            with rasterio.open(global_path) as global_src:
                import rasterio.windows as rw
                window = rw.from_bounds(
                    ref_bounds.left,
                    ref_bounds.bottom,
                    ref_bounds.right,
                    ref_bounds.top,
                    transform=global_src.transform,
                )
                global_data = global_src.read(1, window=window)
        except Exception as exc:
            logger.error("Error reading rasters for sanity check: %s", exc)
            raise SystemExit(5)

        # Align shapes — clip to the smaller of the two arrays.
        min_h = min(global_data.shape[0], ref_data.shape[0])
        min_w = min(global_data.shape[1], ref_data.shape[1])
        global_data = global_data[:min_h, :min_w]
        ref_data = ref_data[:min_h, :min_w]

        # Build valid-data mask (exclude NoData from both sides).
        global_valid = global_data != nodata
        ref_nd_val = float(ref_nodata) if ref_nodata is not None else nodata
        ref_valid = ref_data != ref_nd_val
        both_valid = global_valid & ref_valid

        g_vals = global_data[both_valid]
        r_vals = ref_data[both_valid]

        if layer_name == "flow_dir":
            match = np.array_equal(g_vals, r_vals)
            if not match:
                diff_count = int(np.sum(g_vals != r_vals))
                logger.error(
                    "flow_dir sanity check FAILED: %d / %d valid cells differ. "
                    "Check for pixel-offset mismatch (run gdalinfo on both files "
                    "and compare Origin / Pixel Size fields). Exits 5.",
                    diff_count,
                    len(g_vals),
                )
                raise SystemExit(5)
            logger.info("flow_dir sanity check PASSED: %d valid cells match", len(g_vals))

        else:  # flow_acc
            close = np.allclose(
                g_vals.astype("float64"),
                r_vals.astype("float64"),
                rtol=1e-5,
                atol=1e-3,
            )
            if not close:
                delta = np.abs(g_vals.astype("float64") - r_vals.astype("float64"))
                diff_count = int(np.sum(~np.isclose(g_vals.astype("float64"), r_vals.astype("float64"), rtol=1e-5, atol=1e-3)))
                max_delta = float(delta.max())
                logger.error(
                    "flow_acc sanity check FAILED: %d / %d valid cells outside "
                    "tolerance (max_delta=%.6f). "
                    "Check for pixel-offset mismatch. Exits 5.",
                    diff_count,
                    len(g_vals),
                    max_delta,
                )
                raise SystemExit(5)
            logger.info("flow_acc sanity check PASSED: %d valid cells within tolerance", len(g_vals))


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="build_global_rasters.py",
        description=(
            "Assemble pre-downloaded MERIT Hydro 5-degree tiles into two "
            "planet-wide Cloud-Optimized GeoTIFFs: flow_dir.tif and flow_acc.tif. "
            "See WORKFLOW.md §6 for download and integration instructions."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--source-dir",
        type=Path,
        required=True,
        metavar="PATH",
        help=(
            "Root of the pre-downloaded MERIT Hydro 5-degree tiles. "
            "Must contain dir/ and upa/ subdirectories. See WORKFLOW.md §6.1."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        metavar="PATH",
        help="Directory where flow_dir.tif and flow_acc.tif will be written.",
    )
    parser.add_argument(
        "--tmp-dir",
        type=Path,
        default=None,
        metavar="PATH",
        help="Scratch directory for intermediate files (default: <output-dir>/_tmp_rasters).",
    )
    parser.add_argument(
        "--reference-dir",
        type=Path,
        default=Path("/Users/nicolaslazaro/Desktop/merit-hfx/merit-hfx-pfaf27"),
        metavar="PATH",
        help=(
            "Optional reference HFX output directory (pfaf-27 or similar) for "
            "sanity-checking the global COGs against a known-good sub-region."
        ),
    )
    parser.add_argument(
        "--skip-overviews",
        action="store_true",
        default=False,
        help="Skip building overview pyramid levels (faster but COG not spatially indexed).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Build VRTs and validate tiles but skip the COG translate steps.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help=(
            "Process only the first 4 tiles per layer. "
            "Use to verify tool setup without downloading all tiles."
        ),
    )
    parser.add_argument(
        "--gdal-cachemax",
        type=int,
        default=4096,
        metavar="MB",
        help="GDAL_CACHEMAX in megabytes passed to all GDAL subprocesses.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        metavar="LEVEL",
        help="Logging verbosity.",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------


def main() -> int:
    """Orchestrate tile validation, VRT build, and COG translation."""
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    _check_gdal_tools()

    src = args.source_dir.resolve()
    out = args.output_dir.resolve()
    tmp = (args.tmp_dir or (out / "_tmp_rasters")).resolve()
    out.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        logger.error(
            "Source directory does not exist: %s. "
            "Download and unpack MERIT Hydro tiles per %s.",
            src,
            WORKFLOW_SECTION,
        )
        raise SystemExit(2)

    dir_tiles, upa_tiles = _enumerate_tiles(src, smoke=args.smoke_test)
    _validate_tiles(dir_tiles, layer="dir")
    _validate_tiles(upa_tiles, layer="upa")
    _check_grid_alignment(dir_tiles[:5])

    upa_nodata = _probe_upa_nodata(upa_tiles[0])

    dir_vrt = _build_vrt(
        dir_tiles,
        tmp / "flow_dir.vrt",
        nodata=MERIT_FLOWDIR_INT8_NODATA,
        cachemax=args.gdal_cachemax,
    )
    upa_vrt = _build_vrt(
        upa_tiles,
        tmp / "flow_acc.vrt",
        nodata=upa_nodata,
        cachemax=args.gdal_cachemax,
    )

    if args.dry_run:
        logger.info("--dry-run: VRTs built and tiles validated; skipping COG translation")
        return 0

    _translate_flow_dir(
        dir_vrt,
        out / "flow_dir.tif",
        tmp,
        cachemax=args.gdal_cachemax,
        overviews=not args.skip_overviews,
    )
    _translate_flow_acc(
        upa_vrt,
        out / "flow_acc.tif",
        tmp,
        src_nodata=upa_nodata,
        cachemax=args.gdal_cachemax,
        overviews=not args.skip_overviews,
    )

    _validate_cog(out / "flow_dir.tif", skip_overviews=args.skip_overviews)
    _validate_cog(out / "flow_acc.tif", skip_overviews=args.skip_overviews)

    _sanity_check_against_reference(out, args.reference_dir)

    shutil.rmtree(tmp, ignore_errors=True)
    logger.info("Done. Outputs written to %s", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
