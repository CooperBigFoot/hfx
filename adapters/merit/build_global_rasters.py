#!/usr/bin/env python3
"""Build global MERIT Hydro raster mosaics from mghydro per-basin TIFs.

Purpose
-------
Assembles the 60 mghydro per-basin rasters (Pfafstetter Level-2 codes, minus
pfaf-35) into two planet-wide Cloud-Optimized GeoTIFFs (COGs):

  <output-dir>/flow_dir.tif  — uint8, ESRI D8 encoding, NoData=255
  <output-dir>/flow_acc.tif  — float32, upstream pixel count, NoData=-1.0

This script operates entirely in the raster domain.  It does NOT touch
catchments.parquet, graph.arrow, snap.parquet, or manifest.json — those belong
to build_adapter.py / merge_basins.py.  After the two TIFs land, re-run
merge_basins.py with ``--rasters-ready --force`` to flip ``has_rasters=true``
in the manifest.

Inputs (from build_adapter.py docstring)
-----------------------------------------
Vectors:
  ~/data/merit_basins/pfaf_level_02/cat_pfaf_<NN>_*.shp
  (rclone'd from the Google Drive share — see README.md §2)

Rasters:
  ~/data/merit_hydro_rasters/flow_dir_basins/flowdir<NN>.tif
  ~/data/merit_hydro_rasters/accum_basins/accum<NN>.tif
  (curl'd from mghydro.com — see README.md §3)

Why polygon-masking before VRT-stitching
-----------------------------------------
mghydro's per-basin rasters are clipped to each basin's bounding box, not its
polygon.  Adjacent basins overlap in their bboxes (up to ~35° × 15°).  Inside
the overlap zone, each basin's raster stores value ``0`` outside its own polygon
and real D8 codes inside.  Because ``0`` is also a valid ESRI D8 "sink" code,
``gdalbuildvrt`` cannot disambiguate "outside polygon" from "valid sink" by
value alone — a naive VRT would corrupt real sinks.

Solution: rasterize each basin's catchment polygon onto the TIF's grid, then
write a masked copy where every outside-polygon pixel becomes NoData.  After
masking, the 60 TIFs have strictly non-overlapping valid data (Pfafstetter
basins are hydrologically disjoint by construction), making ``gdalbuildvrt``
unambiguous.

Flow-accumulation units
------------------------
The mghydro ``accum<NN>.tif`` files store upstream pixel counts (int32, NoData=0).
This script preserves them as float32 counts (matching the output of
``build_adapter.py``'s ``transcode_flow_acc`` helper), remapping source NoData 0
and outside-polygon pixels to ``FLOW_ACC_NODATA_OUT = -1.0``.

The per-pixel area formula (km²) is implemented in ``_compute_area_row_km2`` for
reference but is NOT applied in the pipeline — raw counts are retained so the
global COG is directly comparable to per-basin HFX outputs during sanity-check.
If a future version migrates to km², apply ``count × area`` inside
``_mask_basin`` and adjust the sanity-check tolerance accordingly.

Pfaf-35 exclusion
------------------
pfaf-35 (central Africa) wraps past 180°E; mghydro clips the raster to 180°E,
so the raster extent does not contain the full polygon.  The script detects this
mismatch and skips pfaf-35 with a WARN rather than failing.

Disk / RAM / wall-time budget
------------------------------
  Input rasters  : ~44 GB at ~/data/merit_hydro_rasters/
  Masked TIFs    : ~50 GB in <tmp-dir>/
  Output COGs    : ~28 GB (~8 GB flow_dir + ~20 GB flow_acc)
  Peak disk      : ~90 GB  (input stays; intermediates + output overlap)
  Peak RAM       : ~4 GB per worker (masking one int32 30000×66000 basin);
                   at -j 4 → ~16 GB
  Wall-clock     : masking ~60 min at -j 4; COG translate 30–60 min; total ~2 h
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import math
import shutil
import subprocess
import sys
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
import rasterio.features
import rasterio.windows
from rio_cogeo.cogeo import cog_validate
from shapely.ops import unary_union

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("merit-global-rasters")

# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

PIXEL_SIZE_DEG = 0.000833333333333  # 3 arc-seconds in decimal degrees
KM_PER_DEG = 111.32                 # approximate km per degree of latitude

FLOW_DIR_NODATA_OUT: int = 255
FLOW_ACC_NODATA_OUT: float = -1.0

COG_BLOCKSIZE = 512

# pfaf-35 wraps past 180°E — skip rather than hard-fail.
PFAF_SKIP = frozenset({35})

# ---------------------------------------------------------------------------
# Pre-flight helpers
# ---------------------------------------------------------------------------


def _check_gdal_tools() -> None:
    """Verify required GDAL CLI tools and Python imports are on PATH.

    # Errors
    Exits 2 if any tool is missing.
    """
    required_tools = ["gdalinfo", "gdalbuildvrt", "gdal_translate"]
    missing = [t for t in required_tools if shutil.which(t) is None]
    if missing:
        logger.error(
            "Missing GDAL CLI tools: %s. "
            "Install GDAL 3.5+ and ensure the binaries are on PATH.",
            ", ".join(missing),
        )
        raise SystemExit(2)

    try:
        import rio_cogeo.cogeo  # noqa: F401
    except ImportError as exc:
        logger.error(
            "rio_cogeo not importable: %s. Run `uv sync` inside adapters/merit/.",
            exc,
        )
        raise SystemExit(2)

    logger.debug("GDAL tools and Python imports OK")


def _check_directories(raster_root: Path, basins_root: Path) -> None:
    """Verify that the required input directories exist.

    # Errors
    Exits 2 if either directory is absent.
    """
    for label, path in (("--raster-root", raster_root), ("--merit-basins-root", basins_root)):
        if not path.is_dir():
            logger.error(
                "Required input directory missing: %s (arg %s). "
                "Download inputs per README.md §2–3.",
                path,
                label,
            )
            raise SystemExit(2)
    logger.debug("Input directories OK: raster=%s basins=%s", raster_root, basins_root)


# ---------------------------------------------------------------------------
# Basin masking worker (runs in a subprocess pool)
# ---------------------------------------------------------------------------


def _raster_contains_polygon(raster_bounds: rasterio.coords.BoundingBox, poly_bounds) -> bool:
    """Return True if ``raster_bounds`` fully contains ``poly_bounds``."""
    tol = 0.01  # ~10 pixels at 3 arcsec
    return (
        raster_bounds.left - tol <= poly_bounds.minx
        and raster_bounds.bottom - tol <= poly_bounds.miny
        and raster_bounds.right + tol >= poly_bounds.maxx
        and raster_bounds.top + tol >= poly_bounds.maxy
    )


def _compute_area_row_km2(transform: rasterio.Affine, height: int) -> np.ndarray:
    """Compute per-row pixel area (km²) for a 3-arcsec EPSG:4326 raster.

    For row ``r`` the latitude of the row centre is::

        lat = transform.f + (r + 0.5) * transform.e

    and the pixel area is::

        area = (PIXEL_SIZE_DEG × KM_PER_DEG)² × cos(radians(lat))

    Returns a float64 1-D array of length ``height``.
    """
    rows = np.arange(height, dtype=np.float64)
    lat_centres = transform.f + (rows + 0.5) * transform.e  # transform.e is negative
    area = (PIXEL_SIZE_DEG * KM_PER_DEG) ** 2 * np.cos(np.radians(lat_centres))
    return area.astype(np.float64)


def _mask_basin(
    pfaf: int,
    raster_root: Path,
    basins_root: Path,
    tmp_dir: Path,
) -> dict:
    """Mask one basin's flowdir and accum TIFs with its catchment polygon.

    Steps:
      1. Open flowdir<NN>.tif; read transform, shape.
      2. Read cat_pfaf_<NN>_*.shp; union all geometries.
      3. Check raster extent CONTAINS polygon extent. If not, log WARN and skip.
      4. Rasterize polygon mask.
      5. Apply mask to flowdir: outside → 255 (NoData). Write masked TIF.
      6. Apply mask to accum: src NoData (0) and outside-polygon → -1.0. Write masked TIF.

    Returns a dict with keys: pfaf, skipped, masked_flowdir_path,
    masked_accum_path, masked_pixel_count, wall_seconds.
    """
    t0 = time.monotonic()

    # --- paths ---
    flowdir_path = raster_root / "flow_dir_basins" / f"flowdir{pfaf}.tif"
    accum_path = raster_root / "accum_basins" / f"accum{pfaf}.tif"

    if not flowdir_path.is_file():
        raise FileNotFoundError(f"flowdir TIF missing: {flowdir_path}")
    if not accum_path.is_file():
        raise FileNotFoundError(f"accum TIF missing: {accum_path}")

    # --- load catchment polygon ---
    cat_matches = sorted(basins_root.glob(f"cat_pfaf_{pfaf:02d}_*.shp"))
    if not cat_matches:
        raise FileNotFoundError(
            f"No cat_pfaf_{pfaf:02d}_*.shp found under {basins_root}"
        )
    cat = gpd.read_file(cat_matches[0], engine="pyogrio")
    cat = cat.set_crs("EPSG:4326", allow_override=True)
    polygon = unary_union(cat.geometry.values)
    poly_bounds = polygon.bounds  # (minx, miny, maxx, maxy)

    # --- open flowdir to get grid parameters ---
    with rasterio.open(flowdir_path) as src:
        transform = src.transform
        shape = (src.height, src.width)
        crs = src.crs
        raster_bounds = src.bounds

        # --- containment check ---
        class _FakeBounds:
            minx = poly_bounds[0]
            miny = poly_bounds[1]
            maxx = poly_bounds[2]
            maxy = poly_bounds[3]

        if not _raster_contains_polygon(raster_bounds, _FakeBounds()):
            logger.warning(
                "pfaf-%02d: raster extent %s does not contain polygon extent %s "
                "— skipping (likely pfaf-35 180°E wrap issue).",
                pfaf,
                tuple(raster_bounds),
                poly_bounds,
            )
            return {
                "pfaf": pfaf,
                "skipped": True,
                "masked_flowdir_path": None,
                "masked_accum_path": None,
                "masked_pixel_count": 0,
                "wall_seconds": time.monotonic() - t0,
            }

        # --- rasterize polygon mask ---
        mask = rasterio.features.rasterize(
            [(polygon, 1)],
            out_shape=shape,
            transform=transform,
            fill=0,
            dtype=np.uint8,
            all_touched=False,
        ).astype(bool)

        # --- read and mask flowdir ---
        flowdir_data = src.read(1)  # uint8 from mghydro

    # Apply mask: outside polygon → 255 (NoData); inside preserves 0 (valid sink).
    masked_flowdir = np.where(mask, flowdir_data, FLOW_DIR_NODATA_OUT).astype(np.uint8)
    masked_pixel_count = int(mask.sum())

    # --- write masked flowdir ---
    out_flowdir = tmp_dir / f"masked_flowdir{pfaf:02d}.tif"
    with rasterio.open(
        out_flowdir,
        mode="w",
        driver="GTiff",
        dtype="uint8",
        count=1,
        width=shape[1],
        height=shape[0],
        crs=crs,
        transform=transform,
        nodata=FLOW_DIR_NODATA_OUT,
        compress="deflate",
        tiled=True,
        blockxsize=256,
        blockysize=256,
    ) as dst:
        dst.write(masked_flowdir, 1)

    logger.info(
        "pfaf-%02d: flowdir masked (%d valid pixels) -> %s",
        pfaf,
        masked_pixel_count,
        out_flowdir.name,
    )

    # --- read and mask accum ---
    with rasterio.open(accum_path) as src_acc:
        accum_data = src_acc.read(1).astype(np.int32)

    # Preserve raw cell counts as float32 (matching build_adapter.py output).
    # Source NoData 0 → -1.0; outside polygon → -1.0.
    valid_mask = mask & (accum_data != 0)
    masked_accum = np.where(
        valid_mask,
        accum_data.astype(np.float32),
        FLOW_ACC_NODATA_OUT,
    ).astype(np.float32)

    # --- write masked accum ---
    out_accum = tmp_dir / f"masked_accum{pfaf:02d}.tif"
    with rasterio.open(
        out_accum,
        mode="w",
        driver="GTiff",
        dtype="float32",
        count=1,
        width=shape[1],
        height=shape[0],
        crs=crs,
        transform=transform,
        nodata=FLOW_ACC_NODATA_OUT,
        compress="deflate",
        tiled=True,
        blockxsize=256,
        blockysize=256,
    ) as dst:
        dst.write(masked_accum, 1)

    elapsed = time.monotonic() - t0
    logger.info(
        "pfaf-%02d: accum masked (raw counts, NoData=-1.0) -> %s  (%.1f s)",
        pfaf,
        out_accum.name,
        elapsed,
    )

    return {
        "pfaf": pfaf,
        "skipped": False,
        "masked_flowdir_path": str(out_flowdir),
        "masked_accum_path": str(out_accum),
        "masked_pixel_count": masked_pixel_count,
        "wall_seconds": elapsed,
    }


# ---------------------------------------------------------------------------
# GDAL subprocess runner
# ---------------------------------------------------------------------------


def _run_gdal(argv: list[str], *, cachemax: int) -> subprocess.CompletedProcess:
    """Run a GDAL CLI command; exits 3 on non-zero return code."""
    env_prefix = ["--config", "GDAL_CACHEMAX", str(cachemax)]
    # Inject GDAL_CACHEMAX as --config args just before the output path for
    # gdal_translate, or use env for gdalbuildvrt.
    import os
    env = os.environ.copy()
    env["GDAL_CACHEMAX"] = str(cachemax)

    logger.info("GDAL: %s", " ".join(str(a) for a in argv))
    try:
        result = subprocess.run(
            argv,
            check=True,
            capture_output=True,
            text=True,
            env=env,
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


# ---------------------------------------------------------------------------
# VRT builder
# ---------------------------------------------------------------------------


def _build_vrt(
    input_paths: list[Path],
    vrt_path: Path,
    nodata: float | int,
    *,
    cachemax: int,
) -> Path:
    """Build a GDAL VRT from ``input_paths`` at ``vrt_path``."""
    vrt_path.parent.mkdir(parents=True, exist_ok=True)

    if len(input_paths) >= 20:
        input_list = vrt_path.with_suffix(".txt")
        input_list.write_text("\n".join(str(p) for p in input_paths) + "\n")
        argv = [
            "gdalbuildvrt",
            "-resolution", "highest",
            "-vrtnodata", str(nodata),
            str(vrt_path),
            "-input_file_list", str(input_list),
        ]
        logger.info(
            "Using -input_file_list (%d paths) -> %s", len(input_paths), input_list
        )
    else:
        argv = (
            ["gdalbuildvrt", "-resolution", "highest", "-vrtnodata", str(nodata), str(vrt_path)]
            + [str(p) for p in input_paths]
        )

    _run_gdal(argv, cachemax=cachemax)

    # Post-build sanity check.
    result = subprocess.run(
        ["gdalinfo", str(vrt_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.error(
            "gdalinfo on freshly-built VRT failed:\n%s", result.stderr[-1000:]
        )
        raise SystemExit(3)

    logger.info("VRT built and verified: %s", vrt_path)
    return vrt_path


# ---------------------------------------------------------------------------
# COG translation
# ---------------------------------------------------------------------------


def _translate_to_cog(
    src: Path,
    out: Path,
    *,
    dtype: str,
    nodata: str,
    predictor: int,
    overviews: bool,
    cachemax: int,
) -> None:
    """Translate ``src`` (VRT or TIF) to a COG at ``out``."""
    overview_mode = "AUTO" if overviews else "NONE"
    argv = [
        "gdal_translate",
        "-of", "COG",
        "-ot", dtype,
        "-a_nodata", nodata,
        "-co", f"BLOCKSIZE={COG_BLOCKSIZE}",
        "-co", "COMPRESS=DEFLATE",
        "-co", f"PREDICTOR={predictor}",
        "-co", "BIGTIFF=YES",
        "-co", "NUM_THREADS=ALL_CPUS",
        "-co", f"OVERVIEWS={overview_mode}",
        "--config", "GDAL_CACHEMAX", str(cachemax),
        str(src),
        str(out),
    ]
    _run_gdal(argv, cachemax=cachemax)
    logger.info("COG written: %s", out)


# ---------------------------------------------------------------------------
# COG validation
# ---------------------------------------------------------------------------


def _validate_cog(path: Path, *, skip_overviews: bool = False) -> None:
    """Validate ``path`` as a well-formed COG; exits 4 on failure."""
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

    info_result = subprocess.run(
        ["gdalinfo", str(path)],
        capture_output=True,
        text=True,
    )
    if "LAYOUT=COG" not in info_result.stdout:
        logger.error(
            "gdalinfo for %s does not show LAYOUT=COG. Fragment:\n%s",
            path.name,
            info_result.stdout[:2000],
        )
        raise SystemExit(4)

    if not skip_overviews:
        # gdalinfo prints overviews on a single line of the form
        # "  Overviews: 216000x87000, 108000x43500, 54000x21750, ..."
        # — count comma-separated resolutions on that line.
        overview_line = next(
            (
                ln
                for ln in info_result.stdout.splitlines()
                if ln.strip().startswith("Overviews:")
            ),
            "",
        )
        overview_count = overview_line.count(",") + 1 if overview_line.strip() else 0
        if overview_count < 4:
            logger.error(
                "COG %s has only %d overview levels (expected >= 4).",
                path.name,
                overview_count,
            )
            raise SystemExit(4)
        logger.debug("%s: %d overview levels found", path.name, overview_count)

    logger.info("COG validated: %s", path)


# ---------------------------------------------------------------------------
# Sanity check against per-basin reference
# ---------------------------------------------------------------------------


def _sanity_check(
    out_dir: Path,
    reference_dir: Path,
    *,
    flowdir_only: bool = False,
) -> None:
    """Compare global COGs against a per-basin reference directory.

    For flow_dir: bit-exact match inside the reference region's polygon.
    For flow_acc: match within rtol=1e-5 (absolute 1e-3).

    Note: the per-basin reference may store raw cell counts (not km²) if it was
    produced by build_adapter.py < v0.2 (which does not convert to km²).  In
    that case, flow_acc comparison may fail; use --skip-sanity-check to bypass.

    Logs a warning and returns if ``reference_dir`` does not exist.
    Exits 5 on mismatch.
    """
    if not reference_dir.exists():
        logger.warning(
            "Reference directory does not exist: %s — skipping sanity check.",
            reference_dir,
        )
        return

    layers: list[tuple[str, int | float]] = [
        ("flow_dir", FLOW_DIR_NODATA_OUT),
    ]
    if not flowdir_only:
        layers.append(("flow_acc", FLOW_ACC_NODATA_OUT))

    for layer_name, nodata in layers:
        global_path = out_dir / f"{layer_name}.tif"
        ref_path = reference_dir / f"{layer_name}.tif"

        if not global_path.exists():
            logger.warning(
                "Global %s not found at %s; skipping.", layer_name, global_path
            )
            continue
        if not ref_path.exists():
            logger.warning(
                "Reference %s not found at %s; skipping.", layer_name, ref_path
            )
            continue

        logger.info(
            "Sanity-checking %s against reference %s", global_path.name, ref_path
        )

        try:
            with rasterio.open(ref_path) as ref_src:
                ref_bounds = ref_src.bounds
                ref_data = ref_src.read(1)
                ref_nodata = ref_src.nodata

            with rasterio.open(global_path) as global_src:
                window = rasterio.windows.from_bounds(
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

        # Align shapes.
        min_h = min(global_data.shape[0], ref_data.shape[0])
        min_w = min(global_data.shape[1], ref_data.shape[1])
        global_data = global_data[:min_h, :min_w]
        ref_data = ref_data[:min_h, :min_w]

        # Valid-data mask.
        global_valid = global_data != nodata
        ref_nd_val = float(ref_nodata) if ref_nodata is not None else float(nodata)
        ref_valid = ref_data != ref_nd_val
        both_valid = global_valid & ref_valid

        g_vals = global_data[both_valid]
        r_vals = ref_data[both_valid]

        if len(g_vals) == 0:
            logger.warning(
                "%s sanity check: no valid pixels overlap — cannot compare.", layer_name
            )
            continue

        if layer_name == "flow_dir":
            if not np.array_equal(g_vals, r_vals):
                diff_count = int(np.sum(g_vals != r_vals))
                logger.error(
                    "flow_dir sanity check FAILED: %d / %d valid cells differ. "
                    "Check for pixel-offset mismatch (compare gdalinfo Origin/Pixel Size). "
                    "Exits 5.",
                    diff_count,
                    len(g_vals),
                )
                raise SystemExit(5)
            logger.info(
                "flow_dir sanity check PASSED: %d valid cells match (bit-exact)", len(g_vals)
            )

        else:  # flow_acc
            close = np.allclose(
                g_vals.astype(np.float64),
                r_vals.astype(np.float64),
                rtol=1e-5,
                atol=1e-3,
            )
            if not close:
                delta = np.abs(g_vals.astype(np.float64) - r_vals.astype(np.float64))
                diff_count = int(
                    np.sum(
                        ~np.isclose(
                            g_vals.astype(np.float64),
                            r_vals.astype(np.float64),
                            rtol=1e-5,
                            atol=1e-3,
                        )
                    )
                )
                max_delta = float(delta.max())
                logger.error(
                    "flow_acc sanity check FAILED: %d / %d valid cells outside tolerance "
                    "(max_delta=%.6f).  If the reference was built without km² conversion "
                    "(build_adapter.py pre-v0.2), use --skip-sanity-check.  Exits 5.",
                    diff_count,
                    len(g_vals),
                    max_delta,
                )
                raise SystemExit(5)
            logger.info(
                "flow_acc sanity check PASSED: %d valid cells within rtol=1e-5", len(g_vals)
            )


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="build_global_rasters.py",
        description=(
            "Mosaic mghydro per-basin TIFs into global COGs after polygon-masking "
            "each tile.  Produces flow_dir.tif (uint8, ESRI D8, NoData=255) and "
            "flow_acc.tif (float32 pixel counts, NoData=-1.0).  "
            "See README.md § Global raster mosaic for integration instructions."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--raster-root",
        type=Path,
        default=Path("~/data/merit_hydro_rasters").expanduser(),
        metavar="PATH",
        help=(
            "Root of the mghydro rasters.  Must contain flow_dir_basins/ and "
            "accum_basins/ subdirectories."
        ),
    )
    parser.add_argument(
        "--merit-basins-root",
        type=Path,
        default=Path("~/data/merit_basins/pfaf_level_02").expanduser(),
        metavar="PATH",
        help="Directory containing cat_pfaf_<NN>_*.shp catchment shapefiles.",
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
        help="Scratch directory for masked intermediate TIFs (default: <output-dir>/_tmp_rasters).",
    )
    parser.add_argument(
        "--reference-dir",
        type=Path,
        default=Path("/Users/nicolaslazaro/Desktop/merit-hfx/per-basin/merit-hfx-pfaf42"),
        metavar="PATH",
        help=(
            "Per-basin HFX output directory used for sanity-checking the global COGs. "
            "Must contain flow_dir.tif and flow_acc.tif."
        ),
    )
    parser.add_argument(
        "--basins",
        type=str,
        default=None,
        metavar="LIST",
        help=(
            "Comma-separated subset of Pfaf-L2 codes to process, e.g. '27' or '11,42'. "
            "Useful for smoke testing.  Default: all 60 valid codes."
        ),
    )
    parser.add_argument(
        "--parallelism",
        "-j",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel worker processes for the masking phase.",
    )
    parser.add_argument(
        "--skip-overviews",
        action="store_true",
        default=False,
        help="Skip building COG overview pyramid levels.",
    )
    parser.add_argument(
        "--skip-sanity-check",
        action="store_true",
        default=False,
        help="Skip the sanity check against the reference per-basin directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Validate inputs and print the masking plan without writing anything.",
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
# Basin list resolution
# ---------------------------------------------------------------------------


def _resolve_basin_codes(basins_arg: str | None) -> list[int]:
    """Expand the --basins argument into a sorted list of pfaf codes.

    Imports ``VALID_PFAF_CODES`` from ``run_missing_basins.py`` (61 codes) and
    removes pfaf-35 (extent mismatch) to arrive at the 60-basin mosaic set.
    Optionally filters to the user-supplied comma-separated subset.

    # Errors
    Exits 2 if an unknown code is specified.
    """
    # Import VALID_PFAF_CODES from sibling module.
    try:
        from run_missing_basins import VALID_PFAF_CODES
    except ImportError:
        # Fallback for when the module is not on sys.path (e.g. direct invocation).
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "run_missing_basins",
            Path(__file__).parent / "run_missing_basins.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        VALID_PFAF_CODES = mod.VALID_PFAF_CODES

    # Remove known-skip codes.
    mosaic_set = sorted(c for c in VALID_PFAF_CODES if c not in PFAF_SKIP)

    if basins_arg is None:
        return mosaic_set

    # User-specified subset.
    requested = []
    for token in basins_arg.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            code = int(token)
        except ValueError:
            logger.error("Invalid basin code %r — must be an integer.", token)
            raise SystemExit(2)
        if code not in mosaic_set:
            if code in PFAF_SKIP:
                logger.warning(
                    "pfaf-%02d is in PFAF_SKIP (%s); ignoring.", code, sorted(PFAF_SKIP)
                )
            else:
                logger.error(
                    "pfaf-%02d is not a valid mosaic code. Valid codes: %s",
                    code,
                    mosaic_set,
                )
                raise SystemExit(2)
        else:
            requested.append(code)

    return sorted(set(requested))


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------


def main() -> int:
    """Orchestrate masking, VRT build, COG translation, validation, and sanity check."""
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    # ---- Pre-flight ----
    _check_gdal_tools()

    raster_root = args.raster_root.expanduser().resolve()
    basins_root = args.merit_basins_root.expanduser().resolve()
    out_dir = args.output_dir.resolve()
    tmp_dir = (args.tmp_dir or (out_dir / "_tmp_rasters")).resolve()
    reference_dir = args.reference_dir.resolve()

    _check_directories(raster_root, basins_root)

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # ---- Basin list ----
    pfaf_codes = _resolve_basin_codes(args.basins)
    logger.info(
        "Mosaic set: %d basin(s) — %s%s",
        len(pfaf_codes),
        pfaf_codes[:10],
        " ..." if len(pfaf_codes) > 10 else "",
    )

    if args.dry_run:
        logger.info("--dry-run: pre-flight passed, basin list resolved. Exiting.")
        return 0

    # ---- Masking phase (parallel) ----
    logger.info(
        "Phase 1 — masking %d basin(s) with parallelism=%d",
        len(pfaf_codes),
        args.parallelism,
    )

    masked_flowdir_paths: list[Path] = []
    masked_accum_paths: list[Path] = []
    failed_pfafs: list[int] = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.parallelism) as executor:
        futures: dict[concurrent.futures.Future, int] = {
            executor.submit(
                _mask_basin, pfaf, raster_root, basins_root, tmp_dir
            ): pfaf
            for pfaf in pfaf_codes
        }
        for fut in concurrent.futures.as_completed(futures):
            pfaf = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:
                logger.error("pfaf-%02d: masking raised exception: %s", pfaf, exc)
                failed_pfafs.append(pfaf)
                continue

            if result["skipped"]:
                logger.warning(
                    "pfaf-%02d: skipped (%.1f s)", pfaf, result["wall_seconds"]
                )
                continue

            masked_flowdir_paths.append(Path(result["masked_flowdir_path"]))
            masked_accum_paths.append(Path(result["masked_accum_path"]))
            logger.info(
                "pfaf-%02d: done — %d valid pixels, %.1f s",
                pfaf,
                result["masked_pixel_count"],
                result["wall_seconds"],
            )

    if failed_pfafs:
        logger.error(
            "Masking failed for %d basin(s): %s", len(failed_pfafs), failed_pfafs
        )
        raise SystemExit(3)

    if not masked_flowdir_paths:
        logger.error("No masked TIFs produced — nothing to mosaic.")
        raise SystemExit(3)

    logger.info(
        "Phase 1 complete: %d flowdir + %d accum masked TIFs",
        len(masked_flowdir_paths),
        len(masked_accum_paths),
    )

    # ---- VRT phase ----
    logger.info("Phase 2 — building VRTs")
    flowdir_vrt = _build_vrt(
        sorted(masked_flowdir_paths),
        tmp_dir / "flow_dir.vrt",
        nodata=FLOW_DIR_NODATA_OUT,
        cachemax=args.gdal_cachemax,
    )
    accum_vrt = _build_vrt(
        sorted(masked_accum_paths),
        tmp_dir / "flow_acc.vrt",
        nodata=FLOW_ACC_NODATA_OUT,
        cachemax=args.gdal_cachemax,
    )

    # ---- COG translation ----
    logger.info("Phase 3 — translating to COG")
    flow_dir_out = out_dir / "flow_dir.tif"
    flow_acc_out = out_dir / "flow_acc.tif"

    _translate_to_cog(
        flowdir_vrt,
        flow_dir_out,
        dtype="Byte",
        nodata=str(FLOW_DIR_NODATA_OUT),
        predictor=2,
        overviews=not args.skip_overviews,
        cachemax=args.gdal_cachemax,
    )
    _translate_to_cog(
        accum_vrt,
        flow_acc_out,
        dtype="Float32",
        nodata=str(FLOW_ACC_NODATA_OUT),
        predictor=3,
        overviews=not args.skip_overviews,
        cachemax=args.gdal_cachemax,
    )

    # ---- COG validation ----
    logger.info("Phase 4 — COG validation")
    _validate_cog(flow_dir_out, skip_overviews=args.skip_overviews)
    _validate_cog(flow_acc_out, skip_overviews=args.skip_overviews)

    # ---- Sanity check ----
    if not args.skip_sanity_check:
        logger.info("Phase 5 — sanity check against reference: %s", reference_dir)
        _sanity_check(out_dir, reference_dir)
    else:
        logger.info("Phase 5 — sanity check skipped (--skip-sanity-check)")

    # ---- Cleanup ----
    shutil.rmtree(tmp_dir, ignore_errors=True)
    logger.info("Cleanup: removed %s", tmp_dir)
    logger.info("Done.  Outputs written to %s", out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
