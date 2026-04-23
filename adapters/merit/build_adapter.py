#!/usr/bin/env python3
"""MERIT-Basins → HFX adapter.

Starting point (what a researcher should have on disk before running this):

  Vectors — MERIT-Basins v0.7/v1.0_bugfix1 (Lin et al. 2019), per Pfaf-L2 basin.
  Download via rclone from the Google Drive share:

    rclone copy --drive-shared-with-me \\
      "GoogleDrive:MERIT-Hydro_v07_Basins_v01_bugfix1/pfaf_level_02/" \\
      ~/data/merit_basins/pfaf_level_02/ \\
      --include "*pfaf_<NN>_*"

  Rasters — MERIT Hydro flow direction & flow accumulation, basin-merged
  rehost by M. Heberger (mghydro.com), derived from Yamazaki et al. 2019:

    curl -o ~/data/merit_hydro_rasters/flow_dir_basins/flowdir<NN>.tif \\
      https://mghydro.com/watersheds/rasters/flow_dir_basins/flowdir<NN>.tif
    curl -o ~/data/merit_hydro_rasters/accum_basins/accum<NN>.tif \\
      https://mghydro.com/watersheds/rasters/accum_basins/accum<NN>.tif

  <NN> is the 2-digit Pfafstetter Level-2 basin code (11..91).

Cite:
  Lin, P., Pan, M., Beck, H. E., et al. (2019). MERIT-Basins. WRR.
  Yamazaki, D., Ikeshima, D., Sosa, J., et al. (2019). MERIT Hydro. WRR.

Usage
-----
    uv sync
    uv run python build_adapter.py extract \\
        --merit-basins ~/data/merit_basins/pfaf_level_02 \\
        --rasters ~/data/merit_hydro_rasters \\
        --pfaf 27
    uv run python build_adapter.py build \\
        --merit-basins ~/data/merit_basins/pfaf_level_02 \\
        --rasters ~/data/merit_hydro_rasters \\
        --pfaf 27 \\
        --out ./out
    uv run python build_adapter.py validate --out ./out
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import numpy as np
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import rasterio
from geoparquet_io.core.validate import validate_geoparquet
from rasterio.windows import Window, from_bounds
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles
from shapely import make_valid
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("merit-hfx")


# ---------------------------------------------------------------------------
# Module-level configuration
# ---------------------------------------------------------------------------

# ``fabric_name`` is Pfaf-L2-scoped. Build a concrete value per invocation via
# ``FABRIC_NAME_FMT.format(pfaf=27)`` → ``"merit_basins_pfaf27"``.
FABRIC_NAME_FMT = "merit_basins_pfaf{pfaf:02d}"
ADAPTER_VERSION = "0.1.0"
TOPOLOGY = "tree"
HAS_UP_AREA = True
HAS_RASTERS = True
HAS_SNAP = True

ROW_GROUP_MIN = 4096
ROW_GROUP_MAX = 8192

# MERIT Hydro flow-direction uses the ESRI D8 convention (powers of two:
# 1,2,4,8,16,32,64,128). The HFX manifest declares this via ``flow_dir_encoding``.
FLOW_DIR_ENCODING = "esri"

# HFX-required NoData values for the transcoded rasters.
FLOW_DIR_NODATA_OUT = 255
FLOW_ACC_NODATA_OUT = -1.0

# MERIT Hydro encodes "undefined / ocean" in flow_dir as int8 -9, which reads
# back as 247 when the byte is reinterpreted as uint8. The raster transcode
# step must remap this to ``FLOW_DIR_NODATA_OUT``.
MERIT_FLOWDIR_UNDEFINED_AS_UINT8 = 247

# Outward-padding epsilons. Manifest bbox is padded to survive rounding in the
# validator's enclosure check; snap bboxes have axis-aligned LineStrings
# that can produce zero-width bounding boxes.
MANIFEST_BBOX_EPSILON = 1e-4
SNAP_BBOX_EPSILON = 1e-4


# ---------------------------------------------------------------------------
# Source data container
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceData:
    """Holds raw inputs loaded from the MERIT-Basins hydrofabric for one Pfaf-L2 basin.

    Attributes
    ----------
    catchments:
        MERIT-Basins ``cat_pfaf_<NN>_*.shp`` polygon features. CRS is forced
        to EPSG:4326 at load time because the shapefile ships without a .prj.
    rivers:
        MERIT-Basins ``riv_pfaf_<NN>_*.shp`` reach centerlines, used for the
        snap layer and for graph adjacency (``NextDownID`` column).
    flow_dir_path:
        Path to the source MERIT Hydro flow-direction GeoTIFF (pre-transcode).
    flow_acc_path:
        Path to the source MERIT Hydro flow-accumulation GeoTIFF (pre-transcode).
    pfaf:
        Pfafstetter Level-2 basin code (11..91).
    """

    catchments: gpd.GeoDataFrame
    rivers: gpd.GeoDataFrame
    flow_dir_path: Path
    flow_acc_path: Path
    pfaf: int


# ---------------------------------------------------------------------------
# Implemented helpers (vendored verbatim from adapters/grit/build_grit_eu_hfx.py)
# ---------------------------------------------------------------------------

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


def assert_geoparquet_valid(out_path: Path, kind: str = "parquet") -> None:
    """Assert that ``out_path`` passes GeoParquet 1.1 validation.

    Raises ``RuntimeError`` listing every failed check if validation fails.
    ``kind`` is a short label (``"catchments"``/``"snap"``) used in error text.
    """
    result = validate_geoparquet(str(out_path), target_version="1.1")
    if not result.is_valid:
        failures = [c for c in result.checks if c.status.value == "failed"]
        raise RuntimeError(
            f"GeoParquet 1.1 validation failed for {kind} at {out_path}: "
            + "; ".join(f"{c.name}: {c.message}" for c in failures)
        )


def balanced_row_group_bounds(
    total_rows: int,
    min_size: int = ROW_GROUP_MIN,
    max_size: int = ROW_GROUP_MAX,
) -> list[tuple[int, int]]:
    """Split ``total_rows`` into row-group slices of size in ``[min_size, max_size]``."""
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


def outward_bbox(
    bounds: tuple[float, float, float, float],
    pad: float = MANIFEST_BBOX_EPSILON,
) -> list[float]:
    """Pad the dataset bbox outward by ``pad`` on every side.

    Returns ``[minx, miny, maxx, maxy]``. This survives floating-point rounding
    in the validator's manifest-bbox enclosure check against
    ``catchments.parquet`` total bounds.
    """
    minx, miny, maxx, maxy = bounds
    return [
        float(minx) - pad,
        float(miny) - pad,
        float(maxx) + pad,
        float(maxy) + pad,
    ]


def inflate_degenerate_bounds(bounds, epsilon: float = SNAP_BBOX_EPSILON):
    """Pad zero-extent snap bounds by ``epsilon`` so each axis has positive width.

    ``bounds`` is a pandas DataFrame with columns ``minx, miny, maxx, maxy``
    (the output of ``GeoSeries.bounds``). Axis-aligned LineStrings produce
    bounding boxes with zero extent along one axis; this helper nudges them
    outward so downstream strict-inequality checks pass for catchments while
    snap features use non-strict ``<=``.
    """
    bounds = bounds.copy()
    same_x = bounds["minx"] >= bounds["maxx"]
    same_y = bounds["miny"] >= bounds["maxy"]
    bounds.loc[same_x, "minx"] = bounds.loc[same_x, "minx"] - epsilon
    bounds.loc[same_x, "maxx"] = bounds.loc[same_x, "maxx"] + epsilon
    bounds.loc[same_y, "miny"] = bounds.loc[same_y, "miny"] - epsilon
    bounds.loc[same_y, "maxy"] = bounds.loc[same_y, "maxy"] + epsilon
    return bounds


def _coerce_to_polygonal(geom: BaseGeometry) -> BaseGeometry:
    """Repair ``geom`` and coerce the result to ``Polygon``/``MultiPolygon``.

    ``shapely.make_valid`` can return a ``GeometryCollection`` when a fix
    needs to express mixed dimensions (polygons + lines + points).  The HFX
    spec only permits ``Polygon``/``MultiPolygon`` in ``catchments.parquet``,
    so this helper drops non-polygonal shards and merges surviving polygons
    back into a single feature.
    """
    if geom.is_valid:
        return geom
    repaired = make_valid(geom)
    if isinstance(repaired, (Polygon, MultiPolygon)):
        return repaired
    if isinstance(repaired, GeometryCollection):
        polys: list[Polygon] = []
        for part in repaired.geoms:
            if isinstance(part, Polygon):
                polys.append(part)
            elif isinstance(part, MultiPolygon):
                polys.extend(part.geoms)
        if not polys:
            raise ValueError(
                "make_valid produced a GeometryCollection with no polygonal parts"
            )
        if len(polys) == 1:
            return polys[0]
        return MultiPolygon(polys)
    raise ValueError(
        f"make_valid produced unsupported geometry type: {type(repaired).__name__}"
    )


# ---------------------------------------------------------------------------
# Stage stubs — implement each in the MERIT adapter
# ---------------------------------------------------------------------------

def stage_1_inspect_source(
    merit_basins_root: Path,
    rasters_root: Path,
    pfaf: int,
) -> SourceData:
    """Load and inspect the MERIT-Basins shapefiles plus raster paths for ``pfaf``.

    Reads ``cat_pfaf_<NN>_*.shp`` and ``riv_pfaf_<NN>_*.shp`` from
    ``merit_basins_root`` with CRS forced to EPSG:4326 (the source ships no
    .prj alongside the shapefiles), and resolves the two MERIT Hydro raster
    paths under ``rasters_root``. Validates expected columns and returns a
    frozen ``SourceData``.
    """
    # Tolerate both "<root>" and "<root>/pfaf_level_02" as the basins dir.
    candidate = merit_basins_root / "pfaf_level_02"
    basins_dir = candidate if candidate.is_dir() else merit_basins_root
    if not basins_dir.is_dir():
        raise RuntimeError(
            f"MERIT-Basins root does not exist or is not a directory: {basins_dir}"
        )

    cat_glob = f"cat_pfaf_{pfaf}_*.shp"
    riv_glob = f"riv_pfaf_{pfaf}_*.shp"
    cat_matches = sorted(basins_dir.glob(cat_glob))
    riv_matches = sorted(basins_dir.glob(riv_glob))

    if len(cat_matches) != 1:
        raise RuntimeError(
            f"expected exactly one match for {cat_glob} under {basins_dir}, "
            f"found {len(cat_matches)}: {cat_matches}"
        )
    if len(riv_matches) != 1:
        raise RuntimeError(
            f"expected exactly one match for {riv_glob} under {basins_dir}, "
            f"found {len(riv_matches)}: {riv_matches}"
        )

    cat_path = cat_matches[0]
    riv_path = riv_matches[0]

    logger.info("reading catchments shapefile: %s", cat_path)
    cat = gpd.read_file(cat_path, engine="pyogrio")
    logger.info(
        "forcing catchments CRS to EPSG:4326 (source ships without .prj); "
        "original crs=%s",
        cat.crs,
    )
    cat = cat.set_crs("EPSG:4326", allow_override=True)

    logger.info("reading rivers shapefile: %s", riv_path)
    riv = gpd.read_file(riv_path, engine="pyogrio")
    if riv.crs is None:
        raise RuntimeError(f"rivers shapefile {riv_path} has no CRS")
    riv_epsg = riv.crs.to_epsg()
    if riv_epsg != 4326 and str(riv.crs) != "EPSG:4326":
        raise RuntimeError(
            f"rivers shapefile {riv_path} must be EPSG:4326, got {riv.crs}"
        )

    if len(cat) != len(riv):
        raise RuntimeError(
            f"catchment/river row count mismatch: cat={len(cat)} riv={len(riv)}"
        )
    cat_ids = set(cat["COMID"].tolist())
    riv_ids = set(riv["COMID"].tolist())
    if cat_ids != riv_ids:
        cat_only = sorted(cat_ids - riv_ids)[:10]
        riv_only = sorted(riv_ids - cat_ids)[:10]
        raise RuntimeError(
            f"catchment/river COMID set mismatch: "
            f"cat-only={cat_only} riv-only={riv_only}"
        )

    flowdir_path = rasters_root / "flow_dir_basins" / f"flowdir{pfaf}.tif"
    accum_path = rasters_root / "accum_basins" / f"accum{pfaf}.tif"
    if not flowdir_path.is_file():
        raise RuntimeError(f"flow direction raster missing: {flowdir_path}")
    if not accum_path.is_file():
        raise RuntimeError(f"flow accumulation raster missing: {accum_path}")

    cat_bounds = tuple(float(v) for v in cat.total_bounds)
    raster_tol = 0.01  # ~1 pixel margin for 0.000833° MERIT Hydro
    for label, rpath in (("flow_dir", flowdir_path), ("flow_acc", accum_path)):
        with rasterio.open(rpath) as src:
            r_crs = src.crs
            if r_crs is None or r_crs.to_epsg() != 4326:
                raise RuntimeError(
                    f"{label} raster {rpath} must be EPSG:4326, got {r_crs}"
                )
            r_bounds = src.bounds
            if not (
                r_bounds.left - raster_tol <= cat_bounds[0]
                and r_bounds.bottom - raster_tol <= cat_bounds[1]
                and r_bounds.right + raster_tol >= cat_bounds[2]
                and r_bounds.top + raster_tol >= cat_bounds[3]
            ):
                raise RuntimeError(
                    f"{label} raster footprint {tuple(r_bounds)} does not "
                    f"contain catchment bbox {cat_bounds} (tol={raster_tol})"
                )
            logger.info(
                "%s raster ok: path=%s size=%dx%d bounds=%s",
                label,
                rpath,
                src.width,
                src.height,
                tuple(r_bounds),
            )

    comid_min = int(cat["COMID"].min())
    comid_max = int(cat["COMID"].max())
    logger.info(
        "stage_1_inspect_source pfaf=%d catchments=%d rivers=%d "
        "COMID range=%d..%d bbox=%s",
        pfaf,
        len(cat),
        len(riv),
        comid_min,
        comid_max,
        cat_bounds,
    )

    return SourceData(
        catchments=cat,
        rivers=riv,
        flow_dir_path=flowdir_path,
        flow_acc_path=accum_path,
        pfaf=pfaf,
    )


def stage_2_assign_ids(source: SourceData) -> gpd.GeoDataFrame:
    """Map MERIT-Basins ``COMID`` to HFX ``id`` (int64 > 0).

    MERIT ``COMID`` is already a positive unique integer per Pfaf-L2 basin.
    This stage validates uniqueness, asserts no zero/negative values, casts
    to int64, and returns a GeoDataFrame with an ``id`` column.
    """
    gdf = source.catchments.copy()
    gdf["id"] = gdf["COMID"].astype("int64")
    if gdf["id"].isna().any():
        raise RuntimeError("stage_2_assign_ids: COMID contains null values")
    if (gdf["id"] <= 0).any():
        raise RuntimeError(
            "stage_2_assign_ids: COMID contains 0 or negative values "
            "(id=0 is reserved for the terminal sink)"
        )
    if gdf["id"].duplicated().any():
        dupes = gdf.loc[gdf["id"].duplicated(), "id"].tolist()[:10]
        raise RuntimeError(f"stage_2_assign_ids: duplicate COMID values: {dupes}")

    id_min = int(gdf["id"].min())
    id_max = int(gdf["id"].max())
    logger.info(
        "stage_2_assign_ids assigned %d ids, range %d..%d",
        len(gdf),
        id_min,
        id_max,
    )
    return gdf


def stage_3_reproject(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure the geometries are in EPSG:4326.

    MERIT-Basins shapefiles are already in WGS84 but ship without a .prj, so
    stage 1 assigns CRS explicitly. This stage validates the CRS assignment
    and returns ``gdf`` unchanged (or reprojects if a deviation is ever detected).
    """
    if gdf.crs is None:
        raise RuntimeError("stage_3_reproject: gdf has no CRS (stage 1 should force EPSG:4326)")
    epsg = gdf.crs.to_epsg()
    if epsg != 4326:
        raise RuntimeError(
            f"stage_3_reproject: expected EPSG:4326, got {gdf.crs} (epsg={epsg})"
        )
    logger.info("stage_3_reproject: CRS is EPSG:4326, no reprojection needed")
    return gdf


def stage_4_make_valid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Run ``shapely.make_valid`` on every MERIT catchment polygon.

    Uses the vendored ``_coerce_to_polygonal`` helper so any
    ``GeometryCollection`` produced by ``make_valid`` is flattened back to
    ``Polygon``/``MultiPolygon``.
    """
    gdf = gdf.copy()
    invalid_mask = ~gdf.geometry.is_valid
    invalid_count = int(invalid_mask.sum())
    logger.info(
        "stage_4_make_valid: %d/%d invalid geoms before repair",
        invalid_count,
        len(gdf),
    )

    # shapely.make_valid then coerce non-polygonal byproducts back to polygons.
    gdf["geometry"] = gdf.geometry.apply(
        lambda g: _coerce_to_polygonal(g) if not g.is_valid else g
    )

    types = set(gdf.geometry.geom_type.unique())
    allowed = {"Polygon", "MultiPolygon"}
    extras = types - allowed
    if extras:
        raise RuntimeError(
            f"stage_4_make_valid: unexpected geometry types after repair: {extras}"
        )
    logger.info("stage_4_make_valid: geometry types = %s", sorted(types))
    return gdf


def stage_5_hilbert_sort(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Sort catchments by Hilbert-curve index on centroid coordinates.

    Uses ``GeoSeries.hilbert_distance(total_bounds=gdf.total_bounds)`` and
    adds a stable secondary key on ``id`` for determinism.
    """
    gdf = gdf.copy()
    total_bounds = tuple(float(v) for v in gdf.total_bounds)
    centroids = gdf.geometry.centroid
    gdf["_hilbert"] = centroids.hilbert_distance(total_bounds=total_bounds)
    gdf = gdf.sort_values(["_hilbert", "id"], kind="mergesort").reset_index(drop=True)
    logger.debug(
        "stage_5_hilbert_sort first hilbert distances: %s",
        gdf["_hilbert"].head(5).tolist(),
    )
    gdf = gdf.drop(columns=["_hilbert"])
    logger.info("stage_5_hilbert_sort: %d rows Hilbert-sorted", len(gdf))
    return gdf


def stage_6_write_catchments(
    catchments_sorted: gpd.GeoDataFrame,
    rivers: gpd.GeoDataFrame,
    out_dir: Path,
) -> tuple[list[int], tuple[float, float, float, float]]:
    """Write ``catchments.parquet`` conformant with HFX spec §1.

    Joins the MERIT reach ``uparea`` column onto the catchment table to
    populate ``up_area_km2`` (``HAS_UP_AREA = True``), writes the table via
    ``pq.ParquetWriter`` with GeoParquet 1.1 metadata attached to the schema,
    distributes rows across row groups using ``balanced_row_group_bounds``,
    and calls ``assert_geoparquet_valid`` on the result.

    Returns ``(ids, total_bounds)`` for stages 7–9 to consume.
    """
    row_count = len(catchments_sorted)
    total_bounds = tuple(float(v) for v in catchments_sorted.total_bounds)

    # Join rivers.uparea onto catchments by COMID (= id). 1:1 by construction.
    uparea_map = dict(
        zip(
            rivers["COMID"].astype("int64").to_numpy(),
            rivers["uparea"].astype("float64").to_numpy(),
        )
    )
    ids_np = catchments_sorted["id"].astype("int64").to_numpy()
    up_area = np.asarray([uparea_map[int(c)] for c in ids_np], dtype="float32")
    if np.isnan(up_area).any():
        raise RuntimeError(
            "stage_6_write_catchments: up_area_km2 contains NaN after river join"
        )

    area_km2 = catchments_sorted["unitarea"].astype("float32").to_numpy()
    if np.isnan(area_km2).any():
        raise RuntimeError("stage_6_write_catchments: unitarea contains NaN")

    bounds = catchments_sorted.geometry.bounds  # DataFrame: minx, miny, maxx, maxy

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
    schema = schema.with_metadata(
        build_geo_metadata(["Polygon", "MultiPolygon"])
    )

    out_path = out_dir / "catchments.parquet"
    row_groups = balanced_row_group_bounds(row_count, ROW_GROUP_MIN, ROW_GROUP_MAX)
    logger.info(
        "stage_6_write_catchments writing %d rows in %d row groups -> %s",
        row_count,
        len(row_groups),
        out_path,
    )

    with pq.ParquetWriter(
        out_path,
        schema=schema,
        compression="snappy",
        write_statistics=True,
    ) as writer:
        for start, stop in row_groups:
            chunk = catchments_sorted.iloc[start:stop]
            chunk_bounds = bounds.iloc[start:stop]
            geometry_wkb = chunk.geometry.to_wkb(hex=False)
            table = pa.Table.from_arrays(
                [
                    pa.array(ids_np[start:stop], type=pa.int64()),
                    pa.array(area_km2[start:stop], type=pa.float32()),
                    pa.array(up_area[start:stop], type=pa.float32()),
                    pa.array(
                        chunk_bounds["minx"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(
                        chunk_bounds["miny"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(
                        chunk_bounds["maxx"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(
                        chunk_bounds["maxy"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(geometry_wkb.tolist(), type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table)

    assert_geoparquet_valid(out_path, kind="catchments")
    logger.info("stage_6_write_catchments: catchments.parquet validated")

    ids = [int(v) for v in ids_np.tolist()]
    return ids, total_bounds


def stage_7_write_graph(
    ids: list[int],
    rivers: gpd.GeoDataFrame,
    out_dir: Path,
) -> None:
    """Write ``graph.arrow`` from the MERIT ``NextDownID`` reach column.

    For each catchment ``id`` collects the set of upstream reach ``COMID``
    values where ``NextDownID == id``, emits a row per id (empty list for
    headwaters), and serialises as Arrow IPC.
    """
    id_set = set(int(i) for i in ids)
    upstream: dict[int, list[int]] = {i: [] for i in id_set}

    comids = rivers["COMID"].astype("int64").to_numpy()
    next_down = rivers["NextDownID"].astype("int64").to_numpy()
    for comid, nxt in zip(comids, next_down):
        comid = int(comid)
        nxt = int(nxt)
        if nxt == comid:
            continue  # degenerate MERIT terminal sink; no parent
        if nxt == 0:
            continue  # terminal outlet; no parent
        if nxt not in upstream:
            # downstream target missing from catchments — would fail referential
            # integrity. Skip but log; we do not silently invent edges.
            logger.warning(
                "stage_7_write_graph: NextDownID=%d not in catchments for COMID=%d",
                nxt,
                comid,
            )
            continue
        upstream[nxt].append(comid)

    # Acyclicity check via iterative DFS.
    WHITE, GREY, BLACK = 0, 1, 2
    color = {i: WHITE for i in id_set}
    for root in id_set:
        if color[root] != WHITE:
            continue
        stack: list[tuple[int, int]] = [(root, 0)]
        color[root] = GREY
        while stack:
            node, idx = stack[-1]
            children = upstream.get(node, [])
            if idx < len(children):
                stack[-1] = (node, idx + 1)
                child = children[idx]
                c = color.get(child, WHITE)
                if c == GREY:
                    raise RuntimeError(
                        f"stage_7_write_graph: cycle detected at edge {node} -> {child}"
                    )
                if c == WHITE:
                    color[child] = GREY
                    stack.append((child, 0))
            else:
                color[node] = BLACK
                stack.pop()

    ordered_ids = sorted(id_set)
    upstream_lists = [sorted(upstream[i]) for i in ordered_ids]

    list_type = pa.list_(pa.field("item", pa.int64(), nullable=True))
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("upstream_ids", list_type, nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(ordered_ids, type=pa.int64()),
            pa.array(upstream_lists, type=list_type),
        ],
        schema=schema,
    )

    out_path = out_dir / "graph.arrow"
    with pa.OSFile(str(out_path), "wb") as sink:
        with pa_ipc.new_file(sink, schema) as writer:
            writer.write(table)

    headwaters = sum(1 for u in upstream_lists if not u)
    max_upstream = max((len(u) for u in upstream_lists), default=0)
    logger.info(
        "stage_7_write_graph wrote %s: nodes=%d headwaters=%d max_upstream=%d",
        out_path,
        len(ordered_ids),
        headwaters,
        max_upstream,
    )


def stage_8_write_snap(
    ids: list[int],
    rivers: gpd.GeoDataFrame,
    cat_total_bounds: tuple[float, float, float, float],
    out_dir: Path,
) -> None:
    """Write ``snap.parquet`` from MERIT reach centerlines (``riv_pfaf_<NN>``).

    Schema per spec §3: ``id``, ``catchment_id``, ``weight`` (= ``uparea``),
    ``is_mainstem`` (always True in a strict D8 tree — no bifurcations),
    ``bbox_*`` (with ``inflate_degenerate_bounds`` applied), ``geometry``.
    Hilbert-sorts the snap table using ``cat_total_bounds`` as the normalising
    extent so snap rows share the catchment's Hilbert frame.
    """
    snap = rivers.copy()
    # Normalise types early.
    snap["COMID"] = snap["COMID"].astype("int64")
    snap["NextDownID"] = snap["NextDownID"].astype("int64")
    snap["uparea"] = snap["uparea"].astype("float64")

    # Referential integrity: every snap catchment_id must exist in catchments.
    id_set = set(int(i) for i in ids)
    snap_ids = set(snap["COMID"].tolist())
    missing = snap_ids - id_set
    if missing:
        sample = sorted(missing)[:10]
        raise RuntimeError(
            f"stage_8_write_snap: {len(missing)} snap COMIDs missing from "
            f"catchments: {sample}"
        )

    # Build children_of_parent by inverting NextDownID on rows with NextDownID>0.
    # Determine is_mainstem: at each confluence (parent), child with largest
    # uparea is mainstem; all other children are tributaries.
    is_mainstem: dict[int, bool] = {int(c): True for c in snap["COMID"].tolist()}
    children_of_parent: dict[int, list[tuple[int, float]]] = {}
    for comid, nxt, ua in zip(
        snap["COMID"].to_numpy(),
        snap["NextDownID"].to_numpy(),
        snap["uparea"].to_numpy(),
    ):
        nxt_i = int(nxt)
        if nxt_i == 0:
            continue
        children_of_parent.setdefault(nxt_i, []).append((int(comid), float(ua)))

    for parent, kids in children_of_parent.items():
        if len(kids) <= 1:
            continue  # sole child stays mainstem=True
        # Stable tie-break: max uparea, then max COMID (so two kids with equal
        # uparea pick a deterministic winner).
        winner = max(kids, key=lambda kv: (kv[1], kv[0]))
        winner_id = winner[0]
        for kid_id, _ua in kids:
            if kid_id != winner_id:
                is_mainstem[kid_id] = False

    mainstem_count = sum(1 for v in is_mainstem.values() if v)
    tributary_count = len(is_mainstem) - mainstem_count
    logger.info(
        "stage_8_write_snap: is_mainstem counts: mainstem=%d tributary=%d",
        mainstem_count,
        tributary_count,
    )

    # Assemble snap GeoDataFrame with the derived attributes.
    snap = snap.reset_index(drop=True)
    snap["id"] = snap["COMID"]
    snap["catchment_id"] = snap["COMID"]
    snap["weight"] = snap["uparea"].astype("float32")
    snap["is_mainstem"] = snap["COMID"].map(lambda c: is_mainstem[int(c)])

    # Hilbert-sort on LineString centroids using cat_total_bounds. If rivers
    # extend beyond the catchment bbox by epsilon we still accept it — the
    # hilbert_distance only needs a spanning box, not a tight one.
    riv_bounds = tuple(float(v) for v in snap.geometry.total_bounds)
    sort_bounds = (
        min(cat_total_bounds[0], riv_bounds[0]),
        min(cat_total_bounds[1], riv_bounds[1]),
        max(cat_total_bounds[2], riv_bounds[2]),
        max(cat_total_bounds[3], riv_bounds[3]),
    )
    centroids = snap.geometry.centroid
    snap["_hilbert"] = centroids.hilbert_distance(total_bounds=sort_bounds)
    snap = snap.sort_values(["_hilbert", "id"], kind="mergesort").reset_index(drop=True)
    snap = snap.drop(columns=["_hilbert"])

    row_count = len(snap)
    bounds = inflate_degenerate_bounds(snap.geometry.bounds)

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
    schema = schema.with_metadata(build_geo_metadata(["LineString"]))

    out_path = out_dir / "snap.parquet"
    row_groups = balanced_row_group_bounds(row_count, ROW_GROUP_MIN, ROW_GROUP_MAX)
    logger.info(
        "stage_8_write_snap writing %d rows in %d row groups -> %s",
        row_count,
        len(row_groups),
        out_path,
    )

    with pq.ParquetWriter(
        out_path,
        schema=schema,
        compression="snappy",
        write_statistics=True,
    ) as writer:
        for start, stop in row_groups:
            chunk = snap.iloc[start:stop]
            chunk_bounds = bounds.iloc[start:stop]
            geometry_wkb = chunk.geometry.to_wkb(hex=False)
            table = pa.Table.from_arrays(
                [
                    pa.array(chunk["id"].astype("int64").to_numpy(), type=pa.int64()),
                    pa.array(
                        chunk["catchment_id"].astype("int64").to_numpy(),
                        type=pa.int64(),
                    ),
                    pa.array(
                        chunk["weight"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(chunk["is_mainstem"].astype(bool).tolist(), type=pa.bool_()),
                    pa.array(
                        chunk_bounds["minx"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(
                        chunk_bounds["miny"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(
                        chunk_bounds["maxx"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(
                        chunk_bounds["maxy"].astype("float32").to_numpy(),
                        type=pa.float32(),
                    ),
                    pa.array(geometry_wkb.tolist(), type=pa.binary()),
                ],
                schema=schema,
            )
            writer.write_table(table)

    assert_geoparquet_valid(out_path, kind="snap")
    logger.info("stage_8_write_snap: snap.parquet validated")


def stage_9_write_manifest(
    out_dir: Path,
    bbox: tuple[float, float, float, float],
    atom_count: int,
    pfaf: int,
) -> None:
    """Write ``manifest.json`` conformant with HFX spec §6.

    ``fabric_name`` is ``FABRIC_NAME_FMT.format(pfaf=pfaf)`` (e.g.
    ``"merit_basins_pfaf27"``). ``flow_dir_encoding`` is required when
    ``HAS_RASTERS = True`` and is set to ``FLOW_DIR_ENCODING``. The written
    ``bbox`` is ``outward_bbox(bbox)``.
    """
    fabric_name = FABRIC_NAME_FMT.format(pfaf=pfaf)
    manifest = {
        "format_version": "0.1",
        "fabric_name": fabric_name,
        "crs": "EPSG:4326",
        "has_up_area": HAS_UP_AREA,
        "has_rasters": HAS_RASTERS,
        "has_snap": HAS_SNAP,
        "terminal_sink_id": 0,
        "topology": TOPOLOGY,
        "bbox": outward_bbox(bbox, pad=MANIFEST_BBOX_EPSILON),
        "atom_count": int(atom_count),
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adapter_version": ADAPTER_VERSION,
        "flow_dir_encoding": FLOW_DIR_ENCODING,
        "region": f"pfaf{pfaf:02d}",
    }
    out_path = out_dir / "manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    logger.info(
        "stage_9_write_manifest wrote %s fabric=%s atom_count=%d",
        out_path,
        fabric_name,
        atom_count,
    )


# ---------------------------------------------------------------------------
# Raster transcoding helpers
# ---------------------------------------------------------------------------

def _window_for_bbox(
    src: rasterio.io.DatasetReader,
    bbox: tuple[float, float, float, float],
    pad_pixels: int = 10,
) -> tuple[Window, rasterio.Affine]:
    """Compute a pixel-aligned raster window covering ``bbox`` plus a small pad.

    The resulting window is clamped to the raster's extent so off-edge reads
    cannot occur. Returns the window and the affine transform of its origin.
    """
    minx, miny, maxx, maxy = (float(v) for v in bbox)
    raw = from_bounds(minx, miny, maxx, maxy, transform=src.transform)
    # Pad by ``pad_pixels`` on each side, then clamp to raster extent.
    col_off = max(0, int(math.floor(raw.col_off)) - pad_pixels)
    row_off = max(0, int(math.floor(raw.row_off)) - pad_pixels)
    col_end = min(src.width, int(math.ceil(raw.col_off + raw.width)) + pad_pixels)
    row_end = min(src.height, int(math.ceil(raw.row_off + raw.height)) + pad_pixels)
    width = max(1, col_end - col_off)
    height = max(1, row_end - row_off)
    window = Window(col_off=col_off, row_off=row_off, width=width, height=height)
    transform = rasterio.windows.transform(window, src.transform)
    return window, transform


def _write_cog(
    dst_path: Path,
    data: np.ndarray,
    profile: dict,
    blocksize: int = 256,
) -> None:
    """Write ``data`` to ``dst_path`` as a Cloud-Optimized GeoTIFF.

    Uses a rasterio MemoryFile staging buffer then ``rio_cogeo.cog_translate``
    to produce the final COG with internal tiling and overviews.
    """
    cog_profile = cog_profiles.get("deflate")
    cog_profile.update(blockxsize=blocksize, blockysize=blocksize)

    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(**profile) as tmp:
            tmp.write(data, 1)
        with memfile.open() as tmp:
            cog_translate(
                tmp,
                str(dst_path),
                dst_kwargs=cog_profile,
                nodata=profile.get("nodata"),
                dtype=profile["dtype"],
                in_memory=True,
                quiet=True,
            )


def transcode_flow_dir(src_path: Path, dst_path: Path, bbox: tuple) -> None:
    """Transcode the MERIT flow-direction GeoTIFF to an HFX-conformant COG.

    Reads ``src_path`` (MERIT int8 with -9 for undefined, which reads back as
    uint8 247 — see ``MERIT_FLOWDIR_UNDEFINED_AS_UINT8``), remaps to uint8
    with ``FLOW_DIR_NODATA_OUT = 255`` for NoData, crops to ``bbox``, and
    writes a Cloud-Optimized GeoTIFF via rio-cogeo.

    Output must be uint8, internal tiles 256×256 or 512×512, CRS EPSG:4326.
    """
    logger.info("transcode_flow_dir: %s -> %s", src_path, dst_path)
    with rasterio.open(src_path) as src:
        window, transform = _window_for_bbox(src, bbox, pad_pixels=10)
        data = src.read(1, window=window).astype("uint8")

        # Histogram of inputs (first 10 unique values).
        uniques_in, counts_in = np.unique(data, return_counts=True)
        logger.info(
            "transcode_flow_dir input histogram: %s",
            list(zip(uniques_in.tolist()[:10], counts_in.tolist()[:10])),
        )

        valid_values = {0, 1, 2, 4, 8, 16, 32, 64, 128, 255}
        # Map anything not in the valid set to 255 (NoData). This folds 247 and
        # any other stray byte (e.g. uninitialised edges) into NoData.
        out = np.where(np.isin(data, list(valid_values)), data, FLOW_DIR_NODATA_OUT)
        # Explicit: 247 -> 255 (even though np.isin catches it, be defensive).
        out = np.where(out == MERIT_FLOWDIR_UNDEFINED_AS_UINT8, FLOW_DIR_NODATA_OUT, out)
        out = out.astype("uint8")

        uniques_out, counts_out = np.unique(out, return_counts=True)
        logger.info(
            "transcode_flow_dir output histogram: %s",
            list(zip(uniques_out.tolist()[:10], counts_out.tolist()[:10])),
        )

        profile = src.profile.copy()
        profile.update(
            driver="GTiff",
            dtype="uint8",
            count=1,
            width=out.shape[1],
            height=out.shape[0],
            transform=transform,
            nodata=FLOW_DIR_NODATA_OUT,
            crs=src.crs,
            compress="deflate",
        )
        _write_cog(dst_path, out, profile, blocksize=256)

    valid = cog_validate(str(dst_path))
    if not valid[0]:
        raise RuntimeError(
            f"transcode_flow_dir: cog_validate failed for {dst_path}: {valid}"
        )
    logger.info("transcode_flow_dir: COG validated at %s", dst_path)


def transcode_flow_acc(src_path: Path, dst_path: Path, bbox: tuple) -> None:
    """Transcode the MERIT flow-accumulation GeoTIFF to an HFX-conformant COG.

    Reads ``src_path`` (MERIT upstream cell count), casts to float32, maps the
    source NoData (if any) to ``FLOW_ACC_NODATA_OUT = -1.0``, crops to
    ``bbox``, and writes a Cloud-Optimized GeoTIFF via rio-cogeo.
    """
    logger.info("transcode_flow_acc: %s -> %s", src_path, dst_path)
    with rasterio.open(src_path) as src:
        window, transform = _window_for_bbox(src, bbox, pad_pixels=10)
        data = src.read(1, window=window).astype("int32")

        out = data.astype("float32")
        # MERIT NoData is 0 (cell count starts at 1); remap to -1.0.
        out = np.where(data == 0, FLOW_ACC_NODATA_OUT, out).astype("float32")

        valid_mask = out != FLOW_ACC_NODATA_OUT
        if valid_mask.any():
            valid = out[valid_mask]
            logger.info(
                "transcode_flow_acc valid cells: min=%.3f max=%.3f mean=%.3f count=%d",
                float(valid.min()),
                float(valid.max()),
                float(valid.mean()),
                int(valid.size),
            )
        else:
            logger.warning("transcode_flow_acc: no valid cells in clipped window")

        profile = src.profile.copy()
        profile.update(
            driver="GTiff",
            dtype="float32",
            count=1,
            width=out.shape[1],
            height=out.shape[0],
            transform=transform,
            nodata=FLOW_ACC_NODATA_OUT,
            crs=src.crs,
            compress="deflate",
        )
        _write_cog(dst_path, out, profile, blocksize=256)

    valid = cog_validate(str(dst_path))
    if not valid[0]:
        raise RuntimeError(
            f"transcode_flow_acc: cog_validate failed for {dst_path}: {valid}"
        )
    logger.info("transcode_flow_acc: COG validated at %s", dst_path)


# ---------------------------------------------------------------------------
# Validate helper
# ---------------------------------------------------------------------------

def validate(out_dir: Path) -> int:
    """Run both validation layers against the built HFX dataset.

    Layer 1 — authoritative HFX validator CLI: calls
    ``hfx <out_dir>/hfx --strict --sample-pct 100 --format text`` via
    subprocess and captures the exit code.

    Layer 2 — GeoParquet 1.1 structural validation via
    ``assert_geoparquet_valid`` on ``catchments.parquet`` and (since
    ``HAS_SNAP = True``) ``snap.parquet``.
    """
    dataset_path = out_dir / "hfx"
    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset path missing: {dataset_path}")

    logger.info("running hfx validator (strict, 100%% sample)")
    exit_code = subprocess.call(
        [
            "hfx",
            str(dataset_path),
            "--strict",
            "--sample-pct",
            "100",
            "--format",
            "text",
        ]
    )

    catchments_path = dataset_path / "catchments.parquet"
    assert_geoparquet_valid(catchments_path, kind="catchments")
    logger.info("catchments.parquet passed GeoParquet 1.1 validation")

    if HAS_SNAP:
        snap_path = dataset_path / "snap.parquet"
        assert_geoparquet_valid(snap_path, kind="snap")
        logger.info("snap.parquet passed GeoParquet 1.1 validation")

    return exit_code


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MERIT-Basins -> HFX adapter. Extract, build, and validate a per-Pfaf-L2 HFX dataset.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract", help="inspect and load the MERIT-Basins inputs for a Pfaf-L2 basin")
    extract.add_argument(
        "--merit-basins",
        type=Path,
        required=True,
        metavar="PATH",
        help="directory containing cat_pfaf_<NN>_*.shp and riv_pfaf_<NN>_*.shp",
    )
    extract.add_argument(
        "--rasters",
        type=Path,
        required=True,
        metavar="PATH",
        help="MERIT Hydro rasters root (contains flow_dir_basins/ and accum_basins/)",
    )
    extract.add_argument(
        "--pfaf",
        type=int,
        required=True,
        metavar="NN",
        help="Pfafstetter Level-2 basin code (11..91)",
    )

    build = subparsers.add_parser("build", help="build the HFX dataset into <out>/hfx")
    build.add_argument("--merit-basins", type=Path, required=True, metavar="PATH")
    build.add_argument("--rasters", type=Path, required=True, metavar="PATH")
    build.add_argument("--pfaf", type=int, required=True, metavar="NN")
    build.add_argument(
        "--out",
        type=Path,
        default=Path("./out"),
        metavar="PATH",
        help="working directory for built HFX output (default: ./out)",
    )

    validate_parser = subparsers.add_parser("validate", help="validate an existing <out>/hfx dataset")
    validate_parser.add_argument(
        "--out",
        type=Path,
        default=Path("./out"),
        metavar="PATH",
    )

    return parser.parse_args()


def _run_stage(label: str, fn, *args, **kwargs):
    """Run a stage function, surfacing NotImplementedError as exit 2.

    Mirrors the ``_template`` wrapper: caught ``NotImplementedError`` prints a
    labeled message and exits with code 2 so the orchestrator knows the stage
    is still a stub rather than a runtime failure.
    """
    try:
        return fn(*args, **kwargs)
    except NotImplementedError as exc:
        logger.error("[%s] not implemented: %s", label, exc)
        raise SystemExit(2)


def _build_dataset(
    merit_basins_root: Path,
    rasters_root: Path,
    pfaf: int,
    out_dir: Path,
) -> int:
    """Drive the nine stages end-to-end plus raster transcoding and validation."""
    hfx_dir = out_dir / "hfx"
    hfx_dir.mkdir(parents=True, exist_ok=True)

    source: SourceData = _run_stage(
        "stage_1_inspect_source",
        stage_1_inspect_source,
        merit_basins_root,
        rasters_root,
        pfaf,
    )
    gdf = _run_stage("stage_2_assign_ids", stage_2_assign_ids, source)
    gdf = _run_stage("stage_3_reproject", stage_3_reproject, gdf)
    gdf = _run_stage("stage_4_make_valid", stage_4_make_valid, gdf)
    gdf = _run_stage("stage_5_hilbert_sort", stage_5_hilbert_sort, gdf)

    ids, cat_total_bounds = _run_stage(
        "stage_6_write_catchments",
        stage_6_write_catchments,
        gdf,
        source.rivers,
        hfx_dir,
    )

    _run_stage("stage_7_write_graph", stage_7_write_graph, ids, source.rivers, hfx_dir)

    if HAS_SNAP:
        _run_stage(
            "stage_8_write_snap",
            stage_8_write_snap,
            ids,
            source.rivers,
            cat_total_bounds,
            hfx_dir,
        )

    if HAS_RASTERS:
        _run_stage(
            "transcode_flow_dir",
            transcode_flow_dir,
            source.flow_dir_path,
            hfx_dir / "flow_dir.tif",
            cat_total_bounds,
        )
        _run_stage(
            "transcode_flow_acc",
            transcode_flow_acc,
            source.flow_acc_path,
            hfx_dir / "flow_acc.tif",
            cat_total_bounds,
        )

    _run_stage(
        "stage_9_write_manifest",
        stage_9_write_manifest,
        hfx_dir,
        cat_total_bounds,
        len(ids),
        pfaf,
    )

    return validate(out_dir)


def main() -> int:
    """Dispatch subcommands and orchestrate the build pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )
    args = _parse_args()

    if args.command == "extract":
        _run_stage(
            "stage_1_inspect_source",
            stage_1_inspect_source,
            args.merit_basins.resolve(),
            args.rasters.resolve(),
            args.pfaf,
        )
        return 0

    if args.command == "build":
        return _build_dataset(
            args.merit_basins.resolve(),
            args.rasters.resolve(),
            args.pfaf,
            args.out.resolve(),
        )

    if args.command == "validate":
        return validate(args.out.resolve())

    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
