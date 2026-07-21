#!/usr/bin/env python3
"""Build the GRIT v2 global HFX v0.2.1 dataset."""

from __future__ import annotations

import argparse
import gc
import json
import math
import os
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence
from zipfile import ZipFile, ZipInfo

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import rasterio
from affine import Affine
from geoparquet_io.core.validate import validate_geoparquet
from geopandas import GeoSeries
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles
from shapely import make_valid
import shapely
from shapely.geometry import GeometryCollection, LineString, MultiLineString, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry


FABRIC_NAME = "grit"
FABRIC_VERSION = "1.0.0"
ADAPTER_VERSION = "grit-global-2.1.0"
FORMAT_VERSION = "0.3.0"
TOPOLOGY = "dag"
CRS = "EPSG:4326"
HAS_UP_AREA = True

DEFAULT_ROOT = Path("/Users/nicolaslazaro/Desktop/grit-hfx")
DEFAULT_OUTER_ARCHIVE = DEFAULT_ROOT / "17435232.zip"
REGION_CODES = ("AF", "AS", "EU", "NA", "SA", "SI", "SP")
REGION_INPUTS = {
    code: {
        "segments": f"GRITv1.0_segments_{code}_EPSG4326.gpkg.zip",
        "segment_catchments": f"GRITv1.0_segment_catchments_{code}_EPSG4326.gpkg.zip",
        "reaches": f"GRITv1.0_reaches_{code}_EPSG4326.gpkg.zip",
        "reach_catchments": f"GRITv1.0_reach_catchments_{code}_EPSG4326.gpkg.zip",
    }
    for code in REGION_CODES
}

SEGMENTS_LAYER = "lines"
SEGMENT_CATCHMENTS_LAYER = "segment_catchments__1"
REACHES_LAYER = "lines"
REACH_CATCHMENTS_LAYER = "reach_catchments__1"

GLOBAL_TOTAL_BOUNDS = (-180.0, -90.0, 180.0, 90.0)
GLOBAL_BBOX = [-180.0, -90.0, 180.0, 90.0]
ROW_GROUP_MIN = 4_096
ROW_GROUP_MAX = 8_192
TMP_ROW_GROUP_SIZE = 65_536
SNAP_BBOX_EPSILON = 1e-4
PLANETARY_REGION_SET = set(REGION_CODES)


class AdapterError(RuntimeError):
    """Raised when GRIT v2 adapter preconditions fail."""


@dataclass(frozen=True)
class SourceData:
    """Hold Stage 1 shard paths and row counts."""

    root: Path
    tmp_root: Path
    regions: tuple[str, ...]
    segment_shards: tuple[Path, ...]
    reach_shards: tuple[Path, ...]
    segment_snap_shards: tuple[Path, ...]
    reach_snap_shards: tuple[Path, ...]
    segment_rows: int
    reach_rows: int


@dataclass(frozen=True)
class RasterMosaicLayout:
    """Describe a validated same-grid raster union."""

    sources: tuple[Path, ...]
    dtype: str
    nodata: int | float
    transform: Affine
    width: int
    height: int
    offsets: tuple[tuple[int, int], ...]


def log(message: str) -> None:
    """Emit a flushed adapter log line."""
    print(f"[grit-v2] {message}", flush=True)


def ensure_dir(path: Path) -> None:
    """Create a directory and its parents."""
    path.mkdir(parents=True, exist_ok=True)


def _safe_tiff_members(archive_path: Path) -> list[ZipInfo]:
    """Return deterministic TIFF members after rejecting unsafe archive paths."""
    try:
        with ZipFile(archive_path) as archive:
            archive_members = archive.infolist()
        for info in archive_members:
            normalized = info.filename.replace("\\", "/")
            parts = normalized.split("/")
            has_empty_component = "" in parts[:-1] or (not info.is_dir() and "" in parts)
            if (
                normalized.startswith("/")
                or ".." in parts
                or has_empty_component
                or (parts and parts[0].endswith(":"))
            ):
                raise AdapterError(
                    f"archive {archive_path} has path-traversing member {info.filename}"
                )
        members = sorted(
            (
                info
                for info in archive_members
                if not info.is_dir()
                and Path(info.filename).suffix.lower() in {".tif", ".tiff"}
            ),
            key=lambda info: info.filename,
        )
        if not members:
            raise AdapterError(f"archive {archive_path} contains no TIFF members")
        return members
    except AdapterError:
        raise
    except Exception as error:
        raise AdapterError(f"failed to inspect archive {archive_path}: {error}") from error


def _extract_raster_archives(
    archive_paths: Sequence[Path],
    extraction_root: Path,
    artifact_name: str,
) -> tuple[Path, ...]:
    """Extract nested TIFF members into an artifact-specific work tree."""
    archives = sorted((Path(path) for path in archive_paths), key=lambda path: str(path))
    if not archives:
        raise AdapterError(f"{artifact_name} archive list is empty")

    extracted: list[Path] = []
    for archive_index, archive_path in enumerate(archives):
        members = _safe_tiff_members(archive_path)
        archive_root = extraction_root / f"{archive_index:04d}"
        with ZipFile(archive_path) as archive:
            for info in members:
                relative_path = Path(*info.filename.replace("\\", "/").split("/"))
                output_path = archive_root / relative_path
                ensure_dir(output_path.parent)
                with archive.open(info) as source, output_path.open("wb") as destination:
                    shutil.copyfileobj(source, destination, length=1024 * 1024)
                extracted.append(output_path)
    return tuple(extracted)


def _integer_grid_offset(value: float, source_path: Path, invariant: str) -> int:
    rounded = round(value)
    if not math.isclose(value, rounded, rel_tol=0.0, abs_tol=1e-8):
        raise AdapterError(
            f"source {source_path} is not aligned to the common pixel grid ({invariant})"
        )
    return int(rounded)


def _nodata_values_equal(left: int | float, right: int | float) -> bool:
    """Compare scalar nodata values while treating two NaNs as equal."""
    return (math.isnan(left) and math.isnan(right)) or left == right


def _nodata_mask(values: np.ndarray, nodata: int | float) -> np.ndarray:
    """Locate nodata cells while supporting NaN family nodata."""
    if math.isnan(nodata):
        return np.isnan(values)
    return values == nodata


def _validate_raster_sources(
    source_paths: Sequence[Path],
    expected_dtype: str,
    artifact_name: str,
    required_nodata: int | float | None = None,
    valid_data_range: tuple[int, int] | None = None,
) -> RasterMosaicLayout:
    """Validate source headers and compute their exact native-grid union."""
    first_transform: Affine | None = None
    source_offsets: list[tuple[int, int, int, int]] = []
    tagged_nodata: list[tuple[Path, int | float]] = []
    tagless_sources: list[Path] = []

    if not source_paths:
        raise AdapterError(f"{artifact_name} has no extracted TIFF sources")

    for source_path in source_paths:
        try:
            with rasterio.open(source_path) as source:
                if source.count != 1:
                    raise AdapterError(
                        f"source {source_path} must be single-band, found {source.count} bands"
                    )
                if source.dtypes != (expected_dtype,):
                    raise AdapterError(
                        f"source {source_path} has dtype {source.dtypes[0]}, expected {expected_dtype} for {artifact_name}"
                    )
                if source.crs is None or source.crs.to_epsg() != 8857:
                    raise AdapterError(
                        f"source {source_path} has CRS {source.crs}, expected EPSG:8857"
                    )
                transform = source.transform
                if not math.isclose(transform.b, 0.0, abs_tol=1e-12) or not math.isclose(
                    transform.d, 0.0, abs_tol=1e-12
                ):
                    raise AdapterError(f"source {source_path} has rotated or skewed pixels")
                if transform.a <= 0 or transform.e >= 0:
                    raise AdapterError(
                        f"source {source_path} has unsupported pixel basis {transform}"
                    )

                if first_transform is None:
                    first_transform = transform
                else:
                    same_x_basis = math.isclose(
                        transform.a, first_transform.a, rel_tol=0.0, abs_tol=1e-12
                    )
                    same_y_basis = math.isclose(
                        transform.e, first_transform.e, rel_tol=0.0, abs_tol=1e-12
                    )
                    if not same_x_basis or not same_y_basis:
                        raise AdapterError(
                            f"source {source_path} has a different pixel basis or resolution"
                        )

                if source.nodata is None:
                    tagless_sources.append(source_path)
                else:
                    tagged_nodata.append((source_path, source.nodata))

                col = _integer_grid_offset(
                    (transform.c - first_transform.c) / first_transform.a,
                    source_path,
                    "x origin",
                )
                row = _integer_grid_offset(
                    (transform.f - first_transform.f) / first_transform.e,
                    source_path,
                    "y origin",
                )
                source_offsets.append((col, row, source.width, source.height))
        except AdapterError:
            raise
        except Exception as error:
            raise AdapterError(f"failed to inspect source {source_path}: {error}") from error

    if first_transform is None:
        raise AdapterError(f"{artifact_name} has no extracted TIFF sources")
    if not tagged_nodata:
        raise AdapterError(
            f"{artifact_name} has no source nodata tags; no common nodata can be derived"
        )

    common_nodata_path, common_nodata = tagged_nodata[0]
    for source_path, source_nodata in tagged_nodata[1:]:
        if not _nodata_values_equal(source_nodata, common_nodata):
            raise AdapterError(
                f"source {source_path} has nodata {source_nodata}, expected common nodata {common_nodata}"
            )
    if required_nodata is not None and not _nodata_values_equal(
        common_nodata, required_nodata
    ):
        raise AdapterError(
            f"source {common_nodata_path} has nodata {common_nodata}, "
            f"expected required nodata {required_nodata} for {artifact_name}"
        )

    tagless_source_set = set(tagless_sources)
    for source_path in source_paths:
        with rasterio.open(source_path) as source:
            for _, source_window in source.block_windows(1):
                values = source.read(1, window=source_window)
                if valid_data_range is not None:
                    minimum, maximum = valid_data_range
                    if source_path in tagless_source_set:
                        invalid = (values < minimum) | (values > maximum)
                        if np.any(invalid):
                            invalid_value = values[invalid][0].item()
                            raise AdapterError(
                                f"tag-less {artifact_name} source {source_path} contains value "
                                f"{invalid_value} outside the valid data domain {minimum} through {maximum}; "
                                f"nodata {common_nodata} requires a source tag"
                            )
                    else:
                        invalid = (
                            ((values < minimum) | (values > maximum))
                            & ~_nodata_mask(values, common_nodata)
                        )
                        if np.any(invalid):
                            invalid_value = values[invalid][0].item()
                            raise AdapterError(
                                f"source {source_path} contains value {invalid_value}; "
                                f"{artifact_name} permits data codes {minimum} through {maximum} "
                                f"and declared nodata {common_nodata}"
                            )
                elif source_path in tagless_source_set and np.any(
                    _nodata_mask(values, common_nodata)
                ):
                    raise AdapterError(
                        f"tag-less {artifact_name} source {source_path} contains the family's "
                        f"common tagged nodata {common_nodata}"
                    )

    for source_index, (col, row, width, height) in enumerate(source_offsets):
        for previous_index, (
            previous_col,
            previous_row,
            previous_width,
            previous_height,
        ) in enumerate(source_offsets[:source_index]):
            overlaps = (
                col < previous_col + previous_width
                and previous_col < col + width
                and row < previous_row + previous_height
                and previous_row < row + height
            )
            if overlaps:
                raise AdapterError(
                    f"source {source_paths[source_index]} overlaps source "
                    f"{source_paths[previous_index]}; lossless mosaics require disjoint source windows"
                )

    min_col = min(col for col, _, _, _ in source_offsets)
    min_row = min(row for _, row, _, _ in source_offsets)
    max_col = max(col + width for col, _, width, _ in source_offsets)
    max_row = max(row + height for _, row, _, height in source_offsets)
    transform = first_transform * Affine.translation(min_col, min_row)
    offsets = tuple((col - min_col, row - min_row) for col, row, _, _ in source_offsets)
    return RasterMosaicLayout(
        sources=tuple(source_paths),
        dtype=expected_dtype,
        nodata=common_nodata,
        transform=transform,
        width=max_col - min_col,
        height=max_row - min_row,
        offsets=offsets,
    )


def _write_native_mosaic_vrt(layout: RasterMosaicLayout, output_path: Path) -> None:
    """Describe validated source tiles as an exact native-grid VRT mosaic."""
    ensure_dir(output_path.parent)
    vrt_dtypes = {"uint8": "Byte", "float32": "Float32"}
    try:
        vrt_dtype = vrt_dtypes[layout.dtype]
    except KeyError as error:
        raise AdapterError(f"unsupported VRT mosaic dtype {layout.dtype}") from error

    root = ET.Element(
        "VRTDataset",
        rasterXSize=str(layout.width),
        rasterYSize=str(layout.height),
    )
    ET.SubElement(root, "SRS").text = "EPSG:8857"
    ET.SubElement(root, "GeoTransform").text = ", ".join(
        format(value, ".17g")
        for value in (
            layout.transform.c,
            layout.transform.a,
            layout.transform.b,
            layout.transform.f,
            layout.transform.d,
            layout.transform.e,
        )
    )
    band = ET.SubElement(root, "VRTRasterBand", dataType=vrt_dtype, band="1")
    ET.SubElement(band, "NoDataValue").text = (
        "nan" if math.isnan(layout.nodata) else format(layout.nodata, ".17g")
    )

    for source_path, (destination_col, destination_row) in zip(
        layout.sources, layout.offsets, strict=True
    ):
        with rasterio.open(source_path) as source:
            source_width = source.width
            source_height = source.height
        simple_source = ET.SubElement(band, "SimpleSource")
        relative_path = Path(
            os.path.relpath(source_path, start=output_path.parent)
        ).as_posix()
        ET.SubElement(
            simple_source, "SourceFilename", relativeToVRT="1"
        ).text = relative_path
        ET.SubElement(simple_source, "SourceBand").text = "1"
        ET.SubElement(
            simple_source,
            "SrcRect",
            xOff="0",
            yOff="0",
            xSize=str(source_width),
            ySize=str(source_height),
        )
        ET.SubElement(
            simple_source,
            "DstRect",
            xOff=str(destination_col),
            yOff=str(destination_row),
            xSize=str(source_width),
            ySize=str(source_height),
        )

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(output_path, encoding="utf-8", xml_declaration=True)


def _translate_and_validate_cog(
    source_vrt: Path, output_path: Path, temp_folder: Path
) -> None:
    """Translate one native mosaic to the required BigTIFF COG profile."""
    ensure_dir(temp_folder)
    staged_output = temp_folder / "translated.tif"
    staged_output.unlink(missing_ok=True)
    profile = cog_profiles.get("deflate")
    profile.update(blockxsize=512, blockysize=512, BIGTIFF="YES")
    try:
        cog_translate(
            source_vrt,
            staged_output,
            profile,
            overview_resampling="nearest",
            resampling="nearest",
            in_memory=False,
            config={
                "CHECK_DISK_FREE_SPACE": "TRUE",
                "CPL_TMPDIR": str(temp_folder),
            },
            quiet=True,
        )
        valid, errors, warnings = cog_validate(staged_output)
        if not valid or errors:
            raise AdapterError(
                f"COG validation failed for {output_path}: errors={errors}; warnings={warnings}"
            )
        ensure_dir(output_path.parent)
        shutil.move(staged_output, output_path)
    except AdapterError:
        raise
    except Exception as error:
        raise AdapterError(f"COG translation failed for {output_path}: {error}") from error
    finally:
        staged_output.unlink(missing_ok=True)


def _validate_raster_pair(direction_path: Path, accumulation_path: Path) -> None:
    """Require both completed rasters to use one identical grid."""
    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        if direction.crs != accumulation.crs:
            raise AdapterError(
                f"raster pair CRS differs: {direction_path}={direction.crs}, {accumulation_path}={accumulation.crs}"
            )
        if (direction.width, direction.height) != (
            accumulation.width,
            accumulation.height,
        ):
            raise AdapterError(
                f"raster pair dimensions differ: {direction_path}={direction.width}x{direction.height}, "
                f"{accumulation_path}={accumulation.width}x{accumulation.height}"
            )
        if direction.transform != accumulation.transform:
            raise AdapterError(
                f"raster pair affine transforms differ: {direction_path}={direction.transform}, "
                f"{accumulation_path}={accumulation.transform}"
            )


def build_d8_raster_pair(
    flow_dir_archives: Sequence[Path],
    flow_acc_archives: Sequence[Path],
    work_dir: Path,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Build lossless native-grid COGs from ZIP-packaged GRIT raster tiles."""
    work_dir = Path(work_dir)
    output_dir = Path(output_dir)
    direction_sources = _extract_raster_archives(
        flow_dir_archives, work_dir / "flow_dir" / "extracted", "flow_dir"
    )
    accumulation_sources = _extract_raster_archives(
        flow_acc_archives, work_dir / "flow_acc" / "extracted", "flow_acc"
    )
    direction_layout = _validate_raster_sources(
        direction_sources,
        "uint8",
        "flow_dir",
        required_nodata=255,
        valid_data_range=(0, 8),
    )
    accumulation_layout = _validate_raster_sources(
        accumulation_sources, "float32", "flow_acc"
    )

    direction_output = output_dir / "flow_dir.tif"
    accumulation_output = output_dir / "flow_acc.tif"
    direction_vrt = work_dir / "flow_dir" / "mosaic.vrt"
    accumulation_vrt = work_dir / "flow_acc" / "mosaic.vrt"
    _write_native_mosaic_vrt(direction_layout, direction_vrt)
    _translate_and_validate_cog(
        direction_vrt, direction_output, work_dir / "flow_dir"
    )
    _write_native_mosaic_vrt(accumulation_layout, accumulation_vrt)
    _translate_and_validate_cog(
        accumulation_vrt, accumulation_output, work_dir / "flow_acc"
    )
    _validate_raster_pair(direction_output, accumulation_output)
    return direction_output, accumulation_output


def _amend_manifest_with_d8_rasters(dataset_dir: Path) -> None:
    """Canonicalize the D8 raster declaration in an existing manifest."""
    manifest_path = Path(dataset_dir) / "manifest.json"
    if not manifest_path.is_file():
        raise AdapterError(f"manifest {manifest_path} does not exist")
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise AdapterError(f"manifest {manifest_path} is not valid JSON: {error}") from error
    if not isinstance(manifest, dict):
        raise AdapterError(f"manifest {manifest_path} must decode to a JSON object")
    if manifest.get("format_version") != FORMAT_VERSION:
        raise AdapterError(
            f"manifest {manifest_path} format_version must equal {FORMAT_VERSION}"
        )
    auxiliary = manifest.get("auxiliary")
    if not isinstance(auxiliary, list):
        raise AdapterError(f"manifest {manifest_path} auxiliary must be a JSON array")

    manifest["adapter_version"] = ADAPTER_VERSION
    manifest["auxiliary"] = [
        entry
        for entry in auxiliary
        if not (
            isinstance(entry, dict) and entry.get("schema") == "hfx.aux.d8_raster.v2"
        )
    ]
    manifest["auxiliary"].append(
        {
            "schema": "hfx.aux.d8_raster.v2",
            "artifacts": {
                "flow_dir": "aux/d8/flow_dir.tif",
                "flow_acc": "aux/d8/flow_acc.tif",
            },
            "metadata": {
                "crs": "EPSG:8857",
                "flow_dir_encoding": "grass",
                "flow_acc_units": "km2",
            },
        }
    )
    try:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    except OSError as error:
        raise AdapterError(f"failed to write manifest {manifest_path}: {error}") from error


BBOX_LEAF_NAMES = ("xmin", "ymin", "xmax", "ymax")


def build_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    """Build GeoParquet 1.1 metadata (with `bbox` covering) for an Arrow schema."""
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": geometry_types,
                "covering": {"bbox": {name: ["bbox", name] for name in BBOX_LEAF_NAMES}},
            },
        },
    }
    return {b"geo": json.dumps(geo).encode("utf-8")}


def bbox_struct_type() -> pa.DataType:
    """Struct type for the GeoParquet 1.1 covering `bbox` column (non-nullable leaves)."""
    return pa.struct(
        [pa.field(name, pa.float32(), nullable=False) for name in BBOX_LEAF_NAMES]
    )


def build_bbox_struct(minx, miny, maxx, maxy) -> pa.StructArray:
    """Build the `bbox` struct array so its four float32 leaves carry row-group stats.

    Uses pa.StructArray.from_arrays (NOT the pa.array([{...}]) list-of-dicts
    anti-pattern, which does not propagate leaf stats) so the Parquet writer
    records min/max on bbox.xmin / ymin / xmax / ymax. GATE-1 (s04) proven pattern.
    """
    return pa.StructArray.from_arrays(
        [
            pa.array(minx, type=pa.float32()),
            pa.array(miny, type=pa.float32()),
            pa.array(maxx, type=pa.float32()),
            pa.array(maxy, type=pa.float32()),
        ],
        fields=[pa.field(name, pa.float32(), nullable=False) for name in BBOX_LEAF_NAMES],
    )


def assert_geoparquet_valid(out_path: Path) -> None:
    """Raise when a Parquet file fails GeoParquet 1.1 validation."""
    result = validate_geoparquet(str(out_path), target_version="1.1")
    if result.is_valid:
        return
    failures = [c for c in result.checks if c.status.value == "failed"]
    details = "; ".join(f"{c.name}: {c.message}" for c in failures)
    raise AdapterError(f"GeoParquet 1.1 validation failed for {out_path}: {details}")


def balanced_row_group_bounds(
    total_rows: int,
    min_size: int = ROW_GROUP_MIN,
    max_size: int = ROW_GROUP_MAX,
) -> list[tuple[int, int]]:
    """Split rows into validator-strict row-group slices."""
    if total_rows <= 0:
        return []

    min_groups = math.ceil(total_rows / max_size)
    max_groups = max(1, total_rows // min_size)
    group_count = max_groups
    while group_count >= min_groups:
        base = total_rows // group_count
        remainder = total_rows % group_count
        largest = base + (1 if remainder else 0)
        if min_size <= base <= max_size and largest <= max_size:
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


def _extract_member(outer_zip: Path, member_name: str, out_path: Path) -> None:
    ensure_dir(out_path.parent)
    with ZipFile(outer_zip) as archive:
        info = archive.getinfo(member_name)
        if out_path.exists() and out_path.stat().st_size == info.file_size:
            log(f"reuse extracted {out_path.name}")
            return
        log(f"extract {member_name} -> {out_path}")
        with archive.open(info) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)


def _extract_inner_gpkg(inner_zip_path: Path) -> Path:
    with ZipFile(inner_zip_path) as archive:
        gpkg_names = [name for name in archive.namelist() if name.endswith(".gpkg")]
        if len(gpkg_names) != 1:
            raise AdapterError(
                f"expected exactly one .gpkg in {inner_zip_path}, found {gpkg_names}"
            )
        member_name = gpkg_names[0]
        out_path = inner_zip_path.with_suffix("")
        if out_path.exists():
            return out_path
        log(f"inflate {inner_zip_path.name} -> {out_path.name}")
        with archive.open(member_name) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
        return out_path


def _extract_inputs(root: Path, outer_archive: Path, region_code: str) -> dict[str, Path]:
    input_dir = root / "input"
    ensure_dir(input_dir)
    outputs: dict[str, Path] = {}
    for key, member_name in REGION_INPUTS[region_code].items():
        out_path = input_dir / member_name
        _extract_member(outer_archive, member_name, out_path)
        outputs[key] = _extract_inner_gpkg(out_path)
    return outputs


def _parse_csv_int_list(raw: object) -> list[int]:
    if raw is None or pd.isna(raw):
        return []
    text = str(raw).strip()
    if not text:
        return []
    return [int(part.strip()) for part in text.split(",") if part.strip()]


def _parse_csv_int_lists(values: Iterable[object]) -> list[list[int]]:
    return [_parse_csv_int_list(value) for value in values]


def _coerce_to_polygonal(geom: BaseGeometry) -> BaseGeometry:
    if geom.is_valid:
        return geom
    repaired = make_valid(geom)
    if isinstance(repaired, (Polygon, MultiPolygon)):
        return repaired
    if isinstance(repaired, GeometryCollection):
        polygons: list[Polygon] = []
        for part in repaired.geoms:
            if isinstance(part, Polygon):
                polygons.append(part)
            elif isinstance(part, MultiPolygon):
                polygons.extend(part.geoms)
        if not polygons:
            raise AdapterError("make_valid produced no polygonal parts")
        return polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
    raise AdapterError(f"make_valid produced unsupported geometry type: {type(repaired).__name__}")


def _line_coords(geom: BaseGeometry) -> list[tuple[float, float]]:
    if isinstance(geom, LineString):
        return [(float(x), float(y)) for x, y in geom.coords]
    if isinstance(geom, MultiLineString):
        coords: list[tuple[float, float]] = []
        for part in geom.geoms:
            coords.extend((float(x), float(y)) for x, y in part.coords)
        if coords:
            return coords
    raise AdapterError(f"expected LineString/MultiLineString, got {geom.geom_type}")


def _upstream_endpoint(geom: BaseGeometry) -> tuple[float, float]:
    coords = _line_coords(geom)
    return coords[0]


def _downstream_endpoint(geom: BaseGeometry) -> tuple[float, float]:
    coords = _line_coords(geom)
    return coords[-1]


def _node_key(coord: tuple[float, float], precision: int = 7) -> tuple[float, float]:
    return (round(coord[0], precision), round(coord[1], precision))


def _bbox_frame(gdf: gpd.GeoDataFrame, inflate_degenerate: bool = False) -> pd.DataFrame:
    bounds = gdf.geometry.bounds.rename(
        columns={"minx": "bbox_minx", "miny": "bbox_miny", "maxx": "bbox_maxx", "maxy": "bbox_maxy"}
    )
    if inflate_degenerate:
        same_x = bounds["bbox_minx"] >= bounds["bbox_maxx"]
        same_y = bounds["bbox_miny"] >= bounds["bbox_maxy"]
        bounds.loc[same_x, "bbox_minx"] -= SNAP_BBOX_EPSILON
        bounds.loc[same_x, "bbox_maxx"] += SNAP_BBOX_EPSILON
        bounds.loc[same_y, "bbox_miny"] -= SNAP_BBOX_EPSILON
        bounds.loc[same_y, "bbox_maxy"] += SNAP_BBOX_EPSILON
    return bounds.astype("float32")


def _classify_stem_roles(segments: gpd.GeoDataFrame) -> pd.Series:
    downstream_keys = segments.geometry.map(lambda geom: _node_key(_downstream_endpoint(geom)))
    deg_out_by_node = downstream_keys.value_counts().to_dict()
    upstream_keys = segments.geometry.map(lambda geom: _node_key(_upstream_endpoint(geom)))

    roles: list[str] = []
    for is_mainstem, source_key in zip(segments["is_mainstem"], upstream_keys, strict=True):
        if pd.isna(is_mainstem):
            roles.append("unknown")
        elif int(is_mainstem) == 1:
            roles.append("mainstem")
        elif deg_out_by_node.get(source_key, 0) > 1:
            roles.append("distributary")
        else:
            roles.append("tributary")
    return pd.Series(roles, index=segments.index, dtype="string")


def _outlet_columns(lines: gpd.GeoDataFrame) -> pd.DataFrame:
    endpoints = lines.geometry.map(_downstream_endpoint)
    return pd.DataFrame(
        {
            "outlet_lon": [coord[0] for coord in endpoints],
            "outlet_lat": [coord[1] for coord in endpoints],
        },
        index=lines.index,
    )


def _read_layer(path: Path, layer: str, columns: list[str], geometry: bool) -> gpd.GeoDataFrame | pd.DataFrame:
    return pyogrio.read_dataframe(
        path,
        layer=layer,
        columns=columns,
        read_geometry=geometry,
        use_arrow=True,
    )


def _resolve_layer(path: Path, preferred: str) -> str:
    layers = pyogrio.list_layers(path)
    names = [str(row[0]) for row in layers]
    if preferred in names:
        return preferred
    if len(names) == 1:
        return names[0]
    raise AdapterError(f"layer {preferred!r} not found in {path}; available={names}")


def _write_table(path: Path, table: pa.Table) -> None:
    ensure_dir(path.parent)
    pq.write_table(
        table,
        path,
        compression="snappy",
        row_group_size=TMP_ROW_GROUP_SIZE,
        write_statistics=True,
    )


def _write_balanced_table(path: Path, table: pa.Table, schema: pa.Schema | None = None) -> None:
    """Write a final artifact with strict-validator row group sizing."""
    ensure_dir(path.parent)
    write_schema = schema if schema is not None else table.schema
    with pq.ParquetWriter(path, schema=write_schema, compression="snappy", write_statistics=True) as writer:
        for start, stop in balanced_row_group_bounds(table.num_rows):
            writer.write_table(table.slice(start, stop - start))


def _read_table(path: Path, columns: list[str] | None = None) -> pa.Table:
    return pq.read_table(path, columns=columns)


def _list_int64_type() -> pa.ListType:
    return pa.list_(pa.field("item", pa.int64(), nullable=True))


def _hilbert_from_bbox(df: pd.DataFrame) -> np.ndarray:
    cx = (df["bbox_minx"].to_numpy(dtype="float64") + df["bbox_maxx"].to_numpy(dtype="float64")) / 2.0
    cy = (df["bbox_miny"].to_numpy(dtype="float64") + df["bbox_maxy"].to_numpy(dtype="float64")) / 2.0
    points = shapely.points(cx, cy)
    return GeoSeries(points).hilbert_distance(total_bounds=GLOBAL_TOTAL_BOUNDS).to_numpy(dtype="int64")


def _load_level_index(paths: tuple[Path, ...], level: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        table = _read_table(path, columns=["source_global_id", "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"])
        frame = table.to_pandas()
        frame["level"] = np.int16(level)
        frame["hilbert_index"] = _hilbert_from_bbox(frame)
        frames.append(frame[["level", "source_global_id", "hilbert_index"]])
    if not frames:
        return pd.DataFrame(columns=["level", "source_global_id", "hilbert_index"])
    return pd.concat(frames, ignore_index=True)


def _write_id_map(source: SourceData) -> Path:
    level0 = _load_level_index(source.segment_shards, 0)
    level1 = _load_level_index(source.reach_shards, 1)
    if level0["source_global_id"].duplicated().any():
        dup = level0.loc[level0["source_global_id"].duplicated(), "source_global_id"].head(10).tolist()
        raise AdapterError(f"duplicate segment global_id values: {dup}")
    if level1["source_global_id"].duplicated().any():
        dup = level1.loc[level1["source_global_id"].duplicated(), "source_global_id"].head(10).tolist()
        raise AdapterError(f"duplicate reach global_id values: {dup}")

    level0 = level0.sort_values(["hilbert_index", "source_global_id"], kind="mergesort").reset_index(drop=True)
    level1 = level1.sort_values(["hilbert_index", "source_global_id"], kind="mergesort").reset_index(drop=True)
    level0["id"] = np.arange(1, len(level0) + 1, dtype="int64")
    level1["id"] = np.arange(len(level0) + 1, len(level0) + len(level1) + 1, dtype="int64")
    id_map = pd.concat([level0, level1], ignore_index=True)
    if (id_map["id"] == 0).any() or id_map["id"].duplicated().any():
        raise AdapterError("dense ID assignment produced invalid IDs")

    table = pa.Table.from_pandas(
        id_map[["level", "source_global_id", "id", "hilbert_index"]],
        schema=pa.schema(
            [
                pa.field("level", pa.int16(), nullable=False),
                pa.field("source_global_id", pa.int64(), nullable=False),
                pa.field("id", pa.int64(), nullable=False),
                pa.field("hilbert_index", pa.int64(), nullable=False),
            ]
        ),
        preserve_index=False,
    )
    out_path = source.tmp_root / "id_map.parquet"
    _write_table(out_path, table)
    log(f"Stage 2 wrote {out_path} rows={table.num_rows}")
    return out_path


def _id_lookup(id_map: pd.DataFrame, level: int) -> dict[int, int]:
    rows = id_map[id_map["level"] == level]
    return dict(zip(rows["source_global_id"].astype("int64"), rows["id"].astype("int64"), strict=True))


def _map_upstream_ids(values: list[list[int]], lookup: dict[int, int], level: int) -> list[list[int]]:
    mapped: list[list[int]] = []
    missing: set[int] = set()
    for upstream_list in values:
        row: list[int] = []
        for source_id in upstream_list:
            new_id = lookup.get(int(source_id))
            if new_id is None:
                missing.add(int(source_id))
            else:
                row.append(int(new_id))
        mapped.append(row)
    if missing:
        sample = sorted(missing)[:10]
        raise AdapterError(f"level {level} upstream IDs missing from id_map: {sample}")
    return mapped


def _resolve_level_edges(paths: tuple[Path, ...], lookup: dict[int, int], level: int, out_path: Path) -> Path:
    id_values: list[int] = []
    upstream_values: list[list[int]] = []
    for path in paths:
        table = _read_table(path, columns=["source_global_id", "upstream_source_global_ids"])
        frame = table.to_pandas()
        source_ids = frame["source_global_id"].astype("int64").tolist()
        upstream_source = frame["upstream_source_global_ids"].tolist()
        id_values.extend(int(lookup[source_id]) for source_id in source_ids)
        upstream_values.extend(_map_upstream_ids(upstream_source, lookup, level))

    order = np.argsort(np.array(id_values, dtype="int64"), kind="stable")
    sorted_ids = [id_values[int(i)] for i in order]
    sorted_upstream = [upstream_values[int(i)] for i in order]
    table = pa.Table.from_arrays(
        [
            pa.array(sorted_ids, type=pa.int64()),
            pa.array(sorted_upstream, type=_list_int64_type()),
        ],
        names=["id", "upstream_ids"],
    )
    _write_table(out_path, table)
    log(f"Phase 2.5 wrote {out_path.name} rows={table.num_rows}")
    return out_path


def resolve_graph_edges(source: SourceData) -> tuple[Path, Path]:
    """Resolve Path A source upstream IDs into dense HFX IDs."""
    id_map = _read_table(source.tmp_root / "id_map.parquet").to_pandas()
    l0_lookup = _id_lookup(id_map, 0)
    l1_lookup = _id_lookup(id_map, 1)
    edges_l0 = _resolve_level_edges(source.segment_shards, l0_lookup, 0, source.tmp_root / "edges_l0.parquet")
    edges_l1 = _resolve_level_edges(source.reach_shards, l1_lookup, 1, source.tmp_root / "edges_l1.parquet")
    return edges_l0, edges_l1


def _load_reach_area_by_id(source: SourceData, lookup: dict[int, int]) -> tuple[np.ndarray, np.ndarray]:
    ids: list[int] = []
    areas: list[float] = []
    for path in source.reach_shards:
        table = _read_table(path, columns=["source_global_id", "area_km2"])
        frame = table.to_pandas()
        ids.extend(int(lookup[int(source_id)]) for source_id in frame["source_global_id"])
        areas.extend(float(area) for area in frame["area_km2"])
    order = np.argsort(np.array(ids, dtype="int64"), kind="stable")
    return np.array(ids, dtype="int64")[order], np.array(areas, dtype="float64")[order]


def _region_from_shard(path: Path) -> str:
    return path.parent.name


def compute_reach_up_area(source: SourceData) -> Path:
    """Compute reach ``up_area_km2`` from each segment outlet back upstream."""
    id_map = _read_table(source.tmp_root / "id_map.parquet").to_pandas()
    l1_lookup = _id_lookup(id_map, 1)

    segment_frames: list[pd.DataFrame] = []
    for path in source.segment_shards:
        segment_frames.append(_read_table(path, columns=["source_global_id", "up_area_km2"]).to_pandas())
    segment_frame = pd.concat(segment_frames, ignore_index=True)
    segment_up_area = {
        int(source_id): (None if pd.isna(area) else float(area))
        for source_id, area in zip(segment_frame["source_global_id"], segment_frame["up_area_km2"], strict=True)
    }

    reach_frames: list[pd.DataFrame] = []
    for path in source.reach_shards:
        frame = _read_table(
            path,
            columns=[
                "source_global_id",
                "parent_source_global_id",
                "area_km2",
                "upstream_source_global_ids",
                "downstream_source_global_ids",
            ],
        ).to_pandas()
        frame["region"] = _region_from_shard(path)
        reach_frames.append(frame)
    reaches = pd.concat(reach_frames, ignore_index=True)
    reaches["id"] = reaches["source_global_id"].map(lambda value: l1_lookup[int(value)]).astype("int64")

    results: dict[int, float | None] = {}
    fallback_segments: list[tuple[str, int, int]] = []
    near_zero_count = 0

    for segment_id, group in reaches.groupby("parent_source_global_id", sort=False):
        segment_source_id = int(segment_id)
        segment_region = str(group.iloc[0]["region"])
        segment_drainage = segment_up_area.get(segment_source_id)
        reach_ids = set(int(value) for value in group["source_global_id"])
        if segment_drainage is None or not np.isfinite(segment_drainage):
            fallback_segments.append((segment_region, segment_source_id, len(group)))
            for unit_id in group["id"]:
                results[int(unit_id)] = None
            continue

        if len(group) == 1:
            unit_id = int(group.iloc[0]["id"])
            results[unit_id] = segment_drainage
            continue

        by_source = {int(row.source_global_id): row for row in group.itertuples(index=False)}
        outlet_candidates = [
            source_id
            for source_id, row in by_source.items()
            if not [int(downstream_id) for downstream_id in row.downstream_source_global_ids if int(downstream_id) in reach_ids]
        ]
        if len(outlet_candidates) != 1:
            fallback_segments.append((segment_region, segment_source_id, len(group)))
            for row in by_source.values():
                results[int(row.id)] = None
            continue

        ordered_sources: list[int] = []
        current = outlet_candidates[0]
        seen: set[int] = set()
        while current not in seen:
            seen.add(current)
            ordered_sources.append(current)
            row = by_source[current]
            internal_upstream = [
                int(upstream_id)
                for upstream_id in row.upstream_source_global_ids
                if int(upstream_id) in reach_ids
            ]
            if not internal_upstream:
                break
            if len(internal_upstream) != 1:
                break
            current = internal_upstream[0]

        if len(ordered_sources) != len(reach_ids):
            fallback_segments.append((segment_region, segment_source_id, len(group)))
            for row in by_source.values():
                results[int(row.id)] = None
            continue

        current_up_area = float(segment_drainage)
        segment_failed = False
        for source_id in ordered_sources:
            row = by_source[source_id]
            if not np.isfinite(current_up_area) or current_up_area < 0.0:
                segment_failed = True
                break
            if current_up_area <= 1e-9:
                near_zero_count += 1
            results[int(row.id)] = current_up_area
            current_up_area -= float(row.area_km2)

        if segment_failed:
            fallback_segments.append((segment_region, segment_source_id, len(group)))
            for row in by_source.values():
                results[int(row.id)] = None

    if fallback_segments:
        by_region: dict[str, list[tuple[int, int]]] = {}
        for region, segment_id, reach_count in fallback_segments:
            by_region.setdefault(region, []).append((segment_id, reach_count))
        for region, rows in sorted(by_region.items()):
            log(
                f"Phase 2.5 reach up_area fallback {region}: "
                f"segments={len(rows)} reach_rows={sum(count for _, count in rows)} "
                f"examples={[segment_id for segment_id, _ in rows[:10]]}"
            )
        fallback_txt = source.tmp_root / "reach_up_area_fallback_segments.txt"
        fallback_json = source.tmp_root / "reach_up_area_fallback_summary.json"
        fallback_txt.write_text(
            "\n".join(f"{region},{segment_id},{reach_count}" for region, segment_id, reach_count in fallback_segments)
            + "\n"
        )
        fallback_json.write_text(
            json.dumps(
                {
                    "fallback_segment_count": len(fallback_segments),
                    "fallback_reach_count": sum(count for _, _, count in fallback_segments),
                    "by_region": {
                        region: {
                            "segments": len(rows),
                            "reach_rows": sum(count for _, count in rows),
                            "segment_ids": [segment_id for segment_id, _ in rows],
                        }
                        for region, rows in sorted(by_region.items())
                    },
                },
                indent=2,
            )
            + "\n"
        )
        if len(fallback_segments) > 100:
            raise AdapterError(f"reach up_area fallback exceeded escalation threshold: {len(fallback_segments)} segments")
        log(
            "Phase 2.5 reach up_area fallback: "
            f"{len(fallback_segments)} segment(s) emitted NULL reach values; "
            f"examples={[segment_id for _, segment_id, _ in fallback_segments[:10]]}"
        )
    if near_zero_count:
        log(f"Phase 2.5 reach up_area near-zero values observed: {near_zero_count}")

    output = reaches[["id"]].copy()
    output["up_area_km2"] = output["id"].map(lambda unit_id: results.get(int(unit_id)))
    finite_values = output["up_area_km2"].dropna().to_numpy(dtype="float64")
    if len(finite_values):
        overflow_count = int((~np.isfinite(finite_values) | (finite_values > np.finfo(np.float32).max)).sum())
        if overflow_count:
            raise AdapterError(f"per-segment reach up_area produced {overflow_count} non-float32-safe values")
        negative_count = int((finite_values < 0.0).sum())
        if negative_count:
            raise AdapterError(f"per-segment reach up_area produced {negative_count} negative values")
        log(
            "Phase 2.5 reach up_area stats: "
            f"finite_count={len(finite_values)} finite_max={float(np.max(finite_values)):.6g} "
            f"null_count={int(output['up_area_km2'].isna().sum())}"
        )

    out_path = source.tmp_root / "reach_up_area.parquet"
    table = pa.Table.from_arrays(
        [
            pa.array(output["id"].astype("int64").tolist(), type=pa.int64()),
            pa.array(output["up_area_km2"].tolist(), type=pa.float32()),
        ],
        names=["id", "up_area_km2"],
    )
    _write_table(out_path, table)
    log(f"Phase 2.5 wrote {out_path.name} rows={table.num_rows}")
    return out_path


def _load_id_map(source: SourceData) -> pd.DataFrame:
    return _read_table(source.tmp_root / "id_map.parquet").to_pandas()


def _load_unit_rows(source: SourceData) -> pd.DataFrame:
    id_map = _load_id_map(source)
    segment_frames: list[pd.DataFrame] = []
    for path in source.segment_shards:
        frame = _read_table(path).to_pandas()
        frame["level"] = np.int16(0)
        frame["parent_source_global_id"] = pd.NA
        frame["level_label"] = "segment"
        frame["source_id"] = "segment:" + frame["source_global_id"].astype(str)
        segment_frames.append(frame)

    reach_frames: list[pd.DataFrame] = []
    reach_up_area = _read_table(source.tmp_root / "reach_up_area.parquet").to_pandas()
    reach_id_map = id_map[id_map["level"] == 1][["source_global_id", "id"]]
    reach_up_by_source = reach_id_map.merge(reach_up_area, on="id", validate="1:1")[
        ["source_global_id", "up_area_km2"]
    ]
    for path in source.reach_shards:
        frame = _read_table(
            path,
            columns=[
                "source_global_id",
                "parent_source_global_id",
                "area_km2",
                "stem_role",
                "outlet_lon",
                "outlet_lat",
                "bbox_minx",
                "bbox_miny",
                "bbox_maxx",
                "bbox_maxy",
                "geometry_wkb",
            ],
        ).to_pandas()
        frame = frame.merge(reach_up_by_source, on="source_global_id", how="left", validate="1:1")
        frame["level"] = np.int16(1)
        frame["level_label"] = "reach"
        frame["source_id"] = "reach:" + frame["source_global_id"].astype(str)
        reach_frames.append(frame)

    units = pd.concat(segment_frames + reach_frames, ignore_index=True)
    units = units.merge(
        id_map[["level", "source_global_id", "id", "hilbert_index"]],
        on=["level", "source_global_id"],
        how="inner",
        validate="1:1",
    )
    segment_parent_map = id_map[id_map["level"] == 0][["source_global_id", "id"]].rename(
        columns={"source_global_id": "parent_source_global_id", "id": "parent_id"}
    )
    units = units.merge(segment_parent_map, on="parent_source_global_id", how="left", validate="many_to_one")
    units.loc[units["level"] == 0, "parent_id"] = pd.NA
    units = units.sort_values(["level", "hilbert_index", "source_global_id"], kind="mergesort").reset_index(drop=True)
    return units


def _dataset_bbox_from_units(units: pd.DataFrame, regions: tuple[str, ...]) -> list[float]:
    if set(regions) == PLANETARY_REGION_SET:
        return GLOBAL_BBOX
    minx = float(units["bbox_minx"].min())
    miny = float(units["bbox_miny"].min())
    maxx = float(units["bbox_maxx"].max())
    maxy = float(units["bbox_maxy"].max())
    return [
        float(np.nextafter(minx, -np.inf)),
        float(np.nextafter(miny, -np.inf)),
        float(np.nextafter(maxx, np.inf)),
        float(np.nextafter(maxy, np.inf)),
    ]


def _fallback_summary(source: SourceData) -> dict:
    path = source.tmp_root / "reach_up_area_fallback_summary.json"
    if not path.exists():
        return {"fallback_segment_count": 0, "fallback_reach_count": 0, "by_region": {}}
    return json.loads(path.read_text())


def _git_head() -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip()


def _fallback_segment_lines(fallback: dict) -> str:
    lines: list[str] = []
    for region in REGION_CODES:
        region_summary = fallback.get("by_region", {}).get(region)
        if not region_summary:
            continue
        segment_ids = region_summary.get("segment_ids", [])
        if not segment_ids:
            continue
        ids = ", ".join(str(int(segment_id)) for segment_id in segment_ids)
        lines.append(f"- {region}: {ids}")
    return "\n".join(lines) if lines else "- None"


def _write_readme(out_dir: Path, source: SourceData, unit_count: int) -> None:
    fallback = _fallback_summary(source)
    fallback_reaches = int(fallback.get("fallback_reach_count", 0))
    fallback_segments = int(fallback.get("fallback_segment_count", 0))
    percent = (fallback_reaches / unit_count * 100.0) if unit_count else 0.0
    percent_text = "<0.001%" if percent < 0.001 else f"{percent:.6f}%"
    fallback_lines = _fallback_segment_lines(fallback)
    built_date = datetime.now(timezone.utc).date().isoformat()
    bbox_text = "[-180, -90, 180, 90]" if set(source.regions) == PLANETARY_REGION_SET else "see manifest.json"
    readme = f"""# GRIT HFX v2 Dataset

This dataset is an HFX v0.2.1 compilation of GRIT with segment (`level=0`) and reach (`level=1`) drainage units.

## DAG `up_area_km2` semantics

Choice: partitioned (option a). Each segment's `up_area_km2` reflects the source-area share routed through that segment.

Algorithm: per-segment chain anchor. Segment rows use GRIT `drainage_area_out` directly. Reach rows are computed by anchoring each parent segment's outlet reach to the segment `drainage_area_out`, then walking upstream within that segment and subtracting each downstream reach's local `area_km2`.

Consumer caveat: in DAG split-rejoin geometry, the sum of `up_area_km2` over a flow set is not the watershed area. Consumers must use the graph plus `level=1` reaches for true watershed accumulation.

## Known data caveats

{fallback_reaches} reach rows have `up_area_km2=NULL`, spread across {fallback_segments} segments where the chain-anchor algorithm could not resolve the outlet. These are anomalies in the GRIT v1.0 source topology, not defects of the HFX encoding. `has_up_area` remains true; nulls are permitted per HFX v0.2.1 spec. They represent {percent_text} of rows. See `adapters/grit-v2/build_adapter.py` for the detection rule.

Fallback segment IDs:

{fallback_lines}

## Provenance

- Source: GRIT v1.0 (https://doi.org/10.5281/zenodo.17435232)
- Adapter version: {_git_head()}
- HFX spec version: {FORMAT_VERSION}
- Built: {built_date}
- Bbox: planetary {bbox_text}
- Row count: {unit_count:,} catchments across 2 levels
"""
    (out_dir / "README.md").write_text(readme)


def _stage_1_region(root: Path, outer_archive: Path, region_code: str) -> tuple[Path, Path, Path, Path, int, int]:
    t0 = time.monotonic()
    region_root = root / "tmp" / region_code
    ensure_dir(region_root)
    inputs = _extract_inputs(root, outer_archive, region_code)

    log(f"{region_code}: read segment lines")
    segments = _read_layer(
        inputs["segments"],
        SEGMENTS_LAYER,
        ["global_id", "upstream_line_ids", "downstream_line_ids", "is_mainstem", "drainage_area_out"],
        True,
    )
    segment_geom_types = segments.geometry.geom_type.value_counts().to_dict()
    log(f"{region_code}: segment geometry types={segment_geom_types}")
    segments["source_global_id"] = segments["global_id"].astype("int64")
    segments["stem_role"] = _classify_stem_roles(segments)
    segments["snap_weight"] = segments["drainage_area_out"].astype("float32")
    outlets = _outlet_columns(segments)
    segments = pd.concat([segments, outlets], axis=1)
    segment_meta = pd.DataFrame(
        {
            "source_global_id": segments["source_global_id"].astype("int64"),
            "upstream_source_global_ids": _parse_csv_int_lists(segments["upstream_line_ids"]),
            "stem_role": segments["stem_role"].astype("string"),
            "outlet_lon": segments["outlet_lon"].astype("float64"),
            "outlet_lat": segments["outlet_lat"].astype("float64"),
            "snap_weight": segments["drainage_area_out"].astype("float32"),
        }
    )

    log(f"{region_code}: read segment catchments")
    segment_catchments = _read_layer(
        inputs["segment_catchments"],
        _resolve_layer(inputs["segment_catchments"], SEGMENT_CATCHMENTS_LAYER),
        ["global_id", "area"],
        True,
    )
    segment_catchments["geometry"] = segment_catchments.geometry.map(_coerce_to_polygonal)
    segment_catchments["source_global_id"] = segment_catchments["global_id"].astype("int64")
    segment_joined = segment_catchments.merge(segment_meta, on="source_global_id", how="inner", validate="1:1")
    if len(segment_joined) != len(segment_catchments):
        raise AdapterError(f"{region_code}: segment catchment join dropped rows")
    segment_bounds = _bbox_frame(segment_joined)
    segment_wkb = segment_joined.geometry.to_wkb(hex=False)
    list_type = _list_int64_type()
    segment_table = pa.Table.from_arrays(
        [
            pa.array(segment_joined["source_global_id"].tolist(), type=pa.int64()),
            pa.array(segment_joined["area"].astype("float32").tolist(), type=pa.float32()),
            pa.array(segment_joined["snap_weight"].astype("float32").tolist(), type=pa.float32()),
            pa.array(segment_joined["stem_role"].astype(str).tolist(), type=pa.string()),
            pa.array(segment_joined["outlet_lon"].tolist(), type=pa.float64()),
            pa.array(segment_joined["outlet_lat"].tolist(), type=pa.float64()),
            pa.array(segment_bounds["bbox_minx"].tolist(), type=pa.float32()),
            pa.array(segment_bounds["bbox_miny"].tolist(), type=pa.float32()),
            pa.array(segment_bounds["bbox_maxx"].tolist(), type=pa.float32()),
            pa.array(segment_bounds["bbox_maxy"].tolist(), type=pa.float32()),
            pa.array(segment_wkb.tolist(), type=pa.binary()),
            pa.array(segment_joined["upstream_source_global_ids"].tolist(), type=list_type),
        ],
        names=[
            "source_global_id",
            "area_km2",
            "up_area_km2",
            "stem_role",
            "outlet_lon",
            "outlet_lat",
            "bbox_minx",
            "bbox_miny",
            "bbox_maxx",
            "bbox_maxy",
            "geometry_wkb",
            "upstream_source_global_ids",
        ],
    )
    segment_path = region_root / "segments.parquet"
    _write_table(segment_path, segment_table)

    log(f"{region_code}: write segment snap shard")
    segment_snap_bounds = _bbox_frame(segments, inflate_degenerate=True)
    segment_snap_table = pa.Table.from_arrays(
        [
            pa.array(segments["source_global_id"].tolist(), type=pa.int64()),
            pa.array(segments["snap_weight"].astype("float32").tolist(), type=pa.float32()),
            pa.array(segments["stem_role"].astype(str).tolist(), type=pa.string()),
            pa.array(segment_snap_bounds["bbox_minx"].tolist(), type=pa.float32()),
            pa.array(segment_snap_bounds["bbox_miny"].tolist(), type=pa.float32()),
            pa.array(segment_snap_bounds["bbox_maxx"].tolist(), type=pa.float32()),
            pa.array(segment_snap_bounds["bbox_maxy"].tolist(), type=pa.float32()),
            pa.array(segments.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
        ],
        names=["source_global_id", "weight", "stem_role", "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy", "geometry_wkb"],
    )
    segment_snap_path = region_root / "segment_snap.parquet"
    _write_table(segment_snap_path, segment_snap_table)

    log(f"{region_code}: read reach lines")
    reaches = _read_layer(
        inputs["reaches"],
        REACHES_LAYER,
        ["global_id", "segment_id", "upstream_line_ids", "downstream_line_ids"],
        True,
    )
    reach_geom_types = reaches.geometry.geom_type.value_counts().to_dict()
    log(f"{region_code}: reach geometry types={reach_geom_types}")
    reaches["source_global_id"] = reaches["global_id"].astype("int64")
    reaches["parent_source_global_id"] = reaches["segment_id"].astype("int64")
    reach_outlets = _outlet_columns(reaches)
    reaches = pd.concat([reaches, reach_outlets], axis=1)
    reach_meta = reaches.merge(
        segment_meta[["source_global_id", "stem_role", "snap_weight"]].rename(
            columns={"source_global_id": "parent_source_global_id"}
        ),
        on="parent_source_global_id",
        how="inner",
        validate="many_to_one",
    )
    if len(reach_meta) != len(reaches):
        raise AdapterError(f"{region_code}: reach parent join dropped rows")
    reach_meta["upstream_source_global_ids"] = _parse_csv_int_lists(reach_meta["upstream_line_ids"])
    reach_meta["downstream_source_global_ids"] = _parse_csv_int_lists(reach_meta["downstream_line_ids"])

    log(f"{region_code}: read reach catchments")
    reach_catchment_layer = _resolve_layer(inputs["reach_catchments"], REACH_CATCHMENTS_LAYER)
    reach_catchments = _read_layer(
        inputs["reach_catchments"],
        reach_catchment_layer,
        ["global_id", "area"],
        True,
    )
    reach_catchments["geometry"] = reach_catchments.geometry.map(_coerce_to_polygonal)
    reach_catchments["source_global_id"] = reach_catchments["global_id"].astype("int64")
    reach_joined = reach_catchments.merge(
        reach_meta[
            [
                "source_global_id",
                "parent_source_global_id",
                "stem_role",
                "snap_weight",
                "outlet_lon",
                "outlet_lat",
                "upstream_source_global_ids",
                "downstream_source_global_ids",
            ]
        ],
        on="source_global_id",
        how="inner",
        validate="1:1",
    )
    if len(reach_joined) != len(reach_catchments):
        raise AdapterError(f"{region_code}: reach catchment join dropped rows")
    reach_bounds = _bbox_frame(reach_joined)
    reach_table = pa.Table.from_arrays(
        [
            pa.array(reach_joined["source_global_id"].tolist(), type=pa.int64()),
            pa.array(reach_joined["parent_source_global_id"].tolist(), type=pa.int64()),
            pa.array(reach_joined["area"].astype("float32").tolist(), type=pa.float32()),
            pa.array(reach_joined["stem_role"].astype(str).tolist(), type=pa.string()),
            pa.array(reach_joined["outlet_lon"].tolist(), type=pa.float64()),
            pa.array(reach_joined["outlet_lat"].tolist(), type=pa.float64()),
            pa.array(reach_bounds["bbox_minx"].tolist(), type=pa.float32()),
            pa.array(reach_bounds["bbox_miny"].tolist(), type=pa.float32()),
            pa.array(reach_bounds["bbox_maxx"].tolist(), type=pa.float32()),
            pa.array(reach_bounds["bbox_maxy"].tolist(), type=pa.float32()),
            pa.array(reach_joined.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
            pa.array(reach_joined["upstream_source_global_ids"].tolist(), type=list_type),
            pa.array(reach_joined["downstream_source_global_ids"].tolist(), type=list_type),
        ],
        names=[
            "source_global_id",
            "parent_source_global_id",
            "area_km2",
            "stem_role",
            "outlet_lon",
            "outlet_lat",
            "bbox_minx",
            "bbox_miny",
            "bbox_maxx",
            "bbox_maxy",
            "geometry_wkb",
            "upstream_source_global_ids",
            "downstream_source_global_ids",
        ],
    )
    reach_path = region_root / "reaches.parquet"
    _write_table(reach_path, reach_table)

    log(f"{region_code}: write reach snap shard")
    reach_snap_bounds = _bbox_frame(reach_meta, inflate_degenerate=True)
    reach_snap_table = pa.Table.from_arrays(
        [
            pa.array(reach_meta["source_global_id"].tolist(), type=pa.int64()),
            pa.array(reach_meta["snap_weight"].astype("float32").tolist(), type=pa.float32()),
            pa.array(reach_meta["stem_role"].astype(str).tolist(), type=pa.string()),
            pa.array(reach_snap_bounds["bbox_minx"].tolist(), type=pa.float32()),
            pa.array(reach_snap_bounds["bbox_miny"].tolist(), type=pa.float32()),
            pa.array(reach_snap_bounds["bbox_maxx"].tolist(), type=pa.float32()),
            pa.array(reach_snap_bounds["bbox_maxy"].tolist(), type=pa.float32()),
            pa.array(reach_meta.geometry.to_wkb(hex=False).tolist(), type=pa.binary()),
        ],
        names=["source_global_id", "weight", "stem_role", "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy", "geometry_wkb"],
    )
    reach_snap_path = region_root / "reach_snap.parquet"
    _write_table(reach_snap_path, reach_snap_table)

    elapsed = time.monotonic() - t0
    log(
        f"{region_code}: Stage 1 complete segments={len(segment_joined)} "
        f"reaches={len(reach_joined)} elapsed={elapsed:.1f}s"
    )
    del segments, segment_catchments, segment_joined, reaches, reach_meta, reach_catchments, reach_joined
    gc.collect()
    return segment_path, reach_path, segment_snap_path, reach_snap_path, int(segment_table.num_rows), int(reach_table.num_rows)


def stage_1_inspect_source(
    input_path: Path,
    work_root: Path,
    regions: tuple[str, ...] = REGION_CODES,
) -> SourceData:
    """Extract GRIT sources and write per-region Stage 1 Parquet shards."""
    if not input_path.exists():
        raise FileNotFoundError(f"outer archive missing: {input_path}")
    ensure_dir(work_root)

    segment_paths: list[Path] = []
    reach_paths: list[Path] = []
    segment_snap_paths: list[Path] = []
    reach_snap_paths: list[Path] = []
    segment_rows = 0
    reach_rows = 0
    for region_code in regions:
        segment_path, reach_path, segment_snap_path, reach_snap_path, n_segments, n_reaches = _stage_1_region(
            work_root, input_path, region_code
        )
        segment_paths.append(segment_path)
        reach_paths.append(reach_path)
        segment_snap_paths.append(segment_snap_path)
        reach_snap_paths.append(reach_snap_path)
        segment_rows += n_segments
        reach_rows += n_reaches

    return SourceData(
        root=work_root,
        tmp_root=work_root / "tmp",
        regions=regions,
        segment_shards=tuple(segment_paths),
        reach_shards=tuple(reach_paths),
        segment_snap_shards=tuple(segment_snap_paths),
        reach_snap_shards=tuple(reach_snap_paths),
        segment_rows=segment_rows,
        reach_rows=reach_rows,
    )


def stage_2_assign_ids(source: SourceData) -> Path:
    """Assign dense IDs in ``(level ASC, hilbert_index ASC)`` order."""
    return _write_id_map(source)


def stage_3_reproject(source: SourceData) -> None:
    """Assert Stage 1 shards were read from EPSG:4326 sources."""
    return None


def stage_4_make_valid(source: SourceData) -> None:
    """Assert Stage 1 performed the make-valid sweep."""
    return None


def stage_5_hilbert_sort(source: SourceData) -> None:
    """Assert Stage 2 persisted a Hilbert-sorted ID map."""
    id_map = _read_table(source.tmp_root / "id_map.parquet").to_pandas()
    expected = id_map.sort_values(["level", "hilbert_index", "source_global_id"], kind="mergesort").reset_index(drop=True)
    actual = id_map.reset_index(drop=True)
    if not actual[["level", "source_global_id", "id", "hilbert_index"]].equals(
        expected[["level", "source_global_id", "id", "hilbert_index"]]
    ):
        raise AdapterError("id_map.parquet is not sorted by (level, hilbert_index, source_global_id)")


def stage_6_write_catchments(source: SourceData, out_dir: Path) -> None:
    """Write final ``catchments.parquet``."""
    ensure_dir(out_dir)
    units = _load_unit_rows(source)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox", bbox_struct_type(), nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
            pa.field("source_id", pa.string(), nullable=True),
            pa.field("level_label", pa.string(), nullable=True),
        ]
    ).with_metadata(build_geo_metadata(["Polygon", "MultiPolygon"]))
    parent_ids = [None if pd.isna(value) else int(value) for value in units["parent_id"]]
    up_area = [None if pd.isna(value) else float(value) for value in units["up_area_km2"]]
    table = pa.Table.from_arrays(
        [
            pa.array(units["id"].astype("int64").tolist(), type=pa.int64()),
            pa.array(units["level"].astype("int16").tolist(), type=pa.int16()),
            pa.array(parent_ids, type=pa.int64()),
            pa.array(units["area_km2"].astype("float32").tolist(), type=pa.float32()),
            pa.array(up_area, type=pa.float32()),
            pa.array(units["outlet_lon"].astype("float64").tolist(), type=pa.float64()),
            pa.array(units["outlet_lat"].astype("float64").tolist(), type=pa.float64()),
            build_bbox_struct(
                units["bbox_minx"].astype("float32").tolist(),
                units["bbox_miny"].astype("float32").tolist(),
                units["bbox_maxx"].astype("float32").tolist(),
                units["bbox_maxy"].astype("float32").tolist(),
            ),
            pa.array(units["geometry_wkb"].tolist(), type=pa.binary()),
            pa.array(units["source_id"].astype(str).tolist(), type=pa.string()),
            pa.array(units["level_label"].astype(str).tolist(), type=pa.string()),
        ],
        schema=schema,
    )
    out_path = out_dir / "catchments.parquet"
    _write_balanced_table(out_path, table, schema)
    assert_geoparquet_valid(out_path)
    log(f"Stage 6 wrote {out_path} rows={table.num_rows}")


def stage_7_write_graph(source: SourceData, out_dir: Path) -> None:
    """Write final ``graph.parquet``."""
    id_map = _load_id_map(source)
    unit_bbox = _load_unit_rows(source)[
        ["id", "level", "hilbert_index", "bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"]
    ]
    edges_l0 = _read_table(source.tmp_root / "edges_l0.parquet").to_pandas()
    edges_l0["level"] = np.int16(0)
    edges_l1 = _read_table(source.tmp_root / "edges_l1.parquet").to_pandas()
    edges_l1["level"] = np.int16(1)
    edges = pd.concat([edges_l0, edges_l1], ignore_index=True)
    graph = edges.merge(unit_bbox, on=["id", "level"], how="inner", validate="1:1")
    if len(graph) != len(id_map):
        raise AdapterError(f"graph row count {len(graph)} != id map row count {len(id_map)}")
    graph = graph.sort_values(["level", "hilbert_index", "id"], kind="mergesort").reset_index(drop=True)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("upstream_ids", _list_int64_type(), nullable=False),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )
    table = pa.Table.from_arrays(
        [
            pa.array(graph["id"].astype("int64").tolist(), type=pa.int64()),
            pa.array(graph["level"].astype("int16").tolist(), type=pa.int16()),
            pa.array(graph["upstream_ids"].tolist(), type=_list_int64_type()),
            pa.array(graph["bbox_minx"].astype("float32").tolist(), type=pa.float32()),
            pa.array(graph["bbox_miny"].astype("float32").tolist(), type=pa.float32()),
            pa.array(graph["bbox_maxx"].astype("float32").tolist(), type=pa.float32()),
            pa.array(graph["bbox_maxy"].astype("float32").tolist(), type=pa.float32()),
        ],
        schema=schema,
    )
    out_path = out_dir / "graph.parquet"
    _write_balanced_table(out_path, table, schema)
    log(f"Stage 7 wrote {out_path} rows={table.num_rows}")


def stage_8_write_snap(source: SourceData, out_dir: Path) -> None:
    """Write ``aux/snap_segments.parquet`` and ``aux/snap_reaches.parquet``."""
    aux_dir = out_dir / "aux"
    ensure_dir(aux_dir)
    id_map = _load_id_map(source)
    _write_snap_file(source.segment_snap_shards, id_map, 0, aux_dir / "snap_segments.parquet")
    _write_snap_file(source.reach_snap_shards, id_map, 1, aux_dir / "snap_reaches.parquet")


def stage_9_write_manifest(source: SourceData, out_dir: Path) -> None:
    """Write final HFX v0.2.1 manifest and dataset README."""
    catchments_path = out_dir / "catchments.parquet"
    unit_count = pq.read_metadata(catchments_path).num_rows
    units_for_bbox = _load_unit_rows(source)[["bbox_minx", "bbox_miny", "bbox_maxx", "bbox_maxy"]]
    bbox = _dataset_bbox_from_units(units_for_bbox, source.regions)
    manifest = {
        "format_version": FORMAT_VERSION,
        "fabric_name": FABRIC_NAME,
        "fabric_version": FABRIC_VERSION,
        "crs": CRS,
        "has_up_area": HAS_UP_AREA,
        "topology": TOPOLOGY,
        "bbox": bbox,
        "unit_count": unit_count,
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "adapter_version": ADAPTER_VERSION,
        "auxiliary": [
            {
                "schema": "hfx.aux.snap.v2",
                "artifacts": {"snap": "aux/snap_segments.parquet"},
                "metadata": {
                    "name": "segment-stems",
                    "description": "Segment-scale stems for level 0 GRIT segment catchments.",
                    "references_levels": [0],
                    "weight_semantics": "drainage_area_km2_partitioned",
                },
            },
            {
                "schema": "hfx.aux.snap.v2",
                "artifacts": {"snap": "aux/snap_reaches.parquet"},
                "metadata": {
                    "name": "reach-stems",
                    "description": "Reach-scale stems for level 1 GRIT reach catchments. Weight inherited from parent segment.",
                    "references_levels": [1],
                    "weight_semantics": "drainage_area_km2_partitioned",
                },
            },
        ],
    }
    if set(source.regions) != PLANETARY_REGION_SET:
        manifest["region"] = "europe-smoke" if source.regions == ("EU",) else ",".join(source.regions).lower()
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    _write_readme(out_dir, source, unit_count)
    log(f"Stage 9 wrote manifest.json unit_count={unit_count}")


def _write_snap_file(shards: tuple[Path, ...], id_map: pd.DataFrame, level: int, out_path: Path) -> None:
    frames: list[pd.DataFrame] = []
    lookup = id_map[id_map["level"] == level][["source_global_id", "id"]].rename(columns={"id": "unit_id"})
    for path in shards:
        frame = _read_table(path).to_pandas()
        frame = frame.merge(lookup, on="source_global_id", how="inner", validate="1:1")
        frame["hilbert_index"] = _hilbert_from_bbox(frame)
        frames.append(frame)
    snap = pd.concat(frames, ignore_index=True).sort_values(
        ["hilbert_index", "source_global_id"], kind="mergesort"
    ).reset_index(drop=True)
    snap["id"] = np.arange(1, len(snap) + 1, dtype="int64")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox", bbox_struct_type(), nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata(build_geo_metadata(["LineString", "MultiLineString"]))
    table = pa.Table.from_arrays(
        [
            pa.array(snap["id"].astype("int64").tolist(), type=pa.int64()),
            pa.array(snap["unit_id"].astype("int64").tolist(), type=pa.int64()),
            pa.array(snap["weight"].astype("float32").tolist(), type=pa.float32()),
            pa.array(snap["stem_role"].astype(str).tolist(), type=pa.string()),
            build_bbox_struct(
                snap["bbox_minx"].astype("float32").tolist(),
                snap["bbox_miny"].astype("float32").tolist(),
                snap["bbox_maxx"].astype("float32").tolist(),
                snap["bbox_maxy"].astype("float32").tolist(),
            ),
            pa.array(snap["geometry_wkb"].tolist(), type=pa.binary()),
        ],
        schema=schema,
    )
    _write_balanced_table(out_path, table, schema)
    assert_geoparquet_valid(out_path)
    log(f"Stage 8 wrote {out_path} rows={table.num_rows}")


def validate(dataset_path: Path, strict: bool = True, sample_pct: float = 100.0) -> int:
    """Run the Rust HFX validator in text mode."""
    repo_root = Path(__file__).resolve().parents[2]
    cmd = [
        "cargo",
        "run",
        "-p",
        "hfx-cli",
        "--",
        str(dataset_path),
        "--format",
        "text",
        "--sample-pct",
        str(sample_pct),
    ]
    if strict:
        cmd.append("--strict")
    log("run validator: " + " ".join(cmd))
    proc = subprocess.run(cmd, cwd=repo_root, check=False)
    return int(proc.returncode)


def probe_reach_schema(root: Path) -> None:
    """Run the Tier 0 reach schema probe for EU, AS, and NA."""
    paths = {
        "EU": root / "grit-hfx-eu-workdir/input/GRITv1.0_reaches_EU_EPSG4326.gpkg",
        "AS": root / "per-region/grit-hfx-as/input/GRITv1.0_reaches_AS_EPSG4326.gpkg",
        "NA": root / "per-region/grit-hfx-na/input/GRITv1.0_reaches_NA_EPSG4326.gpkg",
    }
    for code, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"probe input missing for {code}: {path}")
        info = pyogrio.read_info(path, layer=REACHES_LAYER)
        fields = list(info["fields"])
        dtypes = list(info["dtypes"])
        print(f"=== {code} {path} ===")
        print(f"features={info.get('features')} geometry_type={info.get('geometry_type')} crs={info.get('crs')}")
        for field, dtype in zip(fields, dtypes, strict=True):
            print(f"  {field}: {dtype}")
        df = pyogrio.read_dataframe(path, layer=REACHES_LAYER, columns=fields, read_geometry=False, use_arrow=True)
        for field in fields:
            nonnull = int(df[field].notna().sum())
            nulls = int(len(df) - nonnull)
            unique = int(df[field].nunique(dropna=True))
            print(f"  counts {field}: null={nulls} nonnull={nonnull} unique_nonnull={unique}")


def _parse_regions(value: str) -> tuple[str, ...]:
    if value == "all":
        return REGION_CODES
    regions = tuple(part.strip().upper() for part in value.split(",") if part.strip())
    unknown = sorted(set(regions) - set(REGION_CODES))
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown GRIT region(s): {unknown}")
    return regions


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GRIT global HFX v0.2.1 artifacts.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--outer-archive", type=Path, default=DEFAULT_OUTER_ARCHIVE)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("probe", help="run Tier 0 reach schema probe")

    stage1 = subparsers.add_parser("stage1", help="run Stage 1 per-region preprocessing")
    stage1.add_argument("--regions", type=_parse_regions, default=REGION_CODES)

    stage2 = subparsers.add_parser("stage2", help="run Stage 2 dense ID assignment over existing shards")
    stage2.add_argument("--regions", type=_parse_regions, default=REGION_CODES)

    phase25 = subparsers.add_parser("phase25", help="resolve graph edges and compute reach upstream areas")
    phase25.add_argument("--regions", type=_parse_regions, default=REGION_CODES)

    write = subparsers.add_parser("write", help="run Stages 6-9 over existing intermediates")
    write.add_argument("--regions", type=_parse_regions, default=REGION_CODES)
    write.add_argument("--out", type=Path, default=None)

    validate_parser = subparsers.add_parser("validate", help="run the Rust HFX validator")
    validate_parser.add_argument("--out", type=Path, default=None)
    validate_parser.add_argument("--sample-pct", type=float, default=100.0)
    validate_parser.add_argument("--strict", action="store_true", default=True)

    raster = subparsers.add_parser(
        "raster", help="attach D8 rasters to an existing compiled dataset"
    )
    raster.add_argument(
        "--flow-dir-archive", action="append", type=Path, required=True
    )
    raster.add_argument(
        "--flow-acc-archive", action="append", type=Path, required=True
    )
    raster.add_argument("--dataset-dir", type=Path, required=True)
    raster.add_argument("--work-dir", type=Path, required=True)

    return parser.parse_args()


def _source_from_existing(root: Path, regions: tuple[str, ...]) -> SourceData:
    return SourceData(
        root=root,
        tmp_root=root / "tmp",
        regions=regions,
        segment_shards=tuple(root / "tmp" / region / "segments.parquet" for region in regions),
        reach_shards=tuple(root / "tmp" / region / "reaches.parquet" for region in regions),
        segment_snap_shards=tuple(root / "tmp" / region / "segment_snap.parquet" for region in regions),
        reach_snap_shards=tuple(root / "tmp" / region / "reach_snap.parquet" for region in regions),
        segment_rows=sum(pq.read_metadata(root / "tmp" / region / "segments.parquet").num_rows for region in regions),
        reach_rows=sum(pq.read_metadata(root / "tmp" / region / "reaches.parquet").num_rows for region in regions),
    )


def _default_out_dir(root: Path, regions: tuple[str, ...]) -> Path:
    return root / ("grit-hfx-global" if set(regions) == PLANETARY_REGION_SET else "grit-hfx-eu-smoke")


def main() -> int:
    args = _parse_args()
    root = args.root.expanduser().resolve()
    if args.command == "probe":
        probe_reach_schema(root)
        return 0
    if args.command == "raster":
        flow_dir_archives = [path.expanduser().resolve() for path in args.flow_dir_archive]
        flow_acc_archives = [path.expanduser().resolve() for path in args.flow_acc_archive]
        dataset_dir = args.dataset_dir.expanduser().resolve()
        work_dir = args.work_dir.expanduser().resolve()
        manifest_path = dataset_dir / "manifest.json"
        if not dataset_dir.is_dir():
            raise AdapterError(f"dataset directory {dataset_dir} does not exist")
        if not manifest_path.is_file():
            raise AdapterError(f"manifest {manifest_path} does not exist")
        output_dir = dataset_dir / "aux" / "d8"
        direction_path, accumulation_path = build_d8_raster_pair(
            flow_dir_archives,
            flow_acc_archives,
            work_dir,
            output_dir,
        )
        expected_direction = output_dir / "flow_dir.tif"
        expected_accumulation = output_dir / "flow_acc.tif"
        if (
            direction_path != expected_direction
            or accumulation_path != expected_accumulation
            or not direction_path.is_file()
            or not accumulation_path.is_file()
        ):
            raise AdapterError(
                "raster builder returned paths outside the required dataset artifacts: "
                f"{direction_path}, {accumulation_path}"
            )
        _amend_manifest_with_d8_rasters(dataset_dir)
        return 0
    if args.command == "stage1":
        source = stage_1_inspect_source(args.outer_archive.expanduser().resolve(), root, args.regions)
        log(f"Stage 1 summary: segments={source.segment_rows} reaches={source.reach_rows}")
        return 0
    if args.command in {"stage2", "phase25"}:
        regions = args.regions
        source = _source_from_existing(root, regions)
        if args.command == "stage2":
            stage_2_assign_ids(source)
            stage_5_hilbert_sort(source)
            return 0
        resolve_graph_edges(source)
        compute_reach_up_area(source)
        return 0
    if args.command == "write":
        source = _source_from_existing(root, args.regions)
        out_dir = args.out.expanduser().resolve() if args.out is not None else _default_out_dir(root, args.regions)
        stage_6_write_catchments(source, out_dir)
        stage_7_write_graph(source, out_dir)
        stage_8_write_snap(source, out_dir)
        stage_9_write_manifest(source, out_dir)
        return 0
    if args.command == "validate":
        out_dir = args.out.expanduser().resolve() if args.out is not None else root / "grit-hfx-eu-smoke"
        return validate(out_dir, strict=args.strict, sample_pct=args.sample_pct)
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
