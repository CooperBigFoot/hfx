"""Exercise the GRIT raster archive-to-COG pipeline."""

from pathlib import Path
from zipfile import ZipFile

import numpy as np
from affine import Affine
from numpy.testing import assert_array_equal
import pytest
import rasterio
from rasterio.windows import Window
from rio_cogeo.cogeo import cog_validate

from build_adapter import AdapterError, build_d8_raster_pair


WEST_TRANSFORM = Affine(30, 0, 0, 0, -30, 60)
EAST_TRANSFORM = Affine(30, 0, 60, 0, -30, 60)

FLOW_DIR_WEST = np.array([[-1, 2], [3, -128]], dtype=np.int8)
FLOW_DIR_EAST = np.array([[4, -8], [0, 7]], dtype=np.int8)
FLOW_ACC_WEST = np.array([[10, 20], [30, -9999]], dtype=np.int32)
FLOW_ACC_EAST = np.array([[40, 50], [60, 70]], dtype=np.int32)


def _write_tile(
    path: Path,
    values: np.ndarray,
    transform: Affine,
    nodata: int | None,
) -> None:
    profile = {
        "driver": "GTiff",
        "width": 2,
        "height": 2,
        "count": 1,
        "dtype": values.dtype,
        "crs": "EPSG:8857",
        "transform": transform,
    }
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(path, "w", **profile) as dataset:
        dataset.write(values, 1)


def _archive(archive_path: Path, members: list[tuple[Path, str]]) -> None:
    with ZipFile(archive_path, "w") as archive:
        for source, member_name in members:
            archive.write(source, member_name)


@pytest.fixture
def raster_archives(tmp_path: Path) -> tuple[Path, Path]:
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    direction_west = source_dir / "direction_west.tif"
    direction_east = source_dir / "direction_east.tif"
    accumulation_west = source_dir / "accumulation_west.tif"
    accumulation_east = source_dir / "accumulation_east.tif"
    _write_tile(direction_west, FLOW_DIR_WEST, WEST_TRANSFORM, -128)
    _write_tile(direction_east, FLOW_DIR_EAST, EAST_TRANSFORM, -128)
    _write_tile(accumulation_west, FLOW_ACC_WEST, WEST_TRANSFORM, -9999)
    _write_tile(accumulation_east, FLOW_ACC_EAST, EAST_TRANSFORM, -9999)

    direction_archive = tmp_path / "flow_dir_tiles.zip"
    accumulation_archive = tmp_path / "flow_acc_tiles.zip"
    _archive(
        direction_archive,
        [
            (direction_west, "synthetic/drainage_direction_west.tif"),
            (direction_east, "synthetic/drainage_direction_east.tif"),
        ],
    )
    _archive(
        accumulation_archive,
        [
            (accumulation_west, "synthetic/drainage_area_west.tif"),
            (accumulation_east, "synthetic/drainage_area_east.tif"),
        ],
    )
    return direction_archive, accumulation_archive


def test_build_d8_raster_pair_preserves_native_grid_and_values(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    direction_archive, accumulation_archive = raster_archives
    output_dir = tmp_path / "output"

    direction_path, accumulation_path = build_d8_raster_pair(
        [direction_archive],
        [accumulation_archive],
        tmp_path / "work",
        output_dir,
    )

    assert direction_path == output_dir / "flow_dir.tif"
    assert accumulation_path == output_dir / "flow_acc.tif"
    assert direction_path.is_file()
    assert accumulation_path.is_file()

    expected_direction = np.array(
        [[-1, 2, 4, -8], [3, -128, 0, 7]], dtype=np.int8
    )
    expected_accumulation = np.array(
        [[10, 20, 40, 50], [30, -9999, 60, 70]], dtype=np.int32
    )

    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("int8",)
        assert accumulation.dtypes == ("int32",)
        assert direction.nodata == -128
        assert accumulation.nodata == -9999
        assert_array_equal(direction.read(1), expected_direction)
        assert_array_equal(accumulation.read(1), expected_accumulation)
        assert_array_equal(direction.read(1, window=Window(0, 0, 2, 2)), FLOW_DIR_WEST)
        assert_array_equal(direction.read(1, window=Window(2, 0, 2, 2)), FLOW_DIR_EAST)
        assert_array_equal(accumulation.read(1, window=Window(0, 0, 2, 2)), FLOW_ACC_WEST)
        assert_array_equal(accumulation.read(1, window=Window(2, 0, 2, 2)), FLOW_ACC_EAST)

        assert direction.crs.to_epsg() == 8857
        assert accumulation.crs.to_epsg() == 8857
        assert direction.width == accumulation.width == 4
        assert direction.height == accumulation.height == 2
        assert direction.transform == accumulation.transform == WEST_TRANSFORM
        assert direction.res == accumulation.res == (30.0, 30.0)
        assert direction.bounds == accumulation.bounds
        assert direction.count == accumulation.count == 1
        assert direction.is_tiled and accumulation.is_tiled
        assert direction.block_shapes == [(512, 512)]
        assert accumulation.block_shapes == [(512, 512)]
        assert direction.tags(ns="IMAGE_STRUCTURE")["COMPRESSION"] == "DEFLATE"
        assert accumulation.tags(ns="IMAGE_STRUCTURE")["COMPRESSION"] == "DEFLATE"

    for output_path in (direction_path, accumulation_path):
        assert output_path.read_bytes()[:4] in (b"II+\x00", b"MM\x00+")
        valid, errors, warnings = cog_validate(output_path)
        assert valid, f"{output_path}: errors={errors}, warnings={warnings}"
        assert errors == [], f"{output_path}: errors={errors}, warnings={warnings}"


def test_missing_source_nodata_fails_before_output_creation(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    _, accumulation_archive = raster_archives
    missing_source = tmp_path / "direction_missing_nodata.tif"
    _write_tile(
        missing_source,
        np.array([[1, 2], [3, 4]], dtype=np.int8),
        WEST_TRANSFORM,
        None,
    )
    missing_archive = tmp_path / "missing_nodata.zip"
    member_name = "synthetic/drainage_direction_missing_nodata.tif"
    _archive(missing_archive, [(missing_source, member_name)])
    output_dir = tmp_path / "output"

    with pytest.raises(AdapterError, match=r"drainage_direction_missing_nodata\.tif.*nodata"):
        build_d8_raster_pair(
            [missing_archive],
            [accumulation_archive],
            tmp_path / "work",
            output_dir,
        )

    assert not output_dir.exists()
