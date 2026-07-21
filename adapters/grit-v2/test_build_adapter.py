"""Exercise the GRIT raster archive-to-COG pipeline."""

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from zipfile import ZipFile

import numpy as np
from affine import Affine
from numpy.testing import assert_array_equal
import pytest
import rasterio
from rasterio.windows import Window
from rio_cogeo.cogeo import cog_validate

import build_adapter as adapter
from build_adapter import AdapterError, build_d8_raster_pair


WEST_TRANSFORM = Affine(30, 0, 0, 0, -30, 60)
EAST_TRANSFORM = Affine(30, 0, 60, 0, -30, 60)
GAPPED_EAST_TRANSFORM = Affine(30, 0, 90, 0, -30, 60)

FLOW_DIR_WEST = np.array([[1, 2], [3, 255]], dtype=np.uint8)
FLOW_DIR_EAST = np.array([[4, 8], [0, 7]], dtype=np.uint8)
FLOW_DIR_NODATA = 255
FLOW_ACC_WEST = np.array(
    [[10.0, 20.0], [30.0, np.nan]], dtype=np.float32
)
FLOW_ACC_EAST = np.array(
    [[40.0, 50.0], [0.0009, 70.0]], dtype=np.float32
)
FLOW_ACC_NODATA = float("nan")

SNAP_AUXILIARY = [
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
]

CANONICAL_D8_AUXILIARY = {
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


def _write_tile(
    path: Path,
    values: np.ndarray,
    transform: Affine,
    nodata: int | float | None,
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


def _write_constant_tiled_raster(
    path: Path, dtype: str, value: int | float, nodata: int | float
) -> None:
    width = height = 8192
    block_size = 512
    profile = {
        "driver": "GTiff",
        "width": width,
        "height": height,
        "count": 1,
        "dtype": dtype,
        "crs": "EPSG:8857",
        "transform": Affine(30, 0, 0, 0, -30, height * 30),
        "nodata": nodata,
        "tiled": True,
        "blockxsize": block_size,
        "blockysize": block_size,
        "compress": "deflate",
        "BIGTIFF": "YES",
    }
    block = np.full((block_size, block_size), value, dtype=dtype)
    with rasterio.open(path, "w", **profile) as dataset:
        for _, window in dataset.block_windows(1):
            dataset.write(block, 1, window=window)


def _corrupt_first_member_crc(archive_path: Path) -> None:
    with ZipFile(archive_path) as archive:
        info = archive.infolist()[0]
    contents = bytearray(archive_path.read_bytes())
    filename_length = int.from_bytes(
        contents[info.header_offset + 26 : info.header_offset + 28], "little"
    )
    extra_length = int.from_bytes(
        contents[info.header_offset + 28 : info.header_offset + 30], "little"
    )
    data_offset = info.header_offset + 30 + filename_length + extra_length
    contents[data_offset] ^= 0x01
    archive_path.write_bytes(contents)


def _assert_clean_cog(path: Path) -> None:
    valid, errors, warnings = cog_validate(path)
    assert valid, f"{path}: errors={errors}, warnings={warnings}"
    assert errors == [], f"{path}: errors={errors}, warnings={warnings}"


def _write_raster_manifest(dataset_dir: Path) -> None:
    dataset_dir.mkdir()
    (dataset_dir / "manifest.json").write_text(
        json.dumps(
            {
                "format_version": "0.3.0",
                "adapter_version": "grit-global-2.1.0",
                "auxiliary": [],
            }
        )
        + "\n"
    )


@pytest.fixture
def raster_archives(tmp_path: Path) -> tuple[Path, Path]:
    source_dir = tmp_path / "sources"
    source_dir.mkdir()
    direction_west = source_dir / "direction_west.tif"
    direction_east = source_dir / "direction_east.tif"
    accumulation_west = source_dir / "accumulation_west.tif"
    accumulation_east = source_dir / "accumulation_east.tif"
    _write_tile(direction_west, FLOW_DIR_WEST, WEST_TRANSFORM, FLOW_DIR_NODATA)
    _write_tile(direction_east, FLOW_DIR_EAST, EAST_TRANSFORM, FLOW_DIR_NODATA)
    _write_tile(accumulation_west, FLOW_ACC_WEST, WEST_TRANSFORM, FLOW_ACC_NODATA)
    _write_tile(accumulation_east, FLOW_ACC_EAST, EAST_TRANSFORM, None)

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


@pytest.fixture
def gapped_raster_archives(tmp_path: Path) -> tuple[Path, Path]:
    source_dir = tmp_path / "gapped-sources"
    source_dir.mkdir()
    direction_west = source_dir / "direction_west.tif"
    direction_east = source_dir / "direction_gapped_east.tif"
    accumulation_west = source_dir / "accumulation_west.tif"
    accumulation_east = source_dir / "accumulation_gapped_east.tif"
    _write_tile(direction_west, FLOW_DIR_WEST, WEST_TRANSFORM, FLOW_DIR_NODATA)
    _write_tile(
        direction_east, FLOW_DIR_EAST, GAPPED_EAST_TRANSFORM, FLOW_DIR_NODATA
    )
    _write_tile(accumulation_west, FLOW_ACC_WEST, WEST_TRANSFORM, FLOW_ACC_NODATA)
    _write_tile(
        accumulation_east,
        FLOW_ACC_EAST,
        GAPPED_EAST_TRANSFORM,
        FLOW_ACC_NODATA,
    )

    direction_archive = tmp_path / "gapped_flow_dir_tiles.zip"
    accumulation_archive = tmp_path / "gapped_flow_acc_tiles.zip"
    _archive(
        direction_archive,
        [
            (direction_west, "synthetic/drainage_direction_west.tif"),
            (
                direction_east,
                "synthetic/drainage_direction_gapped_east.tif",
            ),
        ],
    )
    _archive(
        accumulation_archive,
        [
            (accumulation_west, "synthetic/drainage_area_west.tif"),
            (accumulation_east, "synthetic/drainage_area_gapped_east.tif"),
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
        [[1, 2, 4, 8], [3, 255, 0, 7]], dtype=np.uint8
    )
    expected_accumulation = np.array(
        [
            [10.0, 20.0, 40.0, 50.0],
            [30.0, np.nan, 0.0009, 70.0],
        ],
        dtype=np.float32,
    )

    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("uint8",)
        assert accumulation.dtypes == ("float32",)
        assert direction.nodata == 255
        assert math.isnan(accumulation.nodata)
        assert_array_equal(direction.read(1), expected_direction)
        actual = accumulation.read(1)
        expected_nan = np.isnan(expected_accumulation)
        assert np.array_equal(np.isnan(actual), expected_nan)
        assert np.array_equal(
            actual[~expected_nan], expected_accumulation[~expected_nan]
        )
        assert_array_equal(direction.read(1, window=Window(0, 0, 2, 2)), FLOW_DIR_WEST)
        assert_array_equal(direction.read(1, window=Window(2, 0, 2, 2)), FLOW_DIR_EAST)
        actual_west = accumulation.read(1, window=Window(0, 0, 2, 2))
        west_nan = np.isnan(FLOW_ACC_WEST)
        assert np.array_equal(np.isnan(actual_west), west_nan)
        assert np.array_equal(actual_west[~west_nan], FLOW_ACC_WEST[~west_nan])
        assert np.array_equal(
            accumulation.read(1, window=Window(2, 0, 2, 2)), FLOW_ACC_EAST
        )

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


def test_build_d8_raster_pair_reads_sparse_union_gap_as_nodata(
    tmp_path: Path,
    gapped_raster_archives: tuple[Path, Path],
) -> None:
    direction_archive, accumulation_archive = gapped_raster_archives
    direction_path, accumulation_path = build_d8_raster_pair(
        [direction_archive],
        [accumulation_archive],
        tmp_path / "gapped-work",
        tmp_path / "gapped-output",
    )

    expected_gapped_flow_dir = np.array(
        [[1, 2, 255, 4, 8], [3, 255, 255, 0, 7]], dtype=np.uint8
    )
    expected_gapped_flow_acc = np.array(
        [
            [10.0, 20.0, np.nan, 40.0, 50.0],
            [30.0, np.nan, np.nan, 0.0009, 70.0],
        ],
        dtype=np.float32,
    )

    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.width == accumulation.width == 5
        assert direction.height == accumulation.height == 2
        assert direction.transform == accumulation.transform == WEST_TRANSFORM
        assert_array_equal(direction.read(1), expected_gapped_flow_dir)
        actual = accumulation.read(1)
        expected_nan = np.isnan(expected_gapped_flow_acc)
        assert np.array_equal(np.isnan(actual), expected_nan)
        assert np.array_equal(
            actual[~expected_nan], expected_gapped_flow_acc[~expected_nan]
        )
        # Guard the GDAL sparse-block nodata semantics the planetary build relies on.
        assert_array_equal(
            direction.read(1)[:, 2], np.array([255, 255], dtype=np.uint8)
        )
        gap = accumulation.read(1)[:, 2]
        assert np.array_equal(np.isnan(gap), np.array([True, True]))
        assert direction.read(1)[1, 3] == 0


def test_tagless_direction_source_with_nodata_value_fails_before_output_creation(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    _, accumulation_archive = raster_archives
    source_dir = tmp_path / "tagless-rejection-sources"
    source_dir.mkdir()
    tagged_source = source_dir / "direction_tagged_west.tif"
    tagless_source = source_dir / "direction_missing_nodata_east.tif"
    _write_tile(
        tagged_source,
        np.array([[1, 2], [3, 0]], dtype=np.uint8),
        WEST_TRANSFORM,
        FLOW_DIR_NODATA,
    )
    _write_tile(
        tagless_source,
        np.array([[4, 8], [255, 7]], dtype=np.uint8),
        EAST_TRANSFORM,
        None,
    )
    missing_archive = tmp_path / "missing_nodata.zip"
    _archive(
        missing_archive,
        [
            (tagged_source, "synthetic/drainage_direction_tagged_west.tif"),
            (
                tagless_source,
                "synthetic/drainage_direction_missing_nodata_east.tif",
            ),
        ],
    )
    output_dir = tmp_path / "output"

    with pytest.raises(
        AdapterError,
        match=(
            r"drainage_direction_missing_nodata_east\.tif.*"
            r"255.*valid data domain 0 through 8"
        ),
    ):
        build_d8_raster_pair(
            [missing_archive],
            [accumulation_archive],
            tmp_path / "work",
            output_dir,
        )

    assert not output_dir.exists()


def test_tagless_direction_source_with_valid_codes_is_accepted(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    _, accumulation_archive = raster_archives
    source_dir = tmp_path / "tagless-acceptance-sources"
    source_dir.mkdir()
    tagged_source = source_dir / "direction_tagged_west.tif"
    tagless_source = source_dir / "direction_missing_nodata_east.tif"
    _write_tile(
        tagged_source,
        np.array([[1, 2], [3, 255]], dtype=np.uint8),
        WEST_TRANSFORM,
        FLOW_DIR_NODATA,
    )
    _write_tile(
        tagless_source,
        np.array([[4, 8], [6, 7]], dtype=np.uint8),
        EAST_TRANSFORM,
        None,
    )
    direction_archive = tmp_path / "valid_tagless_direction.zip"
    _archive(
        direction_archive,
        [
            (tagged_source, "synthetic/drainage_direction_tagged_west.tif"),
            (
                tagless_source,
                "synthetic/drainage_direction_missing_nodata_east.tif",
            ),
        ],
    )

    direction_path, accumulation_path = build_d8_raster_pair(
        [direction_archive],
        [accumulation_archive],
        tmp_path / "valid-tagless-work",
        tmp_path / "valid-tagless-output",
    )

    expected_direction = np.array(
        [[1, 2, 4, 8], [3, 255, 6, 7]], dtype=np.uint8
    )
    expected_accumulation = np.array(
        [
            [10.0, 20.0, 40.0, 50.0],
            [30.0, np.nan, 0.0009, 70.0],
        ],
        dtype=np.float32,
    )
    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("uint8",)
        assert direction.nodata == 255
        assert_array_equal(direction.read(1), expected_direction)
        assert accumulation.dtypes == ("float32",)
        assert math.isnan(accumulation.nodata)
        actual = accumulation.read(1)
        expected_nan = np.isnan(expected_accumulation)
        assert np.array_equal(np.isnan(actual), expected_nan)
        assert np.array_equal(
            actual[~expected_nan], expected_accumulation[~expected_nan]
        )


def test_tagless_accumulation_source_with_nan_value_fails_before_output_creation(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    direction_archive, _ = raster_archives
    source_dir = tmp_path / "tagless-accumulation-rejection-sources"
    source_dir.mkdir()
    tagged_source = source_dir / "accumulation_tagged_west.tif"
    tagless_source = source_dir / "accumulation_missing_nodata_east.tif"
    tagged_accumulation_west = np.array(
        [[10.0, 20.0], [30.0, np.nan]], dtype=np.float32
    )
    tagged_accumulation_west_nodata = float("nan")
    tagged_accumulation_west_transform = WEST_TRANSFORM
    tagless_accumulation_east = np.array(
        [[40.0, 50.0], [np.nan, 70.0]], dtype=np.float32
    )
    tagless_accumulation_east_nodata = None
    tagless_accumulation_east_transform = EAST_TRANSFORM
    _write_tile(
        tagged_source,
        tagged_accumulation_west,
        tagged_accumulation_west_transform,
        tagged_accumulation_west_nodata,
    )
    _write_tile(
        tagless_source,
        tagless_accumulation_east,
        tagless_accumulation_east_transform,
        tagless_accumulation_east_nodata,
    )
    accumulation_archive = tmp_path / "missing_accumulation_nodata.zip"
    _archive(
        accumulation_archive,
        [
            (tagged_source, "synthetic/drainage_area_tagged_west.tif"),
            (
                tagless_source,
                "synthetic/drainage_area_missing_nodata_east.tif",
            ),
        ],
    )
    output_dir = tmp_path / "output"

    with pytest.raises(
        AdapterError,
        match=(
            r"tag-less flow_acc source .*drainage_area_missing_nodata_east\.tif "
            r"contains the family's common tagged nodata nan"
        ),
    ):
        build_d8_raster_pair(
            [direction_archive],
            [accumulation_archive],
            tmp_path / "work",
            output_dir,
        )

    assert not output_dir.exists()


def test_raster_cli_attaches_pair_without_touching_vector_artifacts(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    direction_archive, accumulation_archive = raster_archives
    dataset_dir = tmp_path / "compiled-dataset"
    dataset_dir.mkdir()
    manifest = {
        "format_version": "0.3.0",
        "fabric_name": "grit",
        "fabric_version": "1.0.0",
        "crs": "EPSG:4326",
        "has_up_area": True,
        "topology": "dag",
        "bbox": [-10.0, 40.0, 10.0, 60.0],
        "unit_count": 2,
        "created_at": "2026-07-20T00:00:00Z",
        "adapter_version": "grit-global-2.0.0",
        "auxiliary": copy.deepcopy(SNAP_AUXILIARY),
        "region": "europe-smoke",
    }
    manifest_path = dataset_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    original_manifest = copy.deepcopy(json.loads(manifest_path.read_text()))

    vector_files = {
        "catchments.parquet": b"synthetic catchments parquet bytes\n",
        "graph.parquet": b"synthetic graph parquet bytes\n",
        "aux/snap_segments.parquet": b"synthetic segment snap parquet bytes\n",
        "aux/snap_reaches.parquet": b"synthetic reach snap parquet bytes\n",
    }
    vector_paths = {name: dataset_dir / name for name in vector_files}
    for name, contents in vector_files.items():
        path = vector_paths[name]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
    vector_hashes = {
        name: hashlib.sha256(path.read_bytes()).hexdigest()
        for name, path in vector_paths.items()
    }

    command = [
        sys.executable,
        str(Path(__file__).with_name("build_adapter.py")),
        "raster",
        "--flow-dir-archive",
        str(direction_archive),
        "--flow-acc-archive",
        str(accumulation_archive),
        "--dataset-dir",
        str(dataset_dir),
        "--work-dir",
        str(tmp_path / "raster-work"),
    ]
    subprocess.run(command, check=True)

    direction_path = dataset_dir / "aux/d8/flow_dir.tif"
    accumulation_path = dataset_dir / "aux/d8/flow_acc.tif"
    assert direction_path.is_file()
    assert accumulation_path.is_file()
    expected_direction = np.array(
        [[1, 2, 4, 8], [3, 255, 0, 7]], dtype=np.uint8
    )
    expected_accumulation = np.array(
        [
            [10.0, 20.0, 40.0, 50.0],
            [30.0, np.nan, 0.0009, 70.0],
        ],
        dtype=np.float32,
    )
    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("uint8",)
        assert accumulation.dtypes == ("float32",)
        assert direction.nodata == 255
        assert math.isnan(accumulation.nodata)
        assert direction.crs.to_epsg() == accumulation.crs.to_epsg() == 8857
        assert direction.width == accumulation.width == 4
        assert direction.height == accumulation.height == 2
        assert direction.transform == accumulation.transform == WEST_TRANSFORM
        assert direction.res == accumulation.res == (30.0, 30.0)
        assert direction.bounds == accumulation.bounds
        assert_array_equal(direction.read(1), expected_direction)
        actual = accumulation.read(1)
        expected_nan = np.isnan(expected_accumulation)
        assert np.array_equal(np.isnan(actual), expected_nan)
        assert np.array_equal(
            actual[~expected_nan], expected_accumulation[~expected_nan]
        )

    amended_manifest = json.loads(manifest_path.read_text())
    assert amended_manifest["format_version"] == "0.3.0"
    assert amended_manifest["adapter_version"] == "grit-global-2.1.0"
    d8_entries = [
        entry
        for entry in amended_manifest["auxiliary"]
        if entry.get("schema") == "hfx.aux.d8_raster.v2"
    ]
    assert d8_entries == [CANONICAL_D8_AUXILIARY]
    assert amended_manifest["auxiliary"][:2] == original_manifest["auxiliary"]
    for key, value in original_manifest.items():
        if key not in {"adapter_version", "auxiliary"}:
            assert amended_manifest[key] == value
    for name, path in vector_paths.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == vector_hashes[name]

    first_manifest_bytes = manifest_path.read_bytes()
    subprocess.run(command, check=True)

    assert manifest_path.read_bytes() == first_manifest_bytes
    repeated_manifest = json.loads(manifest_path.read_text())
    repeated_d8_entries = [
        entry
        for entry in repeated_manifest["auxiliary"]
        if entry.get("schema") == "hfx.aux.d8_raster.v2"
    ]
    assert repeated_d8_entries == [CANONICAL_D8_AUXILIARY]
    assert repeated_manifest["auxiliary"][:2] == original_manifest["auxiliary"]
    for key, value in original_manifest.items():
        if key not in {"adapter_version", "auxiliary"}:
            assert repeated_manifest[key] == value
    for name, path in vector_paths.items():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == vector_hashes[name]


def test_streaming_cog_translation_peak_work_dir_below_quarter_raw_size(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "large-sources"
    source_dir.mkdir()
    direction_source = source_dir / "direction.tif"
    accumulation_source = source_dir / "accumulation.tif"
    _write_constant_tiled_raster(
        direction_source, "uint8", np.uint8(1), FLOW_DIR_NODATA
    )
    _write_constant_tiled_raster(
        accumulation_source, "float32", np.float32(1.25), FLOW_ACC_NODATA
    )
    direction_archive = tmp_path / "large_flow_dir.zip"
    accumulation_archive = tmp_path / "large_flow_acc.zip"
    _archive(
        direction_archive,
        [(direction_source, "flow_dir/drainage_direction.tif")],
    )
    _archive(
        accumulation_archive,
        [(accumulation_source, "flow_acc/drainage_area.tif")],
    )

    work_dir = tmp_path / "large-work"
    output_dir = tmp_path / "large-output"
    peak_size = [0]
    stop_sampling = threading.Event()

    def sample_work_dir() -> None:
        total = 0
        for root, _, filenames in os.walk(work_dir):
            for filename in filenames:
                try:
                    path = Path(root) / filename
                    if path.is_file():
                        total += path.stat().st_size
                except FileNotFoundError:
                    continue
        peak_size[0] = max(peak_size[0], total)

    def sampler() -> None:
        while not stop_sampling.is_set():
            sample_work_dir()
            time.sleep(0.005)

    sample_work_dir()
    sampling_thread = threading.Thread(target=sampler)
    sampling_thread.start()
    try:
        _, accumulation_path = build_d8_raster_pair(
            [direction_archive],
            [accumulation_archive],
            work_dir,
            output_dir,
        )
        sample_work_dir()
    finally:
        stop_sampling.set()
        sampling_thread.join()

    raw_bytes = 8192 * 8192 * np.dtype(np.float32).itemsize
    assert raw_bytes == 268_435_456
    threshold = raw_bytes * 0.25
    assert peak_size[0] < threshold
    assert accumulation_path.stat().st_size < threshold
    with rasterio.open(accumulation_path) as accumulation:
        assert accumulation.overviews(1) == [2, 4, 8, 16]
        assert accumulation.read(1, window=Window(0, 0, 1, 1))[0, 0] == np.float32(
            1.25
        )
        assert accumulation.read(1, window=Window(8191, 8191, 1, 1))[
            0, 0
        ] == np.float32(1.25)
        assert math.isnan(accumulation.nodata)
    _assert_clean_cog(accumulation_path)


def test_raster_defaults_preserve_inputs_and_enable_disk_check(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direction_archive, accumulation_archive = raster_archives
    observed_disk_checks: list[str | None] = []
    real_rio_copy = adapter.rio_copy

    def recording_copy(*args: object, **kwargs: object) -> object:
        observed_disk_checks.append(
            rasterio.env.get_gdal_config("CHECK_DISK_FREE_SPACE")
        )
        return real_rio_copy(*args, **kwargs)

    monkeypatch.setattr(adapter, "rio_copy", recording_copy)
    work_dir = tmp_path / "defaults-work"
    build_d8_raster_pair(
        [direction_archive],
        [accumulation_archive],
        work_dir,
        tmp_path / "defaults-output",
    )

    assert observed_disk_checks == ["TRUE", "TRUE"]
    assert direction_archive.exists()
    assert accumulation_archive.exists()
    assert (work_dir / "flow_dir" / "extracted").is_dir()
    assert (work_dir / "flow_acc" / "extracted").is_dir()


def test_raster_opt_ins_delete_at_verified_boundaries_and_disable_disk_check(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    direction_archive, accumulation_archive = raster_archives
    dataset_dir = tmp_path / "opt-in-dataset"
    work_dir = tmp_path / "opt-in-work"
    _write_raster_manifest(dataset_dir)
    real_extract = adapter._extract_raster_archives
    real_validate_sources = adapter._validate_raster_sources
    real_translate = adapter._translate_and_validate_cog
    real_rio_copy = adapter.rio_copy
    extraction_returns: list[str] = []
    validation_calls = [0]
    observed_disk_checks: list[str | None] = []

    def recording_extract(*args: object, **kwargs: object) -> tuple[Path, ...]:
        extracted = real_extract(*args, **kwargs)
        artifact_name = str(args[2])
        extraction_returns.append(artifact_name)
        assert direction_archive.exists()
        assert accumulation_archive.exists()
        return extracted

    def recording_validation(*args: object, **kwargs: object) -> object:
        validation_calls[0] += 1
        if validation_calls[0] == 1:
            assert not direction_archive.exists()
            assert not accumulation_archive.exists()
        return real_validate_sources(*args, **kwargs)

    def recording_translation(
        source_vrt: Path,
        output_path: Path,
        temp_folder: Path,
        disable_disk_free_space_check: bool = False,
    ) -> None:
        direction_tree = work_dir / "flow_dir" / "extracted"
        accumulation_tree = work_dir / "flow_acc" / "extracted"
        if source_vrt.parent.name == "flow_dir":
            assert direction_tree.is_dir()
            assert accumulation_tree.is_dir()
        else:
            assert not direction_tree.exists()
            assert accumulation_tree.is_dir()
        real_translate(
            source_vrt,
            output_path,
            temp_folder,
            disable_disk_free_space_check,
        )

    def recording_copy(*args: object, **kwargs: object) -> object:
        observed_disk_checks.append(
            rasterio.env.get_gdal_config("CHECK_DISK_FREE_SPACE")
        )
        return real_rio_copy(*args, **kwargs)

    monkeypatch.setattr(adapter, "_extract_raster_archives", recording_extract)
    monkeypatch.setattr(adapter, "_validate_raster_sources", recording_validation)
    monkeypatch.setattr(adapter, "_translate_and_validate_cog", recording_translation)
    monkeypatch.setattr(adapter, "rio_copy", recording_copy)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_adapter.py",
            "raster",
            "--flow-dir-archive",
            str(direction_archive),
            "--flow-acc-archive",
            str(accumulation_archive),
            "--dataset-dir",
            str(dataset_dir),
            "--work-dir",
            str(work_dir),
            "--consume-archives",
            "--reclaim-extracted",
            "--disable-disk-free-space-check",
        ],
    )

    assert adapter.main() == 0
    assert extraction_returns == ["flow_dir", "flow_acc"]
    assert observed_disk_checks == ["FALSE", "FALSE"]
    assert not (work_dir / "flow_dir" / "extracted").exists()
    assert not (work_dir / "flow_acc" / "extracted").exists()
    direction_path = dataset_dir / "aux" / "d8" / "flow_dir.tif"
    accumulation_path = dataset_dir / "aux" / "d8" / "flow_acc.tif"
    assert direction_path.is_file()
    assert accumulation_path.is_file()
    _assert_clean_cog(direction_path)
    _assert_clean_cog(accumulation_path)


def test_consume_archives_preserves_all_archives_when_second_family_extraction_fails(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
) -> None:
    direction_archive, accumulation_archive = raster_archives
    _corrupt_first_member_crc(accumulation_archive)

    with pytest.raises(AdapterError, match=r"failed to extract archive .* member"):
        build_d8_raster_pair(
            [direction_archive],
            [accumulation_archive],
            tmp_path / "corrupt-work",
            tmp_path / "corrupt-output",
            consume_archives=True,
        )

    assert direction_archive.exists()
    assert accumulation_archive.exists()


@pytest.mark.parametrize("failed_family", ["flow_dir", "flow_acc"])
def test_reclaim_extracted_preserves_unvalidated_family_on_cog_failure(
    tmp_path: Path,
    raster_archives: tuple[Path, Path],
    monkeypatch: pytest.MonkeyPatch,
    failed_family: str,
) -> None:
    direction_archive, accumulation_archive = raster_archives
    work_dir = tmp_path / f"reclaim-{failed_family}-work"
    real_cog_validate = adapter.cog_validate

    def selected_failure(path: Path) -> tuple[bool, list[str], list[str]]:
        if Path(path).parent.name == failed_family:
            return False, ["forced validation failure"], []
        return real_cog_validate(path)

    monkeypatch.setattr(adapter, "cog_validate", selected_failure)
    with pytest.raises(AdapterError, match="COG validation failed"):
        build_d8_raster_pair(
            [direction_archive],
            [accumulation_archive],
            work_dir,
            tmp_path / f"reclaim-{failed_family}-output",
            reclaim_extracted=True,
        )

    direction_tree = work_dir / "flow_dir" / "extracted"
    accumulation_tree = work_dir / "flow_acc" / "extracted"
    if failed_family == "flow_dir":
        assert direction_tree.is_dir()
        assert accumulation_tree.is_dir()
    else:
        assert not direction_tree.exists()
        assert accumulation_tree.is_dir()
