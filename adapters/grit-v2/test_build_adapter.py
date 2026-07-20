"""Exercise the GRIT raster archive-to-COG pipeline."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
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
GAPPED_EAST_TRANSFORM = Affine(30, 0, 90, 0, -30, 60)

FLOW_DIR_WEST = np.array([[1, 2], [3, 255]], dtype=np.uint8)
FLOW_DIR_EAST = np.array([[4, 8], [0, 7]], dtype=np.uint8)
FLOW_DIR_NODATA = 255
FLOW_ACC_WEST = np.array([[10, 20], [30, -9999]], dtype=np.int32)
FLOW_ACC_EAST = np.array([[40, 50], [60, 70]], dtype=np.int32)
FLOW_ACC_NODATA = -9999

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
    _write_tile(direction_west, FLOW_DIR_WEST, WEST_TRANSFORM, FLOW_DIR_NODATA)
    _write_tile(direction_east, FLOW_DIR_EAST, EAST_TRANSFORM, FLOW_DIR_NODATA)
    _write_tile(accumulation_west, FLOW_ACC_WEST, WEST_TRANSFORM, FLOW_ACC_NODATA)
    _write_tile(accumulation_east, FLOW_ACC_EAST, EAST_TRANSFORM, FLOW_ACC_NODATA)

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
        [[10, 20, 40, 50], [30, -9999, 60, 70]], dtype=np.int32
    )

    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("uint8",)
        assert accumulation.dtypes == ("int32",)
        assert direction.nodata == 255
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
        [[10, 20, -9999, 40, 50], [30, -9999, -9999, 60, 70]],
        dtype=np.int32,
    )

    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.width == accumulation.width == 5
        assert direction.height == accumulation.height == 2
        assert direction.transform == accumulation.transform == WEST_TRANSFORM
        assert_array_equal(direction.read(1), expected_gapped_flow_dir)
        assert_array_equal(accumulation.read(1), expected_gapped_flow_acc)
        # Guard the GDAL sparse-block nodata semantics the planetary build relies on.
        assert_array_equal(
            direction.read(1)[:, 2], np.array([255, 255], dtype=np.uint8)
        )
        assert_array_equal(
            accumulation.read(1)[:, 2], np.array([-9999, -9999], dtype=np.int32)
        )
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
        [[10, 20, 40, 50], [30, -9999, 60, 70]], dtype=np.int32
    )
    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("uint8",)
        assert direction.nodata == 255
        assert_array_equal(direction.read(1), expected_direction)
        assert accumulation.dtypes == ("int32",)
        assert accumulation.nodata == -9999
        assert_array_equal(accumulation.read(1), expected_accumulation)


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
        [[10, 20, 40, 50], [30, -9999, 60, 70]], dtype=np.int32
    )
    with rasterio.open(direction_path) as direction, rasterio.open(
        accumulation_path
    ) as accumulation:
        assert direction.dtypes == ("uint8",)
        assert accumulation.dtypes == ("int32",)
        assert direction.nodata == 255
        assert accumulation.nodata == -9999
        assert direction.crs.to_epsg() == accumulation.crs.to_epsg() == 8857
        assert direction.width == accumulation.width == 4
        assert direction.height == accumulation.height == 2
        assert direction.transform == accumulation.transform == WEST_TRANSFORM
        assert direction.res == accumulation.res == (30.0, 30.0)
        assert direction.bounds == accumulation.bounds
        assert_array_equal(direction.read(1), expected_direction)
        assert_array_equal(accumulation.read(1), expected_accumulation)

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
