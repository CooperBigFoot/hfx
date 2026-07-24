"""Verify assembled TDX-Hydro output with bounded batch-local memory.

The sole dataset-sized retained structure is a set of catchment IDs. At roughly
16 million distinct rows, that set is expected to occupy about 0.8 to 1.1 GiB
on CPython, depending on the interpreter build and set capacity. It shrinks as
snap references are consumed. Arrow buffers, WKB lists, centroids, and Hilbert
values use an additional O(batch_size) memory.
"""

import argparse
import math
import json
import os
import random
import subprocess
import sys
import tempfile
import time
import warnings
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

import geopandas as gpd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import LineString, Polygon

CATCHMENT_ORDER_FAILURE = (
    "catchments.parquet is not non-decreasing by (hilbert, id) in file order"
)
SNAP_ID_FAILURE = (
    "aux/snap_stems.parquet ids are not exactly 1..N in file order"
)
MANIFEST_AUX_FAILURE = (
    "manifest.json must declare hfx.aux.snap.v2 at "
    "aux/snap_stems.parquet with references_levels [0]"
)
SNAP_REFERENCE_FAILURE = (
    "aux/snap_stems.parquet unit_id does not resolve exactly once in "
    "catchments.parquet"
)
MANIFEST_SCHEMA_FAILURE = "manifest.json failed schema validation"

ABS_SCHEMA_PATH = (
    Path(__file__).resolve().parents[2] / "schemas" / "manifest.schema.json"
).resolve()

DEFAULT_BATCH_SIZE = 65_536
WORLD_BOUNDS = [-180, -90, 180, 90]
ASSEMBLE_INPUT_BATCH_SIZE = 1024
ASSEMBLE_ROW_GROUP_MIN = 4096
ASSEMBLE_ROW_GROUP_MAX = 8192
GLOBAL_LINKNO_STRIDE = 10_000_000
DEFAULT_BASIN_COUNT = 62
DEFAULT_TOTAL_UNITS = 16_000_000
DEFAULT_SEED = 20_260_723
DEFAULT_GENERATOR_BATCH_SIZE = 1024
DEFAULT_SAMPLE_INTERVAL_MS = 50
DEFAULT_RSS_CEILING_BYTES = 32_212_254_720
DEFAULT_SCRATCH_CEILING_BYTES = 68_719_476_736
RSS_CEILING_FAILURE = (
    "rehearsal failed: process-tree peak RSS exceeded ceiling"
)
SCRATCH_CEILING_FAILURE = (
    "rehearsal failed: scratch-tree peak usage exceeded ceiling"
)


class VerificationError(Exception):
    """Report a planetary-output verification failure."""


class RehearsalError(Exception):
    """Report a synthetic assembly rehearsal failure."""


@contextmanager
def _filter_arrow_sandbox_diagnostics() -> Iterator[None]:
    """Filter denied-sysctl diagnostics while retaining all other stderr."""
    try:
        stderr_fd = sys.stderr.fileno()
    except (AttributeError, OSError):
        yield
        return

    with tempfile.TemporaryFile() as captured:
        saved_stderr_fd = os.dup(stderr_fd)
        try:
            os.dup2(captured.fileno(), stderr_fd)
            yield
        finally:
            os.dup2(saved_stderr_fd, stderr_fd)
            os.close(saved_stderr_fd)
        captured.seek(0)
        diagnostics = captured.read().decode(errors="replace")

    retained = [
        line
        for line in diagnostics.splitlines(keepends=True)
        if not (
            "/arrow/util/cpu_info.cc:" in line
            and "sysctlbyname failed for" in line
        )
    ]
    if retained:
        sys.stderr.write("".join(retained))


def _validate_manifest(manifest_path: Path) -> None:
    if not ABS_SCHEMA_PATH.is_absolute() or not ABS_SCHEMA_PATH.is_file():
        raise VerificationError(MANIFEST_SCHEMA_FAILURE)

    try:
        result = subprocess.run(
            [
                "uv",
                "run",
                "--with",
                "check-jsonschema",
                "check-jsonschema",
                "--schemafile",
                str(ABS_SCHEMA_PATH),
                str(manifest_path.resolve()),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as error:
        raise VerificationError(MANIFEST_SCHEMA_FAILURE) from error
    if result.returncode != 0:
        raise VerificationError(MANIFEST_SCHEMA_FAILURE)

    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise VerificationError("manifest.json could not be parsed") from error

    auxiliary = manifest.get("auxiliary")
    if not isinstance(auxiliary, list):
        raise VerificationError(MANIFEST_AUX_FAILURE)

    for declaration in auxiliary:
        if not isinstance(declaration, dict):
            continue
        artifacts = declaration.get("artifacts")
        metadata = declaration.get("metadata")
        if not isinstance(artifacts, dict) or not isinstance(metadata, dict):
            continue
        references_levels = metadata.get("references_levels")
        references_level_zero = (
            isinstance(references_levels, list)
            and len(references_levels) == 1
            and type(references_levels[0]) is int
            and references_levels[0] == 0
        )
        if (
            declaration.get("schema") == "hfx.aux.snap.v2"
            and artifacts.get("snap") == "aux/snap_stems.parquet"
            and references_level_zero
        ):
            return
    raise VerificationError(MANIFEST_AUX_FAILURE)


def _catchment_membership(
    catchment_path: Path, batch_size: int
) -> set[int]:
    catchment_ids: set[int] = set()
    previous_key: tuple[int, int] | None = None
    parquet_file = pq.ParquetFile(catchment_path)
    try:
        for batch in parquet_file.iter_batches(
            batch_size=batch_size, columns=["id", "geometry"]
        ):
            ids = batch.column("id").to_pylist()
            wkb_values = batch.column("geometry").to_pylist()
            geometry = gpd.GeoSeries.from_wkb(wkb_values, crs="EPSG:4326")
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message=(
                        "Geometry is in a geographic CRS. Results from "
                        "'centroid' are likely incorrect."
                    ),
                    category=UserWarning,
                )
                hilbert_values = geometry.centroid.hilbert_distance(
                    total_bounds=WORLD_BOUNDS
                )
            for hilbert, raw_id in zip(hilbert_values, ids):
                catchment_id = int(raw_id)
                key = (int(hilbert), catchment_id)
                if previous_key is not None and key < previous_key:
                    raise VerificationError(CATCHMENT_ORDER_FAILURE)
                if catchment_id in catchment_ids:
                    raise VerificationError(SNAP_REFERENCE_FAILURE)
                catchment_ids.add(catchment_id)
                previous_key = key
    finally:
        parquet_file.close()
    return catchment_ids


def _validate_snap_rows(
    snap_path: Path, batch_size: int, catchment_ids: set[int]
) -> None:
    expected_snap_id = 1
    parquet_file = pq.ParquetFile(snap_path)
    try:
        for batch in parquet_file.iter_batches(
            batch_size=batch_size, columns=["id", "unit_id"]
        ):
            ids = batch.column("id").to_pylist()
            unit_ids = batch.column("unit_id").to_pylist()
            for raw_id, raw_unit_id in zip(ids, unit_ids):
                if int(raw_id) != expected_snap_id:
                    raise VerificationError(SNAP_ID_FAILURE)
                expected_snap_id += 1
                unit_id = int(raw_unit_id)
                if unit_id not in catchment_ids:
                    raise VerificationError(SNAP_REFERENCE_FAILURE)
                catchment_ids.remove(unit_id)
    finally:
        parquet_file.close()


def verify_planetary_output(
    dataset_dir: Path, *, batch_size: int = DEFAULT_BATCH_SIZE
) -> None:
    """Verify ordering, snap IDs and references, and manifest declarations."""
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size <= 0
    ):
        raise VerificationError("batch_size must be a positive integer")

    dataset_path = Path(dataset_dir)
    try:
        _validate_manifest(dataset_path / "manifest.json")
        catchment_ids = _catchment_membership(
            dataset_path / "catchments.parquet", batch_size
        )
        _validate_snap_rows(
            dataset_path / "aux" / "snap_stems.parquet",
            batch_size,
            catchment_ids,
        )
    except VerificationError:
        raise
    except Exception as error:
        raise VerificationError(
            "planetary output verification could not be completed"
        ) from error


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "batch size must be a positive integer"
        ) from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError(
            "batch size must be a positive integer"
        )
    return parsed


def _named_positive_integer(option: str) -> Callable[[str], int]:
    def parse(value: str) -> int:
        try:
            parsed = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"{option} must be a positive integer"
            ) from error
        if parsed <= 0:
            raise argparse.ArgumentTypeError(
                f"{option} must be a positive integer"
            )
        return parsed

    return parse


def _schemas() -> tuple[pa.Schema, pa.Schema, pa.Schema, pa.Schema]:
    bbox_type = pa.struct(
        [
            pa.field("xmin", pa.float32(), nullable=False),
            pa.field("ymin", pa.float32(), nullable=False),
            pa.field("xmax", pa.float32(), nullable=False),
            pa.field("ymax", pa.float32(), nullable=False),
        ]
    )
    geo_object = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Polygon", "MultiPolygon"],
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }
    catchment_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field("parent_id", pa.int64(), nullable=True),
            pa.field("area_km2", pa.float32(), nullable=False),
            pa.field("up_area_km2", pa.float32(), nullable=True),
            pa.field("outlet_lon", pa.float64(), nullable=False),
            pa.field("outlet_lat", pa.float64(), nullable=False),
            pa.field("bbox", bbox_type, nullable=False),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata({b"geo": json.dumps(geo_object).encode("utf-8")})
    graph_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("item", pa.int64(), nullable=True)),
                nullable=False,
            ),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )
    graph_read_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("level", pa.int16(), nullable=False),
            pa.field(
                "upstream_ids",
                pa.list_(pa.field("element", pa.int64(), nullable=True)),
                nullable=False,
            ),
            pa.field("bbox_minx", pa.float32(), nullable=False),
            pa.field("bbox_miny", pa.float32(), nullable=False),
            pa.field("bbox_maxx", pa.float32(), nullable=False),
            pa.field("bbox_maxy", pa.float32(), nullable=False),
        ]
    )
    geo_object["columns"]["geometry"]["geometry_types"] = ["LineString"]
    snap_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("unit_id", pa.int64(), nullable=False),
            pa.field("weight", pa.float32(), nullable=False),
            pa.field("stem_role", pa.string(), nullable=True),
            pa.field("bbox", bbox_type, nullable=True),
            pa.field("geometry", pa.binary(), nullable=False),
        ]
    ).with_metadata({b"geo": json.dumps(geo_object).encode("utf-8")})
    return catchment_schema, graph_schema, graph_read_schema, snap_schema


def _selected_basins(basin_count: int) -> list[tuple[str, int]]:
    crosswalk_path = (
        Path(__file__).resolve().parent / "data" / "tdx_header_numbers.json"
    )
    try:
        raw_crosswalk = json.loads(crosswalk_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise RehearsalError("TDX-Hydro header crosswalk could not be read") from error
    if not isinstance(raw_crosswalk, dict):
        raise RehearsalError("TDX-Hydro header crosswalk must be an object")
    entries: list[tuple[str, int]] = []
    seen_headers: set[int] = set()
    for raw_region, raw_header in raw_crosswalk.items():
        region = str(raw_region)
        if not region.isdigit() or isinstance(raw_header, bool):
            raise RehearsalError("TDX-Hydro header crosswalk is invalid")
        try:
            header = int(raw_header)
        except (TypeError, ValueError) as error:
            raise RehearsalError(
                "TDX-Hydro header crosswalk is invalid"
            ) from error
        if header <= 0 or header in seen_headers:
            raise RehearsalError("TDX-Hydro header crosswalk is invalid")
        seen_headers.add(header)
        entries.append((region, header))
    entries.sort(key=lambda entry: entry[0])
    if len(entries) != 62 or not 1 <= basin_count <= 62:
        raise RehearsalError("basin count must be between 1 and 62")
    return entries[:basin_count]


def _distribute_counts(
    total_units: int,
    basin_count: int,
    distribution: str,
    seed: int,
) -> list[int]:
    if total_units < basin_count:
        raise RehearsalError("total unit count must be at least basin count")
    if distribution == "even":
        quotient, remainder = divmod(total_units, basin_count)
        counts = [
            quotient + (1 if index < remainder else 0)
            for index in range(basin_count)
        ]
    else:
        rng = random.Random(seed)
        weights = [0.5 + 1.5 * rng.random() for _ in range(basin_count)]
        remaining = total_units - basin_count
        allocations = [remaining * weight / sum(weights) for weight in weights]
        floors = [math.floor(allocation) for allocation in allocations]
        counts = [1 + floor for floor in floors]
        leftovers = remaining - sum(floors)
        order = sorted(
            range(basin_count),
            key=lambda index: (-(allocations[index] - floors[index]), index),
        )
        for index in order[:leftovers]:
            counts[index] += 1
    if any(count < 1 for count in counts):
        raise RehearsalError("generated per-basin count must be positive")
    if any(count >= GLOBAL_LINKNO_STRIDE for count in counts):
        raise RehearsalError(
            "generated per-basin count must be below 10000000"
        )
    return counts


def _rotate(
    side: int, x: int, y: int, rx: int, ry: int
) -> tuple[int, int]:
    if ry == 0:
        if rx == 1:
            x = side - 1 - x
            y = side - 1 - y
        x, y = y, x
    return x, y


def _hilbert_xy(distance: int) -> tuple[int, int]:
    x = y = 0
    remainder = distance
    side = 1
    while side < 65_536:
        rx = 1 & (remainder // 2)
        ry = 1 & (remainder ^ rx)
        x, y = _rotate(side, x, y, rx, ry)
        x += side * rx
        y += side * ry
        remainder //= 4
        side *= 2
    return x, y


def _scratch_tree_bytes(root: Path) -> int:
    total = 0
    pending = [root]
    while pending:
        path = pending.pop()
        try:
            with os.scandir(path) as entries:
                for entry in entries:
                    try:
                        stat = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    total += stat.st_blocks * 512
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
        except FileNotFoundError:
            continue
    try:
        total += root.stat().st_blocks * 512
    except FileNotFoundError:
        pass
    return total


def _assert_parquet(
    path: Path, schema: pa.Schema, row_count: int
) -> int:
    parquet = pq.ParquetFile(path)
    try:
        if not parquet.schema_arrow.equals(schema, check_metadata=True):
            raise RehearsalError(f"generated schema mismatch: {path}")
        if parquet.metadata.num_rows != row_count:
            raise RehearsalError(f"generated row count mismatch: {path}")
        if parquet.num_row_groups <= 0:
            raise RehearsalError(f"generated row groups missing: {path}")
        return parquet.num_row_groups
    finally:
        parquet.close()


def _generate_inputs(
    output_root: Path,
    *,
    basin_count: int,
    total_units: int,
    distribution: str,
    seed: int,
    generator_batch_size: int,
    after_write: Callable[[], None] | None = None,
) -> dict[str, object]:
    if output_root.exists():
        raise RehearsalError("output destination already exists")
    basins = _selected_basins(basin_count)
    counts = _distribute_counts(
        total_units, basin_count, distribution, seed
    )
    output_root.mkdir(parents=True)
    inputs_root = output_root / "inputs"
    inputs_root.mkdir()
    catchment_schema, graph_schema, graph_read_schema, snap_schema = _schemas()
    virtual_slot_count = max(counts) * basin_count
    half_width = (360 / 65_535) / 8
    half_height = (180 / 65_535) / 8

    for basin_index, ((region, header), count) in enumerate(
        zip(basins, counts)
    ):
        basin_root = inputs_root / region
        auxiliary_root = basin_root / "aux"
        auxiliary_root.mkdir(parents=True)
        catchment_path = basin_root / "catchments.parquet"
        graph_path = basin_root / "graph.parquet"
        snap_path = auxiliary_root / "snap_stems.parquet"
        covering = [math.inf, math.inf, -math.inf, -math.inf]
        previous_distance: int | None = None
        with (
            pq.ParquetWriter(
                catchment_path,
                catchment_schema,
                compression="snappy",
                write_statistics=True,
            ) as catchment_writer,
            pq.ParquetWriter(
                graph_path,
                graph_schema,
                compression="snappy",
                write_statistics=True,
            ) as graph_writer,
            pq.ParquetWriter(
                snap_path,
                snap_schema,
                compression="snappy",
                write_statistics=True,
            ) as snap_writer,
        ):
            for batch_start in range(0, count, generator_batch_size):
                batch_stop = min(batch_start + generator_batch_size, count)
                catchment_rows: list[dict[str, object]] = []
                graph_rows: list[dict[str, object]] = []
                snap_rows: list[dict[str, object]] = []
                polygons: list[Polygon] = []
                authored_distances: list[int] = []
                for ordinal in range(batch_start, batch_stop):
                    slot = ordinal * basin_count + basin_index
                    distance = (
                        (slot + 1)
                        * ((1 << 30) - 1)
                        // (virtual_slot_count + 1)
                    )
                    x, y = _hilbert_xy(distance)
                    lon = -180 + (x + 0.25) * 360 / 65_535
                    lat = -90 + (y + 0.25) * 180 / 65_535
                    polygon = Polygon(
                        [
                            (lon - half_width, lat - half_height),
                            (lon + half_width, lat - half_height),
                            (lon + half_width, lat + half_height),
                            (lon - half_width, lat + half_height),
                            (lon - half_width, lat - half_height),
                        ]
                    )
                    line = LineString(
                        [
                            (lon - half_width, lat - half_height),
                            (lon + half_width, lat + half_height),
                        ]
                    )
                    polygon_bounds = tuple(
                        pa.scalar(value, type=pa.float32()).as_py()
                        for value in polygon.bounds
                    )
                    line_bounds = tuple(
                        pa.scalar(value, type=pa.float32()).as_py()
                        for value in line.bounds
                    )
                    covering = [
                        min(covering[0], polygon_bounds[0]),
                        min(covering[1], polygon_bounds[1]),
                        max(covering[2], polygon_bounds[2]),
                        max(covering[3], polygon_bounds[3]),
                    ]
                    native_linkno = ordinal + 1
                    global_id = header * GLOBAL_LINKNO_STRIDE + native_linkno
                    bbox = {
                        "xmin": polygon_bounds[0],
                        "ymin": polygon_bounds[1],
                        "xmax": polygon_bounds[2],
                        "ymax": polygon_bounds[3],
                    }
                    catchment_rows.append(
                        {
                            "id": global_id,
                            "level": 0,
                            "parent_id": None,
                            "area_km2": 1.0,
                            "up_area_km2": float(native_linkno),
                            "outlet_lon": lon,
                            "outlet_lat": lat,
                            "bbox": bbox,
                            "geometry": polygon.wkb,
                        }
                    )
                    graph_rows.append(
                        {
                            "id": global_id,
                            "level": 0,
                            "upstream_ids": (
                                [] if ordinal == 0 else [global_id - 1]
                            ),
                            "bbox_minx": polygon_bounds[0],
                            "bbox_miny": polygon_bounds[1],
                            "bbox_maxx": polygon_bounds[2],
                            "bbox_maxy": polygon_bounds[3],
                        }
                    )
                    snap_rows.append(
                        {
                            "id": native_linkno,
                            "unit_id": global_id,
                            "weight": float(native_linkno),
                            "stem_role": None,
                            "bbox": {
                                "xmin": line_bounds[0],
                                "ymin": line_bounds[1],
                                "xmax": line_bounds[2],
                                "ymax": line_bounds[3],
                            },
                            "geometry": line.wkb,
                        }
                    )
                    polygons.append(polygon)
                    authored_distances.append(distance)
                geometry = gpd.GeoSeries(polygons, crs="EPSG:4326")
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=(
                            "Geometry is in a geographic CRS. Results from "
                            "'centroid' are likely incorrect."
                        ),
                        category=UserWarning,
                    )
                    actual_distances = geometry.centroid.hilbert_distance(
                        total_bounds=WORLD_BOUNDS
                    )
                for actual, authored in zip(
                    actual_distances, authored_distances
                ):
                    actual_value = int(actual)
                    if actual_value != authored or (
                        previous_distance is not None
                        and actual_value <= previous_distance
                    ):
                        raise RehearsalError(
                            "generated Hilbert order did not round-trip"
                        )
                    previous_distance = actual_value
                catchment_writer.write_table(
                    pa.Table.from_pylist(
                        catchment_rows, schema=catchment_schema
                    )
                )
                graph_writer.write_table(
                    pa.Table.from_pylist(graph_rows, schema=graph_schema)
                )
                snap_writer.write_table(
                    pa.Table.from_pylist(snap_rows, schema=snap_schema)
                )
                if after_write is not None:
                    after_write()

        _assert_parquet(catchment_path, catchment_schema, count)
        _assert_parquet(graph_path, graph_read_schema, count)
        _assert_parquet(snap_path, snap_schema, count)
        manifest = {
            "adapter_version": "0.1.0",
            "auxiliary": [
                {
                    "schema": "hfx.aux.snap.v2",
                    "artifacts": {"snap": "aux/snap_stems.parquet"},
                    "metadata": {
                        "name": "stems",
                        "description": "Native TDX-Hydro LineString reaches for polygon-bearing level 0 drainage units.",
                        "references_levels": [0],
                        "weight_semantics": "Drainage-area weight equals inclusive DSContArea in km2; higher values indicate stronger drainage dominance.",
                    },
                }
            ],
            "bbox": covering,
            "created_at": "2026-07-23T00:00:00+00:00",
            "crs": "EPSG:4326",
            "fabric_name": "tdx_hydro",
            "fabric_version": "synthetic-2026.07",
            "format_version": "0.3.0",
            "has_up_area": True,
            "region": region,
            "topology": "tree",
            "unit_count": count,
        }
        (basin_root / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        )
        if after_write is not None:
            after_write()

    return {
        "regions": [region for region, _ in basins],
        "headers": [header for _, header in basins],
        "basin_unit_counts": counts,
        "basin_snap_counts": counts.copy(),
        "peak_authored_rows_bound": 3 * generator_batch_size,
    }


def _terminate_process_tree(popen: subprocess.Popen[object]) -> None:
    try:
        root = psutil.Process(popen.pid)
        try:
            processes = root.children(recursive=True)
        except PermissionError:
            processes = []
        processes.append(root)
    except psutil.NoSuchProcess:
        processes = []
    for process in reversed(processes):
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(processes, timeout=2)
    for process in alive:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
    try:
        popen.wait(timeout=2)
    except subprocess.TimeoutExpired:
        popen.kill()
        popen.wait()


def _process_tree_rss(pid: int) -> int:
    try:
        root = psutil.Process(pid)
        try:
            children = root.children(recursive=True)
        except PermissionError:
            children = []
        processes = [root, *children]
    except psutil.NoSuchProcess:
        return 0
    total = 0
    for process in processes:
        try:
            total += process.memory_info().rss
        except psutil.NoSuchProcess:
            continue
    return total


def _inspect_interleaving(
    inputs: list[Path],
    assembled_root: Path,
    headers: list[int],
) -> dict[str, object]:
    artifacts = [
        "catchments.parquet",
        "graph.parquet",
        "aux/snap_stems.parquet",
    ]
    input_groups: list[list[int]] = [[], [], []]
    input_batches: list[int] = []
    for input_root in inputs:
        rows = pq.ParquetFile(input_root / artifacts[0]).metadata.num_rows
        input_batches.append(math.ceil(rows / ASSEMBLE_INPUT_BATCH_SIZE))
        for artifact_index, artifact in enumerate(artifacts):
            parquet = pq.ParquetFile(input_root / artifact)
            if parquet.num_row_groups <= 1:
                raise RehearsalError(
                    "interleaving proof requires multiple input row groups"
                )
            input_groups[artifact_index].append(parquet.num_row_groups)
        if input_batches[-1] <= 1:
            raise RehearsalError(
                "interleaving proof requires multiple input batches"
            )

    catchment = pq.ParquetFile(assembled_root / artifacts[0])
    graph = pq.ParquetFile(assembled_root / artifacts[1])
    snap = pq.ParquetFile(assembled_root / artifacts[2])
    if min(
        catchment.num_row_groups,
        graph.num_row_groups,
        snap.num_row_groups,
    ) <= 1:
        raise RehearsalError(
            "interleaving proof requires multiple output row groups"
        )
    known_headers = set(headers)
    runs = {header: 0 for header in headers}
    output_groups = {header: 0 for header in headers}
    transitions = 0
    suffix_transitions = 0
    previous_header: int | None = None
    suffix_previous: int | None = None
    suffix_headers: set[int] = set()
    catchment_multi_groups = 0
    snap_multi_groups = 0

    for row_group in range(catchment.num_row_groups):
        catchment_ids = [
            int(value)
            for value in catchment.read_row_group(
                row_group, columns=["id"]
            ).column("id").to_pylist()
        ]
        graph_ids = [
            int(value)
            for value in graph.read_row_group(
                row_group, columns=["id"]
            ).column("id").to_pylist()
        ]
        if catchment_ids != graph_ids:
            raise RehearsalError(
                "graph row groups do not align with catchments"
            )
        group_headers = {
            identifier // GLOBAL_LINKNO_STRIDE
            for identifier in catchment_ids
        }
        if len(group_headers) < 2:
            raise RehearsalError(
                "catchment output row group lacks multiple basins"
            )
        catchment_multi_groups += 1
        for header in group_headers:
            if header not in known_headers:
                raise RehearsalError("assembled ID has unknown basin header")
            output_groups[header] += 1
        for identifier in catchment_ids:
            header = identifier // GLOBAL_LINKNO_STRIDE
            ordinal = identifier % GLOBAL_LINKNO_STRIDE
            if header not in known_headers:
                raise RehearsalError("assembled ID has unknown basin header")
            if header != previous_header:
                runs[header] += 1
                if previous_header is not None:
                    transitions += 1
            previous_header = header
            if ordinal > ASSEMBLE_INPUT_BATCH_SIZE:
                suffix_headers.add(header)
                if suffix_previous is not None and header != suffix_previous:
                    suffix_transitions += 1
                suffix_previous = header

    snap_groups = {header: 0 for header in headers}
    for row_group in range(snap.num_row_groups):
        unit_ids = [
            int(value)
            for value in snap.read_row_group(
                row_group, columns=["unit_id"]
            ).column("unit_id").to_pylist()
        ]
        group_headers = {
            identifier // GLOBAL_LINKNO_STRIDE for identifier in unit_ids
        }
        if len(group_headers) < 2:
            raise RehearsalError("snap output row group lacks multiple basins")
        snap_multi_groups += 1
        for header in group_headers:
            if header not in known_headers:
                raise RehearsalError("assembled ID has unknown basin header")
            snap_groups[header] += 1

    if (
        any(run < 2 for run in runs.values())
        or suffix_headers != known_headers
        or suffix_transitions < 1
        or any(groups < 2 for groups in output_groups.values())
        or any(groups < 2 for groups in snap_groups.values())
    ):
        raise RehearsalError("assembled output is not genuinely interleaved")
    return {
        "input_batches_per_basin": input_batches,
        "catchment_input_row_groups_per_basin": input_groups[0],
        "graph_input_row_groups_per_basin": input_groups[1],
        "snap_input_row_groups_per_basin": input_groups[2],
        "output_catchment_row_groups": catchment.num_row_groups,
        "output_graph_row_groups": graph.num_row_groups,
        "output_snap_row_groups": snap.num_row_groups,
        "basin_origin_transitions": transitions,
        "post_first_input_batch_origin_transitions": suffix_transitions,
        "catchment_row_groups_with_multiple_basins": catchment_multi_groups,
        "snap_row_groups_with_multiple_basins": snap_multi_groups,
        "passed": True,
    }


def _rehearse(arguments: argparse.Namespace) -> dict[str, object]:
    scratch_root = arguments.scratch_root.resolve()
    if scratch_root.exists():
        raise RehearsalError("scratch root already exists")
    inputs_root = scratch_root / "inputs"
    assembled_root = scratch_root / "assembled"
    if len({scratch_root, inputs_root, assembled_root}) != 3:
        raise RehearsalError("scratch, input, and output roots must differ")
    scratch_peak = 0

    def sample_generation_scratch() -> None:
        nonlocal scratch_peak
        scratch_peak = max(scratch_peak, _scratch_tree_bytes(scratch_root))
        if scratch_peak > arguments.scratch_ceiling_bytes:
            raise RehearsalError(SCRATCH_CEILING_FAILURE)

    generation = _generate_inputs(
        scratch_root,
        basin_count=arguments.basin_count,
        total_units=arguments.total_units,
        distribution=arguments.distribution,
        seed=arguments.seed,
        generator_batch_size=arguments.generator_batch_size,
        after_write=sample_generation_scratch,
    )
    input_roots = [
        inputs_root / region for region in generation["regions"]
    ]
    argv = [
        sys.executable,
        str(Path(__file__).with_name("build_adapter.py").resolve()),
        "assemble",
    ]
    for input_root in input_roots:
        argv.extend(["--input", str(input_root.resolve())])
    argv.extend(["--out", str(assembled_root.resolve())])
    stdout_path = scratch_root / "assemble.stdout"
    stderr_path = scratch_root / "assemble.stderr"
    peak_rss = 0
    referential_peak_rss = 0
    pre_staging_samples = 0
    start = time.perf_counter()
    with (
        stdout_path.open("wb") as child_stdout,
        stderr_path.open("wb") as child_stderr,
    ):
        popen = subprocess.Popen(
            argv, stdout=child_stdout, stderr=child_stderr
        )
        while True:
            rss = _process_tree_rss(popen.pid)
            peak_rss = max(peak_rss, rss)
            staging_exists = any(
                scratch_root.glob(f".{assembled_root.name}.tmp-*")
            )
            if not staging_exists:
                pre_staging_samples += 1
                referential_peak_rss = max(referential_peak_rss, rss)
            if peak_rss > arguments.rss_ceiling_bytes:
                _terminate_process_tree(popen)
                raise RehearsalError(RSS_CEILING_FAILURE)
            scratch_peak = max(
                scratch_peak, _scratch_tree_bytes(scratch_root)
            )
            if scratch_peak > arguments.scratch_ceiling_bytes:
                _terminate_process_tree(popen)
                raise RehearsalError(SCRATCH_CEILING_FAILURE)
            if popen.poll() is not None:
                break
            time.sleep(arguments.sample_interval_ms / 1000)
        returncode = popen.wait()
    wall_time = time.perf_counter() - start
    final_scratch = _scratch_tree_bytes(scratch_root)
    scratch_peak = max(scratch_peak, final_scratch)
    if scratch_peak > arguments.scratch_ceiling_bytes:
        raise RehearsalError(SCRATCH_CEILING_FAILURE)
    if pre_staging_samples < 1:
        raise RehearsalError("referential proof phase was not sampled")
    if returncode != 0:
        try:
            diagnostic = stderr_path.read_bytes()[-16_384:].decode(
                errors="replace"
            )
        except OSError:
            diagnostic = ""
        raise RehearsalError(
            f"assemble failed with return code {returncode}: {diagnostic}"
        )
    interleaving = _inspect_interleaving(
        input_roots, assembled_root, generation["headers"]
    )
    with _filter_arrow_sandbox_diagnostics():
        verify_planetary_output(
            assembled_root, batch_size=arguments.verify_batch_size
        )
    generation_report = {
        key: value for key, value in generation.items() if key != "headers"
    }
    return {
        "schema_version": 1,
        "status": "passed",
        "configuration": {
            "seed": arguments.seed,
            "basin_count": arguments.basin_count,
            "total_units": arguments.total_units,
            "distribution": arguments.distribution,
            "generator_batch_size": arguments.generator_batch_size,
            "verify_batch_size": arguments.verify_batch_size,
            "sample_interval_ms": arguments.sample_interval_ms,
            "assemble_input_batch_size": ASSEMBLE_INPUT_BATCH_SIZE,
            "assemble_row_group_min": ASSEMBLE_ROW_GROUP_MIN,
            "assemble_row_group_max": ASSEMBLE_ROW_GROUP_MAX,
            "rss_ceiling_bytes": arguments.rss_ceiling_bytes,
            "scratch_ceiling_bytes": arguments.scratch_ceiling_bytes,
        },
        "generation": generation_report,
        "assembly": {
            "argv": argv,
            "returncode": returncode,
            "wall_time_seconds": wall_time,
            "process_tree_peak_rss_bytes": peak_rss,
            "referential_proof_peak_rss_bytes": referential_peak_rss,
            "scratch_tree_peak_bytes": scratch_peak,
            "final_scratch_tree_bytes": final_scratch,
        },
        "interleaving": interleaving,
        "verification": {
            "batch_size": arguments.verify_batch_size,
            "passed": True,
        },
    }


def _add_generation_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--basin-count",
        type=_named_positive_integer("basin count"),
        default=DEFAULT_BASIN_COUNT,
    )
    parser.add_argument(
        "--total-units",
        type=_named_positive_integer("total units"),
        default=DEFAULT_TOTAL_UNITS,
    )
    parser.add_argument(
        "--distribution",
        choices=["even", "seeded-skew"],
        default="even",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--generator-batch-size",
        type=_named_positive_integer("generator batch size"),
        default=DEFAULT_GENERATOR_BATCH_SIZE,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("dataset_directory", type=Path)
    verify_parser.add_argument(
        "--batch-size", type=_positive_integer, default=DEFAULT_BATCH_SIZE
    )
    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("--out", type=Path, required=True)
    _add_generation_options(generate_parser)
    rehearse_parser = subparsers.add_parser("rehearse")
    rehearse_parser.add_argument("--scratch-root", type=Path, required=True)
    _add_generation_options(rehearse_parser)
    rehearse_parser.add_argument(
        "--verify-batch-size",
        type=_named_positive_integer("verify batch size"),
        default=DEFAULT_BATCH_SIZE,
    )
    rehearse_parser.add_argument(
        "--sample-interval-ms",
        type=_named_positive_integer("sample interval"),
        default=DEFAULT_SAMPLE_INTERVAL_MS,
    )
    rehearse_parser.add_argument(
        "--rss-ceiling-bytes",
        type=_named_positive_integer("RSS ceiling"),
        default=DEFAULT_RSS_CEILING_BYTES,
    )
    rehearse_parser.add_argument(
        "--scratch-ceiling-bytes",
        type=_named_positive_integer("scratch ceiling"),
        default=DEFAULT_SCRATCH_CEILING_BYTES,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the planetary-output verifier command."""
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "verify":
            with _filter_arrow_sandbox_diagnostics():
                verify_planetary_output(
                    arguments.dataset_directory,
                    batch_size=arguments.batch_size,
                )
        elif arguments.command == "generate":
            with _filter_arrow_sandbox_diagnostics():
                generation = _generate_inputs(
                    arguments.out.resolve(),
                    basin_count=arguments.basin_count,
                    total_units=arguments.total_units,
                    distribution=arguments.distribution,
                    seed=arguments.seed,
                    generator_batch_size=arguments.generator_batch_size,
                )
            generation.pop("headers")
            print(json.dumps(generation, sort_keys=True))
        else:
            with _filter_arrow_sandbox_diagnostics():
                report = _rehearse(arguments)
            print(json.dumps(report, sort_keys=True))
    except (VerificationError, RehearsalError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
