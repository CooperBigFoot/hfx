"""Verify assembled TDX-Hydro output with bounded batch-local memory.

The sole dataset-sized retained structure is a set of catchment IDs. At roughly
16 million distinct rows, that set is expected to occupy about 0.8 to 1.1 GiB
on CPython, depending on the interpreter build and set capacity. It shrinks as
snap references are consumed. Arrow buffers, WKB lists, centroids, and Hilbert
values use an additional O(batch_size) memory.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import warnings
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import geopandas as gpd
import pyarrow.parquet as pq

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


class VerificationError(Exception):
    """Report a planetary-output verification failure."""


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("dataset_directory", type=Path)
    verify_parser.add_argument(
        "--batch-size", type=_positive_integer, default=DEFAULT_BATCH_SIZE
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
    except VerificationError as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
