#!/usr/bin/env python3
"""invariance : (ReferenceArtifact, CandidateArtifact, DerivedOutlets) -> InvarianceEvidence.

Exhaustively verify an outlet-only compilation in bounded Arrow row batches.
DerivedOutlets and provenance must come from authenticated native derivation.
This function proves equality to those inputs, not their historical authenticity.
The regeneration CLI owns source authentication and independent re-derivation.
No historical change count or distance threshold participates in acceptance.
"""
from __future__ import annotations

import hashlib
from itertools import zip_longest
import json
import math
from pathlib import Path
import re

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import Geod

OUTLET_DTYPE = np.dtype([
    ("id", "<i8"), ("downstream_id", "<i8"),
    ("outlet_lon", "<f8"), ("outlet_lat", "<f8"),
])
_OFFSET = 10_000_000


class InvarianceRefusal(ValueError):
    """The candidate, derived index, or evidence destination violates invariance."""


def _require(condition, message):
    if not condition:
        raise InvarianceRefusal(message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _files(root: Path) -> dict[str, Path]:
    _require(root.is_dir(), f"dataset directory missing: {root}")
    paths = {}
    for path in root.rglob("*"):
        _require(not path.is_symlink(), f"symlink artifact refused: {path}")
        if path.is_file():
            paths[path.relative_to(root).as_posix()] = path
        else:
            _require(path.is_dir(), f"non-regular artifact refused: {path}")
    return paths


def _positions(index, ids, context):
    values = np.asarray(ids, dtype=np.int64)
    positions = np.searchsorted(index["id"], values)
    _require(np.all(positions < len(index)), f"{context}: unknown drainage-unit ID")
    _require(np.array_equal(index["id"][positions], values), f"{context}: unknown drainage-unit ID")
    return positions


def _exact_array_equal(left, right):
    # Arrow numerical equality alone treats +0 and -0 as equal and NaNs as unequal.
    # Floating bit equality preserves both unchanged NaNs and signed zero.
    if not left.is_valid().equals(right.is_valid()):
        return False
    dtype = left.type
    if pa.types.is_floating(dtype):
        valid = left.is_valid().to_numpy(zero_copy_only=False)
        a = left.to_numpy(zero_copy_only=False)
        b = right.to_numpy(zero_copy_only=False)
        bits = np.dtype(f"u{dtype.bit_width // 8}")
        return np.array_equal(a.view(bits)[valid], b.view(bits)[valid])
    if pa.types.is_struct(dtype):
        return all(_exact_array_equal(left.field(i), right.field(i)) for i in range(dtype.num_fields))
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        a = left.offsets.to_numpy(zero_copy_only=False)
        b = right.offsets.to_numpy(zero_copy_only=False)
        return np.array_equal(a - a[0], b - b[0]) and _exact_array_equal(
            left.values.slice(int(a[0]), int(a[-1] - a[0])),
            right.values.slice(int(b[0]), int(b[-1] - b[0])))
    return left.equals(right)


def _compare_batch(left, right, row_offset):
    _require(left.num_rows == right.num_rows, f"catchments row {row_offset}: batch row count differs")
    for name in left.schema.names:
        if name not in ("outlet_lon", "outlet_lat"):
            _require(_exact_array_equal(left.column(name), right.column(name)),
                     f"catchments row batch {row_offset}: non-outlet column {name} differs")


def _basin_records(index, provenance, batch_size):
    records = provenance.get("basins")
    _require(isinstance(records, list) and records, "native derivation basin provenance missing")
    by_header = {}
    basin_ids = set()
    for record in records:
        header = record.get("header_number")
        basin_id = record.get("processing_basin_id")
        _require(type(header) is int and 0 < header < 1000, "invalid provenance header_number")
        _require(isinstance(basin_id, str) and basin_id not in basin_ids,
                 "duplicate or invalid processing_basin_id")
        _require(header not in by_header, "duplicate provenance header_number")
        for key in ("orientation_digest", "source_sha256", "native_evidence_sha256"):
            _require(isinstance(record.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", record[key]),
                     f"processing basin {basin_id}: missing or invalid {key}")
        start = int(np.searchsorted(index["id"], header * _OFFSET))
        end = int(np.searchsorted(index["id"], (header + 1) * _OFFSET))
        _require(end > start, f"processing basin {basin_id}: no index units")
        digest = hashlib.sha256()
        for name in OUTLET_DTYPE.names:
            for offset in range(start, end, batch_size):
                values = index[name][offset:min(offset + batch_size, end)].copy()
                if name in ("id", "downstream_id"):
                    present = values != -1
                    _require(np.all(values[present] // _OFFSET == header),
                             f"processing basin {basin_id}: cross-basin or invalid {name}")
                    values[present] -= header * _OFFSET
                    _require(np.all(values[present] >= 0), f"processing basin {basin_id}: invalid native ID")
                digest.update(values.tobytes())
        _require(digest.hexdigest() == record["orientation_digest"],
                 f"processing basin {basin_id}: orientation digest differs")
        by_header[header] = dict(record, unit_count=end - start, changed_unit_count=0, max_shift_deg=0.0, max_shift_m=0.0)
        basin_ids.add(basin_id)
    _require(sum(r["unit_count"] for r in by_header.values()) == len(index),
             "outlet index contains units without basin provenance")
    return by_header


def _stamp(path):
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _resolution_index(index, records, evidence, batch_size):
    """Project authenticated basin resolution sidecars to a disk-backed unit index."""
    result = np.lib.format.open_memmap(evidence / "resolution.npy", mode="w+", dtype="uint8", shape=(len(index),))
    dtype = np.dtype([("native_id", "<i8"), ("resolution", "i1")])
    for header, record in records.items():
        names = record.get("resolution_code_names")
        _require(isinstance(names, list) and 0 < len(names) <= 128
                 and all(isinstance(name, str) and name for name in names), "native resolution code names missing")
        native_path = record.get("unit_resolution_path")
        _require(isinstance(native_path, str), "unit resolution path missing")
        native_path = Path(native_path)
        stamp = _stamp(native_path)
        _require(_sha256(native_path) == record.get("unit_resolution_sha256"), "unit resolution digest differs")
        rows = np.load(native_path, mmap_mode="r", allow_pickle=False)
        _require(rows.ndim == 1 and rows.dtype == dtype, "unit resolution array schema differs")
        start = int(np.searchsorted(index["id"], header * _OFFSET))
        end = int(np.searchsorted(index["id"], (header + 1) * _OFFSET))
        _require(len(rows) == end - start, "unit resolution count differs")
        for offset in range(start, end, batch_size):
            stop = min(offset + batch_size, end)
            unit_ids = index["id"][offset:stop] - header * _OFFSET
            native = rows[offset - start:stop - start]
            _require(np.array_equal(native["native_id"], unit_ids), "unit resolution IDs differ")
            values = native["resolution"]
            _require(np.all((values >= 0) & (values < len(names))), "unit resolution code out of range")
            result[offset:stop] = values
        _require(_stamp(native_path) == stamp, "unit resolution changed during verification")
        del rows
    result.flush()
    return result

def _verify_graph(path, index, batch_size):
    seen = np.zeros(len(index), dtype=np.bool_)
    connected = np.zeros(len(index), dtype=np.bool_)
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size, columns=["id", "upstream_ids"], use_threads=False):
        ids_column, upstream_column = batch.columns
        _require(ids_column.type == pa.int64()
                 and (pa.types.is_list(upstream_column.type) or pa.types.is_large_list(upstream_column.type))
                 and upstream_column.type.value_type == pa.int64(), "graph: IDs must be int64 lists")
        _require(ids_column.null_count == upstream_column.null_count == 0, "graph: null ID or upstream list")
        ids = ids_column.to_numpy()
        positions = _positions(index, ids, "graph")
        _require(len(np.unique(positions)) == len(positions) and not np.any(seen[positions]), "graph: duplicate ID")
        seen[positions] = True
        offsets = upstream_column.offsets.to_numpy()
        upstream = upstream_column.values.slice(int(offsets[0]), int(offsets[-1] - offsets[0]))
        _require(upstream.null_count == 0, "graph: null upstream ID")
        upstream_positions = _positions(index, upstream.to_numpy(), "graph upstream_ids")
        _require(len(np.unique(upstream_positions)) == len(upstream_positions)
                 and not np.any(connected[upstream_positions]), "graph: duplicate downstream edge")
        targets = np.repeat(ids, np.diff(offsets))
        _require(np.array_equal(index["downstream_id"][upstream_positions], targets),
                 "graph: contracted downstream differs")
        connected[upstream_positions] = True
    _require(np.all(seen), "graph: missing drainage units")
    for offset in range(0, len(index), batch_size):
        _require(np.array_equal(connected[offset:offset + batch_size],
                               index["downstream_id"][offset:offset + batch_size] != -1),
                 "graph: missing downstream edge or incorrect root")


def verify_outlet_invariance(reference: Path, candidate: Path, outlet_index: Path,
                             provenance: dict, evidence: Path, batch_size: int = 1024) -> dict:
    """Verify every artifact and persist actual changes plus a final success summary.

    Raises InvarianceRefusal on any mismatch or unsafe evidence destination.
    I/O and Parquet errors propagate. A failed run never writes summary.json.
    Native provenance must be authenticated by the caller. Basin coverage here
    must equal provenance exactly; the global caller enforces the pinned 62 keys.
    Geometry memory is bounded by two Arrow batches, decoder buffers, and the
    largest individual geometry. Compact index/coverage state scales with IDs.
    """
    _require(type(batch_size) is int and batch_size > 0, "batch_size must be positive")
    reference, candidate, outlet_index, evidence = map(Path, (reference, candidate, outlet_index, evidence))
    for path in (reference, candidate, outlet_index, evidence):
        _require(not any(p.is_symlink() for p in (path, *path.parents)), f"symlink path refused: {path}")
    reference, candidate, outlet_index, evidence = (p.resolve() for p in (reference, candidate, outlet_index, evidence))
    _require(reference != candidate and not reference.is_relative_to(candidate)
             and not candidate.is_relative_to(reference), "reference and candidate alias or overlap")
    _require(not evidence.exists() and not evidence.is_relative_to(reference)
             and not evidence.is_relative_to(candidate), "evidence must be a new external directory")
    reference_files, candidate_files = _files(reference), _files(candidate)
    _require(reference_files.keys() == candidate_files.keys(), "artifact file inventory differs")
    _require({"catchments.parquet", "graph.parquet", "manifest.json"} <= reference_files.keys(), "mandatory artifact missing")
    stamps = {path: _stamp(path) for path in (*reference_files.values(), *candidate_files.values(), outlet_index)}
    reference_inodes = {stamps[path][:2] for path in reference_files.values()}
    _require(not any(stamps[path][:2] in reference_inodes for path in candidate_files.values()),
             "candidate artifact hardlink aliases reference content")
    unchanged = {}
    for name, path in reference_files.items():
        if name != "catchments.parquet":
            digest = _sha256(path)
            if name == "README.md" and "replacement_readme" in provenance:
                replacement = provenance["replacement_readme"]
                _require(isinstance(replacement, dict), "replacement README pin missing")
                _require(isinstance(replacement.get("path"), str)
                         and Path(replacement["path"]).is_absolute()
                         and type(replacement.get("bytes")) is int
                         and replacement["bytes"] > 0
                         and isinstance(replacement.get("sha256"), str)
                         and re.fullmatch(r"[0-9a-f]{64}", replacement["sha256"]), "replacement README pin invalid")
                replacement_path = Path(replacement["path"])
                stamps[replacement_path] = _stamp(replacement_path)
                _require(replacement_path.stat().st_size == replacement["bytes"]
                         and _sha256(replacement_path) == replacement["sha256"], "replacement README source differs from pin")
                _require(candidate_files[name].stat().st_size == replacement["bytes"]
                         and _sha256(candidate_files[name]) == replacement["sha256"], "candidate README differs from replacement pin")
            else:
                _require(digest == _sha256(candidate_files[name]), f"non-outlet artifact differs: {name}")
                unchanged[name] = digest
    _require("replacement_readme" not in provenance or "README.md" in reference_files,
             "replacement README declared but README.md artifact missing")
    index = np.load(outlet_index, mmap_mode="r", allow_pickle=False)
    _require(index.ndim == 1 and index.dtype == OUTLET_DTYPE and len(index) > 0, "invalid outlet index schema or empty index")
    previous = None
    for offset in range(0, len(index), batch_size):
        rows = index[offset:offset + batch_size]
        ids = rows["id"]
        _require(np.all(ids[1:] > ids[:-1]) and (previous is None or int(ids[0]) > previous),
                 "outlet index IDs must be sorted and unique")
        previous = int(ids[-1])
        for name, limit in (("outlet_lon", 180), ("outlet_lat", 90)):
            _require(np.all(np.isfinite(rows[name])) and np.all(np.abs(rows[name]) <= limit),
                     f"outlet index: invalid {name}")
    basin_records = _basin_records(index, provenance, batch_size)
    left, right = pq.ParquetFile(reference_files["catchments.parquet"]), pq.ParquetFile(candidate_files["catchments.parquet"])
    _require(left.schema_arrow.equals(right.schema_arrow, check_metadata=True), "catchments schema/field/metadata differs")
    _require(left.schema.equals(right.schema), "catchments physical schema differs")
    _require(left.metadata.metadata == right.metadata.metadata, "catchments file metadata differs")
    _require(len(set(left.schema_arrow.names)) == len(left.schema_arrow.names), "catchments duplicate column names")
    _require({"id", "outlet_lon", "outlet_lat"} <= set(left.schema_arrow.names), "catchments required column missing")
    _require(left.metadata.num_rows == right.metadata.num_rows == len(index), "catchments/index row count differs")
    manifest = json.loads(reference_files["manifest.json"].read_text())
    _require(manifest.get("unit_count") == len(index), "manifest unit_count differs")
    _verify_graph(reference_files["graph.parquet"], index, batch_size)
    evidence.mkdir(parents=True)
    resolution = _resolution_index(index, basin_records, evidence, batch_size)
    seen = np.zeros(len(index), dtype=np.bool_)
    row_offset = 0
    geod = Geod(ellps="WGS84")
    with (evidence / "changes.jsonl").open("x") as changes:
        batches = zip_longest(left.iter_batches(batch_size=batch_size, use_threads=False),
                             right.iter_batches(batch_size=batch_size, use_threads=False))
        for old, new in batches:
            _require(old is not None and new is not None, "catchments batch coverage differs")
            _compare_batch(old, new, row_offset)
            _require(new.column("id").null_count == 0, "catchments: null ID")
            positions = _positions(index, new.column("id").to_numpy(), "catchments")
            _require(len(np.unique(positions)) == len(positions) and not np.any(seen[positions]), "catchments: duplicate ID")
            seen[positions] = True
            selected = index[positions]
            outlets = {}
            for name in ("outlet_lon", "outlet_lat"):
                _require(old.column(name).null_count == new.column(name).null_count == 0,
                         f"catchments: null {name}")
                a, b = old.column(name).to_numpy(), new.column(name).to_numpy()
                _require(np.all(np.isfinite(a)), f"reference: nonfinite {name}")
                _require(b.dtype == np.dtype("float64") and np.array_equal(b.view("uint64"), selected[name].copy().view("uint64")),
                         f"catchments row batch {row_offset}: {name} differs from native derivation")
                outlets[name] = (a, b)
            changed = (outlets["outlet_lon"][0].view("uint64") != outlets["outlet_lon"][1].view("uint64")) | (outlets["outlet_lat"][0].view("uint64") != outlets["outlet_lat"][1].view("uint64"))
            changed_positions = np.flatnonzero(changed)
            _, _, shifts_m = geod.inv(
                outlets["outlet_lon"][0][changed_positions], outlets["outlet_lat"][0][changed_positions],
                outlets["outlet_lon"][1][changed_positions], outlets["outlet_lat"][1][changed_positions])
            _require(np.all(np.isfinite(shifts_m)), "outlet geodesic shift is not finite")
            for i, shift_m in zip(changed_positions, shifts_m):
                unit_id = int(selected["id"][i])
                record = basin_records[unit_id // _OFFSET]
                before = [float(outlets[n][0][i]) for n in ("outlet_lon", "outlet_lat")]
                after = [float(outlets[n][1][i]) for n in ("outlet_lon", "outlet_lat")]
                shift = math.hypot(after[0] - before[0], after[1] - before[1])
                record["changed_unit_count"] += 1
                record["max_shift_deg"] = max(record["max_shift_deg"], shift)
                record["max_shift_m"] = max(record["max_shift_m"], float(shift_m))
                changes.write(json.dumps({"global_id": unit_id, "native_id": unit_id % _OFFSET,
                    "processing_basin_id": record["processing_basin_id"], "old_outlet": before,
                    "new_outlet": after, "shift_deg": shift, "shift_m": float(shift_m),
                    "resolution": record["resolution_code_names"][int(resolution[positions[i]])],
                    "native_evidence_sha256": record["native_evidence_sha256"]}, allow_nan=False) + "\n")
            row_offset += old.num_rows
            # Drop views before requesting the next Arrow batch (large WKB lifetime).
            del old, new, selected, outlets, a, b
    _require(row_offset == len(index) and np.all(seen), "catchments: incomplete outlet coverage")
    summary = {"status": "passed", "verification": "outlet-only-artifact-invariance",
        "unit_count": row_offset, "changed_unit_count": sum(r["changed_unit_count"] for r in basin_records.values()),
        "max_shift_deg": max(r["max_shift_deg"] for r in basin_records.values()),
        "max_shift_m": max(r["max_shift_m"] for r in basin_records.values()),
        "distance_model": "WGS84 ellipsoid geodesic",
        "basins": sorted(basin_records.values(), key=lambda r: r["processing_basin_id"]),
        "unchanged_artifact_sha256": unchanged, "outlet_index_sha256": _sha256(outlet_index),
        "changes_sha256": _sha256(evidence / "changes.jsonl"),
        "resolution_index_sha256": _sha256(evidence / "resolution.npy"),
        "reference_catchments_sha256": _sha256(reference_files["catchments.parquet"]),
        "candidate_catchments_sha256": _sha256(candidate_files["catchments.parquet"]),
        "batch_size": batch_size}
    if "replacement_readme" in provenance:
        summary["replacement_readme"] = provenance["replacement_readme"]
        summary["reference_readme_sha256"] = _sha256(reference_files["README.md"])
    _require(_files(reference).keys() == reference_files.keys()
             and _files(candidate).keys() == candidate_files.keys(), "artifact inventory changed during verification")
    for path, stamp in stamps.items():
        _require(_stamp(path) == stamp, f"artifact changed during verification: {path}")
    with (evidence / ".summary.json.tmp").open("x") as stream:
        json.dump(summary, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    (evidence / ".summary.json.tmp").replace(evidence / "summary.json")
    return summary
