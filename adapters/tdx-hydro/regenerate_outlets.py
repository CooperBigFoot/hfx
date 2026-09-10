"""regenerate : (AuthenticatedReference, NativeStreamSources) -> CorrectedOutlets.

Offline outlet-only compilation. Native topology is shared with the full adapter;
reference drainage-unit identities supply the historical polygon-bearing join.
"""
from __future__ import annotations

import argparse
import ctypes
from dataclasses import asdict
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import shutil
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import build_adapter as adapter

OUTLET_DTYPE = np.dtype([
    ("id", "<i8"), ("downstream_id", "<i8"),
    ("outlet_lon", "<f8"), ("outlet_lat", "<f8"),
])


def _plain_path(path: Path) -> Path:
    path = Path(os.path.abspath(path.expanduser()))
    for component in (path, *path.parents):
        if component.is_symlink():
            raise ValueError(f"symlink path refused: {component}")
    return path


def _stamp(path: Path) -> tuple[int, int, int, int, int]:
    stat = _plain_path(path).stat()
    if not path.is_file():
        raise ValueError(f"not a regular file: {path}")
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def sha256_file(path: Path) -> str:
    before = _stamp(path)
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(8 * 1024 * 1024):
            digest.update(block)
    if _stamp(path) != before:
        raise ValueError(f"file changed while hashing: {path}")
    return digest.hexdigest()


def _json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    if not isinstance(value, dict):
        raise ValueError(f"JSON object required: {path}")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("expected lowercase complete-content SHA-256")
    return value


def _authenticate(path: Path, record: dict) -> None:
    if type(record.get("bytes")) is not int or record["bytes"] <= 0:
        raise ValueError(f"invalid byte count: {path}")
    if _stamp(path)[2] != record["bytes"] or sha256_file(path) != _digest(record.get("sha256")):
        raise ValueError(f"source/reference identity mismatch: {path}")


def _relative(value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError("reference path must be a nonempty relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value or value == ".":
        raise ValueError(f"unsafe reference path: {value}")
    return path


def _files(root: Path) -> set[str]:
    result = set()
    for path in root.rglob("*"):
        _plain_path(path)
        if path.is_file():
            result.add(path.relative_to(root).as_posix())
        elif not path.is_dir():
            raise ValueError(f"nonregular reference entry: {path}")
    return result


def authenticate_inventory(reference: Path, inventory_path: Path, inventory_sha256: str) -> tuple[dict, dict[str, int]]:
    """Authenticate explicitly supplied historical pins and all reference bytes.

    Raises ValueError on incomplete coverage, identity drift, or unsafe paths.
    The inventory digest is an out-of-band reviewed authority, not a new-source
    checksum or a claim that source identity can be inferred from delivered HFX.
    """
    stamp = _stamp(inventory_path)
    inventory_bytes = inventory_path.read_bytes()
    if _stamp(inventory_path) != stamp or hashlib.sha256(inventory_bytes).hexdigest() != _digest(inventory_sha256):
        raise ValueError("inventory SHA-256 mismatch or mutation")
    inventory = json.loads(inventory_bytes, object_pairs_hook=_unique_object)
    if not isinstance(inventory, dict):
        raise ValueError("inventory must be a JSON object")
    if inventory.get("schema_version") != 1:
        raise ValueError("unsupported inventory schema_version")
    if not isinstance(inventory.get("build_identity"), str) or not inventory["build_identity"].strip():
        raise ValueError("inventory requires historical build_identity")
    if sha256_file(adapter.CROSSWALK_PATH) != _digest(inventory.get("crosswalk_sha256")):
        raise ValueError("crosswalk SHA-256 mismatch")
    crosswalk = adapter.load_header_crosswalk()
    if len(crosswalk) != 62:
        raise ValueError("canonical crosswalk must cover all 62 processing basins")
    sources = inventory.get("sources")
    if not isinstance(sources, list) or any(not isinstance(row, dict) for row in sources):
        raise ValueError("sources must be explicit records")
    basins = [row.get("processing_basin_id") for row in sources]
    if len(basins) != 62 or len(set(basins)) != 62 or set(basins) != set(crosswalk):
        raise ValueError("source inventory must cover exactly all 62 canonical processing basins without duplicates")
    for record in sources:
        if "header_number" in record and record["header_number"] != crosswalk[record["processing_basin_id"]]:
            raise ValueError("source header_number disagrees with pinned crosswalk")
        path = Path(record["path"])
        if not path.is_absolute():
            raise ValueError("native source path must be absolute")
        _plain_path(path)
        if path.suffix.lower() != ".gpkg":
            raise ValueError("native source must be a complete GeoPackage")
        _digest(record.get("sha256"))
        if type(record.get("bytes")) is not int or record["bytes"] <= 0:
            raise ValueError("native source bytes must be positive")
    records = inventory.get("reference_files")
    if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
        raise ValueError("reference_files must be complete-content identity records")
    names = [_relative(row.get("path")).as_posix() for row in records]
    if len(names) != len(set(names)) or set(names) != _files(reference):
        raise ValueError("reference inventory has missing, extra, or duplicate payloads")
    required = {"manifest.json", "catchments.parquet", "graph.parquet", "README.md", "NOTICE", "CITATION.txt"}
    if not required <= set(names):
        raise ValueError("reference inventory lacks required HFX core artifacts")
    for record in records:
        _authenticate(reference / record["path"], record)
    manifest = _read_json(reference / "manifest.json")
    if manifest.get("format_version") != "0.3.0" or manifest.get("fabric_name") != "tdx_hydro" or manifest.get("topology") != "tree":
        raise ValueError("reference must be TDX-Hydro HFX 0.3.0 tree")
    auxiliaries = manifest.get("auxiliary", [])
    if auxiliaries != [adapter.SNAP_AUXILIARY_DECLARATION]:
        raise ValueError("reference must declare the unchanged TDX snap v2 artifact")
    for entry in auxiliaries:
        for artifact in entry["artifacts"].values():
            if _relative(artifact).as_posix() not in names:
                raise ValueError("declared auxiliary lacks authenticated payload")
    replacement = inventory.get("replacement_readme")
    if not isinstance(replacement, dict) or not isinstance(replacement.get("path"), str) or not Path(replacement["path"]).is_absolute():
        raise ValueError("inventory requires explicit absolute replacement_readme identity record")
    _authenticate(_plain_path(Path(replacement["path"])), replacement)
    return inventory, crosswalk


def derive_native_outlets(native_ids: np.ndarray, source_path: Path, header_number: int, *, reference_up_area_km2: np.ndarray, native_evidence_path: Path | None = None) -> tuple[adapter._CompactTopology, dict]:
    """Derive historical polygon-bearing outlets from EVERY native reach.

    Raises ValueError for every refusal of the shared native parser/topology.
    This low-level function does not authenticate historical source identity.
    """
    before = _stamp(source_path)
    for suffix in ("-wal", "-journal", "-shm"):
        if Path(str(source_path) + suffix).exists():
            raise ValueError(f"GeoPackage sidecar refused: {source_path}{suffix}")
    columns = adapter._read_streamnet_topology_columns(source_path)
    for suffix in ("-wal", "-journal", "-shm"):
        if Path(str(source_path) + suffix).exists():
            raise ValueError(f"GeoPackage sidecar appeared during native read: {source_path}{suffix}")
    basin, native, downstream = adapter._validated_native_identities(native_ids, columns.native_ids, columns.downstream_native_ids)
    if np.any(native >= adapter.GLOBAL_LINKNO_STRIDE):
        raise ValueError("native LINKNO exceeds crosswalk stride")
    order = np.argsort(columns.native_ids, kind="stable")
    endpoints = columns.endpoints[order]
    degenerate = columns.degenerate[order]
    raw_area = columns.dscontarea_raw[order]
    reference_area = np.asarray(reference_up_area_km2)
    if reference_area.dtype != np.dtype("float32") or reference_area.shape != native_ids.shape or not np.all(np.isfinite(reference_area) & (reference_area > 0)):
        raise ValueError("reference up_area_km2 must be positive finite float32 for every polygon-bearing ID")
    reference_area = reference_area[np.argsort(native_ids, kind="stable")]
    polygon_rows = np.searchsorted(native, basin)
    matching_units = []
    for source_unit in ("m2", "km2"):
        samples = raw_area[polygon_rows]
        if source_unit == "m2":
            samples = samples / 1_000_000
        with np.errstate(over="ignore", under="ignore"):
            encoded = samples.astype("float32")
        if np.array_equal(encoded.view("uint32"), reference_area.view("uint32")):
            matching_units.append(source_unit)
    if len(matching_units) != 1:
        raise ValueError("reference up_area_km2 does not establish exactly one native DSContArea normalization")
    source_unit = matching_units[0]
    # Match full build arithmetic exactly, including float64 division rounding.
    area = raw_area / 1_000_000 if source_unit == "m2" else raw_area.copy()
    topology = adapter._build_compact_topology(basin, native, downstream, endpoints, degenerate, area, header_number, adapter.DEFAULT_ENDPOINT_TOLERANCE)
    if _stamp(source_path) != before:
        raise ValueError(f"source changed during native derivation: {source_path}")
    resolutions = topology.reach_resolutions
    if resolutions is None or topology.diagnostics.basin_polarity is None:
        raise RuntimeError("shared topology omitted native orientation evidence")
    evidence = dict(native_ids=native, downstream_native_ids=downstream, endpoints=endpoints, degenerate=degenerate, dscontarea_raw=raw_area, up_area_km2=area, reference_up_area_km2=reference_area, polygon_native_ids=basin, downstream_endpoint_index=resolutions.downstream_endpoint_index, resolution=resolutions.resolution)
    normalized_digest = hashlib.sha256()
    for name, values in evidence.items():
        normalized_digest.update(name.encode("ascii") + b"\0")
        normalized_digest.update(np.ascontiguousarray(values).tobytes())
    report = {
        "header_number": header_number,
        "endpoint_tolerance": adapter.DEFAULT_ENDPOINT_TOLERANCE,
        "orientation_digest": adapter._orientation_digest(topology),
        "normalized_native_sha256": normalized_digest.hexdigest(),
        "dscontarea_normalization": {"source_unit": source_unit, "operation": "float64 / 1000000" if source_unit == "m2" else "float64 identity", "authority": "all authenticated reference polygon-bearing up_area_km2 float32 values, bit-exact", "matched_unit_count": len(basin)},
        "native_reach_count": len(native), "unit_count": len(basin),
        "polygonless_reach_count": len(native) - len(basin),
        "streamnet_clamp": asdict(columns.clamp),
        "basin_polarity": asdict(topology.diagnostics.basin_polarity),
        "resolution_counts": resolutions.counts(),
        "resolution_code_names": [value.value for value in adapter._REACH_RESOLUTION_CODES],
        "streamnet_diagnostics": asdict(topology.diagnostics),
    }
    if native_evidence_path is not None:
        np.savez(native_evidence_path, **evidence)
        report["native_evidence_path"] = str(native_evidence_path)
        report["native_evidence_sha256"] = sha256_file(native_evidence_path)
    return topology, report


def _positions(index: np.ndarray, ids: np.ndarray) -> np.ndarray:
    positions = np.searchsorted(index["id"], ids)
    if np.any(positions == len(index)) or np.any(index["id"][np.minimum(positions, len(index)-1)] != ids):
        raise ValueError("missing or extra drainage-unit ID")
    return positions


def _legal_row_groups(parquet: pq.ParquetFile) -> tuple[int, ...]:
    groups = tuple(parquet.metadata.row_group(i).num_rows for i in range(parquet.num_row_groups))
    count = parquet.metadata.num_rows
    if count <= 0 or (count < adapter.ROW_GROUP_MIN and len(groups) != 1) or (count >= adapter.ROW_GROUP_MIN and any(not adapter.ROW_GROUP_MIN <= size <= adapter.ROW_GROUP_MAX for size in groups)):
        raise ValueError("catchments source row groups violate HFX Spatial Partitioning (one group below 4096 rows; otherwise 4096-8192 rows per group)")
    return groups


def _reference_index(reference: Path, crosswalk: dict[str, int], path: Path, batch_size: int) -> tuple[np.ndarray, np.ndarray]:
    manifest = _read_json(reference / "manifest.json")
    count = manifest.get("unit_count")
    parquet = pq.ParquetFile(reference / "catchments.parquet")
    _legal_row_groups(parquet)
    if type(count) is not int or count <= 0 or count != parquet.metadata.num_rows:
        raise ValueError("reference manifest unit_count mismatch")
    if not pa.types.is_int64(parquet.schema_arrow.field("id").type):
        raise ValueError("reference drainage-unit id must be int64")
    index = np.lib.format.open_memmap(path, mode="w+", dtype=OUTLET_DTYPE, shape=(count,))
    if parquet.schema_arrow.field("up_area_km2").type != pa.float32():
        raise ValueError("reference up_area_km2 must be float32")
    reference_area = np.lib.format.open_memmap(path.with_name("reference-up-area.npy"), mode="w+", dtype="float32", shape=(count,))
    cursor = 0
    for batch in parquet.iter_batches(batch_size=batch_size, columns=["id", "up_area_km2"]):
        column = batch.column(0)
        if column.null_count:
            raise ValueError("null drainage-unit ID")
        ids = column.to_numpy(zero_copy_only=False)
        area_column = batch.column(1)
        if area_column.null_count:
            raise ValueError("null reference up_area_km2")
        area = area_column.to_numpy(zero_copy_only=False)
        if not np.all(np.isfinite(area) & (area > 0)):
            raise ValueError("reference up_area_km2 must be positive finite")
        index["id"][cursor:cursor+len(ids)] = ids
        reference_area[cursor:cursor+len(ids)] = area
        cursor += len(ids)
    order = np.argsort(index["id"], kind="stable")
    index["id"][:] = index["id"][order]
    reference_area[:] = reference_area[order]
    reference_area.flush()
    del order
    ids = index["id"]
    if np.any(ids[1:] == ids[:-1]):
        raise ValueError("duplicate reference drainage-unit ID")
    headers = ids // adapter.GLOBAL_LINKNO_STRIDE
    if set(np.unique(headers).tolist()) != set(crosswalk.values()):
        raise ValueError("reference drainage units must cover exactly all 62 canonical headers")
    index["downstream_id"] = -1
    index.flush()
    return index, reference_area


def _verify_graph(reference: Path, index: np.ndarray, batch_size: int) -> None:
    seen = np.zeros(len(index), dtype=np.bool_)
    linked = np.zeros(len(index), dtype=np.bool_)
    for batch in pq.ParquetFile(reference / "graph.parquet").iter_batches(batch_size=batch_size, columns=["id", "upstream_ids"]):
        ids_column, upstream_column = batch.columns
        if ids_column.null_count or upstream_column.null_count or upstream_column.values.null_count:
            raise ValueError("null graph ID or upstream list")
        if ids_column.type != pa.int64() or upstream_column.type.value_type != pa.int64():
            raise ValueError("graph IDs must be int64")
        ids = ids_column.to_numpy(zero_copy_only=False)
        positions = _positions(index, ids)
        if seen[positions].any() or len(np.unique(positions)) != len(positions):
            raise ValueError("duplicate graph drainage-unit ID")
        seen[positions] = True
        offsets = upstream_column.offsets.to_numpy(zero_copy_only=False)
        upstream = upstream_column.values.slice(int(offsets[0]), int(offsets[-1] - offsets[0])).to_numpy(zero_copy_only=False)
        if len(upstream):
            source_positions = _positions(index, upstream)
            targets = np.repeat(ids, np.diff(offsets))
            mismatch = index["downstream_id"][source_positions] != targets
            if linked[source_positions].any() or len(np.unique(source_positions)) != len(source_positions) or mismatch.any():
                raise ValueError("contracted graph mismatch for upstream drainage-unit IDs")
            linked[source_positions] = True
    if not seen.all() or not np.array_equal(linked, index["downstream_id"] != -1):
        raise ValueError("contracted graph coverage mismatch")

def _derive(reference: Path, inventory: dict, crosswalk: dict[str, int], evidence: Path, batch_size: int) -> tuple[Path, dict]:
    path = evidence / "outlets.npy"
    index, reference_area = _reference_index(reference, crosswalk, path, batch_size)
    provenance = {
        "schema_version": 1, "basins": [], "historical_build_identity": inventory["build_identity"],
        "crosswalk_sha256": inventory["crosswalk_sha256"],
        "adapter_version": adapter.ADAPTER_VERSION,
        "build_sources": {name: sha256_file(Path(__file__).with_name(name)) for name in ("build_adapter.py", "regenerate_outlets.py", "outlet_invariance.py", "uv.lock")},
        "environment": {"python": platform.python_version(), **{name: importlib.metadata.version(name) for name in ("numpy", "pyarrow", "pyogrio", "shapely")}},
        "manifest_policy": "byte-identical, including original timestamp",
        "replacement_readme": dict(inventory["replacement_readme"]),
        "readme_policy": "README.md equals the separately pinned operator-supplied replacement; original retained as historical evidence",
    }
    sources = {record["processing_basin_id"]: record for record in inventory["sources"]}
    for basin, header in sorted(crosswalk.items(), key=lambda pair: pair[1]):
        record = sources[basin]
        source_path = _plain_path(Path(record["path"]))
        _authenticate(source_path, record)
        stamp = _stamp(source_path)
        start, stop = np.searchsorted(index["id"], [header * adapter.GLOBAL_LINKNO_STRIDE, (header+1) * adapter.GLOBAL_LINKNO_STRIDE])
        native_ids = index["id"][start:stop] - header * adapter.GLOBAL_LINKNO_STRIDE
        topology, report = derive_native_outlets(native_ids, source_path, header, reference_up_area_km2=reference_area[start:stop], native_evidence_path=evidence / f"native-{basin}.npz")
        if _stamp(source_path) != stamp:
            raise ValueError(f"native source changed after authentication: {source_path}")
        _authenticate(source_path, record)
        index["downstream_id"][start:stop] = topology.downstream_global_ids
        index["outlet_lon"][start:stop] = topology.outlet_lons
        index["outlet_lat"][start:stop] = topology.outlet_lats
        resolutions = topology.reach_resolutions
        unit_resolution = np.empty(len(native_ids), dtype=[("native_id", "<i8"), ("resolution", "i1")])
        unit_resolution["native_id"] = native_ids
        unit_resolution["resolution"] = resolutions.resolution[np.searchsorted(resolutions.native_ids, native_ids)]
        resolution_path = evidence / f"native-resolution-{basin}.npy"
        np.save(resolution_path, unit_resolution, allow_pickle=False)
        report.update(processing_basin_id=basin, source=dict(record), source_sha256=record["sha256"], crosswalk_sha256=inventory["crosswalk_sha256"], build_sources=provenance["build_sources"], unit_resolution_path=str(resolution_path), unit_resolution_sha256=sha256_file(resolution_path))
        provenance["basins"].append(report)
        _json(evidence / f"orientation-{basin}.json", report)
        del topology
    index.flush()
    _verify_graph(reference, index, batch_size)
    provenance["outlet_index_sha256"] = sha256_file(path)
    _json(evidence / "provenance.json", provenance)
    return path, provenance


def rewrite_catchments(reference: Path, destination: Path, outlet_index: Path, *, batch_size: int = 1024) -> None:
    """Rewrite outlet columns while preserving every legal input row group.

    Raises ValueError for missing IDs, incompatible types, or illegal row groups.
    Memory includes one full source row group (at most 8192 rows), Arrow decoder
    and writer buffers. Decoding batch size cannot reduce that row-group floor.
    """
    index = np.load(outlet_index, mmap_mode="r", allow_pickle=False)
    source = pq.ParquetFile(reference)
    groups = _legal_row_groups(source)
    schema = source.schema_arrow
    for name in ("outlet_lon", "outlet_lat"):
        if schema.field(name).type != pa.float64():
            raise ValueError(f"outlet column must be float64: {name}")
    with pq.ParquetWriter(destination, schema, compression="zstd") as writer:
        for group, group_rows in enumerate(groups):
            rewritten = []
            for batch in source.iter_batches(batch_size=batch_size, row_groups=[group], use_threads=False):
                positions = _positions(index, batch.column(schema.get_field_index("id")).to_numpy(zero_copy_only=False))
                columns = list(batch.columns)
                for name in ("outlet_lon", "outlet_lat"):
                    columns[schema.get_field_index(name)] = pa.array(index[name][positions], type=schema.field(name).type)
                rewritten.append(pa.RecordBatch.from_arrays(columns, schema=schema))
            writer.write_table(pa.Table.from_batches(rewritten, schema=schema), row_group_size=group_rows)
            rewritten.clear()
            del batch, columns, positions


def _locations(reference: Path, evidence: Path, destination: Path | None) -> tuple[Path, Path, Path | None]:
    reference = _plain_path(reference)
    evidence = _plain_path(evidence)
    if not reference.is_dir():
        raise ValueError("reference directory does not exist")
    paths = [reference, evidence]
    if destination is not None:
        destination = _plain_path(destination)
        paths.append(destination)
    for left in paths:
        for right in paths:
            if left != right and (left.is_relative_to(right) or right.is_relative_to(left)):
                raise ValueError("input, output, and evidence must not overlap")
    if len(set(paths)) != len(paths):
        raise ValueError("input, output, and evidence alias")
    if evidence.exists() or (destination is not None and destination.exists()):
        raise ValueError("evidence and destination must be absent/new")
    return reference, evidence, destination


def _publish_new_directory(source: Path, destination: Path) -> None:
    """Atomically rename a local staged directory without replacing any entry.

    Raises OSError on collision or unsupported filesystem no-replace operation.
    Only Linux and macOS are supported; there is no check-then-replace fallback.
    """
    libc = ctypes.CDLL(None, use_errno=True)
    encoded_source, encoded_destination = os.fsencode(source), os.fsencode(destination)
    if platform.system() == "Linux" and hasattr(libc, "renameat2"):
        function = libc.renameat2
        function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(-100, encoded_source, -100, encoded_destination, 1)
    elif platform.system() == "Darwin" and hasattr(libc, "renamex_np"):
        function = libc.renamex_np
        function.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        function.restype = ctypes.c_int
        result = function(encoded_source, encoded_destination, 4)
    else:
        raise OSError("atomic no-replace directory publication unavailable on this platform")
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(destination))


def _execute(reference: Path, destination: Path, inventory_path: Path, evidence: Path, *, inventory_sha256: str, batch_size: int, operation: str) -> dict:
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if operation == "regenerate":
        reference, evidence, destination = _locations(reference, evidence, destination)
    else:
        reference, evidence, _ = _locations(reference, evidence, None)
        destination = _plain_path(destination)
        if not destination.is_dir() or destination == reference or destination.is_relative_to(reference) or reference.is_relative_to(destination) or evidence.is_relative_to(destination) or destination.is_relative_to(evidence):
            raise ValueError("candidate must be a separate existing directory")
    inventory_path = _plain_path(inventory_path)
    if inventory_path.is_relative_to(evidence) or inventory_path.is_relative_to(destination):
        raise ValueError("inventory overlaps output/evidence")
    evidence.mkdir(parents=True)
    staging = None
    published = False
    try:
        inventory, crosswalk = authenticate_inventory(reference, inventory_path, inventory_sha256)
        for source in inventory["sources"]:
            source_path = _plain_path(Path(source["path"]))
            if source_path.is_relative_to(evidence) or source_path.is_relative_to(destination):
                raise ValueError("native source overlaps output/evidence")
        readme_path = _plain_path(Path(inventory["replacement_readme"]["path"]))
        if readme_path.is_relative_to(evidence) or readme_path.is_relative_to(destination):
            raise ValueError("replacement README overlaps output/evidence")
        shutil.copyfile(inventory_path, evidence / "inventory.json")
        if sha256_file(evidence / "inventory.json") != inventory_sha256:
            raise ValueError("inventory changed before evidence copy")
        shutil.copyfile(reference / "README.md", evidence / "historical-README.md")
        _json(evidence / "invocation.json", dict(operation=operation, reference=str(reference), candidate=str(destination), inventory_sha256=inventory_sha256, batch_size=batch_size))
        index, provenance = _derive(reference, inventory, crosswalk, evidence, batch_size)
        candidate = destination
        if operation == "regenerate":
            destination.parent.mkdir(parents=True, exist_ok=True)
            required = sum(row["bytes"] for row in inventory["reference_files"])
            if shutil.disk_usage(destination.parent).free < required:
                raise ValueError("insufficient scratch for separate candidate payloads")
            staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
            candidate = staging / "dataset"
            candidate.mkdir()
            for record in inventory["reference_files"]:
                relative = record["path"]
                target = candidate / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                if relative == "catchments.parquet":
                    rewrite_catchments(reference / relative, target, index, batch_size=batch_size)
                elif relative == "README.md":
                    shutil.copyfile(readme_path, target)
                elif relative != "manifest.json":
                    shutil.copyfile(reference / relative, target)
            # A staging manifest describes bytes for verification, never a successful output.
            shutil.copyfile(reference / "manifest.json", candidate / "manifest.json")
        from outlet_invariance import verify_outlet_invariance
        candidate_stamps = {name: _stamp(candidate / name) for name in _files(candidate)}
        summary = verify_outlet_invariance(reference, candidate, index, provenance, evidence / "invariance", batch_size=batch_size)
        authenticate_inventory(reference, inventory_path, inventory_sha256)
        if _files(candidate) != set(candidate_stamps) or any(_stamp(candidate / name) != stamp for name, stamp in candidate_stamps.items()):
            raise ValueError("candidate changed after invariance verification")
        if operation == "regenerate":
            # Cooperating writers claim a sibling lock. No replacement of an existing root.
            lock = destination.with_name(f".{destination.name}.publish-lock")
            with lock.open("x") as claim:
                claim.write(str(evidence) + "\n")
                claim.flush()
                if destination.exists() or destination.is_symlink():
                    raise ValueError("destination appeared before publication")
                _plain_path(destination)
                _publish_new_directory(candidate, destination)
                published = True
            lock.unlink()
            staging.rmdir()
        _json(evidence / "status.json", {"status": "complete", "operation": operation, "summary": summary})
        return summary
    except Exception as error:
        if published:
            (destination / "manifest.json").unlink(missing_ok=True)
        if staging is not None:
            marker = staging / "dataset" / "manifest.json"
            marker.unlink(missing_ok=True)
        _json(evidence / "status.json", {"status": "refused", "operation": operation, "error": str(error), "staging": str(staging) if staging else None})
        raise


def regenerate(reference: Path, destination: Path, inventory_path: Path, evidence: Path, *, inventory_sha256: str, batch_size: int = 1024) -> dict:
    """Compile corrected outlets into a new dataset; retain refusal evidence.

    Raises ValueError on identity, topology, coverage, safety, or invariance refusal.
    This operation does not run the HFX validator or authorize publication.
    """
    return _execute(reference, destination, inventory_path, evidence, inventory_sha256=inventory_sha256, batch_size=batch_size, operation="regenerate")


def verify(reference: Path, candidate: Path, inventory_path: Path, evidence: Path, *, inventory_sha256: str, batch_size: int = 1024) -> dict:
    """Reauthenticate inputs, rederive native outlets, and exhaustively verify.

    Raises ValueError on any identity, native derivation, or invariance refusal.
    No caller-supplied outlet table or provenance report is trusted.
    """
    return _execute(reference, candidate, inventory_path, evidence, inventory_sha256=inventory_sha256, batch_size=batch_size, operation="verify")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("regenerate", "verify"))
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--inventory-sha256", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=1024)
    args = parser.parse_args(argv)
    operation = regenerate if args.operation == "regenerate" else verify
    operation(args.reference, args.candidate, args.inventory, args.evidence, inventory_sha256=args.inventory_sha256, batch_size=args.batch_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
