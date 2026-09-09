"""delineate : dataset identity × geographic outlet -> saved watershed evidence."""

from __future__ import annotations

import importlib
import json
import time
from pathlib import Path

from shapely import from_wkb
from shapely.geometry import shape

from .models import DelineationRequest, finite_point, sha256, write_json


def verify_consumer(identity):
    """Verify installed native extension and retained build inputs before opening data."""
    for path, expected in (
        (identity.wheel_path, identity.wheel_sha256),
        (identity.build_receipt_path, identity.build_receipt_sha256),
    ):
        if sha256(Path(path)) != expected:
            raise ValueError("consumer build artifact checksum mismatch")
    receipt = json.loads(Path(identity.build_receipt_path).read_text())
    for field in ("source_sha", "wheel_sha256", "extension_sha256"):
        if receipt[field] != getattr(identity, field):
            raise ValueError("consumer build receipt identity mismatch")
    if receipt["build_exit_code"] != 0 or not receipt["source_tree_clean"]:
        raise ValueError("consumer receipt lacks clean successful source build")
    for library, expected in receipt["native_library_sha256"].items():
        if sha256(Path(library)) != expected:
            raise ValueError("linked native library differs from build receipt")
    extension = importlib.import_module("pourpoint._pourpoint")
    if sha256(Path(extension.__file__)) != identity.extension_sha256:
        raise ValueError("installed consumer extension checksum mismatch")
    return importlib.import_module("pourpoint")


def save_result(result, request: DelineationRequest, output: Path, timings: dict):
    """Preserve complete native outputs, then reject unusable delineation evidence."""
    wkb = result.geometry_wkb
    geojson = result.to_geojson()
    (output / "watershed.wkb").write_bytes(wkb)
    (output / "watershed.geojson").write_text(geojson)
    geometry = from_wkb(wkb)
    feature = json.loads(geojson)
    if (
        geometry.geom_type not in ("Polygon", "MultiPolygon")
        or geometry.is_empty
        or not geometry.is_valid
    ):
        raise ValueError("consumer returned empty, invalid or non-polygonal geometry")
    if feature.get("type") != "Feature" or not geometry.equals_exact(
        shape(feature["geometry"]), 0
    ):
        raise ValueError("consumer WKB and GeoJSON geometry disagree")
    ids = result.upstream_unit_ids
    if not ids or result.terminal_unit_id not in ids or len(set(ids)) != len(ids):
        raise ValueError(
            "consumer traversal lacks terminal or has duplicate upstream units"
        )
    input_point = finite_point(result.input_outlet, "input_outlet")
    if input_point != list(request.input_outlet):
        raise ValueError("consumer changed requested point")
    resolved = finite_point(result.resolved_outlet, "resolved_outlet")
    refined = (
        None
        if result.refined_outlet is None
        else finite_point(result.refined_outlet, "refined_outlet")
    )
    reason = result.refinement_skip_reason
    reason_fields = (
        "kind",
        "category",
        "schema",
        "failure_kind",
        "requested_threshold",
        "effective_threshold",
        "units",
        "mapped_cell",
        "measured_accumulation",
    )
    skipped = (
        None
        if reason is None
        else {name: getattr(reason, name) for name in reason_fields}
    )
    seed = result.refinement_seed_kind
    if (refined is None and (skipped is None or seed != "coarse")) or (
        refined is not None
        and (skipped is not None or seed not in ("vector_quantized", "raster_ranked"))
    ):
        raise ValueError("inconsistent best-effort refinement outcome")
    area = result.area_km2
    if not 0 < area < float("inf"):
        raise ValueError("consumer area must be finite and positive")
    properties = feature["properties"]
    if properties["terminal_unit_id"] != result.terminal_unit_id or properties[
        "upstream_unit_count"
    ] != len(ids):
        raise ValueError("consumer GeoJSON traversal metadata disagrees")
    metadata = {
        "dataset": request.dataset.model_dump(),
        "consumer": request.consumer.model_dump(),
        "settings": request.settings.model_dump(),
        "input_outlet": input_point,
        "resolved_outlet": resolved,
        "refined_outlet": refined,
        "terminal_unit_id": result.terminal_unit_id,
        "upstream_unit_count": len(ids),
        "area_km2": area,
        "engine_area_method": "WGS84 ellipsoid, Karney geodesic area",
        "resolution_method": result.resolution_method,
        "refinement": {
            "status": "applied" if refined is not None else "best_effort_skipped",
            "seed_kind": seed,
            "skip_reason": skipped,
            "consumer_description": properties["refinement"],
        },
        "timing_seconds": timings,
        "geometry_sha256": {
            "wkb": sha256(output / "watershed.wkb"),
            "geojson": sha256(output / "watershed.geojson"),
        },
    }
    write_json(output / "upstream_unit_ids.json", ids)
    return metadata


def delineate(request: DelineationRequest, output: Path) -> None:
    """Run exactly one native best-effort delineation; preserve failure without retry."""
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "request.json", request.model_dump())
    started = time.perf_counter()
    stage = "consumer_identity"
    try:
        pourpoint = verify_consumer(request.consumer)
        stage = "session_open"
        with pourpoint.bench_trace(output / "trace.jsonl"):
            before_open = time.perf_counter()
            engine = pourpoint.Engine(
                request.dataset.uri, **request.settings.model_dump()
            )
            opened = time.perf_counter()
            stage = "delineation"
            lon, lat = request.input_outlet
            result = engine.delineate(lat=lat, lon=lon, geometry=True)
            delineated = time.perf_counter()
            stage = "serialization"
            timing = {
                "session_open": opened - before_open,
                "delineation": delineated - opened,
            }
            metadata = save_result(result, request, output, timing)
            metadata["unreadable_auxiliary_schemas"] = (
                engine.unreadable_auxiliary_schemas
            )
            timing["serialization"] = time.perf_counter() - delineated
            timing["total"] = time.perf_counter() - started
            write_json(output / "metadata.json", metadata)
        if (
            not (output / "trace.jsonl").is_file()
            or (output / "trace.jsonl").stat().st_size == 0
        ):
            raise ValueError("consumer trace is absent or empty")
        write_json(
            output / "success.json",
            {"metadata_sha256": sha256(output / "metadata.json")},
        )
    except BaseException as exc:
        # Avoid credential-bearing SDK exception text; stage/type plus retained native
        # trace and stderr identify the failure without serializing secret values.
        write_json(
            output / "failure.json",
            {
                "stage": stage,
                "exception_type": type(exc).__name__,
                "elapsed_seconds": time.perf_counter() - started,
                "action": "stop; request maintainer decision; no retry performed",
            },
        )
        raise
