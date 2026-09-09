"""delineate : dataset identity × geographic outlet -> saved watershed evidence."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from shapely import from_wkb
from shapely.geometry import shape

from .consumer import verify_consumer, verify_loaded_libraries
from .manifest import observe_manifest
from .models import DelineationRequest, finite_point, sha256, write_json
from .native_data import verify_native_data


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
    metadata["upstream_unit_ids_sha256"] = sha256(output / "upstream_unit_ids.json")
    return metadata


def delineate(request: DelineationRequest, output: Path) -> None:
    """Run exactly one native best-effort delineation; preserve failure without retry."""
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "request.json", request.model_dump())
    started = time.perf_counter()
    stage = "consumer_import_isolation"
    try:
        if any(
            name == "pourpoint" or name.startswith("pourpoint.") for name in sys.modules
        ):
            raise ValueError(
                "consumer must not be imported before fresh worker cache setup"
            )
        bytecode = output / "python-bytecode"
        bytecode.mkdir(exist_ok=False)
        sys.pycache_prefix = str(bytecode)
        os.environ["PYTHONPYCACHEPREFIX"] = str(bytecode)
        stage = "consumer_identity"
        pourpoint = verify_consumer(request.consumer)
        identity_verified = time.perf_counter()
        stage = "manifest_before"
        s3 = {key: value for key, value in os.environ.items() if key.startswith("AWS_")}
        before_manifest = observe_manifest(request.dataset, output, "before", s3)
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
            stage = "runtime_identity_after"
            build = json.loads(Path(request.consumer.build_receipt_path).read_bytes())
            loaded_after = verify_loaded_libraries(build["native_library_sha256"])
            data_after = verify_native_data(request.consumer, loaded_after)
            write_json(output / "runtime-data-after.json", data_after)
            write_json(output / "loaded-libraries-after.json", loaded_after)
            stage = "manifest_after"
            after_manifest = observe_manifest(request.dataset, output, "after", s3)
            write_json(
                output / "observations.json",
                {
                    "before": before_manifest,
                    "after": after_manifest,
                    "guarantee": "Matching bounded manifest bytes observed before and after session use; engine performs independent reads. No immutable session snapshot or object-version pinning is claimed. Remote objects can change between observations.",
                },
            )
            stage = "serialization"
            serialization_started = time.perf_counter()
            timing = {
                "consumer_identity": identity_verified - started,
                "manifest_before": before_open - identity_verified,
                "post_delineation_identity_and_manifest": serialization_started
                - delineated,
                "session_open": opened - before_open,
                "delineation": delineated - opened,
            }
            metadata = save_result(result, request, output, timing)
            metadata["consumer_import_policy"] = {
                "bytecode_cache": str(bytecode),
                "policy": "Fresh empty per-worker sys.pycache_prefix established before any consumer import; pre-imported consumer modules are refused. Wheel source/data hashes remain verified before import. Existing package __pycache__ entries are not read.",
            }
            metadata["runtime_data_after_sha256"] = sha256(
                output / "runtime-data-after.json"
            )
            metadata["loaded_libraries_after_sha256"] = sha256(
                output / "loaded-libraries-after.json"
            )
            metadata["observed_manifest_sha256"] = sha256(output / "observations.json")
            metadata["unreadable_auxiliary_schemas"] = (
                engine.unreadable_auxiliary_schemas
            )
            timing["serialization"] = time.perf_counter() - serialization_started
            timing["total_before_metadata_write"] = time.perf_counter() - started
            write_json(output / "metadata.json", metadata)
        if (
            not (output / "trace.jsonl").is_file()
            or (output / "trace.jsonl").stat().st_size == 0
        ):
            raise ValueError("consumer trace is absent or empty")
        write_json(
            output / "success.json",
            {
                "metadata_sha256": sha256(output / "metadata.json"),
                "trace_sha256": sha256(output / "trace.jsonl"),
            },
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
