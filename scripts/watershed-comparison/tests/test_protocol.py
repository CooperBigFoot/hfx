"""Fault injection tests cover protocol checks, not successful native delineation."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from shapely.geometry import box, mapping

from watershed_comparison.delineation import save_result
from watershed_comparison.models import CONSUMER_SHA, DelineationRequest, ResourceLimits
from watershed_comparison.supervision import (
    GIB,
    compare_sequentially,
    isolated_environment,
    violation,
)


def request(uri="/local/fixture"):
    return DelineationRequest.model_validate(
        {
            "dataset": {
                "uri": uri,
                "fabric_name": "tdx_hydro",
                "fabric_version": "test",
                "manifest_sha256": "a" * 64,
                "attribution": "test source",
                "license": "test-only",
            },
            "consumer": {
                "source_sha": CONSUMER_SHA,
                "wheel_path": "/unused",
                "wheel_sha256": "b" * 64,
                "extension_sha256": "c" * 64,
                "runtime_data_receipt_path": "/unused",
                "runtime_data_receipt_sha256": "e" * 64,
                "build_receipt_path": "/unused",
                "build_receipt_sha256": "d" * 64,
            },
            "input_outlet": [7.5890, 47.5596],
            "settings": {},
        }
    )


def result():
    geometry = box(7, 47, 8, 48)
    reason = SimpleNamespace(
        kind="no_d8_aux_declared",
        category="availability",
        schema=None,
        failure_kind=None,
        requested_threshold=None,
        effective_threshold=None,
        units=None,
        mapped_cell=None,
        measured_accumulation=None,
    )
    feature = {
        "type": "Feature",
        "geometry": mapping(geometry),
        "properties": {
            "terminal_unit_id": 1,
            "upstream_unit_count": 2,
            "refinement": "best_effort_skipped",
        },
    }
    return SimpleNamespace(
        geometry_wkb=geometry.wkb,
        to_geojson=lambda: json.dumps(feature),
        upstream_unit_ids=[1, 2],
        terminal_unit_id=1,
        input_outlet=(7.5890, 47.5596),
        resolved_outlet=(7.58, 47.55),
        refined_outlet=None,
        refinement_skip_reason=reason,
        refinement_seed_kind="coarse",
        area_km2=1,
        resolution_method="snap",
    )


def test_serializes_all_skip_fields_and_full_geometry(tmp_path):
    metadata = save_result(result(), request(), tmp_path, {})
    assert metadata["refinement"]["skip_reason"]["kind"] == "no_d8_aux_declared"
    assert metadata["upstream_unit_count"] == 2
    assert (tmp_path / "watershed.wkb").read_bytes() == result().geometry_wkb


@pytest.mark.parametrize(
    "attribute,value",
    [
        ("upstream_unit_ids", [2]),
        ("refinement_seed_kind", "disabled"),
        ("input_outlet", (1, 2)),
        ("area_km2", float("nan")),
    ],
)
def test_rejects_bad_native_protocol_and_preserves_geometry(tmp_path, attribute, value):
    fake = result()
    setattr(fake, attribute, value)
    with pytest.raises(ValueError):
        save_result(fake, request(), tmp_path, {})
    assert (tmp_path / "watershed.wkb").exists()


def test_public_environment_excludes_all_inherited_secrets(tmp_path, monkeypatch):
    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ENDPOINT",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "PYTHONPATH",
        "GDAL_HTTP_HEADERS",
    ):
        monkeypatch.setenv(key, "secret-must-not-cross")
    env = isolated_environment(tmp_path, None)
    assert not any("secret-must-not-cross" in value for value in env.values())
    assert not any(key.startswith("AWS_") or "proxy" in key.lower() for key in env)
    assert env["HOME"] == str(tmp_path / "home")


def test_private_environment_rejects_proxy_config(tmp_path):
    with pytest.raises(ValueError):
        isolated_environment(tmp_path, {"HTTPS_PROXY": "secret"})


@pytest.mark.parametrize(
    "field,value",
    [
        ("rss_bytes", 41 * GIB),
        ("available_bytes", 7 * GIB),
        ("swap_used_bytes", 2 * GIB),
        ("free_disk_bytes", 49 * GIB),
        ("elapsed_seconds", 15000),
    ],
)
def test_every_resource_gate(field, value):
    initial = {
        "rss_bytes": 0,
        "available_bytes": 32 * GIB,
        "swap_used_bytes": 0,
        "free_disk_bytes": 200 * GIB,
        "elapsed_seconds": 0,
    }
    sample = dict(initial, **{field: value})
    assert violation(sample, initial, ResourceLimits())


def test_changed_semantics_rejected():
    value = request().model_dump()
    value["settings"]["refine"] = False
    with pytest.raises(ValueError):
        DelineationRequest.model_validate(value)


def test_pair_failure_stops_without_second_attempt(tmp_path, monkeypatch):
    from watershed_comparison import supervision
    from watershed_comparison.models import GRIT_URI

    called = []

    def fail(req, output, credentials, limits):
        called.append(req.dataset.uri)
        raise RuntimeError("protocol fault injection")

    monkeypatch.setattr(supervision, "execute_request", fail)
    monkeypatch.setattr(
        supervision,
        "verify_delivery",
        lambda *_: {
            "endpoint": "https://verified.example",
            "region": "verified-region",
        },
    )
    first = request("s3://pourpoint-hfx/hfx/dataset/")
    second = request(GRIT_URI)
    output = tmp_path / "comparison"
    with pytest.raises(RuntimeError):
        compare_sequentially(
            first,
            second,
            output,
            {
                "AWS_ENDPOINT": "https://verified.example",
                "AWS_REGION": "verified-region",
            },
            ResourceLimits(),
            Path("unused"),
        )
    assert called == [first.dataset.uri]
    assert (
        json.loads((output / "failure.json").read_text())["stage"] == "private_dataset"
    )


def test_delivery_receipt_requires_complete_matching_payload(tmp_path):
    from watershed_comparison.supervision import verify_delivery

    dataset = request("s3://pourpoint-hfx/hfx/dataset/").dataset
    receipt = {
        "schema": "hfx-dataset-delivery-v1",
        "status": "verified-dataset",
        "plan": {
            "bucket": "pourpoint-hfx",
            "endpoint": "https://verified.example",
            "region": "verified-region",
            "destination_prefix": "hfx/dataset/",
            "objects": [
                {
                    "path": "manifest.json",
                    "sha256": "a" * 64,
                    "identity": {"bytes": 123},
                }
            ],
        },
        "objects": {
            "manifest.json": {"verification": {"sha256": "a" * 64, "bytes": 123}}
        },
    }
    receipt["plan"].update(readme_sha256="b" * 64, readme_bytes=5)
    receipt["objects"]["README.md"] = {"verification": {"sha256": "b" * 64, "bytes": 5}}
    for obj in receipt["objects"].values():
        obj["status"] = "verified"
        obj["identity"] = {"bytes": obj["verification"]["bytes"]}
        obj["verification"]["method"] = "full-ordered-range-stream-sha256"
    receipt["final_bytes"] = 128
    path = tmp_path / "delivery.json"
    path.write_text(json.dumps(receipt))
    assert verify_delivery(path, dataset)["sha256"]
    receipt["objects"]["manifest.json"]["verification"]["bytes"] = 122
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="incomplete"):
        verify_delivery(path, dataset)
    receipt["status"] = "copying"
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="verified"):
        verify_delivery(path, dataset)


def test_pair_rejects_endpoint_different_from_verified_delivery(tmp_path, monkeypatch):
    from watershed_comparison import supervision
    from watershed_comparison.models import GRIT_URI

    monkeypatch.setattr(
        supervision,
        "verify_delivery",
        lambda *_: {
            "endpoint": "https://verified.example",
            "region": "verified-region",
        },
    )
    calls = []
    monkeypatch.setattr(
        supervision, "execute_request", lambda *args: calls.append(args)
    )
    with pytest.raises(ValueError, match="endpoint"):
        compare_sequentially(
            request("s3://pourpoint-hfx/hfx/dataset/"),
            request(GRIT_URI),
            tmp_path / "comparison",
            {"AWS_ENDPOINT": "https://wrong.example", "AWS_REGION": "verified-region"},
            ResourceLimits(),
            Path("unused"),
        )
    assert calls == []


def test_wallclock_gate_terminates_actual_child_process(tmp_path, monkeypatch):
    """A real sleeping process is a protocol fault, not a mock delineation success."""
    import subprocess
    import sys

    from watershed_comparison import supervision

    actual_popen = subprocess.Popen
    children = []

    def sleeping_process(command, **kwargs):
        child = actual_popen(
            [sys.executable, "-c", "import time; time.sleep(60)"], **kwargs
        )
        children.append(child)
        return child

    monkeypatch.setattr(supervision.subprocess, "Popen", sleeping_process)
    with pytest.raises(RuntimeError, match="wallclock"):
        supervision.execute_request(
            request(),
            tmp_path / "bounded",
            None,
            ResourceLimits(max_elapsed_seconds=0.05, sample_seconds=0.01),
        )
    assert len(children) == 1
    assert children[0].poll() is not None
    assert (tmp_path / "bounded/resources.jsonl").stat().st_size > 0


def test_supervisor_sigterm_reaps_detached_child(tmp_path):
    """Real OS cancellation regression; sleeping child models a protocol fault."""
    import os
    import signal
    import subprocess
    import sys
    import time

    import psutil

    request_path = tmp_path / "request.json"
    request_path.write_text(request().model_dump_json())
    child_pid = tmp_path / "child.pid"
    script = tmp_path / "signal_probe.py"
    script.write_text(f"""
import subprocess, sys
from pathlib import Path
from watershed_comparison import supervision
from watershed_comparison.models import read_request, ResourceLimits
original = subprocess.Popen
def sleeper(command, **kwargs):
    child = original([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
    Path({str(child_pid)!r}).write_text(str(child.pid))
    return child
supervision.subprocess.Popen = sleeper
supervision.execute_request(read_request(Path({str(request_path)!r})), Path({str(tmp_path / "supervised")!r}), None, ResourceLimits())
""")
    parent = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pid = None
    try:
        deadline = time.monotonic() + 10
        while not child_pid.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert child_pid.exists()
        pid = int(child_pid.read_text())
        parent.send_signal(signal.SIGTERM)
        parent.wait(timeout=10)
        assert not psutil.pid_exists(pid), (
            "supervisor cancellation left detached child alive"
        )
    finally:
        if parent.poll() is None:
            parent.kill()
            parent.wait(timeout=5)
        if pid is not None and psutil.pid_exists(pid):
            os.killpg(pid, signal.SIGKILL)


@pytest.mark.parametrize(
    "change", ["missing_readme", "extra", "total", "method", "status"]
)
def test_source_produced_receipt_requires_full_inventory(tmp_path, change):
    from watershed_comparison.supervision import verify_delivery

    value = json.loads(
        (Path(__file__).parent / "fixtures/verified-delivery.json").read_text()
    )
    plan = value["plan"]
    dataset = request(
        f"s3://{plan['bucket']}/{plan['destination_prefix']}"
    ).dataset.model_copy(
        update={
            "manifest_sha256": value["objects"]["manifest.json"]["verification"][
                "sha256"
            ]
        }
    )
    valid = tmp_path / "valid.json"
    valid.write_text(json.dumps(value))
    assert verify_delivery(valid, dataset)
    if change == "missing_readme":
        del value["objects"]["README.md"]
    elif change == "extra":
        value["objects"]["unexpected"] = value["objects"]["README.md"]
    elif change == "total":
        value["final_bytes"] += 1
    elif change == "method":
        value["objects"]["README.md"]["verification"]["method"] = "etag-only"
    elif change == "status":
        value["objects"]["README.md"]["status"] = "copying"
    damaged = tmp_path / "damaged.json"
    damaged.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        verify_delivery(damaged, dataset)


def test_only_absent_isolated_proj_user_candidate_can_relocate(tmp_path, monkeypatch):
    from watershed_comparison.native_data import _verify_directory

    monkeypatch.setenv("HOME", str(tmp_path))
    user_path = tmp_path / "Library/Application Support/proj"
    entry = {
        "resolved_path": "/original/home/Library/Application Support/proj",
        "exists": False,
        "files_sha256": {},
    }
    _verify_directory(entry, str(user_path), empty_user_candidate=True)
    user_path.mkdir(parents=True)
    with pytest.raises(ValueError):
        _verify_directory(entry, str(user_path), empty_user_candidate=True)


def test_stop_process_escalates_for_descendant_after_leader_exit(tmp_path):
    import os
    import signal
    import subprocess
    import sys
    import time

    import psutil

    from watershed_comparison.supervision import stop_process

    pidfile = tmp_path / "descendant.pid"
    child_code = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)"
    parent_code = f"import subprocess,sys,time; from pathlib import Path; p=subprocess.Popen([sys.executable,'-c',{child_code!r}]); Path({str(pidfile)!r}).write_text(str(p.pid)); time.sleep(60)"
    leader = subprocess.Popen(
        [sys.executable, "-c", parent_code], start_new_session=True
    )
    descendant = None
    try:
        deadline = time.monotonic() + 10
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pidfile.exists()
        descendant = int(pidfile.read_text())
        time.sleep(0.1)  # Let fault child install its deliberate SIGTERM refusal.
        stop_process(leader)
        deadline = time.monotonic() + 1
        while (
            psutil.pid_exists(descendant)
            and psutil.Process(descendant).status() != psutil.STATUS_ZOMBIE
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert (
            not psutil.pid_exists(descendant)
            or psutil.Process(descendant).status() == psutil.STATUS_ZOMBIE
        )
    finally:
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        leader.wait(timeout=5)


def test_historical_delivery_success_never_overrides_current_refusal(tmp_path):
    from watershed_comparison.supervision import verify_delivery

    value = json.loads(
        (Path(__file__).parent / "fixtures/verified-delivery.json").read_text()
    )
    plan = value["plan"]
    dataset = request(
        f"s3://{plan['bucket']}/{plan['destination_prefix']}"
    ).dataset.model_copy(
        update={
            "manifest_sha256": value["objects"]["manifest.json"]["verification"][
                "sha256"
            ]
        }
    )
    value["verification_history"] = [{"status": "verified-dataset"}]
    value["status"] = "refused"
    value["last_failure"] = {
        "reason": "destination changed",
        "observed_at": "2026-09-09",
    }
    path = tmp_path / "refused.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="verified destination"):
        verify_delivery(path, dataset)
