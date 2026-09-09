"""Opt-in real installed-wheel fixture test; never opens a remote dataset."""

import json
import os
from pathlib import Path

import pytest

from watershed_comparison.models import DelineationRequest, ResourceLimits, sha256
from watershed_comparison.supervision import execute_request


def test_native_worker_in_supervised_isolated_process(tmp_path):
    identity_path = os.environ.get("HFX_TEST_CONSUMER_IDENTITY")
    fixture_path = os.environ.get("HFX_TEST_LOCAL_FIXTURE")
    if not identity_path or not fixture_path:
        pytest.skip("requires explicit pinned wheel identity and tiny local fixture")
    fixture = Path(fixture_path).resolve(strict=True)
    if not fixture.is_dir():
        pytest.fail("offline fixture must be a local directory")
    manifest = json.loads((fixture / "manifest.json").read_text())
    request = DelineationRequest.model_validate(
        {
            "dataset": {
                "uri": str(fixture),
                "fabric_name": manifest["fabric_name"],
                "fabric_version": manifest.get("fabric_version"),
                "manifest_sha256": sha256(fixture / "manifest.json"),
                "attribution": "pourpoint tiny synthetic fixture; no real-world watershed claim",
                "license": "MIT synthetic fixture",
            },
            "consumer": json.loads(Path(identity_path).read_text()),
            "input_outlet": [2.5, -2.5],
            "settings": {},
        }
    )
    output = tmp_path / "native"
    execute_request(request, output, None, ResourceLimits())
    metadata = json.loads((output / "watershed/metadata.json").read_text())
    assert metadata["upstream_unit_count"] > 0
    assert metadata["refinement"]["status"] in ("applied", "best_effort_skipped")
    assert metadata["settings"]["refine"] is True
    assert (output / "watershed/success.json").is_file()
    assert (output / "resources.jsonl").stat().st_size > 0
    assert (output / "watershed/trace.jsonl").stat().st_size > 0


def test_native_rejects_false_observed_dataset_identity(tmp_path):
    identity_path = os.environ.get("HFX_TEST_CONSUMER_IDENTITY")
    fixture_path = os.environ.get("HFX_TEST_LOCAL_FIXTURE")
    if not identity_path or not fixture_path:
        pytest.skip("requires explicit pinned wheel identity and tiny local fixture")
    request = DelineationRequest.model_validate(
        {
            "dataset": {
                "uri": str(Path(fixture_path).resolve()),
                "fabric_name": "WRONG-FABRIC",
                "fabric_version": "WRONG-VERSION",
                "manifest_sha256": "0" * 64,
                "attribution": "fault-injection fixture",
                "license": "MIT",
            },
            "consumer": json.loads(Path(identity_path).read_text()),
            "input_outlet": [2.5, -2.5],
            "settings": {},
        }
    )
    output = tmp_path / "false-identity"
    with pytest.raises(RuntimeError, match="consumer process failed"):
        execute_request(request, output, None, ResourceLimits())
    assert not (output / "watershed/success.json").exists()


def test_native_consumer_rejects_changed_installed_facade(tmp_path):
    """Tamper a wheel package copy; shared installed dependency remains untouched."""
    identity_path = os.environ.get("HFX_TEST_CONSUMER_IDENTITY")
    if not identity_path:
        pytest.skip("requires explicit pinned wheel identity")
    import subprocess
    import sys
    import zipfile

    identity = json.loads(Path(identity_path).read_bytes())
    copy = tmp_path / "wheel-copy"
    with zipfile.ZipFile(identity["wheel_path"]) as wheel:
        for member in wheel.infolist():
            if member.filename.startswith("pourpoint/") and not member.is_dir():
                target = copy / member.filename
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(wheel.read(member))
    facade = copy / "pourpoint/__init__.py"
    facade.write_bytes(
        facade.read_bytes() + b"\n# deliberate integrity regression fault\n"
    )
    script = f"""
import sys
from pathlib import Path
sys.path.insert(0, {str(copy)!r})
from watershed_comparison.models import ConsumerIdentity
from watershed_comparison.delineation import verify_consumer
identity = ConsumerIdentity.model_validate_json(Path({identity_path!r}).read_bytes())
try:
    verify_consumer(identity)
except ValueError as error:
    assert 'wheel' in str(error)
else:
    raise AssertionError('changed facade accepted')
"""
    subprocess.run([sys.executable, "-c", script], check=True, timeout=30)


def test_native_consumer_rejects_unmatched_loaded_dependency_inventory(monkeypatch):
    """Fault inject empty dyld inventory, not a native delineation success proxy."""
    identity_path = os.environ.get("HFX_TEST_CONSUMER_IDENTITY")
    if not identity_path:
        pytest.skip("requires explicit pinned wheel identity")
    import ctypes
    from types import SimpleNamespace

    from watershed_comparison.delineation import verify_consumer
    from watershed_comparison.models import ConsumerIdentity

    identity = ConsumerIdentity.model_validate_json(Path(identity_path).read_bytes())
    no_images = SimpleNamespace(
        _dyld_image_count=lambda: 0, _dyld_get_image_name=lambda _: None
    )
    monkeypatch.setattr(ctypes, "CDLL", lambda _: no_images)
    with pytest.raises(ValueError, match="loaded"):
        verify_consumer(identity)


def test_native_consumer_rejects_unbound_system_data_receipt(tmp_path):
    identity_path = os.environ.get("HFX_TEST_CONSUMER_IDENTITY")
    if not identity_path:
        pytest.skip("requires explicit pinned wheel identity")
    from types import SimpleNamespace

    from watershed_comparison.delineation import verify_consumer

    raw = json.loads(Path(identity_path).read_bytes())
    changed = tmp_path / "runtime-data.json"
    changed.write_text("{}")
    raw.update(
        runtime_data_receipt_path=str(changed), runtime_data_receipt_sha256="0" * 64
    )
    with pytest.raises(ValueError, match="runtime data"):
        verify_consumer(SimpleNamespace(**raw))


def test_worker_does_not_execute_unchecked_cached_facade(tmp_path, monkeypatch):
    """Real worker/package-copy bytecode fault; shared installed package is untouched."""
    identity_path = os.environ.get("HFX_TEST_CONSUMER_IDENTITY")
    fixture_path = os.environ.get("HFX_TEST_LOCAL_FIXTURE")
    if not identity_path or not fixture_path:
        pytest.skip("requires explicit pinned wheel identity and tiny local fixture")
    import py_compile
    import subprocess
    import sys
    import zipfile

    from watershed_comparison import supervision

    identity = json.loads(Path(identity_path).read_bytes())
    copy = tmp_path / "wheel-copy"
    with zipfile.ZipFile(identity["wheel_path"]) as wheel:
        for member in wheel.infolist():
            if member.filename.startswith("pourpoint/") and not member.is_dir():
                target = copy / member.filename
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(wheel.read(member))
    facade = copy / "pourpoint/__init__.py"
    original = facade.read_bytes()
    marker = tmp_path / "unchecked-bytecode-executed"
    facade.write_bytes(
        original
        + f"\nfrom pathlib import Path\nPath({str(marker)!r}).write_text('stale bytecode executed')\n".encode()
    )
    py_compile.compile(
        str(facade),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
    )
    facade.write_bytes(original)
    original_popen = subprocess.Popen

    def copied_package_worker(command, **kwargs):
        script = f"import sys; sys.path.insert(0, {str(copy)!r}); from watershed_comparison.cli import main; sys.argv = {['hfx-watershed', *command[3:]]!r}; main()"
        return original_popen([sys.executable, "-c", script], **kwargs)

    monkeypatch.setattr(supervision.subprocess, "Popen", copied_package_worker)
    fixture = Path(fixture_path).resolve()
    manifest = json.loads((fixture / "manifest.json").read_bytes())
    request = DelineationRequest.model_validate(
        {
            "dataset": {
                "uri": str(fixture),
                "fabric_name": manifest["fabric_name"],
                "fabric_version": manifest.get("fabric_version"),
                "manifest_sha256": sha256(fixture / "manifest.json"),
                "attribution": "synthetic copied-wheel bytecode fault test",
                "license": "MIT",
            },
            "consumer": identity,
            "input_outlet": [2.5, -2.5],
            "settings": {},
        }
    )
    output = tmp_path / "native-bytecode"
    execute_request(request, output, None, ResourceLimits())
    assert (output / "watershed/success.json").exists()
    assert not marker.exists(), (
        "worker executed unchecked bytecode instead of verified facade source"
    )
