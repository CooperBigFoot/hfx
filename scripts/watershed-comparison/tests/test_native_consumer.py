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
                "fabric_version": "undeclared in pinned synthetic fixture",
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
