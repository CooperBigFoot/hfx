"""Bounded observed-manifest tests use actual local bytes, never network."""

import hashlib

import pytest

from watershed_comparison.manifest import MAX_MANIFEST_BYTES, observe_manifest
from watershed_comparison.models import DatasetIdentity


def identity(tmp_path, raw):
    return DatasetIdentity(
        uri=str(tmp_path),
        fabric_name="synthetic",
        fabric_version="1",
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        attribution="synthetic test",
        license="MIT",
    )


def test_observed_manifest_preserved_and_checked_before_use(tmp_path):
    raw = b'{"fabric_name":"synthetic","fabric_version":"1"}'
    (tmp_path / "manifest.json").write_bytes(raw)
    output = tmp_path / "output"
    output.mkdir()
    request = identity(tmp_path, raw)
    before = observe_manifest(request, output, "before", {})
    assert before["sha256"] == request.manifest_sha256
    assert (output / "manifest-before.json").read_bytes() == raw
    (tmp_path / "manifest.json").write_bytes(raw + b" ")
    with pytest.raises(ValueError, match="checksum"):
        observe_manifest(request, output, "after", {})
    assert (output / "manifest-after.json").exists()


def test_observation_rejects_wrong_fabric_even_with_correct_hash(tmp_path):
    raw = b'{"fabric_name":"different","fabric_version":"1"}'
    (tmp_path / "manifest.json").write_bytes(raw)
    with pytest.raises(ValueError, match="fabric identity"):
        observe_manifest(identity(tmp_path, raw), tmp_path, "before", {})


def test_manifest_observation_has_fixed_read_bound(tmp_path):
    raw = b" " * (MAX_MANIFEST_BYTES + 1)
    (tmp_path / "manifest.json").write_bytes(raw)
    with pytest.raises(ValueError, match="1 MiB"):
        observe_manifest(identity(tmp_path, raw), tmp_path, "before", {})
