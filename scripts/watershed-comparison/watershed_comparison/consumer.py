"""verify : consumer build receipt × installed files × loaded libraries -> consumer API."""

from __future__ import annotations

import ctypes
import hashlib
import importlib
import importlib.util
import json
import sys
import zipfile
from pathlib import Path

from .models import sha256
from .native_data import verify_native_data


def verify_loaded_libraries(expected: dict[str, str]) -> dict:
    """Bind actual macOS dyld images to resolved build paths and file hashes.

    This is file/path identity, not in-memory attestation. Apple shared-cache
    images are separately disclosed by the build receipt. No basename matching.
    """
    if sys.platform != "darwin":
        raise ValueError(
            "this pinned native build requires macOS loaded-library verification"
        )
    dyld = ctypes.CDLL(None)
    count = dyld._dyld_image_count
    count.restype = ctypes.c_uint32
    count.argtypes = []
    name = dyld._dyld_get_image_name
    name.restype = ctypes.c_char_p
    name.argtypes = [ctypes.c_uint32]
    loaded = {}
    for index in range(count()):
        path = name(index)
        if path is not None:
            original = Path(path.decode())
            if original.is_file():
                loaded[str(original.resolve(strict=True))] = str(original)
    observed = {}
    for build_path, checksum in expected.items():
        resolved = str(Path(build_path).resolve(strict=True))
        if resolved not in loaded:
            raise ValueError(
                "loaded native dependency path differs from consumer build"
            )
        if sha256(Path(resolved)) != checksum:
            raise ValueError(
                "loaded native dependency file differs from consumer build"
            )
        observed[resolved] = {"loaded_path": loaded[resolved], "sha256": checksum}
    return observed


def verify_wheel_package(identity):
    """Hash wheel-owned package files, including facade, stubs and bundled data."""
    spec = importlib.util.find_spec("pourpoint")
    if spec is None or spec.origin is None:
        raise ValueError("consumer wheel package is not installed")
    directory = Path(spec.origin).resolve().parent
    expected = set()
    with zipfile.ZipFile(identity.wheel_path) as wheel:
        for item in wheel.infolist():
            if not item.filename.startswith("pourpoint/") or item.is_dir():
                continue
            relative = Path(item.filename).relative_to("pourpoint")
            if ".." in relative.parts:
                raise ValueError("consumer wheel has an unsafe package member")
            expected.add(relative.as_posix())
            installed = directory / relative
            if (
                not installed.is_file()
                or sha256(installed) != hashlib.sha256(wheel.read(item)).hexdigest()
            ):
                raise ValueError("installed consumer package file differs from wheel")
    if not expected or "__init__.py" not in expected:
        raise ValueError("consumer wheel has no Python facade")
    actual = {
        path.relative_to(directory).as_posix()
        for path in directory.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }
    if actual != expected:
        raise ValueError("installed consumer package inventory differs from wheel")
    return directory


def verify_consumer(identity):
    """Verify complete installed package and actual loaded native dependencies."""
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
    directory = verify_wheel_package(identity)
    package = importlib.import_module("pourpoint")
    extension = importlib.import_module("pourpoint._pourpoint")
    if (
        Path(package.__file__).resolve().parent != directory
        or sha256(Path(extension.__file__)) != identity.extension_sha256
    ):
        raise ValueError("imported consumer differs from wheel package")
    extension._self_test_proj()
    loaded = verify_loaded_libraries(receipt["native_library_sha256"])
    verify_native_data(identity, loaded)
    return package
