"""verify : native data receipt × GDAL lookup state -> verified local data inventory."""

from __future__ import annotations

import ctypes
import json
from pathlib import Path

from .models import sha256


def _paths(gdal, function):
    lookup = getattr(gdal, function)
    lookup.argtypes = []
    lookup.restype = ctypes.POINTER(ctypes.c_char_p)
    pointer = lookup()
    paths = []
    if pointer:
        try:
            index = 0
            while pointer[index]:
                paths.append(pointer[index].decode())
                index += 1
        finally:
            gdal.CSLDestroy(pointer)
    return paths


def _verify_directory(entry, actual_path, *, empty_user_candidate=False):
    current = Path(actual_path).resolve()
    suffix = Path("Library/Application Support/proj")
    isolated_absence = (
        empty_user_candidate
        and not entry["exists"]
        and not current.exists()
        and Path(entry["resolved_path"]).parts[-3:] == suffix.parts
        and current == (Path.home() / suffix).resolve()
    )
    if not isolated_absence and (
        str(current) != entry["resolved_path"] or current.exists() != entry["exists"]
    ):
        raise ValueError("native data lookup directory differs from receipt")
    if not current.exists():
        return
    actual = {}
    for member in current.rglob("*"):
        if member.is_file():
            actual[str(member.relative_to(current))] = {
                "resolved_path": str(member.resolve(strict=True)),
                "sha256": sha256(member),
            }
        elif not member.is_dir():
            raise ValueError(
                "native data directory has an unsupported filesystem entry"
            )
    if actual != entry["files_sha256"]:
        raise ValueError("native data directory inventory differs from receipt")


def verify_native_data(identity, loaded: dict):
    """Check observed API search paths, disabled network and complete local inventories.

    GDAL's anchor parent establishes its gdalvrt.xsd lookup location, not every
    possible resource lookup. PROJ candidates are checked in API order. This is
    a local file/config identity check, not a trace of every resource consumed.
    """
    path = Path(identity.runtime_data_receipt_path)
    if sha256(path) != identity.runtime_data_receipt_sha256:
        raise ValueError("runtime data receipt checksum mismatch")
    receipt = json.loads(path.read_bytes())
    if (
        receipt["schema"] != "hfx.comparison.runtime-data-receipt.v1"
        or receipt["source_sha"] != identity.source_sha
        or receipt["extension_sha256"] != identity.extension_sha256
        or receipt["build_receipt_sha256"] != identity.build_receipt_sha256
    ):
        raise ValueError("runtime data receipt does not bind this consumer build")
    gdal_path = receipt["loaded_gdal_path"]
    if (
        gdal_path not in loaded
        or loaded[gdal_path]["sha256"] != receipt["loaded_gdal_sha256"]
    ):
        raise ValueError("runtime data lookup library is not the verified loaded GDAL")
    gdal = ctypes.CDLL(gdal_path)
    gdal.CSLDestroy.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
    gdal.CSLDestroy.restype = None
    gdal.CPLGetConfigOption.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
    gdal.CPLGetConfigOption.restype = ctypes.c_char_p
    for key, expected in receipt["gdal_config_options"].items():
        raw = gdal.CPLGetConfigOption(key.encode(), None)
        if (raw.decode() if raw else None) != expected:
            raise ValueError("native data configuration differs from receipt")
    gdal.OSRGetPROJEnableNetwork.argtypes = []
    gdal.OSRGetPROJEnableNetwork.restype = ctypes.c_int
    if gdal.OSRGetPROJEnableNetwork() != 0 or receipt["proj_network_enabled"]:
        raise ValueError("remote PROJ grid access is forbidden")
    if _paths(gdal, "OSRGetPROJAuxDbPaths") != receipt["proj_aux_db_paths"]:
        raise ValueError("PROJ auxiliary databases differ from receipt")
    gdal.CPLFindFile.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
    gdal.CPLFindFile.restype = ctypes.c_char_p
    raw = gdal.CPLFindFile(b"gdal", b"gdalvrt.xsd")
    if raw is None:
        raise ValueError("GDAL data anchor lookup failed")
    anchor = Path(raw.decode()).resolve(strict=True)
    if (
        str(anchor) != receipt["gdal_anchor"]["path"]
        or sha256(anchor) != receipt["gdal_anchor"]["sha256"]
    ):
        raise ValueError("GDAL data anchor differs from receipt")
    _verify_directory(receipt["gdal_directory"], str(anchor.parent))
    paths = _paths(gdal, "OSRGetPROJSearchPaths")
    entries = receipt["proj_search_directories"]
    if len(paths) != len(entries):
        raise ValueError("PROJ search path inventory differs from receipt")
    for index, (entry, current) in enumerate(zip(entries, paths, strict=True)):
        _verify_directory(entry, current, empty_user_candidate=index == 0)
    return {
        "gdal_anchor": str(anchor),
        "proj_search_paths": paths,
        "proj_network_enabled": False,
        "guarantee": "Existing data directories and complete inventories match the runtime data receipt. The first absent per-user PROJ candidate may relocate under isolated HOME only while remaining absent. API lookup state is checked; no per-resource access trace or immutable filesystem snapshot is claimed.",
    }
