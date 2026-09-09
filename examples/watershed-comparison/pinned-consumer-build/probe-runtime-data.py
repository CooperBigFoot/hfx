"""Record API-resolved system GDAL/PROJ data without changing the build receipt."""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import pourpoint

root = Path(__file__).resolve().parent

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

pourpoint._pourpoint._self_test_proj()
dyld = ctypes.CDLL(None)
dyld._dyld_image_count.restype = ctypes.c_uint32
dyld._dyld_get_image_name.argtypes = [ctypes.c_uint32]
dyld._dyld_get_image_name.restype = ctypes.c_char_p
images = [Path(dyld._dyld_get_image_name(i).decode()).resolve()
          for i in range(dyld._dyld_image_count())]
candidates = [path for path in images if path.name.startswith("libgdal.")]
if len(candidates) != 1:
    raise RuntimeError(f"expected one loaded GDAL library: {candidates}")
gdal_path = candidates[0]
gdal = ctypes.CDLL(str(gdal_path))
gdal.CPLGetConfigOption.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
gdal.CPLGetConfigOption.restype = ctypes.c_char_p
gdal.CPLFindFile.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
gdal.CPLFindFile.restype = ctypes.c_char_p
gdal.OSRGetPROJSearchPaths.argtypes = []
gdal.OSRGetPROJSearchPaths.restype = ctypes.POINTER(ctypes.c_char_p)
gdal.CSLDestroy.argtypes = [ctypes.POINTER(ctypes.c_char_p)]
gdal.CSLDestroy.restype = None

gdal.OSRGetPROJEnableNetwork.argtypes = []
gdal.OSRGetPROJEnableNetwork.restype = ctypes.c_int
if gdal.OSRGetPROJEnableNetwork() != 0:
    raise RuntimeError("PROJ network is enabled")
gdal.OSRGetPROJAuxDbPaths.argtypes = []
gdal.OSRGetPROJAuxDbPaths.restype = ctypes.POINTER(ctypes.c_char_p)
aux = gdal.OSRGetPROJAuxDbPaths()
aux_paths = []
if aux:
    try:
        index = 0
        while aux[index]:
            aux_paths.append(aux[index].decode())
            index += 1
    finally:
        gdal.CSLDestroy(aux)
if aux_paths:
    raise RuntimeError(f"auxiliary PROJ databases require separate inventory: {aux_paths}")

keys = ["GDAL_DATA", "PROJ_DATA", "PROJ_LIB", "PROJ_NETWORK", "GDAL_DRIVER_PATH", "PROJ_AUX_DB", "PROJ_USER_WRITABLE_DIRECTORY"]
config = {}
for key in keys:
    value = gdal.CPLGetConfigOption(key.encode(), None)
    config[key] = value.decode() if value else None

found = gdal.CPLFindFile(b"gdal", b"gdalvrt.xsd")
if not found:
    raise RuntimeError("CPLFindFile(gdal, gdalvrt.xsd) returned no path")
gdal_anchor = Path(found.decode()).resolve(strict=True)
proj_paths = []
ptr = gdal.OSRGetPROJSearchPaths()
if not ptr:
    raise RuntimeError("OSRGetPROJSearchPaths returned NULL")
try:
    index = 0
    while ptr[index]:
        proj_paths.append(ptr[index].decode())
        index += 1
finally:
    gdal.CSLDestroy(ptr)


def inventory(raw):
    path = Path(raw).resolve()
    if not path.exists():
        return {"api_path": raw, "resolved_path": str(path), "exists": False,
                "files_sha256": {}}
    if not path.is_dir():
        raise RuntimeError(f"data path is not directory: {path}")
    files = {}
    for member in sorted(path.rglob("*")):
        if member.is_file():
            files[str(member.relative_to(path))] = {
                "resolved_path": str(member.resolve(strict=True)), "sha256": sha(member)}
        elif not member.is_dir():
            raise RuntimeError(f"unexpected data directory entry: {member}")
    return {"api_path": raw, "resolved_path": str(path), "exists": True,
            "files_sha256": files}

result = {
    "schema": "hfx.comparison.runtime-data-receipt.v1",
    "source_sha": json.loads((root / "consumer-identity.json").read_text())["source_sha"],
    "build_receipt_path": str(root / "receipt.json"),
    "build_receipt_sha256": sha(root / "receipt.json"),
    "extension_sha256": sha(pourpoint._pourpoint.__file__),
    "loaded_gdal_path": str(gdal_path), "loaded_gdal_sha256": sha(gdal_path),
    "lookup_method": {
        "gdal": "CPLFindFile('gdal', 'gdalvrt.xsd') parent directory",
        "proj": "OSRGetPROJSearchPaths() ordered search directory list",
        "guarantee": "Current API lookup state after _self_test_proj in a fresh installed-wheel process. PROJ search paths are candidates, not a per-file access trace. GDAL anchor parent is the current gdalvrt.xsd lookup directory, not proof that every GDAL resource resolves there.",
        "coverage": "Every current regular file recursively under the GDAL anchor parent and every existing PROJ API search directory. Missing search directories are explicit and must remain absent for exact state equality. Recheck API paths and full inventories at execution; reject changes or undeclared overrides. Dynamic plugins, later API configuration and remote PROJ grids are outside this receipt."},
    "environment": {key: os.environ.get(key) for key in keys},
    "gdal_config_options": config,
    "proj_network_enabled": False, "proj_aux_db_paths": aux_paths,
    "gdal_anchor": {"api_lookup": "gdal/gdalvrt.xsd", "path": str(gdal_anchor), "sha256": sha(gdal_anchor)},
    "gdal_directory": inventory(str(gdal_anchor.parent)),
    "proj_search_directories": [inventory(path) for path in proj_paths],
}
print(json.dumps(result, indent=2))
