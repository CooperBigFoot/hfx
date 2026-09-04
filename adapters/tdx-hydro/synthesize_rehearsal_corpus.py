#!/usr/bin/env python3
"""Synthesize a tiny deterministic TDX-Hydro source corpus for a campaign rehearsal.

synthesize : (ProcessingBasinIds, OutDir) -> SourceCorpus
             (one basins and one streamnet GeoPackage per processing basin id,
              plus a sha256sum manifest; content is a function of the ids alone)

Every processing basin id must be a key of the vendored header crosswalk. Each
basin is the two-unit canonical fixture from `test_build_adapter.canonical_frames`
translated east by one degree per position of the id in the sorted id list, so
basin extents never overlap. Each GeoPackage carries exactly one layer. GDAL's
`OGR_CURRENT_DATE` is pinned for the process so the GeoPackage bytes, and hence
the manifest digests, repeat across runs.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path
from typing import Sequence

import geopandas as gpd
import pyogrio

from build_adapter import load_header_crosswalk

try:
    from test_build_adapter import canonical_frames
except KeyError as error:
    raise RuntimeError(
        "synthesize_rehearsal_corpus imports the canonical fixture from test_build_adapter, "
        "which requires HFX_BINARY to name the hfx validator binary"
    ) from error

DEFAULT_MANIFEST_NAME = "source-sha256.txt"
BASINS_LAYER = "basins"
STREAMNET_LAYER = "streamnet"
LONGITUDE_STEP_DEGREES = 1.0
FIXTURE_LABEL_COLUMN = "label"
GPKG_LAST_CHANGE = "2026-01-01T00:00:00.000Z"
_PROCESSING_BASIN_ID_LENGTH = 10


class SynthesisRefusal(ValueError):
    """Raised when the requested corpus cannot be synthesized as asked."""


def validated_basin_ids(basin_ids: Sequence[str], crosswalk: dict[str, int]) -> tuple[str, ...]:
    """Return the sorted unique ids; refuse any id absent from the header crosswalk."""
    if not basin_ids:
        raise SynthesisRefusal("at least one --basin is required")
    for basin_id in basin_ids:
        if len(basin_id) != _PROCESSING_BASIN_ID_LENGTH or not basin_id.isdigit():
            raise SynthesisRefusal(f"processing basin id must be a 10-digit string: {basin_id!r}")
        if basin_id not in crosswalk:
            raise SynthesisRefusal(f"processing basin id is not in the TDX-Hydro header crosswalk: {basin_id}")
    if len(set(basin_ids)) != len(basin_ids):
        raise SynthesisRefusal(f"processing basin ids repeat: {sorted(basin_ids)}")
    return tuple(sorted(basin_ids))


def synthetic_pair(index: int) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """The canonical two-unit fixture shifted east by `index` degrees, without its label column."""
    basins, streamnet, _, _ = canonical_frames()
    shift = index * LONGITUDE_STEP_DEGREES
    basins = basins.drop(columns=[FIXTURE_LABEL_COLUMN])
    streamnet = streamnet.drop(columns=[FIXTURE_LABEL_COLUMN])
    basins = basins.set_geometry(basins.geometry.translate(xoff=shift))
    streamnet = streamnet.set_geometry(streamnet.geometry.translate(xoff=shift))
    return basins, streamnet


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_out_dir(out: Path) -> Path:
    out = out.expanduser()
    if out.exists():
        if not out.is_dir():
            raise SynthesisRefusal(f"--out exists and is not a directory: {out}")
        return out.resolve(strict=True)
    if not out.parent.is_dir():
        raise SynthesisRefusal(f"--out parent directory does not exist: {out.parent}")
    out.mkdir()
    return out.resolve(strict=True)


def synthesize_corpus(out: Path, basin_ids: Sequence[str], manifest_name: str = DEFAULT_MANIFEST_NAME) -> list[Path]:
    """Write the corpus and its manifest; return every written path, manifest last."""
    if not manifest_name or os.sep in manifest_name or manifest_name in {".", ".."}:
        raise SynthesisRefusal(f"--manifest must be a bare file name: {manifest_name!r}")
    ordered = validated_basin_ids(basin_ids, load_header_crosswalk())
    out_dir = _resolve_out_dir(out)
    pyogrio.set_gdal_config_options({"OGR_CURRENT_DATE": GPKG_LAST_CHANGE})
    written: list[Path] = []
    for index, basin_id in enumerate(ordered):
        basins, streamnet = synthetic_pair(index)
        basins_path = out_dir / f"{basin_id}-basins.gpkg"
        streamnet_path = out_dir / f"{basin_id}-streamnet.gpkg"
        for path in (basins_path, streamnet_path):
            if path.exists():
                raise SynthesisRefusal(f"refusing to overwrite an existing product: {path}")
        basins.to_file(basins_path, layer=BASINS_LAYER, driver="GPKG", engine="pyogrio")
        streamnet.to_file(streamnet_path, layer=STREAMNET_LAYER, driver="GPKG", engine="pyogrio")
        written.extend((basins_path, streamnet_path))
    manifest_path = out_dir / manifest_name
    if manifest_path.exists():
        raise SynthesisRefusal(f"refusing to overwrite an existing manifest: {manifest_path}")
    lines = [f"{_sha256(path)}  ./{path.name}" for path in sorted(written, key=lambda path: path.name)]
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return written + [manifest_path]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Synthesize a deterministic two-unit TDX-Hydro source corpus per processing basin id."
    )
    parser.add_argument("--out", required=True, type=Path, help="corpus directory (created if its parent exists)")
    parser.add_argument(
        "--basin",
        action="append",
        default=[],
        metavar="ID",
        help="10-digit processing basin id from data/tdx_header_numbers.json (repeatable)",
    )
    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST_NAME,
        help=f"sha256sum -c manifest file name inside --out (default {DEFAULT_MANIFEST_NAME})",
    )
    arguments = parser.parse_args(argv)
    try:
        written = synthesize_corpus(arguments.out, arguments.basin, arguments.manifest)
    except (SynthesisRefusal, OSError) as error:
        print(f"synthesize-rehearsal-corpus: refused: {error}", file=sys.stderr)
        return 1
    for path in written[:-1]:
        print(path)
    print(f"manifest: {written[-1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
