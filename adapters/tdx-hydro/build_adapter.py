#!/usr/bin/env python3
"""Build an NGA TDX-Hydro processing basin as an HFX dataset."""

from __future__ import annotations

import json
from pathlib import Path


CROSSWALK_PATH = Path(__file__).parent / "data" / "tdx_header_numbers.json"
GLOBAL_LINKNO_STRIDE = 10_000_000
TDX_LINKNO_SENTINEL = -1


def load_header_crosswalk(path: Path = CROSSWALK_PATH) -> dict[str, int]:
    """Load processing-basin header numbers from the vendored crosswalk."""
    with path.open(encoding="utf-8") as source:
        raw = json.load(source)

    if not isinstance(raw, dict):
        raise ValueError("TDX-Hydro header crosswalk must be a JSON object")

    try:
        crosswalk = {
            processing_basin_id: int(header_number)
            for processing_basin_id, header_number in raw.items()
        }
    except (TypeError, ValueError) as exc:
        raise ValueError("TDX-Hydro header numbers must be integers") from exc

    if any(not isinstance(key, str) or not key.isdigit() for key in crosswalk):
        raise ValueError("TDX-Hydro processing-basin IDs must be digit strings")
    if any(header_number <= 0 for header_number in crosswalk.values()):
        raise ValueError("TDX-Hydro header numbers must be positive")
    if len(set(crosswalk.values())) != len(crosswalk):
        raise ValueError("TDX-Hydro header numbers must be unique")

    return crosswalk


def global_linkno(linkno: int, header_number: int) -> int:
    """Return a Global LINKNO while preserving the native root sentinel."""
    if linkno == TDX_LINKNO_SENTINEL:
        return TDX_LINKNO_SENTINEL
    if linkno < 0:
        raise ValueError("native LINKNO must be non-negative or -1")
    if header_number <= 0:
        raise ValueError("header number must be positive")
    return linkno + header_number * GLOBAL_LINKNO_STRIDE
