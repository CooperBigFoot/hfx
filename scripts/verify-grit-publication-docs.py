#!/usr/bin/env python3
"""Reject stale GRIT publication claims and incomplete attribution offline."""

import sys
from pathlib import Path

OFFERS = (
    "README.md",
    "hosting/README.md",
    "docs/reference/datasets.md",
    "adapters/README.md",
    "adapters/grit-v2/README.md",
    "hosting/grit-hfx-v0.3.0/README.md",
)
REQUIRED = (
    "10.5281/zenodo.15715535",
    "10.1029/2024WR038308",
    "CC BY-NC 4.0",
    "aux/d8/flow_dir.tif",
    "aux/d8/flow_acc.tif",
)
STALE = (
    "no refinement rasters",
    "does not include refinement rasters",
    "does not publish d8",
    "human-gated, unfired",
    "awaiting publication",
)
AUTHORITY = "hosting/grit-hfx-v0.3.0/AUTHORITY.md"
AUTHORITY_CURRENT = (
    "PUBLISHED AND ACCEPTED",
    "manifest.json` is the current canonical public body",
)
AUTHORITY_STALE = (
    "manifest.json` is future human-controlled publication material",
    "This package does not implement m1-s2, m2, m3, or m4",
    "Delivery is exactly one commit containing exactly the five authority files",
)
BANDS = {
    "adapters/grit-v2/README.md": ("uint8", "nodata `255`", "float32", "NaN nodata"),
    "hosting/grit-hfx-v0.3.0/README.md": ("uint8", "nodata `255`", "float32", "NaN nodata"),
}


def main():
    root = Path(__file__).resolve().parents[1]
    failures = []
    for relative in OFFERS:
        text = (root / relative).read_text(encoding="utf-8")
        for claim in REQUIRED:
            if claim not in text:
                failures.append(f"{relative}: missing {claim}")
        lowered = text.lower()
        for claim in STALE:
            if claim in lowered:
                failures.append(f"{relative}: stale claim {claim!r}")
        if ("hfx.aux.d8_raster.v2" not in text
                and "manifest declares the raster objects" not in lowered):
            failures.append(f"{relative}: missing current D8 publication state")
    authority = (root / AUTHORITY).read_text(encoding="utf-8")
    for claim in AUTHORITY_CURRENT:
        if claim not in authority:
            failures.append(f"{AUTHORITY}: missing current status {claim!r}")
    for claim in AUTHORITY_STALE:
        if claim in authority:
            failures.append(f"{AUTHORITY}: stale preparation status {claim!r}")
    for relative, claims in BANDS.items():
        text = (root / relative).read_text(encoding="utf-8")
        for claim in claims:
            if claim not in text:
                failures.append(f"{relative}: missing band fact {claim!r}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"verified {len(OFFERS)} GRIT dataset offers offline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
