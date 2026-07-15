#!/usr/bin/env python3
"""HydroBASINS HFX adapter command-line shell."""

from __future__ import annotations

import argparse
from pathlib import Path


FABRIC_NAME = "hydrobasins"
ADAPTER_VERSION = "0.1.0"
FORMAT_VERSION = "0.3.0"
TOPOLOGY = "tree"
HAS_UP_AREA = True
HAS_RASTERS = False
HAS_SNAP = False


def build_dataset(args: argparse.Namespace) -> None:
    """Reserve the build command for polygon ingestion in M1-S2."""
    raise NotImplementedError("HydroBASINS polygon ingestion begins in M1-S2")


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the HydroBASINS adapter command-line parser."""
    parser = argparse.ArgumentParser(
        description="Build standard without-lakes HydroBASINS Pfaf-12 HFX datasets."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build", help="build one regional Pfaf-12 dataset")
    build.add_argument("--region", required=True, help="HydroBASINS region code")
    build.add_argument(
        "--basins",
        type=Path,
        required=True,
        help="directory containing the regional Pfaf-12 polygon layer",
    )
    build.add_argument("--out", type=Path, required=True, help="output directory")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Dispatch the selected adapter command."""
    args = build_arg_parser().parse_args(argv)
    if args.command == "build":
        build_dataset(args)
        return 0
    raise AssertionError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
