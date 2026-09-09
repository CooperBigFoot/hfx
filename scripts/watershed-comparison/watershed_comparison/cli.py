"""Command boundary for single delineation, sequential comparison and saved overlays."""

import argparse
import json
import stat
from pathlib import Path

from .models import ResourceLimits, read_request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    one = commands.add_parser("delineate", help="one native dataset request, no retry")
    one.add_argument("request", type=Path)
    one.add_argument("output", type=Path)
    pair = commands.add_parser(
        "compare", help="sequential verified private and supported public dataset"
    )
    pair.add_argument("first", type=Path)
    pair.add_argument("second", type=Path)
    pair.add_argument("output", type=Path)
    pair.add_argument("--credentials", type=Path, required=True)
    pair.add_argument("--limits", type=Path, required=True)
    pair.add_argument("--delivery-receipt", type=Path, required=True)
    plot = commands.add_parser("overlay", help="plot and measure two saved watersheds")
    plot.add_argument("first", type=Path)
    plot.add_argument("second", type=Path)
    plot.add_argument("output", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "delineate":
            from .delineation import delineate

            delineate(read_request(args.request), args.output.resolve())
        elif args.command == "compare":
            from .supervision import compare_sequentially

            if stat.S_IMODE(args.credentials.stat().st_mode) & 0o077:
                raise ValueError("credential file must be accessible only to its owner")
            credentials = json.loads(args.credentials.read_text())
            limits = ResourceLimits.model_validate_json(args.limits.read_bytes())
            compare_sequentially(
                read_request(args.first),
                read_request(args.second),
                args.output.resolve(),
                credentials,
                limits,
                args.delivery_receipt,
            )
        else:
            from .overlay import compare_saved_watersheds

            compare_saved_watersheds(args.first, args.second, args.output)
    except InterruptedError as exc:
        parser.exit(
            128 + getattr(exc, "signum", 2),
            "delineation cancelled; child stopped; evidence retained\n",
        )
    except Exception as exc:  # noqa: BLE001 - named credential-safe CLI isolation point
        # SDK errors can contain connection configuration. Never echo their text.
        parser.exit(
            1,
            f"{type(exc).__name__}: operation stopped; inspect retained evidence; do not retry without review\n",
        )


if __name__ == "__main__":
    main()
