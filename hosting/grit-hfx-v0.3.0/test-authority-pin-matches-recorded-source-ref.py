#!/usr/bin/env python3
"""Prove local authority pins equal the files recorded at inventory source_ref."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "verify-authority.py"
SPEC = importlib.util.spec_from_file_location("grit_authority_verifier", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load authority verifier")
verifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verifier)


def identity(data: bytes) -> tuple[int, str]:
    return len(data), hashlib.sha256(data).hexdigest()


def main() -> None:
    inventory_path = ROOT / "identity-inventory.json"
    inventory_before = inventory_path.read_bytes()
    inventory = json.loads(inventory_before)
    source_ref = inventory["source_ref"]
    object_by_path = {
        item["authority_path"]: item
        for item in inventory["objects"]
        if item["authority_path"] is not None
    }

    verifier.verify_authority(ROOT)
    for path, pinned in verifier.LOCAL_IDENTITIES.items():
        data = ((ROOT / path).read_bytes()
                if path in {"manifest.json", "manifest.former.json"}
                else verifier.source_ref_bytes(source_ref, path))
        recorded = object_by_path[path]["identity"]
        if identity(data) != pinned:
            raise AssertionError(
                f"{path} LOCAL_IDENTITIES pin differs from its recorded file"
            )
        if identity(data) != (recorded["bytes"], recorded["sha256"]):
            raise AssertionError(
                f"{path} inventory pin differs from its recorded file"
            )

    if identity((ROOT / "README.md").read_bytes()) == verifier.LOCAL_IDENTITIES["README.md"]:
        raise AssertionError(
            "current README does not witness the moving-checkout regression"
        )

    original = verifier.LOCAL_IDENTITIES["README.md"]
    verifier.LOCAL_IDENTITIES["README.md"] = (original[0], "0" * 64)
    try:
        verifier.verify_authority(ROOT)
    except verifier.VerificationError:
        pass
    else:
        raise AssertionError("altered README authority pin was accepted")
    finally:
        verifier.LOCAL_IDENTITIES["README.md"] = original

    try:
        verifier.source_ref_bytes("0" * 40, "README.md")
    except verifier.VerificationError:
        pass
    else:
        raise AssertionError("missing source_ref was accepted")

    if inventory_path.read_bytes() != inventory_before:
        raise AssertionError("identity inventory was modified")
    print("authority pins match the identity inventory source_ref")


if __name__ == "__main__":
    main()
