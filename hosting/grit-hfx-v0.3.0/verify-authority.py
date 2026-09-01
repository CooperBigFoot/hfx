#!/usr/bin/env python3
"""verify_authority : AuthorityPackage × IdentityInventory → Verified | Error"""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path


class VerificationError(Exception):
    """Raised when an authority package assertion fails."""


INVENTORY_IDENTITY = (8252, "86c25402910af4c0050e97910c0eff966060cbe50503f9cb24bb44bb77ff402a")
LOCAL_IDENTITIES = {
    "manifest.json": (1426, "02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c"),
    "manifest.former.json": (1132, "0935a7bc09b7c2636786082fd9fd9a669ea1b32c6e2e4d92cb3f8da531c083c4"),
    "NOTICE": (1454, "eac224bf0b70b1494e5abd89f80079d665150ea744a2f730593f7216ca223db3"),
    "CITATION.txt": (2495, "8c7bf86a5962bf42282bbfd401226773601c2551f79685bad9be68d3b41363ac"),
    "README.md": (17601, "4edd32a056a3631b538ba54f872345a4448d44afe37ecc641472a348a1085f82"),
}
ROOT_KEYS = [
    "schema_version", "dataset", "base_url", "source_ref",
    "cross_repository_evidence_ref", "recorded_on", "observer",
    "executor_network_model", "candidate_provenance", "observed_transfer", "objects",
]
OBJECT_KEYS = [
    "role", "authority_path", "hosted_key", "identity",
    "verification_strength", "hosted_observation",
]
OBJECT_TUPLES = [
    ("candidate_manifest", "manifest.json", "manifest.json"),
    ("former_manifest", "manifest.former.json", "manifest.json"),
    ("hosted_cog", None, "aux/d8/flow_dir.tif"),
    ("hosted_cog", None, "aux/d8/flow_acc.tif"),
    ("hosted_attribution", "NOTICE", "NOTICE"),
    ("hosted_attribution", "CITATION.txt", "CITATION.txt"),
    ("hosted_attribution", "README.md", "README.md"),
]
PROVENANCE = {
    "origin": "accepted_planetary_build",
    "accepted_build_started_at": "2026-07-21T16:41:05Z",
    "accepted_build_ended_at": "2026-07-21T21:05:25Z",
    "created_at": "2026-07-21T21:05:12Z",
    "public_former_created_at": "2026-06-29T20:08:11Z",
    "derivation": "former_fields_with_accepted_build_created_at_then_pinned_d8_amendment",
    "measured_field_difference": "created_at_only",
    "serialization": "json.dumps(manifest, indent=2) + newline",
    "direct_public_former_amendment": {
        "bytes": 1426,
        "sha256": "fb79355f85f8a52ff7d693c0152aec2262c96ff7e59a3bc9357993a8e0c6a3e1",
        "matches_candidate": False,
    },
    "discrimination": [
        {"created_at": "2026-07-21T21:05:11Z", "sha256_prefix": "0eeda73b", "matches_candidate": False},
        {"created_at": "2026-07-21T21:05:12Z", "sha256": "02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c", "matches_candidate": True},
        {"created_at": "2026-07-21T21:05:13Z", "sha256_prefix": "b345d904", "matches_candidate": False},
    ],
}
D8 = {
    "schema": "hfx.aux.d8_raster.v2",
    "artifacts": {"flow_dir": "aux/d8/flow_dir.tif", "flow_acc": "aux/d8/flow_acc.tif"},
    "metadata": {"crs": "EPSG:8857", "flow_dir_encoding": "grass", "flow_acc_units": "km2"},
}


def require(condition, message):
    """Raise a concise verification error when condition is false."""
    if not condition:
        raise VerificationError(message)


def identity(data):
    """Return byte count and lowercase SHA-256 for data."""
    return len(data), hashlib.sha256(data).hexdigest()


def source_ref_bytes(source_ref, authority_path):
    """Read an authority input from the identity inventory's recorded Git tree."""
    repository = Path(__file__).resolve().parents[2]
    relative = f"hosting/grit-hfx-v0.3.0/{authority_path}"
    result = subprocess.run(
        ["git", "-C", str(repository), "show", f"{source_ref}:{relative}"],
        check=False,
        capture_output=True,
    )
    require(result.returncode == 0,
            f"cannot read {authority_path} at inventory source_ref")
    return result.stdout


def exact_type_tree(actual, expected, label):
    """Require recursive value, type, key, and key-order equality."""
    require(type(actual) is type(expected), f"{label} has wrong type")
    if isinstance(expected, dict):
        require(list(actual) == list(expected), f"{label} keys differ or are reordered")
        for key in expected:
            exact_type_tree(actual[key], expected[key], f"{label}.{key}")
    elif isinstance(expected, list):
        require(len(actual) == len(expected), f"{label} length differs")
        for index, (left, right) in enumerate(zip(actual, expected)):
            exact_type_tree(left, right, f"{label}[{index}]")
    else:
        require(actual == expected, f"{label} value differs")


def amend(manifest, substitute_created_at):
    """Apply the pinned D8 amendment to a parsed former manifest."""
    auxiliary = manifest.get("auxiliary")
    require(isinstance(auxiliary, list), "former auxiliary is not an array")
    if substitute_created_at:
        manifest["created_at"] = "2026-07-21T21:05:12Z"
    manifest["adapter_version"] = "grit-global-2.1.0"
    manifest["auxiliary"] = [
        entry
        for entry in auxiliary
        if not (isinstance(entry, dict) and entry.get("schema") == "hfx.aux.d8_raster.v2")
    ]
    manifest["auxiliary"].append(D8)
    return (json.dumps(manifest, indent=2) + "\n").encode("utf-8")


def verify_authority(root):
    """Verify one authority package without mutating it."""
    inventory_bytes = (root / "identity-inventory.json").read_bytes()
    require(identity(inventory_bytes) == INVENTORY_IDENTITY, "identity inventory bytes drifted")
    inventory = json.loads(inventory_bytes.decode("utf-8"))
    require(isinstance(inventory, dict), "identity inventory is not an object")
    require(list(inventory) == ROOT_KEYS, "identity inventory root keys differ or are reordered")
    exact_type_tree(inventory["candidate_provenance"], PROVENANCE, "candidate_provenance")
    exact_type_tree(
        inventory["observed_transfer"],
        {"total_body_bytes": 12048, "cog_body_bytes": 0, "full_cog_sha256_bytes_forbidden": 255756386559},
        "observed_transfer",
    )
    expected_scalars = {
        "schema_version": 1,
        "dataset": "grit-hfx-v0.3.0",
        "base_url": "https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/",
        "source_ref": "c1b19aec580ad13af8235ee6037e0b9b5081933d",
        "cross_repository_evidence_ref": "6f12abf2f7d47a31d7c1a4cdee99e30db7400bb6",
        "recorded_on": "2026-08-07",
        "observer": "orchestrator",
        "executor_network_model": "offline_zero_requests",
    }
    for key, value in expected_scalars.items():
        exact_type_tree(inventory[key], value, key)

    objects = inventory["objects"]
    require(isinstance(objects, list) and len(objects) == 7, "objects must contain seven entries")
    require(
        [(item["role"], item["authority_path"], item["hosted_key"]) for item in objects] == OBJECT_TUPLES,
        "object tuple order differs",
    )
    for item in objects:
        require(isinstance(item, dict) and list(item) == OBJECT_KEYS, "object keys differ or are reordered")
        require(list(item["identity"]) == ["bytes", "sha256", "sha256_basis"], "identity keys differ or are reordered")

    full_digest = re.compile(r"[0-9a-f]{64}").fullmatch
    prefix = re.compile(r"[0-9a-f]{8}").fullmatch
    for item in objects:
        require(bool(full_digest(item["identity"]["sha256"])), "object digest is not lowercase SHA-256")
    require(bool(full_digest(PROVENANCE["direct_public_former_amendment"]["sha256"])), "counterexample digest invalid")
    require(bool(full_digest(PROVENANCE["discrimination"][1]["sha256"])), "selected digest invalid")
    require(bool(prefix(PROVENANCE["discrimination"][0]["sha256_prefix"])), "lower neighbor prefix invalid")
    require(bool(prefix(PROVENANCE["discrimination"][2]["sha256_prefix"])), "upper neighbor prefix invalid")

    object_by_path = {item["authority_path"]: item for item in objects if item["authority_path"] is not None}
    checkout_authorities = {"manifest.json", "manifest.former.json"}
    for path, expected in LOCAL_IDENTITIES.items():
        data = ((root / path).read_bytes() if path in checkout_authorities
                else source_ref_bytes(inventory["source_ref"], path))
        measured = identity(data)
        require(measured == expected,
                f"{path} hard-coded identity differs at recorded source_ref")
        inventory_identity = (object_by_path[path]["identity"]["bytes"],
                              object_by_path[path]["identity"]["sha256"])
        require(measured == inventory_identity,
                f"{path} inventory identity differs at recorded source_ref")

    expected_observations = [
        None,
        {"observed_on":"2026-08-07","observer":"orchestrator","method":"GET_full_body","url":"https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/manifest.json","http_status":200,"content_length":None,"etag":None,"last_modified":None,"provider_checksum_sha256":None,"transferred_body_bytes":1132,"body_sha256":"0935a7bc09b7c2636786082fd9fd9a669ea1b32c6e2e4d92cb3f8da531c083c4","matches_authority_bytes":True,"claim":"full_body_sha256_measured"},
        {"observed_on":"2026-08-07","observer":"orchestrator","method":"HEAD_only","url":"https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/aux/d8/flow_dir.tif","http_status":200,"content_length":50686516478,"etag":"\"bc48d1013cf6908fb44c325dd2ad10ab-1511\"","last_modified":"Wed, 22 Jul 2026 16:16:52 GMT","provider_checksum_sha256":None,"transferred_body_bytes":0,"body_sha256":None,"matches_recorded_size":True,"matches_recorded_sha256":None,"claim":"content_length_and_etag_only"},
        {"observed_on":"2026-08-07","observer":"orchestrator","method":"HEAD_only","url":"https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/aux/d8/flow_acc.tif","http_status":200,"content_length":205069870081,"etag":"\"49eab3942a26036aa49e72ea33a1b724-6112\"","last_modified":"Wed, 22 Jul 2026 16:04:21 GMT","provider_checksum_sha256":None,"transferred_body_bytes":0,"body_sha256":None,"matches_recorded_size":True,"matches_recorded_sha256":None,"claim":"content_length_and_etag_only"},
        {"observed_on":"2026-08-07","observer":"orchestrator","method":"GET_full_body","url":"https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/NOTICE","http_status":200,"content_length":None,"etag":None,"last_modified":None,"provider_checksum_sha256":None,"transferred_body_bytes":1454,"body_sha256":"eac224bf0b70b1494e5abd89f80079d665150ea744a2f730593f7216ca223db3","matches_authority_bytes":True,"claim":"full_body_sha256_measured"},
        {"observed_on":"2026-08-07","observer":"orchestrator","method":"GET_full_body","url":"https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/CITATION.txt","http_status":200,"content_length":None,"etag":None,"last_modified":None,"provider_checksum_sha256":None,"transferred_body_bytes":2495,"body_sha256":"8c7bf86a5962bf42282bbfd401226773601c2551f79685bad9be68d3b41363ac","matches_authority_bytes":True,"claim":"full_body_sha256_measured"},
        {"observed_on":"2026-08-07","observer":"orchestrator","method":"GET_full_body","url":"https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/README.md","http_status":200,"content_length":None,"etag":None,"last_modified":None,"provider_checksum_sha256":None,"transferred_body_bytes":6967,"body_sha256":"2b86e8278996aa7540359e6b397c0a042f90c827e9c61730c81fca9eb3e63e56","matches_authority_bytes":False,"claim":"full_body_sha256_measured"},
    ]
    for index, expected in enumerate(expected_observations):
        exact_type_tree(objects[index]["hosted_observation"], expected, f"objects[{index}].hosted_observation")

    require(sum(x["hosted_observation"]["transferred_body_bytes"] for x in objects if x["hosted_observation"] and x["hosted_observation"]["method"] == "GET_full_body") == 12048, "body transfer sum differs")
    cogs = objects[2:4]
    require(sum(x["hosted_observation"]["transferred_body_bytes"] for x in cogs) == 0, "COG body transfer is nonzero")
    cog_hashes = ["eace32b63c4bc09e8172f03cce6dacfbf09a86c6b51c42b50c6cccd498d4d656", "30f16ba3238085289d87e72f3386fa152da7e9b56063f5d610422d20a79fc98b"]
    for item, expected_hash in zip(cogs, cog_hashes):
        observation = item["hosted_observation"]
        require(item["identity"]["sha256"] == expected_hash, "COG historical digest differs")
        require(item["identity"]["sha256_basis"] == "recorded_historical_build_identity_at_source_ref", "COG digest basis differs")
        require(item["verification_strength"] == "content_length_and_etag_only", "COG verification strength differs")
        require(observation["method"] == "HEAD_only" and observation["content_length"] == item["identity"]["bytes"], "COG size evidence differs")
        require(observation["matches_recorded_size"] is True, "COG size match is not true")
        require(re.fullmatch(r'"[0-9a-f]{32}-[0-9]+"', observation["etag"]) is not None, "COG ETag is not multipart")
        require(observation["provider_checksum_sha256"] is None and observation["body_sha256"] is None and observation["matches_recorded_sha256"] is None, "COG live digest was inferred")

    for index in (1, 4, 5):
        item = objects[index]
        observation = item["hosted_observation"]
        require((observation["transferred_body_bytes"], observation["body_sha256"]) == (item["identity"]["bytes"], item["identity"]["sha256"]), "hosted body does not equal authority identity")
        require(observation["matches_authority_bytes"] is True, "hosted authority match is not true")
    readme = objects[6]
    require((readme["identity"]["bytes"], readme["identity"]["sha256"]) == LOCAL_IDENTITIES["README.md"], "local README identity differs")
    require((readme["hosted_observation"]["transferred_body_bytes"], readme["hosted_observation"]["body_sha256"], readme["hosted_observation"]["matches_authority_bytes"]) == (6967, "2b86e8278996aa7540359e6b397c0a042f90c827e9c61730c81fca9eb3e63e56", False), "hosted README evidence differs")

    former_bytes = (root / "manifest.former.json").read_bytes()
    candidate_bytes = (root / "manifest.json").read_bytes()
    former = json.loads(former_bytes.decode("utf-8"))
    candidate = json.loads(candidate_bytes.decode("utf-8"))
    require(isinstance(former, dict) and isinstance(candidate, dict), "manifest is not a JSON object")
    require(former.get("format_version") == "0.3.0" and former.get("created_at") == "2026-06-29T20:08:11Z" and former.get("adapter_version") == "grit-global-2.0.0", "former fixed fields differ")
    former_aux = former.get("auxiliary")
    require(isinstance(former_aux, list), "former auxiliary is not an array")
    require(sum(isinstance(x, dict) and x.get("schema") == "hfx.aux.d8_raster.v2" for x in former_aux) == 0, "former contains D8 declaration")
    reproduced = amend(json.loads(former_bytes), True)
    require(reproduced == candidate_bytes, "candidate is not byte-identical to offline derivation")
    require(candidate.get("format_version") == "0.3.0" and candidate.get("created_at") == "2026-07-21T21:05:12Z" and candidate.get("adapter_version") == "grit-global-2.1.0", "candidate fixed fields differ")
    candidate_aux = candidate.get("auxiliary")
    require(isinstance(candidate_aux, list), "candidate auxiliary is not an array")
    d8_entries = [x for x in candidate_aux if isinstance(x, dict) and x.get("schema") == "hfx.aux.d8_raster.v2"]
    require(len(d8_entries) == 1 and d8_entries[0] == D8 and list(d8_entries[0]) == list(D8), "candidate D8 declaration differs")
    for nested in ("artifacts", "metadata"):
        require(list(d8_entries[0][nested]) == list(D8[nested]), f"candidate D8 {nested} order differs")
    stable = ["format_version", "fabric_name", "fabric_version", "crs", "has_up_area", "topology", "bbox", "unit_count"]
    require(all(former.get(key) == candidate.get(key) for key in stable), "stable former fields changed")
    require(set(former) == set(candidate) and set(former) - set(stable) == {"created_at", "adapter_version", "auxiliary"}, "manifest top-level fields differ")
    require([x for x in candidate_aux if not (isinstance(x, dict) and x.get("schema") == "hfx.aux.d8_raster.v2")] == former_aux, "non-D8 auxiliaries changed")
    counterexample = amend(json.loads(former_bytes), False)
    require(identity(counterexample) == (1426, "fb79355f85f8a52ff7d693c0152aec2262c96ff7e59a3bc9357993a8e0c6a3e1"), "direct-former counterexample identity differs")
    require(counterexample != candidate_bytes, "direct-former counterexample equals candidate")
    require(identity(candidate_bytes) == LOCAL_IDENTITIES["manifest.json"], "candidate final identity differs")
    require(identity(former_bytes) == LOCAL_IDENTITIES["manifest.former.json"], "former final identity differs")


def verify_public(root: Path) -> None:
    """Verify the live canonical declaration and unchanged hosted COG identities."""
    base = "https://basin-delineations-public.upstream.tech/grit/hfx-v0.3.0/"
    headers = {"User-Agent": "hfx-grit-authority-verifier/1"}
    request = urllib.request.Request(base + "manifest.json", headers=headers)
    with urllib.request.urlopen(request, timeout=60) as response:
        require(response.status == 200, "public manifest did not return HTTP 200")
        public_bytes = response.read(LOCAL_IDENTITIES["manifest.json"][0] + 1)
    require(identity(public_bytes) == LOCAL_IDENTITIES["manifest.json"],
            "public manifest identity differs from reviewed candidate")
    public_manifest = json.loads(public_bytes)
    declarations = [entry for entry in public_manifest.get("auxiliary", [])
                    if isinstance(entry, dict)
                    and entry.get("schema") == "hfx.aux.d8_raster.v2"]
    require(len(declarations) == 1 and declarations[0] == D8,
            "public manifest does not carry exactly one reviewed GRIT D8 declaration")
    require(identity((root / "manifest.former.json").read_bytes())
            == LOCAL_IDENTITIES["manifest.former.json"],
            "rollback authority identity differs")
    expected_cogs = {
        "aux/d8/flow_dir.tif": (50686516478, '"bc48d1013cf6908fb44c325dd2ad10ab-1511"'),
        "aux/d8/flow_acc.tif": (205069870081, '"49eab3942a26036aa49e72ea33a1b724-6112"'),
    }
    for key, (expected_length, expected_etag) in expected_cogs.items():
        head = urllib.request.Request(base + key, headers=headers, method="HEAD")
        with urllib.request.urlopen(head, timeout=60) as response:
            require(response.status == 200, f"hosted COG {key} did not return HTTP 200")
            require(int(response.headers.get("Content-Length", "-1")) == expected_length,
                    f"hosted COG {key} length differs")
            require(response.headers.get("ETag") == expected_etag,
                    f"hosted COG {key} ETag differs")
    print("GRIT HFX v0.3.0 public authority verified: manifest only changed; hosted COG identities unchanged")


def main():
    """Parse arguments, run verification, and optionally falsify corruptions."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--public", action="store_true")
    args = parser.parse_args()
    if args.self_test and args.public:
        parser.error("--self-test and --public cannot be combined")
    try:
        verify_authority(args.root)
        print("GRIT HFX v0.3.0 authority verified")
        if args.public:
            verify_public(args.root)
        if args.self_test:
            messages = [
                ("manifest.json", "PASS: candidate manifest byte drift rejected"),
                ("manifest.former.json", "PASS: former manifest byte drift rejected"),
            ]
            with tempfile.TemporaryDirectory() as temporary:
                for index, (path, message) in enumerate(messages):
                    copied = Path(temporary) / str(index)
                    shutil.copytree(args.root, copied)
                    corrupted = bytearray((copied / path).read_bytes())
                    corrupted[0] ^= 1
                    (copied / path).write_bytes(corrupted)
                    try:
                        verify_authority(copied)
                    except (VerificationError, OSError, UnicodeError, json.JSONDecodeError):
                        print(message)
                    else:
                        raise VerificationError(f"{path} corruption was accepted")
            print("authority corruption self-test passed")
    except (VerificationError, OSError, UnicodeError, json.JSONDecodeError,
            urllib.error.URLError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
