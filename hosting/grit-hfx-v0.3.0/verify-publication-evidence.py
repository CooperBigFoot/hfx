#!/usr/bin/env python3
"""verify_publication_evidence : EvidenceRecords -> Verified | Error (offline)"""

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path


class VerificationError(Exception):
    """Raised when immutable publication evidence is inconsistent."""


CANONICAL = "grit/hfx-v0.3.0/manifest.json"
CANDIDATE = (1426, "02339ff92cbfd1d2ea57bb5332cb843b98115cd7a7395f64c14fac78d2ed643c")
FORMER = (1132, "0935a7bc09b7c2636786082fd9fd9a669ea1b32c6e2e4d92cb3f8da531c083c4")
FILES = {
    "candidate-staging.json": "774e5205d3828785e44565ad6ab0f03293ff34a79bd0f8ba003a2c923b2c1d58",
    "canonical-publication.json": "59a69f404651514a6f6b12a6fac3d214c0ba3d4befda9cb8acaffc5fb5c2a86d",
    "containment.json": "cd62b33d445767e2d8f2636add533baf53262eaebc83e9adc9f0c815c971967e",
    "revert-rehearsal.json": "0f127c733a1c621535cedebef6ff7a9eedd29b29b640a723ee18b2541b95f84d",
    "candidate-removal.json": "193538b8f3f6dd75bada081df4d85b379f7c1238114efc8e83876ea03c16c575",
}
KEYS = {
    "candidate-staging.json": ["schema_version", "dataset", "base_url", "bucket", "canonical_key", "authorities", "siblings", "canonical_observations", "staging_events"],
    "canonical-publication.json": ["schema_version", "dataset", "canonical_key", "candidate_sibling_key", "pre_write_observation", "invocations", "write_events", "verdict", "exposure_window"],
    "containment.json": ["schema", "canonical_key", "publication", "rollback", "historical_2026_07_24_evidence"],
    "revert-rehearsal.json": ["schema_version", "dataset", "canonical_key", "revert_sibling", "canonical_before", "events", "canonical_after", "cleanup"],
    "candidate-removal.json": ["schema_version", "dataset", "canonical_key", "staging_record", "events"],
}


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def keys(value, expected, label):
    require(isinstance(value, dict) and list(value) == expected, f"{label} keys differ")


def stamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def pair(value):
    return value["bytes"], value["sha256"]


def load_records(root, check_identities=True):
    records = {}
    for name, digest in FILES.items():
        data = (root / name).read_bytes()
        if check_identities:
            require(hashlib.sha256(data).hexdigest() == digest, f"{name} identity differs")
        records[name] = json.loads(data)
    return records


def verify_records(records):
    for name, expected in KEYS.items():
        keys(records[name], expected, name)
        require(records[name]["canonical_key"] == CANONICAL, f"{name} canonical key differs")

    staging = records["candidate-staging.json"]
    keys(staging["authorities"], ["candidate", "former"], "staging authorities")
    require(pair(staging["authorities"]["candidate"]) == CANDIDATE, "candidate authority differs")
    require(pair(staging["authorities"]["former"]) == FORMER, "former authority differs")
    require(len(staging["siblings"]) == len(staging["staging_events"]) == 1, "staging event count differs")
    sibling, staged = staging["siblings"][0], staging["staging_events"][0]
    require(sibling["key"] != CANONICAL and staged["key"] == sibling["key"], "staging used canonical key")
    require(pair(sibling) == CANDIDATE and (staged["after_bytes"], staged["after_sha256"]) == CANDIDATE, "staged identity differs")
    observations = staging["canonical_observations"]
    require([item["phase"] for item in observations] == ["before_staging", "after_staging"], "staging brackets differ")
    require(all((item["bytes"], item["sha256"]) == FORMER for item in observations), "canonical changed while staging")
    require(stamp(observations[0]["observed_at"]) <= stamp(staged["observed_at"]) <= stamp(observations[1]["observed_at"]), "staging is outside observation bracket")

    publication = records["canonical-publication.json"]
    require(publication["candidate_sibling_key"] == sibling["key"], "publication sibling differs")
    require(pair(publication["pre_write_observation"]) == FORMER, "pre-write identity differs")
    require(len(publication["write_events"]) == 1 and sum(item["writes"] for item in publication["invocations"]) == 1, "publication did not make exactly one write")
    write = publication["write_events"][0]
    require(write["key"] == CANONICAL and pair(write) == CANDIDATE, "canonical write differs")
    require([item["action"] for item in publication["invocations"]] == ["written", "attested"], "publication invocation order differs")
    verdict, resume = publication["verdict"], publication["invocations"][1]
    require(verdict["result"] == "accepted" and verdict["basis"] == "canonical_full_body_read_back" and pair(verdict) == CANDIDATE, "read-back acceptance differs")
    require(stamp(write["completed_at"]) <= stamp(verdict["observed_at"]) <= stamp(resume["invoked_at"]), "resume preceded read-back acceptance")
    require(resume["writes"] == 0 and pair(resume["observed_identity"]) == CANDIDATE, "resume was not read-only attestation")

    rehearsal = records["revert-rehearsal.json"]
    expected_revert_key = f"grit/hfx-v0.3.0/manifest.{FORMER[1]}.json"
    require(rehearsal["revert_sibling"]["key"] == expected_revert_key and pair(rehearsal["revert_sibling"]) == FORMER, "rollback sibling identity differs")
    require(rehearsal["revert_sibling"]["key"] != CANONICAL, "rollback rehearsal used canonical key")
    require([item["action"] for item in rehearsal["events"]] == ["created", "read_back_verified"], "rollback rehearsal events differ")
    require(all(item["key"] == expected_revert_key and pair(item) == FORMER for item in rehearsal["events"]), "rollback read-back identity differs")
    require(all(pair(rehearsal[name]) == FORMER for name in ("canonical_before", "canonical_after")), "rollback rehearsal changed canonical identity")
    require(stamp(rehearsal["events"][0]["observed_at"]) <= stamp(rehearsal["events"][1]["observed_at"]) <= stamp(publication["pre_write_observation"]["observed_at"]), "rollback rehearsal chronology differs")

    containment = records["containment.json"]
    require(containment["publication"]["verdict"] == "accepted" and pair(containment["publication"]["candidate_identity"]) == CANDIDATE, "containment acceptance differs")
    require(pair(containment["rollback"]["identity"]) == FORMER and containment["rollback"]["authority_path"] == "manifest.former.json", "containment rollback differs")
    removal = records["candidate-removal.json"]
    require(removal["staging_record"] == "candidate-staging.json" and len(removal["events"]) == 1, "candidate removal reference differs")
    removed = removal["events"][0]
    require(removed["key"] == sibling["key"] and removed["key"] != CANONICAL, "candidate removal used canonical key")
    require((removed["before_bytes"], removed["before_sha256"], removed["after"]) == (*CANDIDATE, "not_found"), "removed candidate identity differs")
    require(stamp(containment["publication"]["verdict_recorded_at"]) < stamp(removed["observed_at"]), "candidate was removed before containment acceptance")
    require(rehearsal["cleanup"]["key"] == expected_revert_key and rehearsal["cleanup"]["key"] != CANONICAL, "rollback cleanup used canonical key")


def verify(root):
    verify_records(load_records(root))


def main():
    parser = argparse.ArgumentParser(description="Verify retained GRIT publication evidence offline")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    try:
        verify(args.root)
    except (OSError, UnicodeError, json.JSONDecodeError, VerificationError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("GRIT publication evidence verified offline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
