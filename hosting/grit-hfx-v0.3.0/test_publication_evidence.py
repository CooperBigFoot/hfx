#!/usr/bin/env python3
"""Regression tests for immutable GRIT publication evidence."""

import copy
import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("publication_evidence", ROOT / "verify-publication-evidence.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PublicationEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.records = MODULE.load_records(ROOT)

    def reject(self, change):
        records = copy.deepcopy(self.records)
        change(records)
        with self.assertRaises(MODULE.VerificationError):
            MODULE.verify_records(records)

    def test_retained_records_verify_with_pinned_identities(self):
        MODULE.verify(ROOT)

    def test_exact_record_keys_are_required(self):
        self.reject(lambda r: r["candidate-staging.json"].update({"unexpected": True}))

    def test_staging_must_remain_inside_canonical_observation_bracket(self):
        self.reject(lambda r: r["candidate-staging.json"]["staging_events"][0].update(observed_at="2026-08-19T14:19:18Z"))

    def test_publication_must_contain_exactly_one_write(self):
        self.reject(lambda r: r["canonical-publication.json"]["invocations"][1].update(writes=1))

    def test_read_back_acceptance_must_precede_resume(self):
        self.reject(lambda r: r["canonical-publication.json"]["verdict"].update(observed_at="2026-08-19T15:59:32Z"))

    def test_containment_acceptance_must_precede_candidate_removal(self):
        self.reject(lambda r: r["candidate-removal.json"]["events"][0].update(observed_at="2026-08-20T22:40:00Z"))

    def test_rollback_sibling_must_have_former_identity(self):
        self.reject(lambda r: r["revert-rehearsal.json"]["revert_sibling"].update(bytes=1426))

    def test_staging_must_reject_canonical_key(self):
        def change(records):
            records["candidate-staging.json"]["siblings"][0]["key"] = MODULE.CANONICAL
            records["candidate-staging.json"]["staging_events"][0]["key"] = MODULE.CANONICAL
        self.reject(change)


if __name__ == "__main__":
    unittest.main()
