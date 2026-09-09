"""Explicit pending-part recovery through the real CLI and journal."""
import json
from pathlib import Path
from unittest.mock import patch

from dataset_delivery import InvocationInterrupted, digest
from test_exclusive_writer import ExclusiveWriterTests


class PendingPartRecoveryTests(ExclusiveWriterTests):
    def interrupted_cli(self):
        arguments = self.cli_arguments() + ["--publication-protection", "exclusive-writer"]
        original = self.storage.upload_part_copy
        def interrupt_after_copy(**kw):
            original(**kw)
            raise InvocationInterrupted("synthetic interruption after provider copy")
        with patch.object(self.storage, "upload_part_copy", interrupt_after_copy):
            self.assertEqual(self.invoke_cli(arguments)[0], 1)
        journal = Path(arguments[arguments.index("--evidence-dir") + 1]) / "delivery.json"
        state = json.loads(journal.read_bytes())
        self.assertEqual(state["objects"]["CITATION.txt"]["pending_part"], 1)
        self.assertEqual(state["objects"]["CITATION.txt"]["parts"], [])
        self.assertEqual(len(self.storage.uploads), 1)
        return arguments, journal

    def test_cli_interruption_explicit_reconciliation_then_full_sha_resume(self):
        arguments, journal = self.interrupted_cli()
        self.storage.events.clear()
        self.assertEqual(self.invoke_cli(arguments)[0], 1)
        self.assertFalse(self.writes())
        original = journal.read_bytes()
        recovery = arguments.copy()
        recovery[1] = "reconcile-part"
        recovery += ["--object-path", "CITATION.txt", "--confirm-exclusive-writer",
                     "--expected-journal-sha256", digest(original), "--expected-part-etag", '"part-1"']
        status, _, stderr = self.invoke_cli(recovery)
        self.assertEqual((status, stderr), (0, ""))
        self.assertFalse(self.writes())
        receipt = json.loads(journal.read_bytes())
        self.assertEqual(receipt["status"], "partial-unverified")
        event = receipt["part_recoveries"][-1]
        self.assertEqual(event["original_journal"].encode(), original)
        self.assertFalse(event["original_request_success_observed"])
        self.assertNotIn("verification", receipt["objects"]["CITATION.txt"])
        self.assertEqual(self.invoke_cli(arguments)[0], 0)
        receipt = json.loads(journal.read_bytes())
        self.assertEqual(receipt["status"], "verified-dataset")
        for obj in self.source.objects:
            self.assertEqual(receipt["objects"][obj.path]["verification"]["sha256"], obj.sha256)

    def test_deliver_option_value_cannot_select_recovery_or_preserve_acceptance(self):
        arguments = self.cli_arguments() + ["--publication-protection", "exclusive-writer"]
        journal = Path(arguments[arguments.index("--evidence-dir") + 1]) / "delivery.json"
        cases = ((["--max-seconds", "reconcile-part"], "first"),
                 (["--max-sec=reconcile-part"], "first"),
                 (["--max-sec", "reconcile-part"], "last"),
                 (["--exclusive-writer", "reconcile-part"], "last"))
        for invalid, position in cases:
            with self.subTest(invalid=invalid, position=position):
                self.assertEqual(self.invoke_cli(arguments)[0], 0)
                self.storage.events.clear()
                malformed = arguments + invalid
                if position == "last":
                    malformed.remove("deliver")
                    malformed.append("deliver")
                with self.assertRaises(SystemExit):
                    self.invoke_cli(malformed)
                self.assertFalse(self.storage.events)
                self.assertEqual(json.loads(journal.read_bytes())["status"], "refused")

    def test_recovery_action_after_options_and_abbreviations_preserves_refusal(self):
        arguments, journal = self.interrupted_cli()
        original = journal.read_bytes()
        recovery = self.recovery_arguments(arguments, journal)
        recovery.remove("reconcile-part")
        recovery += ["reconcile-part", "--max-sec", "invalid"]
        recovery[recovery.index("--profile")] = "--prof"
        self.storage.events.clear()
        with self.assertRaises(SystemExit):
            self.invoke_cli(recovery)
        self.assertEqual(journal.read_bytes(), original)
        self.assertFalse(self.storage.events)
        recovery[-1] = "60"
        self.assertEqual(self.invoke_cli(recovery)[0], 0)
        self.assertFalse(self.writes())

    def recovery_arguments(self, arguments, journal, path="CITATION.txt", etag='"part-1"'):
        result = arguments.copy()
        result[1] = "reconcile-part"
        return result + ["--object-path", path, "--confirm-exclusive-writer",
                         "--expected-journal-sha256", digest(journal.read_bytes()),
                         "--expected-part-etag", etag]

    def test_cli_reconciliation_refuses_ambiguity_without_journal_or_provider_writes(self):
        from copy import deepcopy
        arguments, journal = self.interrupted_cli()
        original = journal.read_bytes()
        objects, uploads = deepcopy(self.storage.objects), deepcopy(self.storage.uploads)
        upload_id = next(iter(uploads))
        cases = ("missing", "extra", "size", "etag", "foreign", "destination", "source",
                 "reservation", "completing", "initializing", "intent", "authorization",
                 "confirmation", "journal-pin", "etag-pin", "second-list-drift", "deadline",
                 "second-list-foreign", "second-list-source", "second-list-reservation")
        for case in cases:
            with self.subTest(case=case):
                self.storage.objects, self.storage.uploads = deepcopy(objects), deepcopy(uploads)
                journal.write_bytes(original)
                self.storage.hook = None
                self.storage.events.clear()
                recovery = self.recovery_arguments(arguments, journal)
                parts = self.storage.uploads[upload_id]["parts"]
                data, etag = parts[1]
                if case == "missing":
                    parts.clear()
                elif case == "extra":
                    parts[2] = (b"extra", '"extra"')
                elif case == "size":
                    parts[1] = (data + b"!", etag)
                elif case == "etag":
                    parts[1] = (data, '"drift"')
                elif case == "foreign":
                    self.storage.uploads["foreign"] = {"key": "hfx/test/foreign", "parts": {}}
                elif case == "destination":
                    self.storage.add("hfx/test/CITATION.txt", data)
                elif case in {"source", "reservation"}:
                    key = self.source.prefix + "CITATION.txt" if case == "source" else self.reservation_key()
                    self.storage.add(key, self.storage.objects[key][0])
                elif case in {"completing", "initializing", "intent", "authorization"}:
                    value = json.loads(original)
                    state = value["objects"]["CITATION.txt"]
                    if case == "intent":
                        state["pending_part"] = 2
                    elif case == "authorization":
                        value["publication_protection"]["decision_sha256"] = "f" * 64
                    else:
                        state["status"] = case
                    journal.write_text(json.dumps(value))
                    recovery = self.recovery_arguments(arguments, journal)
                elif case == "confirmation":
                    recovery.remove("--confirm-exclusive-writer")
                elif case == "journal-pin":
                    recovery[recovery.index("--expected-journal-sha256") + 1] = "f" * 64
                elif case == "etag-pin":
                    recovery[-1] = '"foreign"'
                elif case == "deadline":
                    recovery += ["--max-seconds", "0"]
                elif case.startswith("second-list-"):
                    calls = []
                    def drift(op, kw):
                        if op == "list_parts":
                            calls.append(True)
                            if len(calls) == 2:
                                if case == "second-list-drift":
                                    self.storage.uploads[upload_id]["parts"][1] = (data, '"drift"')
                                elif case == "second-list-foreign":
                                    self.storage.uploads["foreign"] = {"key": "hfx/test/foreign", "parts": {}}
                                else:
                                    key = (self.source.prefix + "CITATION.txt" if case == "second-list-source"
                                           else self.reservation_key())
                                    self.storage.add(key, self.storage.objects[key][0])
                    self.storage.hook = drift
                before = journal.read_bytes()
                status, _, _ = self.invoke_cli(recovery)
                self.assertEqual(status, 1)
                self.assertEqual(journal.read_bytes(), before)
                self.assertFalse(self.writes())
                self.assertNotIn("hfx/test/manifest.json", self.storage.objects)

    def test_recovered_etag_is_not_content_integrity(self):
        arguments, journal = self.interrupted_cli()
        upload = next(iter(self.storage.uploads.values()))
        data, etag = upload["parts"][1]
        upload["parts"][1] = (b"!" + data[1:], etag)
        self.assertEqual(self.invoke_cli(self.recovery_arguments(arguments, journal))[0], 0)
        status, _, stderr = self.invoke_cli(arguments)
        self.assertEqual(status, 1)
        self.assertIn("SHA-256 mismatch", stderr)
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)

    def test_cli_multiple_parts_preceding_match_pagination_and_resume(self):
        from dataclasses import replace
        from dataset_delivery import DatasetDelivery
        payload = b"x" * (10 * 1024**2 + 3)
        identity = self.storage.add(self.source.prefix + "catchments.parquet", payload)
        self.source = replace(self.source, objects=tuple(
            replace(obj, identity=identity, sha256=digest(payload)) if obj.path == "catchments.parquet"
            else obj for obj in self.source.objects))
        arguments = self.cli_arguments() + ["--publication-protection", "exclusive-writer",
                                            "--max-read-bytes", "40000000"]
        journal = Path(arguments[arguments.index("--evidence-dir") + 1]) / "delivery.json"
        original_init = DatasetDelivery.__init__
        def small_parts(instance, *args, **kwargs):
            kwargs["part_bytes"] = 5 * 1024**2
            original_init(instance, *args, **kwargs)
        original_copy = self.storage.upload_part_copy
        def interrupt(**kw):
            result = original_copy(**kw)
            if kw["Key"].endswith("catchments.parquet") and kw["PartNumber"] == 2:
                raise InvocationInterrupted("interrupted second part after provider copy")
            return result
        with patch.object(DatasetDelivery, "__init__", small_parts):
            with patch.object(self.storage, "upload_part_copy", interrupt):
                self.assertEqual(self.invoke_cli(arguments)[0], 1)
            before = journal.read_bytes()
            state = json.loads(before)["objects"]["catchments.parquet"]
            self.assertEqual(len(state["parts"]), 1)
            self.assertEqual(state["pending_part"], 2)
            recovery = self.recovery_arguments(arguments, journal, "catchments.parquet", '"part-2"')
            upload = self.storage.uploads[state["upload_id"]]
            data, etag = upload["parts"][1]
            upload["parts"][1] = (data, '"changed-preceding"')
            self.storage.events.clear()
            self.assertEqual(self.invoke_cli(recovery)[0], 1)
            self.assertEqual(journal.read_bytes(), before)
            self.assertFalse(self.writes())
            upload["parts"][1] = (data, etag)
            original_list = self.storage.list_parts
            def paginated(**kw):
                response = original_list(**kw)
                parts = [p for p in response["Parts"] if p["PartNumber"] > kw.get("PartNumberMarker", 0)]
                return {"Parts": parts[:1], "IsTruncated": len(parts) > 1,
                        "NextPartNumberMarker": parts[0]["PartNumber"] if parts else None}
            with patch.object(self.storage, "list_parts", paginated):
                self.assertEqual(self.invoke_cli(recovery)[0], 0)
                self.assertFalse(self.writes())
                self.assertEqual(self.invoke_cli(arguments)[0], 0)
            value = json.loads(journal.read_bytes())
            self.assertEqual(value["status"], "verified-dataset")
            self.assertEqual(value["objects"]["catchments.parquet"]["verification"]["sha256"], digest(payload))
            copies = [kw["PartNumber"] for op, kw in self.writes()
                      if op == "upload_part_copy" and kw["Key"].endswith("catchments.parquet")]
            self.assertEqual(copies, [3])

    def test_reconciliation_uses_local_lock_and_atomic_evidence_replacement(self):
        import fcntl
        import os
        arguments, journal = self.interrupted_cli()
        recovery = self.recovery_arguments(arguments, journal)
        before = journal.read_bytes()
        self.storage.events.clear()
        with (journal.parent / "lock").open("r+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.invoke_cli(recovery)[0], 1)
        self.assertFalse(self.storage.events)
        self.assertEqual(journal.read_bytes(), before)
        with patch("dataset_delivery.os.replace", side_effect=OSError("synthetic replace failure")):
            self.assertEqual(self.invoke_cli(recovery)[0], 1)
        self.assertEqual(journal.read_bytes(), before)
        self.assertFalse(self.writes())
        actual_replace = os.replace
        def interrupt_after_replace(source, destination):
            actual_replace(source, destination)
            raise InvocationInterrupted("synthetic interruption after atomic reconciliation")
        with patch("dataset_delivery.os.replace", interrupt_after_replace):
            self.assertEqual(self.invoke_cli(recovery)[0], 1)
        value = json.loads(journal.read_bytes())
        self.assertEqual(value["status"], "partial-unverified")
        self.assertEqual(value["part_recoveries"][-1]["original_journal"].encode(), before)
        self.assertNotIn("pending_part", value["objects"]["CITATION.txt"])
        self.assertFalse(self.writes())


def load_tests(loader, tests, pattern):
    # The parent class supplies fixtures; its protocol tests run in its own module.
    import unittest
    return unittest.TestSuite(PendingPartRecoveryTests(name) for name in
                              PendingPartRecoveryTests.__dict__ if name.startswith("test_"))
