"""Exclusive-writer protocol regressions; no live provider support is implied."""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from conditional_writes_probe import ConditionalWritesProbe
from dataset_delivery import (
    DatasetDelivery, Evidence, ExclusiveWriterAuthorization, PublicationProtection,
    Refusal, TransferBudget, canonical, digest, main,
)
from test_conditional_writes_probe import ProbeStorage
import test_dataset_delivery as delivery_fixtures
from test_dataset_delivery import Storage, failure


class ExclusiveStorage(Storage):
    """Enforce conditional PUT, but deliberately ignore completion conditions."""

    def __init__(self):
        super().__init__()
        self.upload_counter = 0
        self.list_page_size = None

    def create_multipart_upload(self, **kw):
        self.record("create_multipart_upload", kw)
        assert kw["ACL"] == "private"
        self.upload_counter += 1
        upload_id = f"exclusive-upload-{self.upload_counter}"
        self.uploads[upload_id] = {"key": kw["Key"], "parts": {}}
        return {"UploadId": upload_id}

    def complete_multipart_upload(self, **kw):
        self.record("complete_multipart_upload", kw)
        upload = self.uploads[kw["UploadId"]]
        parts = sorted(upload["parts"].items())
        assert kw["MultipartUpload"]["Parts"] == [
            {"PartNumber": number, "ETag": etag} for number, (_, etag) in parts]
        identity = self.add(kw["Key"], b"".join(data for _, (data, _) in parts))
        del self.uploads[kw["UploadId"]]
        return {"ETag": identity.etag, "ResponseMetadata": {"HTTPStatusCode": 200}}

    def list_multipart_uploads(self, **kw):
        self.record("list_multipart_uploads", kw)
        entries = sorted((value["key"], upload_id) for upload_id, value in self.uploads.items()
                         if value["key"].startswith(kw["Prefix"]))
        marker = (kw.get("KeyMarker", ""), kw.get("UploadIdMarker", ""))
        entries = [entry for entry in entries if entry > marker]
        page = entries if self.list_page_size is None else entries[:self.list_page_size]
        response = {"Uploads": [{"Key": key, "UploadId": upload_id} for key, upload_id in page],
                    "IsTruncated": len(page) < len(entries)}
        if response["IsTruncated"]:
            response.update(NextKeyMarker=page[-1][0], NextUploadIdMarker=page[-1][1])
        return response

    def get_object(self, **kw):
        if "Range" in kw:
            return super().get_object(**kw)
        self.record("get_object", kw)
        data, identity = self.objects[kw["Key"]]
        if kw.get("IfMatch") != identity.etag:
            raise failure(412, "PreconditionFailed")
        if "VersionId" in kw:
            assert kw["VersionId"] == identity.version_id
        body = io.BytesIO(data)
        self.bodies.append(body)
        return {"Body": body, "ContentLength": len(data), "ETag": identity.etag,
                "ResponseMetadata": {"HTTPStatusCode": 200}}


class CliStorage(ExclusiveStorage):
    """CLI defaults use normal read buffers; retain all protocol assertions."""

    def get_object(self, **kw):
        response = super().get_object(**kw)
        previous = response["Body"]
        body = io.BytesIO(previous.getvalue())
        previous.close()
        self.bodies.append(body)
        response["Body"] = body
        return response

    def close(self):
        pass


class ExclusiveWriterTests(unittest.TestCase):
    # Reuse fixture construction only, not the strict-mode test methods.
    make_delivery = delivery_fixtures.DeliveryTests.make_delivery
    writes = delivery_fixtures.DeliveryTests.writes

    def setUp(self):
        delivery_fixtures.DeliveryTests.setUp(self)
        storage = ExclusiveStorage()
        storage.objects = self.storage.objects.copy()
        storage.counter = self.storage.counter
        self.storage = storage
        probe_directory = Path(self.temp.name) / "probe"
        probe_directory.mkdir()
        probe_storage = ProbeStorage()
        probe_storage.ignore_condition = "complete_multipart_upload"
        probe_evidence = Evidence(probe_directory)
        probe = ConditionalWritesProbe(probe_storage, self.anonymous, self.source.endpoint,
                                       self.source.region, self.source.bucket, probe_evidence,
                                       TransferBudget(60, 4096), "a" * 32)
        with self.assertRaisesRegex(Refusal, "ignored destination condition"):
            probe.run()
        self.probe = deepcopy(probe_evidence.value)
        self.probe_bytes = probe_evidence.path.read_bytes()
        self.scope = {"endpoint": self.source.endpoint, "region": self.source.region,
                      "bucket": self.source.bucket, "source_prefix": self.source.prefix,
                      "destination_prefix": "hfx/test/"}
        self.decision = {
            "decision": "proceed-with-exclusive-writer-publication",
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "approved_by": "Synthetic operator", "user_statement": "Proceed under the stated weaker guarantee.",
            "scope": self.scope.copy(),
            "guarantee": "Operational exclusive-writer protection, not provider-enforced atomic destination no-overwrite.",
        }
        self.decision_path = Path(self.temp.name) / "decision.json"
        self.binding_path = Path(self.temp.name) / "binding.json"
        self.bind()
        self.delivery = self.exclusive_delivery()

    def bind(self):
        self.decision_path.write_bytes(canonical(self.decision) + b"\n")
        self.binding = {"schema": "hfx-exclusive-writer-authorization-binding-v1",
                        "decision_sha256": digest(self.decision_path.read_bytes()),
                        "failed_probe_sha256": digest(self.probe_bytes), "scope": self.scope.copy(),
                        "recorded_at": datetime.now(timezone.utc).isoformat()}
        self.binding_path.write_bytes(canonical(self.binding) + b"\n")

    def authorization(self):
        return ExclusiveWriterAuthorization.load(self.decision_path, self.binding_path, self.probe_bytes)

    def exclusive_delivery(self):
        return DatasetDelivery(self.storage, self.anonymous, self.source, "hfx/test",
                               b"Private dataset README", self.evidence, TransferBudget(60, 1000000),
                               part_bytes=5 * 1024**2, range_bytes=7, buffer_bytes=3,
                               protection=PublicationProtection.EXCLUSIVE_WRITER)

    def run_delivery(self):
        return self.delivery.deliver(self.probe, authorization=self.authorization())

    def reservation_key(self):
        return "scratch/dataset-publication-ownership/" + digest(canonical({key: value for key, value in self.scope.items()
                         if key != "source_prefix"})) + "/owner.json"

    def dataset_writes(self):
        return [(op, kw) for op, kw in self.writes() if kw["Key"].startswith("hfx/test/")]

    def assert_no_activation(self):
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)

    def cli_arguments(self):
        directory = Path(self.temp.name)
        inventory = directory / "source-inventory.json"
        record = asdict(self.source)
        record.update(schema="hfx-preserved-dataset-v1")
        inventory.write_bytes(canonical(record))
        readme = directory / "README.md"
        readme.write_bytes(self.delivery.readme)
        probe = directory / "failed-probe.json"
        probe.write_bytes(self.probe_bytes)
        journal = directory / "cli-evidence"
        storage = CliStorage()
        storage.objects = self.storage.objects.copy()
        storage.counter = self.storage.counter
        self.storage = storage
        return ["dataset_delivery.py", "deliver", "--source-inventory", str(inventory),
                "--destination-prefix", "hfx/test", "--readme", str(readme),
                "--evidence-dir", str(journal), "--profile", "synthetic-profile",
                "--conditional-writes-evidence", str(probe), "--max-read-bytes", "1000000",
                "--exclusive-writer-decision", str(self.decision_path),
                "--exclusive-writer-authorization-binding", str(self.binding_path)]

    def invoke_cli(self, arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch("sys.argv", arguments), patch("dataset_delivery.boto3.Session") as session, \
                patch("dataset_delivery.boto3.client", return_value=self.anonymous), \
                patch.object(self.anonymous, "close", create=True), \
                redirect_stdout(stdout), redirect_stderr(stderr):
            session.return_value.client.return_value = self.storage
            status = main()
        return status, stdout.getvalue(), stderr.getvalue()

    def test_cli_explicit_flags_load_raw_authorization_and_deliver_full_dataset(self):
        arguments = self.cli_arguments() + ["--publication-protection", "exclusive-writer"]
        status, stdout, stderr = self.invoke_cli(arguments)
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(stdout.splitlines()[-1])
        self.assertEqual(result["status"], "verified-dataset")
        # Stdout points to the durable receipt; the receipt carries the guarantee.
        receipt = json.loads(Path(result["evidence"]).read_bytes())
        self.assertEqual(receipt["publication_protection"]["mode"], "exclusive-writer")
        self.assertEqual(receipt["publication_protection"]["decision_sha256"], digest(self.decision_path.read_bytes()))
        self.assertEqual(receipt["publication_protection"]["failed_probe_sha256"], digest(self.probe_bytes))
        self.assertEqual(receipt["publication_protection"]["authorization_binding_sha256"], digest(self.binding_path.read_bytes()))
        self.assertEqual(len(receipt["objects"]), 7)
        for obj in self.source.objects:
            self.assertEqual(receipt["objects"][obj.path]["verification"]["sha256"], obj.sha256)
            self.assertEqual(digest(self.storage.objects["hfx/test/" + obj.path][0]), obj.sha256)
        completed = [kw["Key"] for op, kw in self.dataset_writes()
                     if op in {"put_object", "complete_multipart_upload"}]
        self.assertEqual(completed[-1], "hfx/test/manifest.json")
        self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_cli_authorization_flags_cannot_implicitly_select_weaker_mode(self):
        status, stdout, stderr = self.invoke_cli(self.cli_arguments())
        self.assertEqual(status, 1)
        self.assertEqual(stdout, "")
        refusal = json.loads(stderr)
        self.assertEqual(refusal["status"], "refused")
        self.assertIn("requires explicit exclusive-writer mode", refusal["reason"])
        self.assertFalse(self.storage.events)
        self.assert_no_activation()

    def test_strict_default_refuses_failed_probe_without_any_storage_calls(self):
        self.delivery = self.make_delivery()
        with self.assertRaises(Refusal):
            self.delivery.deliver(self.probe)
        self.assertEqual(self.storage.events, [])

    def test_explicit_mode_requires_authorization_before_storage(self):
        with self.assertRaises(Refusal):
            self.delivery.deliver(self.probe)
        self.assertEqual(self.storage.events, [])

    def test_approved_success_keeps_full_hashes_manifest_last_private_and_weaker_receipt(self):
        before = self.storage.objects.copy()
        self.run_delivery()
        state = json.loads(self.evidence.path.read_bytes())
        self.assertEqual(state["status"], "verified-dataset")
        self.assertIn("publication_protection", state)
        protection = json.dumps(state["publication_protection"]).lower()
        self.assertIn("exclusive", protection)
        self.assertEqual(state["publication_protection"]["guarantee"], self.decision["guarantee"])
        final = {key for key in self.storage.objects if key.startswith("hfx/test/")}
        self.assertEqual(final, {"hfx/test/" + obj.path for obj in self.source.objects} | {"hfx/test/README.md"})
        completed = [kw["Key"] for op, kw in self.dataset_writes()
                     if op in {"put_object", "complete_multipart_upload"}]
        self.assertEqual(len(completed), 7)
        self.assertEqual(completed[-2:], ["hfx/test/README.md", "hfx/test/manifest.json"])
        for obj in self.source.objects:
            self.assertEqual(self.storage.objects[self.source.prefix + obj.path], before[self.source.prefix + obj.path])
            self.assertEqual(digest(self.storage.objects["hfx/test/" + obj.path][0]), obj.sha256)
            receipt = state["objects"][obj.path]["verification"]
            self.assertEqual((receipt["sha256"], receipt["bytes"]), (obj.sha256, obj.identity.bytes))
        self.assertEqual(state["objects"]["README.md"]["verification"]["sha256"], digest(self.delivery.readme))
        self.assertEqual(state["final_bytes"], sum(obj.identity.bytes for obj in self.source.objects) + len(self.delivery.readme))
        reservations = [kw for op, kw in self.writes() if op == "put_object" and kw["Key"] == self.reservation_key()]
        self.assertEqual(len(reservations), 1)
        self.assertEqual((reservations[0]["IfNoneMatch"], reservations[0]["ACL"]), ("*", "private"))
        self.assertTrue(all(body.closed for body in self.storage.bodies))
        self.assertTrue(all(kw["ACL"] == "private" for op, kw in self.writes()
                            if op in {"put_object", "create_multipart_upload"}))

    def test_raw_decision_whitespace_mutation_invalidates_binding(self):
        self.decision_path.write_bytes(self.decision_path.read_bytes() + b" ")
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_raw_probe_whitespace_mutation_invalidates_binding(self):
        self.probe_bytes += b" "
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_missing_decision_refuses_without_storage(self):
        self.decision_path.unlink()
        with self.assertRaises((Refusal, OSError)):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_invalid_decision_fields_refuse_even_with_matching_binding(self):
        original = deepcopy(self.decision)
        for field, value in (("decision", "proceed"), ("approved_by", ""), ("user_statement", ""),
                             ("guarantee", "Provider-enforced atomic no-overwrite."),
                             ("recorded_at", "2026-09-09T00:00:00")):
            with self.subTest(field=field):
                self.decision = deepcopy(original)
                self.decision[field] = value
                self.bind()
                with self.assertRaises(Refusal):
                    self.run_delivery()
                self.assertFalse(self.storage.events)

    def test_each_foreign_scope_field_refuses_before_storage(self):
        original = deepcopy(self.decision)
        for field in self.scope:
            with self.subTest(field=field):
                self.decision = deepcopy(original)
                self.decision["scope"][field] += "foreign"
                self.bind()
                with self.assertRaises(Refusal):
                    self.run_delivery()
                self.assertFalse(self.storage.events)

    def test_foreign_scope_matching_binding_still_refuses_delivery(self):
        self.scope["destination_prefix"] = "hfx/foreign/"
        self.decision["scope"] = self.scope.copy()
        self.bind()
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_binding_schema_mismatch_refuses(self):
        self.binding["schema"] = "untrusted-schema"
        self.binding_path.write_bytes(canonical(self.binding))
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_parsed_probe_must_match_authorized_raw_bytes(self):
        self.probe["last_failure"]["reason"] = "different failure"
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_inadequate_put_probe_refuses_even_when_bound(self):
        self.probe["operations"]["PutObject"]["replacement_status"] = 200
        self.probe_bytes = canonical(self.probe)
        self.bind()
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_duplicate_probe_upload_ids_refuse_even_when_bound(self):
        completions = [event for event in self.probe["events"] if event["operation"] == "complete_multipart_upload"]
        completions[1]["upload_id"] = completions[0]["upload_id"]
        self.probe_bytes = canonical(self.probe)
        self.bind()
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_foreign_reservation_refuses_without_dataset_writes(self):
        self.storage.add(self.reservation_key(), b'{"token":"foreign"}')
        with self.assertRaises((Refusal, ClientError)):
            self.run_delivery()
        self.assertFalse(self.dataset_writes())
        self.assertEqual(self.storage.objects[self.reservation_key()][0], b'{"token":"foreign"}')

    def test_same_journal_resume_keeps_reservation_and_does_not_write(self):
        self.run_delivery()
        reservation = self.storage.objects[self.reservation_key()]
        self.storage.events.clear()
        self.evidence = Evidence(Path(self.temp.name))
        self.delivery = self.exclusive_delivery()
        self.run_delivery()
        self.assertFalse(self.writes())
        self.assertEqual(self.storage.objects[self.reservation_key()], reservation)
        self.assertEqual(self.evidence.value["status"], "verified-dataset")

    def test_new_journal_cannot_adopt_reservation(self):
        def stop_after_reservation(op, kw):
            if op == "create_multipart_upload":
                raise Refusal("synthetic stop before first upload")
        self.storage.hook = stop_after_reservation
        with self.assertRaises(Refusal):
            self.run_delivery()
        reservation = self.storage.objects[self.reservation_key()]
        other = Path(self.temp.name) / "other-journal"
        other.mkdir()
        self.evidence = Evidence(other)
        self.delivery = self.exclusive_delivery()
        self.storage.hook = None
        self.storage.events.clear()
        with self.assertRaises((Refusal, ClientError)):
            self.run_delivery()
        self.assertFalse(self.dataset_writes())
        self.assertEqual(self.storage.objects[self.reservation_key()], reservation)

    def test_different_source_plan_cannot_evade_destination_reservation(self):
        def stop_after_reservation(op, kw):
            if op == "create_multipart_upload":
                raise Refusal("synthetic stop before first upload")
        self.storage.hook = stop_after_reservation
        with self.assertRaises(Refusal):
            self.run_delivery()
        reservation = self.storage.objects[self.reservation_key()]
        old_prefix = self.source.prefix
        self.source = replace(self.source, prefix="scratch/other-preserved/", record_sha256="b" * 64)
        for obj in self.source.objects:
            self.storage.objects[self.source.prefix + obj.path] = self.storage.objects[old_prefix + obj.path]
        self.scope["source_prefix"] = self.source.prefix
        self.decision["scope"] = self.scope.copy()
        self.bind()
        other = Path(self.temp.name) / "other-source-journal"
        other.mkdir()
        self.evidence = Evidence(other)
        self.delivery = self.exclusive_delivery()
        self.storage.hook = None
        self.storage.events.clear()
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.writes())
        self.assertEqual(self.storage.objects[self.reservation_key()], reservation)

    def test_changed_reservation_identity_during_copy_blocks_completion(self):
        def mutate(op, kw):
            if op == "upload_part_copy":
                self.storage.hook = None
                key = self.reservation_key()
                self.storage.add(key, self.storage.objects[key][0])
        self.storage.hook = mutate
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(any(op == "complete_multipart_upload" for op, _ in self.writes()))
        self.assert_no_activation()

    def test_changed_reservation_bytes_with_same_identity_blocks_completion(self):
        def mutate(op, kw):
            if op == "upload_part_copy":
                self.storage.hook = None
                key = self.reservation_key()
                data, identity = self.storage.objects[key]
                self.storage.objects[key] = (b"!" + data[1:], identity)
        self.storage.hook = mutate
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(any(op == "complete_multipart_upload" for op, _ in self.writes()))
        self.assert_no_activation()

    def test_unexpected_object_during_copy_blocks_completion(self):
        def mutate(op, kw):
            if op == "upload_part_copy":
                self.storage.hook = None
                self.storage.add("hfx/test/unexpected", b"foreign")
        self.storage.hook = mutate
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(any(op == "complete_multipart_upload" for op, _ in self.writes()))
        self.assert_no_activation()

    def test_unknown_upload_before_delivery_blocks_dataset_writes(self):
        self.storage.uploads["foreign"] = {"key": "hfx/test/CITATION.txt", "parts": {}}
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.dataset_writes())

    def test_unknown_upload_during_copy_blocks_completion(self):
        def mutate(op, kw):
            if op == "upload_part_copy":
                self.storage.hook = None
                self.storage.uploads["foreign"] = {"key": "hfx/test/foreign", "parts": {}}
        self.storage.hook = mutate
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(any(op == "complete_multipart_upload" for op, _ in self.writes()))

    def test_unknown_upload_on_second_listing_page_blocks_completion(self):
        self.storage.list_page_size = 1
        self.test_unknown_upload_during_copy_blocks_completion()
        pages = [kw for op, kw in self.storage.events if op == "list_multipart_uploads"]
        self.assertTrue(any("KeyMarker" in page and "UploadIdMarker" in page for page in pages))

    def test_uncertain_reservation_is_retained_and_never_retried(self):
        original = self.storage.put_object
        def uncertain(**kw):
            response = original(**kw)
            if kw["Key"] == self.reservation_key():
                raise failure(503, "RequestTimeout")
            return response
        self.storage.put_object = uncertain
        with self.assertRaises(ClientError):
            self.run_delivery()
        retained = self.storage.objects[self.reservation_key()]
        self.storage.put_object = original
        self.storage.events.clear()
        self.evidence = Evidence(Path(self.temp.name))
        self.delivery = self.exclusive_delivery()
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.writes())
        self.assertEqual(self.storage.objects[self.reservation_key()], retained)
        self.assert_no_activation()

    def test_uncertain_completion_is_retained_and_never_retried(self):
        original = self.storage.complete_multipart_upload
        def uncertain(**kw):
            original(**kw)
            raise failure(503, "RequestTimeout")
        self.storage.complete_multipart_upload = uncertain
        with self.assertRaises(ClientError):
            self.run_delivery()
        self.assert_no_activation()
        self.storage.complete_multipart_upload = original
        self.storage.events.clear()
        self.evidence = Evidence(Path(self.temp.name))
        self.delivery = self.exclusive_delivery()
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.writes())
        self.assert_no_activation()

    def test_readme_conditional_put_still_blocks_concurrent_writer(self):
        def race(op, kw):
            if op == "put_object" and kw["Key"] == "hfx/test/README.md":
                self.storage.add(kw["Key"], b"foreign README")
        self.storage.hook = race
        with self.assertRaises(ClientError) as caught:
            self.run_delivery()
        self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 412)
        self.assertEqual(self.storage.objects["hfx/test/README.md"][0], b"foreign README")
        self.assert_no_activation()

    def test_reservation_is_read_and_conditioned_before_every_publication(self):
        self.run_delivery()
        checked = False
        publications = 0
        for op, kw in self.storage.events:
            if op == "get_object" and kw["Key"] == self.reservation_key():
                self.assertIn("IfMatch", kw)
                checked = True
            if (op == "complete_multipart_upload" or
                    (op == "put_object" and kw["Key"] == "hfx/test/README.md")):
                self.assertTrue(checked, f"reservation was not re-read before {kw['Key']}")
                publications += 1
                checked = False
        self.assertEqual(publications, 7)

    def test_reservation_change_after_last_payload_blocks_readme(self):
        def mutate(op, kw):
            if op == "get_object" and kw["Key"] == "hfx/test/graph.parquet":
                self.storage.hook = None
                key = self.reservation_key()
                self.storage.add(key, self.storage.objects[key][0])
        self.storage.hook = mutate
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertNotIn("hfx/test/README.md", self.storage.objects)
        self.assert_no_activation()

    def test_reservation_disappearance_during_copy_blocks_completion(self):
        def mutate(op, kw):
            if op == "upload_part_copy":
                self.storage.hook = None
                del self.storage.objects[self.reservation_key()]
        self.storage.hook = mutate
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(any(op == "complete_multipart_upload" for op, _ in self.writes()))
        self.assert_no_activation()

    def test_unexpected_object_before_delivery_blocks_all_writes(self):
        self.storage.add("hfx/test/unexpected", b"foreign")
        with self.assertRaises(Refusal):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_weaker_mode_preserves_sha_failure_and_manifest_gate(self):
        self.storage.range_fault = "sha"
        with self.assertRaisesRegex(Refusal, "SHA-256 mismatch"):
            self.run_delivery()
        self.assert_no_activation()
        self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_weaker_mode_preserves_private_acl_gate(self):
        self.storage.public_acl = True
        with self.assertRaisesRegex(Refusal, "bucket ACL"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_weaker_mode_preserves_deadline_gate(self):
        self.delivery.budget.deadline = 0
        with self.assertRaisesRegex(Refusal, "runtime limit"):
            self.run_delivery()
        self.assertFalse(self.storage.events)


if __name__ == "__main__":
    unittest.main()
