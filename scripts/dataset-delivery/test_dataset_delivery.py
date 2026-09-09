"""Local S3-shaped protocol tests; these do not establish provider support."""

from contextlib import redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest

from botocore.exceptions import ClientError

from dataset_delivery import (
    DatasetDelivery, DatasetObject, Evidence, ObjectIdentity, PreservedDataset,
    Refusal, TransferBudget, digest,
)


def failure(status, code):
    return ClientError({"Error": {"Code": code},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, "TestOperation")


class BoundedBody(io.BytesIO):
    def __init__(self, data, requests):
        super().__init__(data)
        self.requests = requests

    def read(self, size=-1):
        if not 0 < size <= 3:
            raise AssertionError(f"unbounded read: {size}")
        self.requests.append(size)
        return super().read(size)


class Storage:
    """Small in-memory objects with boto3-shaped request/response dictionaries."""

    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self.events = []
        self.read_sizes = []
        self.bodies = []
        self.hook = None
        self.range_fault = None
        self.public_acl = False
        self.policy_denied = False
        self.counter = 0

    def record(self, operation, request):
        self.events.append((operation, request.copy()))
        if self.hook:
            self.hook(operation, request)

    def add(self, key, data):
        self.counter += 1
        identity = ObjectIdentity(len(data), f'"etag-{self.counter}"',
                                  "2026-09-09T00:00:00+00:00", f"version-{self.counter}")
        self.objects[key] = (data, identity)
        return identity

    def head_object(self, **kw):
        self.record("head_object", kw)
        if kw["Key"] not in self.objects:
            raise failure(404, "NoSuchKey")
        identity = self.objects[kw["Key"]][1]
        return {"ContentLength": identity.bytes, "ETag": identity.etag,
                "LastModified": identity.last_modified, "VersionId": identity.version_id}

    def list_objects_v2(self, **kw):
        self.record("list_objects_v2", kw)
        return {"Contents": [{"Key": key, "Size": len(data)}
                             for key, (data, _) in self.objects.items()
                             if key.startswith(kw["Prefix"])], "IsTruncated": False}

    def get_bucket_policy(self, **kw):
        self.record("get_bucket_policy", kw)
        if self.policy_denied:
            raise failure(403, "AccessDenied")
        raise failure(404, "NoSuchBucketPolicy")

    def acl(self):
        grantee = {"Type": "CanonicalUser", "ID": "owner"}
        if self.public_acl:
            grantee = {"Type": "Group", "URI": "http://acs.amazonaws.com/groups/global/AllUsers"}
        return {"Owner": {"ID": "owner"},
                "Grants": [{"Grantee": grantee, "Permission": "FULL_CONTROL"}]}

    def get_bucket_acl(self, **kw):
        self.record("get_bucket_acl", kw)
        return self.acl()

    def get_object_acl(self, **kw):
        self.record("get_object_acl", kw)
        return self.acl()

    def create_multipart_upload(self, **kw):
        self.record("create_multipart_upload", kw)
        assert kw["ACL"] == "private"
        upload = f"upload-{len(self.uploads)}"
        self.uploads[upload] = {"key": kw["Key"], "parts": {}}
        return {"UploadId": upload}

    def list_parts(self, **kw):
        self.record("list_parts", kw)
        parts = self.uploads[kw["UploadId"]]["parts"]
        return {"Parts": [{"PartNumber": n, "ETag": etag, "Size": len(data)}
                          for n, (data, etag) in sorted(parts.items())], "IsTruncated": False}

    def upload_part_copy(self, **kw):
        self.record("upload_part_copy", kw)
        data, identity = self.objects[kw["CopySource"]["Key"]]
        assert kw["CopySourceIfMatch"] == identity.etag
        assert kw["CopySource"]["VersionId"] == identity.version_id
        if "CopySourceRange" in kw:
            if len(data) <= 5 * 1024**2:
                raise failure(400, "InvalidRequest")
            start, end = map(int, kw["CopySourceRange"].removeprefix("bytes=").split("-"))
            part = data[start:end + 1]
        else:
            part = data
        etag = f'"part-{kw["PartNumber"]}"'
        self.uploads[kw["UploadId"]]["parts"][kw["PartNumber"]] = (part, etag)
        return {"CopyPartResult": {"ETag": etag}}

    def complete_multipart_upload(self, **kw):
        self.record("complete_multipart_upload", kw)
        assert kw["IfNoneMatch"] == "*"
        if kw["Key"] in self.objects:
            raise failure(412, "PreconditionFailed")
        parts = self.uploads[kw["UploadId"]]["parts"]
        expected = [{"PartNumber": n, "ETag": etag} for n, (_, etag) in sorted(parts.items())]
        assert kw["MultipartUpload"]["Parts"] == expected
        identity = self.add(kw["Key"], b"".join(data for _, (data, _) in sorted(parts.items())))
        return {"ETag": identity.etag}

    def put_object(self, **kw):
        self.record("put_object", kw)
        assert kw["ACL"] == "private"
        assert kw["IfNoneMatch"] == "*"
        if kw["Key"] in self.objects:
            raise failure(412, "PreconditionFailed")
        return {"ETag": self.add(kw["Key"], kw["Body"]).etag}

    def get_object(self, **kw):
        self.record("get_object", kw)
        data, identity = self.objects[kw["Key"]]
        assert kw["IfMatch"] == identity.etag
        assert kw["VersionId"] == identity.version_id
        start, end = map(int, kw["Range"].removeprefix("bytes=").split("-"))
        payload = data[start:end + 1]
        fault = self.range_fault if kw["Key"].startswith("hfx/") else None
        if fault == "truncated":
            payload = payload[:-1]
        elif fault == "sha":
            payload = bytes(byte ^ 1 for byte in payload)
        elif fault == "overlong":
            payload += b"!"
        body = BoundedBody(payload, self.read_sizes)
        self.bodies.append(body)
        return {"Body": body, "ContentLength": end - start + 1,
                "ContentRange": "bad" if fault == "range" else f"bytes {start}-{end}/{len(data)}",
                "ETag": identity.etag, "ResponseMetadata": {"HTTPStatusCode": 206}}


class Anonymous:
    def __init__(self):
        self.status = 403

    def head_object(self, **kw):
        if self.status != 200:
            raise failure(self.status, "AccessDenied")
        return {}


class InventoryProvenanceTests(unittest.TestCase):
    def test_selected_inventory_matches_preserved_campaign(self):
        repository = Path(__file__).resolve().parents[2]
        inventory = repository / "hosting/tdx-hydro-nga-20230126-global-62basin-hfx-0.3.0-af443be35774/source-inventory.json"
        campaign_path = repository / "scripts/hetzner/CAMPAIGN-tdx-hydro-extension.json"
        campaign = json.loads(campaign_path.read_bytes())
        preservation = next(entry for entry in campaign["preservation"] if entry["category"] == "extension")
        source = PreservedDataset.load(inventory)
        actual = {obj.path: obj.sha256 for obj in source.objects}
        self.assertEqual(set(actual), {"manifest.json", "catchments.parquet", "graph.parquet",
                                       "aux/snap_stems.parquet", "NOTICE", "CITATION.txt"})
        self.assertEqual(actual, {obj["path"]: obj["sha256"] for obj in preservation["objects"]})
        self.assertEqual(len(source.objects), 6)
        self.assertEqual(sum(obj.identity.bytes for obj in source.objects), 142657755797)
        self.assertEqual(preservation["listed_bytes"], 142657755797)
        self.assertEqual(f"s3://{source.bucket}/{source.prefix}", preservation["prefix"])
        self.assertEqual(actual["manifest.json"], "af443be357742550ea76ef774b83a1f86e683ea828bb796c9ca893425777be85")
        self.assertEqual(actual["manifest.json"], campaign["coverage"]["observed_extension_manifest"]["sha256"])
        self.assertEqual(source.record_sha256, digest(inventory.read_bytes()))


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = Storage()
        self.anonymous = Anonymous()
        paths = ("manifest.json", "catchments.parquet", "graph.parquet", "aux/snap_stems.parquet", "NOTICE", "CITATION.txt")
        objects = []
        for path in paths:
            data = (path + " payload").encode()
            identity = self.storage.add("scratch/preserved/" + path, data)
            objects.append(DatasetObject(path, digest(data), identity))
        self.source = PreservedDataset("https://storage.example", "test-1", "private-bucket",
                                       "scratch/preserved/", tuple(objects), "a" * 64)
        self.evidence = Evidence(Path(self.temp.name))
        self.probe = {"schema": "hfx-conditional-writes-probe-v1",
                      "endpoint": self.source.endpoint, "bucket": self.source.bucket,
                      "region": self.source.region, "observed_at": datetime.now(timezone.utc).isoformat(),
                      "operations": {name: {"condition": "IfNoneMatch:*", "initial_status": 200,
                                            "replacement_status": 412, "initial_sha256": "a" * 64,
                                            "replacement_sha256": "b" * 64, "readback_sha256": "a" * 64,
                                            "key": "scratch/dataset-delivery-probes/" + name}
                                     for name in ("PutObject", "CompleteMultipartUpload")}}
        self.probe.update(status="verified", events=[], uploads=[])
        for name, api in (("PutObject", "put_object"), ("CompleteMultipartUpload", "complete_multipart_upload")):
            key = self.probe["operations"][name]["key"]
            for index, status in enumerate((200, 412)):
                event = {"operation": api, "key": key, "if_none_match": "*",
                         "response": {"ResponseMetadata": {"HTTPStatusCode": status}}}
                if name == "CompleteMultipartUpload":
                    event["upload_id"] = f"probe-upload-{index}"
                    self.probe["uploads"].append({"key": key, "upload_id": event["upload_id"]})
                self.probe["events"].append(event)
                self.probe["events"].append({"operation": "full-readback", "key": key,
                                             "sha256": "a" * 64, "bytes": 1})
        self.delivery = self.make_delivery()
        self.output = io.StringIO()
        self.redirect = redirect_stdout(self.output)
        self.redirect.__enter__()
        self.addCleanup(self.redirect.__exit__, None, None, None)

    def make_delivery(self, max_bytes=10000):
        return DatasetDelivery(self.storage, self.anonymous, self.source, "hfx/test",
                               b"Private dataset README", self.evidence,
                               TransferBudget(60, max_bytes), part_bytes=5 * 1024**2,
                               range_bytes=7, buffer_bytes=3)

    def writes(self):
        return [(op, kw) for op, kw in self.storage.events
                if op in {"create_multipart_upload", "upload_part_copy", "complete_multipart_upload", "put_object"}]

    def run_delivery(self):
        self.delivery.deliver(self.probe)

    def test_success_full_hashes_manifest_last_private_conditional_and_bounded_reads(self):
        before = dict(self.storage.objects)
        self.run_delivery()
        state = json.loads(self.evidence.path.read_bytes())
        self.assertEqual(state["status"], "verified-dataset")
        completed = [kw["Key"] for op, kw in self.writes()
                     if op in {"complete_multipart_upload", "put_object"}]
        self.assertEqual(completed[-2:], ["hfx/test/README.md", "hfx/test/manifest.json"])
        self.assertEqual(len(completed), 7)
        for obj in self.source.objects:
            receipt = state["objects"][obj.path]["verification"]
            self.assertEqual((receipt["sha256"], receipt["bytes"]), (obj.sha256, obj.identity.bytes))
            progress = state["objects"][obj.path]["verification_progress"]
            self.assertEqual((progress["path"], progress["bytes"], progress["total"]),
                             (obj.path, obj.identity.bytes, obj.identity.bytes))
            self.assertIn("observed_at", progress)
            self.assertEqual(self.storage.objects[self.source.prefix + obj.path], before[self.source.prefix + obj.path])
        total = sum(obj.identity.bytes for obj in self.source.objects) + len(self.delivery.readme)
        self.assertEqual(self.delivery.budget.bytes, total + 2 * self.source.objects[0].identity.bytes)
        self.assertEqual(state["final_bytes"], total)
        self.assertTrue(self.storage.read_sizes)
        self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_recheck_persists_checking_before_first_storage_call(self):
        self.run_delivery()
        observed = []
        def inspect_checkpoint(operation, _request):
            if not observed:
                observed.append(json.loads(self.evidence.path.read_bytes()))
        self.storage.hook = inspect_checkpoint
        self.run_delivery()
        self.assertEqual(observed[0]["status"], "checking")
        self.assertEqual(observed[0]["verification_history"][-1]["status"], "verified-dataset")

    def test_sigterm_after_atomic_success_write_persists_refusal_and_history(self):
        import os
        import signal
        from dataset_delivery import InvocationInterrupted
        original_save = self.evidence.save
        interrupted = []
        def interrupt_saved_success():
            original_save()
            if self.evidence.value["status"] == "verified-dataset" and not interrupted:
                interrupted.append(True)
                os.kill(os.getpid(), signal.SIGTERM)
        self.evidence.save = interrupt_saved_success
        with self.assertRaisesRegex(InvocationInterrupted, "cancelled"):
            self.run_delivery()
        state = json.loads(self.evidence.path.read_bytes())
        self.assertEqual(state["status"], "refused")
        self.assertEqual(state["verification_history"][-1]["status"], "verified-dataset")
        self.assertIn("cancelled", state["last_failure"]["reason"])
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

    def test_failed_recheck_invalidates_current_success_receipt(self):
        self.run_delivery()
        self.storage.add("hfx/test/graph.parquet", b"replacement")
        with self.assertRaisesRegex(Refusal, "destination identity changed"):
            self.run_delivery()
        state = json.loads(self.evidence.path.read_bytes())
        self.assertEqual(state["status"], "refused")
        self.assertIn("destination identity changed", state["last_failure"]["reason"])
        self.assertEqual(state["verification_history"][-1]["status"], "verified-dataset")

    def test_tiny_source_whole_copy_omits_range_and_keeps_conditions(self):
        self.run_delivery()
        copies = [kw for operation, kw in self.storage.events if operation == "upload_part_copy"]
        self.assertEqual(len(copies), len(self.source.objects))
        for request in copies:
            self.assertNotIn("CopySourceRange", request)
            identity = self.storage.objects[request["CopySource"]["Key"]][1]
            self.assertEqual(request["CopySourceIfMatch"], identity.etag)
            self.assertEqual(request["CopySource"]["VersionId"], identity.version_id)
        self.assertEqual(self.evidence.value["status"], "verified-dataset")

    def test_sha_mismatch_blocks_manifest_and_closes_stream(self):
        self.storage.range_fault = "sha"
        with self.assertRaisesRegex(Refusal, "SHA-256 mismatch"):
            self.run_delivery()
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)
        self.assertEqual(self.evidence.value["status"], "refused")
        self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_bad_streams_refuse(self):
        for fault, message in [("truncated", "truncated response"), ("range", "range response"), ("overlong", "overlong response")]:
            with self.subTest(fault=fault):
                self.storage.range_fault = fault
                with self.assertRaisesRegex(Refusal, message):
                    self.run_delivery()
                self.assertNotIn("hfx/test/manifest.json", self.storage.objects)
                self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_source_mutation_before_copy_refuses(self):
        self.delivery.preflight()
        key = self.source.prefix + "CITATION.txt"
        self.storage.add(key, self.storage.objects[key][0])
        with self.assertRaisesRegex(Refusal, "source identity changed"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_source_mutation_during_copy_refuses_completion(self):
        def mutate(op, kw):
            if op == "upload_part_copy":
                self.storage.hook = None
                key = kw["CopySource"]["Key"]
                data, identity = self.storage.objects[key]
                self.storage.objects[key] = (data, replace(identity, last_modified="changed"))
        self.storage.hook = mutate
        with self.assertRaisesRegex(Refusal, "identity changed before completion"):
            self.run_delivery()
        self.assertFalse(any(op == "complete_multipart_upload" for op, _ in self.writes()))

    def test_unexpected_destination_refuses(self):
        self.storage.add("hfx/test/unexpected", b"unrelated")
        with self.assertRaisesRegex(Refusal, "new destination must be absent"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_resume_verified_objects_reads_only_readme(self):
        self.run_delivery()
        self.storage.events.clear()
        self.evidence = Evidence(Path(self.temp.name))
        self.delivery = self.make_delivery()
        self.run_delivery()
        self.assertFalse(self.writes())
        self.assertEqual({kw["Key"] for op, kw in self.storage.events if op == "get_object"}, {"hfx/test/README.md", "scratch/preserved/manifest.json"})
        self.assertEqual(self.delivery.budget.bytes, len(self.delivery.readme) + 2 * self.source.objects[0].identity.bytes)

    def test_recorded_destination_change_refuses(self):
        self.run_delivery()
        key = "hfx/test/graph.parquet"
        self.storage.add(key, self.storage.objects[key][0])
        self.storage.events.clear()
        with self.assertRaisesRegex(Refusal, "destination identity changed"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_destination_change_during_verification_refuses(self):
        def mutate(op, kw):
            if op == "get_object" and kw["Key"].startswith("hfx/"):
                self.storage.hook = None
                data, identity = self.storage.objects[kw["Key"]]
                self.storage.objects[kw["Key"]] = (data, replace(identity, last_modified="changed"))
        self.storage.hook = mutate
        with self.assertRaisesRegex(Refusal, "destination changed during verification"):
            self.run_delivery()
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)

    def test_resume_known_multipart_part_without_recopy(self):
        self.delivery.preflight()
        obj = next(obj for obj in self.source.objects if obj.path == "CITATION.txt")
        key = "hfx/test/" + obj.path
        upload = self.storage.create_multipart_upload(Key=key, ACL="private")["UploadId"]
        data = self.storage.objects[self.source.prefix + obj.path][0]
        self.storage.uploads[upload]["parts"][1] = (data, '"known-part"')
        self.evidence.value["objects"][obj.path] = {
            "status": "copying", "upload_id": upload,
            "parts": [{"PartNumber": 1, "ETag": '"known-part"', "Size": len(data)}]}
        self.evidence.save()
        self.storage.events.clear()
        self.evidence = Evidence(Path(self.temp.name))
        self.delivery = self.make_delivery()
        self.run_delivery()
        self.assertFalse(any(op == "upload_part_copy" and kw["Key"] == key for op, kw in self.writes()))
        self.assertEqual(self.evidence.value["status"], "verified-dataset")

    def test_uncertain_writes_are_not_repeated(self):
        for status, pending, message in [("initializing", False, "uncertain copy"),
                                         ("completing", False, "uncertain copy"),
                                         ("copying", True, "uncertain part")]:
            with self.subTest(status=status):
                self.delivery.preflight()
                state = {"status": status, "parts": [], "upload_id": "lost"}
                if pending:
                    state["pending_part"] = 1
                self.evidence.value["objects"]["CITATION.txt"] = state
                self.evidence.save()
                self.storage.events.clear()
                with self.assertRaisesRegex(Refusal, message):
                    self.run_delivery()
                self.assertFalse(self.writes())

    def test_uncertain_readme_is_not_repeated(self):
        self.delivery.preflight()
        self.evidence.value["objects"]["README.md"] = {"status": "putting"}
        self.evidence.save()
        with self.assertRaisesRegex(Refusal, "uncertain README"):
            self.run_delivery()
        self.assertFalse(any(op == "put_object" for op, _ in self.writes()))
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)

    def test_conditional_completion_blocks_concurrent_destination(self):
        def race(op, kw):
            if op == "complete_multipart_upload":
                self.storage.add(kw["Key"], b"concurrent writer")
        self.storage.hook = race
        with self.assertRaises(ClientError) as caught:
            self.run_delivery()
        self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 412)
        self.assertEqual(self.storage.objects["hfx/test/CITATION.txt"][0], b"concurrent writer")
        self.assertEqual(self.evidence.value["objects"]["CITATION.txt"]["status"], "completing")
        self.storage.hook = None
        self.storage.events.clear()
        with self.assertRaisesRegex(Refusal, "unexpected or uncertain"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_conditional_readme_blocks_concurrent_destination(self):
        def race(op, kw):
            if op == "put_object":
                self.storage.add(kw["Key"], b"concurrent README")
        self.storage.hook = race
        with self.assertRaises(ClientError) as caught:
            self.run_delivery()
        self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 412)
        self.assertEqual(self.storage.objects["hfx/test/README.md"][0], b"concurrent README")
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)

    def test_unknown_multipart_part_refuses(self):
        self.delivery.preflight()
        upload = self.storage.create_multipart_upload(Key="hfx/test/CITATION.txt", ACL="private")["UploadId"]
        self.storage.uploads[upload]["parts"][1] = (b"unknown", '"unknown"')
        self.evidence.value["objects"]["CITATION.txt"] = {
            "status": "copying", "upload_id": upload, "parts": []}
        self.evidence.save()
        self.storage.events.clear()
        with self.assertRaisesRegex(Refusal, "multipart journal differs"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_access_failure_refuses_before_writes(self):
        self.storage.policy_denied = True
        with self.assertRaisesRegex(Refusal, "bucket policy could not"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_public_acl_refuses(self):
        self.storage.public_acl = True
        with self.assertRaisesRegex(Refusal, "bucket ACL"):
            self.run_delivery()
        self.assertFalse(self.writes())

    def test_anonymous_access_requires_explicit_403(self):
        for status in (200, 404):
            with self.subTest(status=status):
                self.anonymous.status = status
                with self.assertRaisesRegex(Refusal, "anonymous"):
                    self.run_delivery()
                self.assertFalse(self.writes())

    def test_cancel_before_start_has_no_storage_calls(self):
        self.delivery.budget.cancelled = True
        with self.assertRaisesRegex(Refusal, "cancelled"):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_cancel_during_read_preserves_partial_and_closes_body(self):
        def cancel(op, kw):
            if op == "get_object" and kw["Key"].startswith("hfx/"):
                self.delivery.budget.cancelled = True
        self.storage.hook = cancel
        with self.assertRaisesRegex(Refusal, "cancelled"):
            self.run_delivery()
        self.assertEqual(self.evidence.value["status"], "refused")
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)
        self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_byte_limit_stops_full_transfer(self):
        limit = self.source.objects[0].identity.bytes + 3
        self.delivery = self.make_delivery(max_bytes=limit)
        with self.assertRaisesRegex(Refusal, "byte budget"):
            self.run_delivery()
        self.assertLessEqual(self.delivery.budget.bytes, limit)
        self.assertNotIn("hfx/test/manifest.json", self.storage.objects)
        self.assertTrue(all(body.closed for body in self.storage.bodies))

    def test_insufficient_range_budget_refuses_before_get(self):
        self.delivery = self.make_delivery(max_bytes=self.delivery.range_bytes - 1)
        with self.assertRaisesRegex(Refusal, "byte budget"):
            self.run_delivery()
        self.assertFalse(any(op == "get_object" for op, _ in self.storage.events))
        self.assertFalse(self.storage.bodies)
        self.assertEqual(self.delivery.budget.bytes, 0)
        self.assertFalse(self.writes())

    def test_expired_runtime_refuses_before_storage_calls(self):
        self.delivery.budget.deadline = 0
        with self.assertRaisesRegex(Refusal, "runtime limit"):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_stale_probe_refuses_before_storage_calls(self):
        self.probe["observed_at"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        with self.assertRaisesRegex(Refusal, "last 24 hours"):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_refreshed_probe_allows_verified_resume(self):
        self.run_delivery()
        original_hash = self.evidence.value["conditional_writes_evidence_sha256"]
        self.probe["observed_at"] = datetime.now(timezone.utc).isoformat()
        self.storage.events.clear()
        self.evidence = Evidence(Path(self.temp.name))
        self.delivery = self.make_delivery()
        self.run_delivery()
        self.assertFalse(self.writes())
        self.assertEqual(self.evidence.value["status"], "verified-dataset")
        history = self.evidence.value["conditional_writes_history"]
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["sha256"], original_hash)
        self.assertNotEqual(history[1]["sha256"], original_hash)
        self.assertEqual(history[1]["sha256"], self.evidence.value["conditional_writes_evidence_sha256"])
        self.assertEqual(history[1]["observed_at"], self.probe["observed_at"])

    def test_probe_event_mismatch_refuses_before_storage_calls(self):
        self.probe["events"][0]["response"]["ResponseMetadata"]["HTTPStatusCode"] = 201
        with self.assertRaisesRegex(Refusal, "operation records disagree"):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_probe_readback_mismatch_refuses_before_storage_calls(self):
        self.probe["events"][1]["sha256"] = "d" * 64
        with self.assertRaisesRegex(Refusal, "readback records disagree"):
            self.run_delivery()
        self.assertFalse(self.storage.events)

    def test_missing_conditional_proof_refuses_before_storage_calls(self):
        self.probe["operations"]["PutObject"]["replacement_status"] = 200
        with self.assertRaisesRegex(Refusal, "conditional-write enforcement"):
            self.run_delivery()
        self.assertFalse(self.storage.events)


if __name__ == "__main__":
    unittest.main()
