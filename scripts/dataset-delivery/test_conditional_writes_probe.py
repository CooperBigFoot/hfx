"""Local protocol simulation only; no provider compatibility claim or cloud I/O."""

from contextlib import redirect_stderr, redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from conditional_writes_probe import ConditionalWritesProbe, INITIAL, main
from dataset_delivery import DatasetDelivery, DatasetObject, Evidence, PreservedDataset, Refusal, TransferBudget, digest
from test_dataset_delivery import Anonymous, Storage, failure


class ProbeStorage(Storage):
    def __init__(self):
        super().__init__()
        self.ignore_condition = None
        self.rejection = 412
        self.corrupt_readback = False

    def list_multipart_uploads(self, **kw):
        self.record("list_multipart_uploads", kw)
        return {"Uploads": [{"Key": value["key"], "UploadId": upload_id}
                            for upload_id, value in self.uploads.items()
                            if value["key"].startswith(kw["Prefix"])], "IsTruncated": False}

    def upload_part(self, **kw):
        self.record("upload_part", kw)
        assert kw["PartNumber"] == 1
        etag = '"part-one"'
        self.uploads[kw["UploadId"]]["parts"][1] = (kw["Body"], etag)
        return {"ETag": etag, "ResponseMetadata": {"HTTPStatusCode": 200}}

    def replacement(self, operation, kw):
        if kw["Key"] not in self.objects:
            return
        if self.ignore_condition == operation:
            del self.objects[kw["Key"]]
        elif self.rejection != 412:
            self.record(operation, kw)
            raise failure(self.rejection, "AccessDenied")

    def put_object(self, **kw):
        self.replacement("put_object", kw)
        response = super().put_object(**kw)
        response["ResponseMetadata"] = {"HTTPStatusCode": 200}
        return response

    def complete_multipart_upload(self, **kw):
        self.replacement("complete_multipart_upload", kw)
        response = super().complete_multipart_upload(**kw)
        response["ResponseMetadata"] = {"HTTPStatusCode": 200}
        return response

    def get_object(self, **kw):
        if "Range" in kw:
            return super().get_object(**kw)
        self.record("get_object", kw)
        data, identity = self.objects[kw["Key"]]
        assert kw["IfMatch"] == identity.etag
        body = io.BytesIO(b"x" * len(data) if self.corrupt_readback else data)
        self.bodies.append(body)
        return {"Body": body, "ETag": identity.etag, "ContentLength": len(data),
                "ResponseMetadata": {"HTTPStatusCode": 200}}


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = ProbeStorage()
        self.evidence = Evidence(Path(self.temp.name))
        self.anonymous = Anonymous()
        self.probe = ConditionalWritesProbe(self.storage, self.anonymous, "https://storage.example",
                                             "test-1", "private-bucket", self.evidence,
                                             TransferBudget(60, 4096), "a" * 32)

    def writes(self):
        return [op for op, _ in self.storage.events
                if op in {"put_object", "create_multipart_upload", "upload_part", "complete_multipart_upload"}]

    def test_success_receipt_is_accepted_by_delivery_and_retains_probes(self):
        receipt = self.probe.run()
        self.assertEqual(receipt["status"], "verified")
        self.assertEqual(len(receipt["uploads"]), 2)
        self.assertEqual([entry["status"] for entry in receipt["uploads"]], ["completed", "incomplete"])
        pending = receipt["uploads"][-1]
        self.assertIn(pending["upload_id"], self.storage.uploads)
        for record in receipt["operations"].values():
            self.assertEqual(record["initial_status"], 200)
            self.assertEqual(record["replacement_status"], 412)
            self.assertEqual(record["readback_sha256"], digest(INITIAL))
            self.assertEqual(self.storage.objects[record["key"]][0], INITIAL)
        self.assertFalse(any(op.startswith(("delete", "abort")) for op, _ in self.storage.events))
        self.assertTrue(all(body.closed for body in self.storage.bodies))
        objects = []
        for path in ("manifest.json", "graph.parquet", "catchments.parquet", "NOTICE", "CITATION.txt"):
            data = path.encode()
            identity = self.storage.add("scratch/preserved/" + path, data)
            objects.append(DatasetObject(path, digest(data), identity))
        source = PreservedDataset(self.probe.endpoint, self.probe.region, self.probe.bucket,
                                  "scratch/preserved/", tuple(objects), "c" * 64)
        directory = Path(self.temp.name) / "delivery"
        directory.mkdir()
        delivery = DatasetDelivery(self.storage, self.anonymous, source, "hfx/test", b"README",
                                   Evidence(directory), TransferBudget(60, 4096),
                                   range_bytes=7, buffer_bytes=3)
        with redirect_stdout(io.StringIO()):
            delivery.deliver(receipt)
        self.assertEqual(delivery.evidence.value["status"], "verified-dataset")

    def test_ignored_put_condition_refuses(self):
        self.storage.ignore_condition = "put_object"
        with self.assertRaisesRegex(Refusal, "ignored destination condition"):
            self.probe.run()
        self.assertEqual(self.evidence.value["status"], "refused")

    def test_ignored_multipart_condition_refuses(self):
        self.storage.ignore_condition = "complete_multipart_upload"
        with self.assertRaisesRegex(Refusal, "ignored destination condition"):
            self.probe.run()
        self.assertEqual(self.evidence.value["status"], "refused")

    def test_wrong_http_rejection_refuses(self):
        self.storage.rejection = 403
        with self.assertRaisesRegex(Refusal, "HTTP 412"):
            self.probe.run()
        self.assertEqual(self.evidence.value["status"], "refused")

    def test_changed_readback_refuses_and_closes_body(self):
        self.storage.corrupt_readback = True
        with self.assertRaisesRegex(Refusal, "changed original bytes"):
            self.probe.run()
        self.assertTrue(self.storage.bodies)
        self.assertTrue(all(body.closed for body in self.storage.bodies))
        self.assertEqual(self.evidence.value["status"], "refused")

    def test_existing_prefix_refuses_without_writes(self):
        self.storage.add(self.probe.prefix + "unrelated", b"retained")
        with self.assertRaisesRegex(Refusal, "prefix is not empty"):
            self.probe.run()
        self.assertFalse(self.writes())

    def test_existing_upload_refuses_without_writes(self):
        self.storage.uploads["existing"] = {"key": self.probe.prefix + "old", "parts": {}}
        with self.assertRaisesRegex(Refusal, "existing uploads"):
            self.probe.run()
        self.assertFalse(self.writes())

    def test_key_appearing_after_inventory_refuses_without_writes(self):
        def race(operation, kw):
            if operation == "head_object":
                self.storage.hook = None
                self.storage.add(kw["Key"], b"another writer")
        self.storage.hook = race
        with self.assertRaisesRegex(Refusal, "key already exists"):
            self.probe.run()
        self.assertFalse(self.writes())

    def test_rerun_of_successful_journal_has_no_calls(self):
        self.probe.run()
        self.storage.events.clear()
        with self.assertRaisesRegex(Refusal, "journal already exists"):
            self.probe.run()
        self.assertFalse(self.storage.events)

    def test_rerun_of_failed_journal_has_no_calls(self):
        self.storage.rejection = 403
        with self.assertRaises(Refusal):
            self.probe.run()
        self.storage.events.clear()
        with self.assertRaisesRegex(Refusal, "journal already exists"):
            self.probe.run()
        self.assertFalse(self.storage.events)

    def test_cli_requires_execute_before_files_or_clients(self):
        stderr = io.StringIO()
        argv = ["conditional_writes_probe.py", "--source-inventory", "/missing/inventory",
                "--profile", "unused", "--evidence-dir", self.temp.name]
        with patch("sys.argv", argv), patch("conditional_writes_probe.boto3.Session") as session, \
                patch("conditional_writes_probe.boto3.client") as client, redirect_stderr(stderr):
            self.assertEqual(main(), 1)
        session.assert_not_called()
        client.assert_not_called()
        self.assertIn("require --execute", stderr.getvalue())
        self.assertFalse(self.evidence.path.exists())


if __name__ == "__main__":
    unittest.main()
