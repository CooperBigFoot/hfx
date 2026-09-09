#!/usr/bin/env python3
"""probe : PrivateStorage × FreshProbeKeys → ConditionalWriteEvidence | Refusal.

Writes tiny private probe objects only. Every probe object and upload is retained.
"""

import argparse
from dataclasses import asdict
import json
import re
import sys
import uuid

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from dataset_delivery import (InvocationInterrupted, ObjectIdentity, PreservedDataset, Refusal,
                              TransferBudget, delivery_attempt, digest, private_evidence, require, stamp)
from pathlib import Path


INITIAL = b"HFX conditional-write probe: preserved original\n"
REPLACEMENT = b"HFX conditional-write probe: forbidden replacement\n"


def response_record(response):
    """Retain protocol results without opaque header or exception text."""
    result = {key: response[key] for key in ("ETag", "UploadId", "VersionId", "Bucket", "Key",
                                           "ContentLength", "Owner", "Grants") if key in response}
    if "LastModified" in response:
        modified = response["LastModified"]
        result["LastModified"] = modified.isoformat() if hasattr(modified, "isoformat") else modified
    metadata = response.get("ResponseMetadata", {})
    result["ResponseMetadata"] = {key: metadata[key] for key in
                                  ("HTTPStatusCode", "RequestId", "HostId", "RetryAttempts") if key in metadata}
    result["ResponseMetadata"]["HTTPHeaders"] = {
        key: value for key, value in metadata.get("HTTPHeaders", {}).items()
        if key.lower() in {"etag", "content-length", "date", "x-amz-request-id", "x-amz-version-id"}}
    if "Error" in response:
        result["Error"] = {"Code": response["Error"]["Code"]}
    return result


class ConditionalWritesProbe:
    """Observe conditional creation and replacement rejection on isolated keys."""

    def __init__(self, storage, anonymous, endpoint, region, bucket, evidence, budget, probe_id):
        require(re.fullmatch(r"[a-f0-9]{32}", probe_id), "probe ID must be a fresh 32-digit lowercase hex value")
        self.storage, self.anonymous = storage, anonymous
        self.endpoint, self.region, self.bucket = endpoint, region, bucket
        self.evidence, self.budget = evidence, budget
        self.prefix = f"scratch/dataset-delivery-probes/{probe_id}/"

    def call(self, operation, **kwargs):
        self.budget.check()
        event = {"operation": operation, "key": kwargs.get("Key"), "started_at": stamp()}
        if "UploadId" in kwargs:
            event["upload_id"] = kwargs["UploadId"]
        if "IfNoneMatch" in kwargs:
            event["if_none_match"] = kwargs["IfNoneMatch"]
        self.evidence.value["events"].append(event)
        self.evidence.save()
        try:
            response = getattr(self.storage, operation)(Bucket=self.bucket, **kwargs)
        except ClientError as error:
            event.update(response=response_record(error.response), finished_at=stamp())
            self.evidence.save()
            raise
        event.update(response=response_record(response), finished_at=stamp())
        self.evidence.save()
        return response

    def absent(self, key):
        try:
            self.call("head_object", Key=key)
        except ClientError as error:
            require(error.response["ResponseMetadata"]["HTTPStatusCode"] == 404, "probe absence not established")
        else:
            raise Refusal("probe key already exists; do not reuse probe keys")

    def private(self, key):
        acl = self.call("get_object_acl", Key=key)
        owner = acl["Owner"]["ID"]
        require(all(g["Grantee"].get("Type") == "CanonicalUser" and
                    g["Grantee"].get("ID") == owner for g in acl["Grants"]), "probe object ACL is not owner-only")
        self.budget.check()
        try:
            self.anonymous.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            status = error.response["ResponseMetadata"]["HTTPStatusCode"]
            self.evidence.value["events"].append({"operation": "anonymous_head_object", "key": key,
                                                  "status": status, "observed_at": stamp()})
            self.evidence.save()
            require(status == 403, "probe anonymous access was not explicitly denied")
        else:
            raise Refusal("probe anonymous access allowed")

    def readback(self, key, identity):
        response = self.call("get_object", Key=key, IfMatch=identity.etag)
        body = response["Body"]
        try:
            require(response["ResponseMetadata"]["HTTPStatusCode"] == 200 and
                    response["ContentLength"] == len(INITIAL) and response["ETag"] == identity.etag,
                    "probe original identity changed")
            self.budget.check(len(INITIAL) + 1)
            data = body.read(len(INITIAL) + 1)
            self.evidence.value["events"].append({"operation": "full-readback", "key": key,
                                                  "bytes": len(data), "sha256": digest(data), "observed_at": stamp()})
            self.evidence.save()
            require(data == INITIAL, "probe replacement changed original bytes")
        finally:
            body.close()
        require(ObjectIdentity.from_head(self.call("head_object", Key=key)) == identity,
                "probe object changed during readback")
        return digest(data)

    def upload(self, key, payload):
        created = self.call("create_multipart_upload", Key=key, ACL="private", ContentType="text/plain")
        upload_id = created["UploadId"]
        self.evidence.value["uploads"].append({"key": key, "upload_id": upload_id, "status": "incomplete"})
        self.evidence.save()
        part = self.call("upload_part", Key=key, UploadId=upload_id, PartNumber=1, Body=payload)
        return upload_id, {"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]}

    def run(self):
        return delivery_attempt(self.evidence, self.budget, self._run)

    def _run(self):
        require(self.evidence.value is None, "probe journal already exists; inspect it without rerunning writes")
        self.evidence.value = {"schema": "hfx-conditional-writes-probe-v1", "endpoint": self.endpoint,
                               "region": self.region, "bucket": self.bucket, "status": "unverified",
                               "prefix": self.prefix, "operations": {}, "events": [], "uploads": []}
        self.evidence.save()
        try:
            self.call("get_bucket_policy")
        except ClientError as error:
            require(error.response["Error"]["Code"] == "NoSuchBucketPolicy", "probe needs absent bucket policy")
        else:
            raise Refusal("probe refuses bucket policy; no policy mutation")
        acl = self.call("get_bucket_acl")
        owner = acl["Owner"]["ID"]
        require(all(g["Grantee"].get("Type") == "CanonicalUser" and
                    g["Grantee"].get("ID") == owner for g in acl["Grants"]), "probe needs owner-only bucket ACL")
        # One random reserved prefix. Refuse even unrelated complete objects or uploads within it.
        listed = self.call("list_objects_v2", Prefix=self.prefix, MaxKeys=1)
        require(not listed.get("Contents") and not listed.get("IsTruncated"), "probe prefix is not empty")
        uploads = self.call("list_multipart_uploads", Prefix=self.prefix, MaxUploads=1)
        require(not uploads.get("Uploads") and not uploads.get("IsTruncated"), "probe prefix has existing uploads")
        for operation, name in (("PutObject", "put"), ("CompleteMultipartUpload", "multipart")):
            key = self.prefix + name
            self.absent(key)
            record = {"key": key, "condition": "IfNoneMatch:*", "initial_sha256": digest(INITIAL),
                      "replacement_sha256": digest(REPLACEMENT)}
            self.evidence.value["operations"][operation] = record
            self.evidence.save()
            if operation == "PutObject":
                response = self.call("put_object", Key=key, Body=INITIAL, ACL="private", IfNoneMatch="*")
            else:
                upload_id, parts = self.upload(key, INITIAL)
                response = self.call("complete_multipart_upload", Key=key, UploadId=upload_id,
                                     MultipartUpload=parts, IfNoneMatch="*")
                self.evidence.value["uploads"][-1]["status"] = "completed"
            record["initial_status"] = response["ResponseMetadata"]["HTTPStatusCode"]
            require(record["initial_status"] == 200 and response.get("ETag"), "probe initial write did not succeed")
            identity = ObjectIdentity.from_head(self.call("head_object", Key=key))
            require(identity.etag == response["ETag"] and identity.bytes == len(INITIAL), "initial probe HEAD mismatch")
            record["identity"] = asdict(identity)
            self.private(key)
            self.readback(key, identity)
            if operation == "CompleteMultipartUpload":
                upload_id, parts = self.upload(key, REPLACEMENT)
            try:
                if operation == "PutObject":
                    self.call("put_object", Key=key, Body=REPLACEMENT, ACL="private", IfNoneMatch="*")
                else:
                    self.call("complete_multipart_upload", Key=key, UploadId=upload_id,
                              MultipartUpload=parts, IfNoneMatch="*")
            except ClientError as error:
                record["replacement_status"] = error.response["ResponseMetadata"]["HTTPStatusCode"]
                self.evidence.save()
                require(record["replacement_status"] == 412, "provider did not reject replacement with HTTP 412")
            else:
                raise Refusal("provider ignored destination condition; promotion is prohibited")
            record["readback_sha256"] = self.readback(key, identity)
            self.private(key)
            self.evidence.save()
        self.evidence.value.update(status="verified", observed_at=stamp())
        self.evidence.save()
        return self.evidence.value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-inventory", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--max-seconds", type=float, default=300)
    parser.add_argument("--execute", action="store_true", help="explicitly authorize tiny probe writes; no deletion")
    args = parser.parse_args()
    try:
        require(args.execute, "probe writes require --execute after review")
        source = PreservedDataset.load(args.source_inventory)
        require(0 < args.max_seconds <= 300, "probe runtime must be greater than zero and at most 300 seconds")
        budget = TransferBudget(args.max_seconds, 4096)
        config = dict(connect_timeout=15, read_timeout=15, retries={"total_max_attempts": 1},
                      s3={"addressing_style": "path"}, request_checksum_calculation="when_required",
                      response_checksum_validation="when_required")
        with private_evidence(args.evidence_dir) as evidence:
            def execute():
                storage = boto3.Session(profile_name=args.profile).client(
                    "s3", endpoint_url=source.endpoint, region_name=source.region, config=Config(**config))
                anonymous = boto3.client("s3", endpoint_url=source.endpoint, region_name=source.region,
                                         config=Config(signature_version=UNSIGNED, **config))
                try:
                    probe = ConditionalWritesProbe(storage, anonymous, source.endpoint, source.region,
                                                    source.bucket, evidence, budget, uuid.uuid4().hex)
                    probe._run()
                finally:
                    storage.close()
                    anonymous.close()
            delivery_attempt(evidence, budget, execute)
            print(json.dumps({"status": "verified", "evidence": str(evidence.path), "prefix": evidence.value["prefix"]}))
    except (Refusal, InvocationInterrupted, OSError, ValueError, KeyError, TypeError, BotoCoreError, ClientError) as error:
        message = str(error) if isinstance(error, (Refusal, InvocationInterrupted)) else type(error).__name__
        print(json.dumps({"status": "refused", "reason": message}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
