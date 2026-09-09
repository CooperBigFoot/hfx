#!/usr/bin/env python3
"""deliver : PreservedDataset × PrivateDestination × PublicationProtection → VerifiedDataset | Refusal.

Storage I/O stays in this standalone composition root, outside the HFX library.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
import uuid

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


class Refusal(Exception):
    """An identity, access, transfer, or evidence prerequisite is absent or changed."""


class InvocationInterrupted(BaseException):
    """A total deadline or operator signal interrupts blocked main-thread I/O.

    BaseException prevents SDK exception retry/wrapping from delaying shutdown.
    """


def require(condition, message):
    if not condition:
        raise Refusal(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_small_file(path, maximum=1024**2):
    with path.open("rb") as stream:
        data = stream.read(maximum + 1)
    require(len(data) <= maximum, "local metadata exceeds its bounded read size")
    return data


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def stamp():
    return datetime.now(timezone.utc).isoformat()


def relative_path(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9._/-]+", value),
            "object path has unsupported characters")
    require(all(part not in ("", ".", "..") for part in value.split("/")),
            "object path contains an unsafe component")
    return value


@dataclass(frozen=True)
class ObjectIdentity:
    """Object version observation; ETag is a condition token, never SHA proof."""

    bytes: int
    etag: str
    last_modified: str
    version_id: str | None

    @classmethod
    def from_head(cls, head):
        require(head["ContentLength"] > 0 and head.get("ETag"), "incomplete object HEAD")
        modified = head["LastModified"]
        return cls(head["ContentLength"], head["ETag"],
                   modified.isoformat() if hasattr(modified, "isoformat") else modified,
                   head.get("VersionId"))


@dataclass(frozen=True)
class DatasetObject:
    path: str
    sha256: str
    identity: ObjectIdentity


@dataclass(frozen=True)
class PreservedDataset:
    endpoint: str
    region: str
    bucket: str
    prefix: str
    objects: tuple[DatasetObject, ...]
    record_sha256: str

    @classmethod
    def load(cls, path):
        data = read_small_file(path)
        record = json.loads(data)
        require(record["schema"] == "hfx-preserved-dataset-v1", "unsupported source inventory")
        require(re.fullmatch(r"https://[a-z0-9.-]+", record["endpoint"]), "HTTPS endpoint required")
        require(re.fullmatch(r"[a-z0-9-]+", record["bucket"]), "invalid bucket")
        require(re.fullmatch(r"[a-z0-9-]+", record["region"]), "invalid region")
        prefix = relative_path(record["prefix"].removesuffix("/")) + "/"
        objects = []
        for item in record["objects"]:
            relative_path(item["path"])
            require(re.fullmatch(r"[a-f0-9]{64}", item["sha256"]), "invalid expected SHA-256")
            identity = ObjectIdentity(**item["identity"])
            require(type(identity.bytes) is int and identity.bytes > 0 and
                    isinstance(identity.etag, str) and identity.etag.startswith('"') and
                    identity.etag.endswith('"') and isinstance(identity.last_modified, str),
                    "invalid preserved object identity")
            objects.append(DatasetObject(item["path"], item["sha256"], identity))
        paths = [obj.path for obj in objects]
        require(len(paths) == len(set(paths)), "duplicate source object path")
        require({"manifest.json", "catchments.parquet", "graph.parquet", "NOTICE", "CITATION.txt"}
                <= set(paths) and "README.md" not in paths, "source bundle or README boundary differs")
        return cls(record["endpoint"], record["region"], record["bucket"], prefix,
                   tuple(objects), digest(data))


class PublicationProtection(str, Enum):
    """The selected destination-write guarantee; no automatic fallback exists."""

    PROVIDER_CONDITIONAL = "provider-conditional"
    EXCLUSIVE_WRITER = "exclusive-writer"


@dataclass(frozen=True)
class PublicationScope:
    endpoint: str
    region: str
    bucket: str
    source_prefix: str
    destination_prefix: str


@dataclass(frozen=True)
class ExclusiveWriterAuthorization:
    """An operator decision bound to exact raw decision and failed-probe records."""

    scope: PublicationScope
    decision_sha256: str
    failed_probe_sha256: str
    authorization_binding_sha256: str
    probe_content_sha256: str

    @classmethod
    def load(cls, decision_path, binding_path, probe_bytes):
        decision_bytes = read_small_file(decision_path)
        binding_bytes = read_small_file(binding_path)
        decision, binding = json.loads(decision_bytes), json.loads(binding_bytes)
        require(binding["schema"] == "hfx-exclusive-writer-authorization-binding-v1",
                "unsupported exclusive-writer authorization binding")
        require(binding["decision_sha256"] == digest(decision_bytes) and
                binding["failed_probe_sha256"] == digest(probe_bytes),
                "exclusive-writer decision or failed probe hash differs")
        require(decision["decision"] == "proceed-with-exclusive-writer-publication" and
                isinstance(decision["approved_by"], str) and decision["approved_by"].strip() and
                isinstance(decision["user_statement"], str) and decision["user_statement"].strip(),
                "explicit operator exclusive-writer decision is required")
        require(decision["guarantee"] ==
                "Operational exclusive-writer protection, not provider-enforced atomic destination no-overwrite.",
                "exclusive-writer decision must state the weaker guarantee")
        scope = PublicationScope(**binding["scope"])
        require(asdict(scope) == decision["scope"], "authorization and decision scopes differ")
        approved_at = datetime.fromisoformat(decision["recorded_at"])
        bound_at = datetime.fromisoformat(binding["recorded_at"])
        require(approved_at.tzinfo is not None and bound_at.tzinfo is not None and
                approved_at <= bound_at <= datetime.now(timezone.utc), "invalid authorization chronology")
        return cls(scope, digest(decision_bytes), digest(probe_bytes), digest(binding_bytes),
                   digest(canonical(json.loads(probe_bytes))))


class TransferBudget:
    """One bounded, cancellable delivery invocation."""

    def __init__(self, seconds, max_bytes):
        require(seconds > 0 and max_bytes > 0, "positive runtime and byte limits required")
        self.deadline = time.monotonic() + seconds
        self.max_bytes = max_bytes
        self.bytes = 0
        self.cancelled = False

    def check(self, additional=0):
        require(not self.cancelled, "cancelled; preservation and partial destination retained")
        require(time.monotonic() < self.deadline, "runtime limit reached; partial destination retained")
        require(self.bytes + additional <= self.max_bytes, "verification byte budget exceeded")
        self.bytes += additional


@contextmanager
def bounded_invocation(budget):
    """Interrupt blocking POSIX main-thread I/O at the deadline or operator signal."""
    budget.check()
    require(signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0), "another invocation timer is active")
    signals = (signal.SIGALRM, signal.SIGINT, signal.SIGTERM)
    previous = {number: signal.getsignal(number) for number in signals}

    def interrupt(number, _frame):
        budget.cancelled = number != signal.SIGALRM
        reason = "runtime limit reached" if number == signal.SIGALRM else "cancelled"
        raise InvocationInterrupted(reason + "; uncertain storage effects remain retained")

    try:
        for number in signals:
            signal.signal(number, interrupt)
        remaining = budget.deadline - time.monotonic()
        require(remaining > 0, "runtime limit reached")
        signal.setitimer(signal.ITIMER_REAL, remaining)
        yield
        budget.check()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for number, handler in previous.items():
            signal.signal(number, handler)


def archive_verification(evidence):
    value = evidence.value
    if value is not None and value.get("status") in {"verified-dataset", "verified"}:
        snapshot = {key: item for key, item in value.items() if key != "verification_history"}
        value.setdefault("verification_history", []).append(json.loads(canonical(snapshot)))


def record_failure(evidence, error):
    if evidence.value is not None:
        archive_verification(evidence)
        evidence.value.update(status="refused", last_failure={
            "reason": str(error) if isinstance(error, (Refusal, InvocationInterrupted)) else type(error).__name__,
            "observed_at": stamp()})
        evidence.save()


@contextmanager
def evidence_attempt(evidence):
    # Invalidate current acceptance before fallible invocation prerequisites.
    if evidence.value is not None:
        archive_verification(evidence)
        previous_failure = evidence.value.pop("last_failure", None)
        if previous_failure is not None:
            evidence.value.setdefault("failure_history", []).append(previous_failure)
        evidence.value.update(status="checking", attempt_started_at=stamp())
        evidence.save()
    try:
        yield
    except (Exception, InvocationInterrupted, SystemExit) as error:
        record_failure(evidence, error)
        raise


def delivery_attempt(evidence, budget, operation):
    with evidence_attempt(evidence), bounded_invocation(budget):
        return operation()


@contextmanager
def command_evidence(parser):
    """Lock the explicit journal before parsing remaining invocation inputs."""
    if any(option in sys.argv[1:] for option in ("--help", "-h")):
        parser.parse_args()  # Informational help never starts an attempt.
    locator = argparse.ArgumentParser(add_help=False)
    locator.add_argument("--evidence-dir", type=Path)
    location, _unknown = locator.parse_known_args()
    arguments = None
    if location.evidence_dir is None:
        # Without an identifiable journal, preserve normal argparse diagnostics.
        arguments = parser.parse_args()
        location.evidence_dir = arguments.evidence_dir
    # Reconciliation preserves the exact interrupted journal, including on refusal.
    reconciliation = "reconcile-part" in sys.argv[1:]
    if reconciliation:
        arguments = parser.parse_args()
        reconciliation = arguments.action == "reconcile-part"
    with private_evidence(location.evidence_dir) as evidence, (
            nullcontext() if reconciliation else evidence_attempt(evidence)):
        if arguments is None:
            arguments = parser.parse_args()
        require(arguments.evidence_dir == location.evidence_dir, "ambiguous evidence directory arguments")
        yield arguments, evidence


class Evidence:
    """Private atomic journal, locked for one local writer by the CLI."""

    def __init__(self, directory):
        self.directory = directory
        self.path = directory / "delivery.json"
        self.value = json.loads(read_small_file(self.path, 32 * 1024**2)) if self.path.exists() else None

    def save(self):
        self.value["updated_at"] = stamp()
        temporary = self.directory / "delivery.json.new"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical(self.value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextmanager
def private_evidence(directory):
    require(directory.is_absolute(), "evidence directory must be absolute")
    require(not directory.is_symlink(), "evidence directory must not be a symlink")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(directory.stat().st_mode & 0o077 == 0, "evidence directory must have mode 0700")
    require(not any(part in {"target", ".venv", "build", "dist"} for part in directory.parts),
            "evidence must be outside build directories")
    for name in ("delivery.json", "delivery.json.new", "lock"):
        require(not (directory / name).is_symlink(), "symlink in evidence directory")
    fd = os.open(directory / "lock", os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal("another local delivery invocation holds the evidence lock") from error
        yield Evidence(directory)
    finally:
        os.close(fd)


class DatasetDelivery:
    """Promote preserved objects privately, verify bytes, then activate the manifest."""

    def __init__(self, storage, anonymous, source, destination, readme, evidence, budget,
                 part_bytes=256 * 1024**2, range_bytes=64 * 1024**2, buffer_bytes=1024**2,
                 protection=PublicationProtection.PROVIDER_CONDITIONAL):
        require(isinstance(protection, PublicationProtection), "publication protection must be a named mode")
        self.protection = protection
        self.storage, self.anonymous = storage, anonymous
        self.source, self.evidence, self.budget = source, evidence, budget
        self.destination = relative_path(destination.removesuffix("/")) + "/"
        require(re.fullmatch(r"hfx/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/", self.destination),
                "destination must be one distinct dataset prefix under hfx/")
        require(not self.destination.startswith(source.prefix) and
                not source.prefix.startswith(self.destination), "source/destination overlap")
        require(5 * 1024**2 <= part_bytes <= 5 * 1000**3, "multipart size outside S3 limits")
        require(0 < buffer_bytes <= range_bytes <= 64 * 1024**2, "verification buffer/range bounds differ")
        require(0 < len(readme) <= 1024**2, "README must contain at most 1 MiB")
        self.readme = readme
        self.part_bytes, self.range_bytes, self.buffer_bytes = part_bytes, range_bytes, buffer_bytes
        self.plan = {"source_record_sha256": source.record_sha256,
                     "endpoint": source.endpoint, "region": source.region, "bucket": source.bucket,
                     "source_prefix": source.prefix, "destination_prefix": self.destination,
                     "objects": [asdict(obj) for obj in source.objects],
                     "readme_sha256": digest(readme), "readme_bytes": len(readme),
                     "part_bytes": part_bytes, "range_bytes": range_bytes}

    def call(self, operation, **kwargs):
        self.budget.check()
        return getattr(self.storage, operation)(Bucket=self.source.bucket, **kwargs)

    def head(self, key):
        try:
            return ObjectIdentity.from_head(self.call("head_object", Key=key))
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            raise

    def inventory(self, prefix):
        objects, token, seen = {}, None, set()
        while True:
            request = {"Prefix": prefix}
            if token:
                request["ContinuationToken"] = token
            page = self.call("list_objects_v2", **request)
            for item in page.get("Contents", []):
                require(item["Key"].startswith(prefix) and item["Key"] not in objects,
                        "invalid or duplicate listed object")
                objects[item["Key"]] = item["Size"]
            if not page.get("IsTruncated"):
                return objects
            token = page.get("NextContinuationToken")
            require(token and token not in seen, "invalid listing continuation")
            seen.add(token)

    def private_access(self, key):
        acl = self.call("get_object_acl", Key=key)
        owner = acl["Owner"]["ID"]
        require(all(grant["Grantee"].get("Type") == "CanonicalUser" and
                    grant["Grantee"].get("ID") == owner for grant in acl["Grants"]),
                f"object ACL is not owner-only: {key}")
        self.budget.check()
        try:
            self.anonymous.head_object(Bucket=self.source.bucket, Key=key)
        except ClientError as error:
            require(error.response["ResponseMetadata"]["HTTPStatusCode"] == 403,
                    f"anonymous access was not explicitly denied: {key}")
        else:
            raise Refusal(f"anonymous object access allowed: {key}")

    def preflight(self):
        return delivery_attempt(self.evidence, self.budget, self._preflight)

    def _preflight(self):
        # Conservative: no policy is the only accepted bucket-policy state.
        # This never attempts to change an existing policy to make delivery pass.
        try:
            self.call("get_bucket_policy")
        except ClientError as error:
            require(error.response["Error"]["Code"] == "NoSuchBucketPolicy",
                    "bucket policy could not be established absent")
        else:
            raise Refusal("bucket has a policy; stop for access review, do not mutate policy")
        acl = self.call("get_bucket_acl")
        owner = acl["Owner"]["ID"]
        require(all(g["Grantee"].get("Type") == "CanonicalUser" and
                    g["Grantee"].get("ID") == owner for g in acl["Grants"]), "bucket ACL is not owner-only")
        expected = {self.source.prefix + obj.path: obj.identity.bytes for obj in self.source.objects}
        require(self.inventory(self.source.prefix) == expected, "source inventory changed")
        for obj in self.source.objects:
            key = self.source.prefix + obj.path
            require(self.head(key) == obj.identity, f"source identity changed: {key}")
            self.private_access(key)
        manifest = next(obj for obj in self.source.objects if obj.path == "manifest.json")
        require(manifest.identity.bytes <= 1024**2, "manifest exceeds the small preflight bound")
        self.verify(manifest.path, manifest.sha256, manifest.identity.bytes, manifest.identity,
                    prefix=self.source.prefix)
        actual = self.inventory(self.destination)
        if self.evidence.value is None:
            require(not actual, "new destination must be absent")
            self.evidence.value = {"schema": "hfx-dataset-delivery-v1", "plan": self.plan,
                                   "status": "planned", "objects": {}, "created_at": stamp()}
            self.evidence.save()
        else:
            require(self.evidence.value["schema"] == "hfx-dataset-delivery-v1" and
                    self.evidence.value["plan"] == self.plan, "delivery plan changed")
        for key in actual:
            path = key.removeprefix(self.destination)
            state = self.evidence.value["objects"].get(path)
            require(state is not None and state["status"] in {"copied", "verified"},
                    f"unexpected or uncertain destination object: {key}")
            require(asdict(self.head(key)) == state["identity"], f"destination identity changed: {key}")
        for path, state in self.evidence.value["objects"].items():
            if state["status"] in {"copied", "verified"}:
                require(self.destination + path in actual, f"recorded destination object missing: {path}")
        if self.protection is PublicationProtection.EXCLUSIVE_WRITER:
            self._check_destination_ownership()
        return self.evidence.value

    def verify(self, path, expected_sha, expected_bytes, identity, *, prefix=None):
        key = (self.destination if prefix is None else prefix) + path
        require(identity.bytes == expected_bytes, f"destination size mismatch: {path}")
        self.private_access(key)
        sha, count = hashlib.sha256(), 0
        for start in range(0, expected_bytes, self.range_bytes):
            end = min(start + self.range_bytes, expected_bytes) - 1
            request = {"Key": key, "Range": f"bytes={start}-{end}", "IfMatch": identity.etag}
            if identity.version_id:
                request["VersionId"] = identity.version_id
            require(self.budget.bytes + end - start + 1 <= self.budget.max_bytes,
                    "verification byte budget cannot cover the next complete range")
            response = self.call("get_object", **request)
            body = response["Body"]
            try:
                require(response["ResponseMetadata"]["HTTPStatusCode"] == 206 and
                        response.get("ContentRange") == f"bytes {start}-{end}/{expected_bytes}" and
                        response["ContentLength"] == end - start + 1 and
                        response.get("ETag") == identity.etag,
                        f"range response identity or bounds mismatch: {path}")
                received = 0
                while True:
                    self.budget.check()
                    chunk = body.read(min(self.buffer_bytes, end - start + 2 - received))
                    if not chunk:
                        break
                    self.budget.check(len(chunk))
                    received += len(chunk)
                    require(received <= end - start + 1, f"overlong response: {path}")
                    sha.update(chunk)
                    count += len(chunk)
                require(received == end - start + 1, f"truncated response: {path}")
            finally:
                body.close()
            progress = {"event": "verified-range", "path": path, "bytes": count,
                        "total": expected_bytes, "observed_at": stamp()}
            if prefix is None and self.evidence.value is not None and path in self.evidence.value["objects"]:
                self.evidence.value["objects"][path]["verification_progress"] = progress
                self.evidence.save()
            print(json.dumps(progress), flush=True)
        require(count == expected_bytes and sha.hexdigest() == expected_sha, f"SHA-256 mismatch: {path}")
        require(self.head(key) == identity, f"destination changed during verification: {path}")
        return {"sha256": sha.hexdigest(), "bytes": count, "method": "full-ordered-range-stream-sha256",
                "verified_at": stamp()}

    def _listed_parts(self, key, upload_id):
        observed, marker = [], None
        while True:
            request = {"Key": key, "UploadId": upload_id}
            if marker is not None:
                request["PartNumberMarker"] = marker
            page = self.call("list_parts", **request)
            observed.extend({"PartNumber": p["PartNumber"], "ETag": p["ETag"], "Size": p["Size"]}
                            for p in page.get("Parts", []))
            if not page.get("IsTruncated"):
                break
            next_marker = page.get("NextPartNumberMarker")
            require(next_marker is not None and (marker is None or next_marker > marker), "invalid part pagination")
            marker = next_marker
        return observed

    def copy(self, obj):
        key, source_key = self.destination + obj.path, self.source.prefix + obj.path
        states = self.evidence.value["objects"]
        state = states.get(obj.path)
        if state is None:
            self._guard_exclusive_writer()
            require(self.head(key) is None, f"destination already exists: {key}")
            state = {"status": "initializing", "parts": []}
            states[obj.path] = state
            self.evidence.save()  # An uncertain create is never automatically repeated.
            response = self.call("create_multipart_upload", Key=key, ACL="private",
                                 ContentType="application/json" if obj.path == "manifest.json" else "application/octet-stream")
            state.update(status="copying", upload_id=response["UploadId"])
            self.evidence.save()
        require(state["status"] in {"copying", "copied", "verified"}, f"uncertain copy requires inspection: {obj.path}")
        if state["status"] == "copying":
            require(not state.get("pending_part"), f"uncertain part requires inspection: {obj.path}")
            observed = self._listed_parts(key, state["upload_id"])
            require(observed == state["parts"], f"multipart journal differs: {obj.path}")
            total = (obj.identity.bytes + self.part_bytes - 1) // self.part_bytes
            require(total <= 10000, "too many multipart parts")
            for number in range(len(observed) + 1, total + 1):
                require(self.head(source_key) == obj.identity, f"source changed before part: {obj.path}")
                start = (number - 1) * self.part_bytes
                end = min(start + self.part_bytes, obj.identity.bytes) - 1
                state["pending_part"] = number
                self.evidence.save()
                copy_source = {"Bucket": self.source.bucket, "Key": source_key}
                if obj.identity.version_id:
                    copy_source["VersionId"] = obj.identity.version_id
                request = {"Key": key, "UploadId": state["upload_id"], "PartNumber": number,
                           "CopySource": copy_source, "CopySourceIfMatch": obj.identity.etag}
                # S3 documents CopySourceRange only for sources greater than 5 MB.
                # A single part copies the whole source, so no range is needed.
                if total > 1:
                    request["CopySourceRange"] = f"bytes={start}-{end}"
                result = self.call("upload_part_copy", **request)
                require(result.get("CopyPartResult", {}).get("ETag"), f"part copy has no success ETag: {obj.path}")
                state["parts"].append({"PartNumber": number, "ETag": result["CopyPartResult"]["ETag"], "Size": end-start+1})
                del state["pending_part"]
                self.evidence.save()
                print(json.dumps({"event": "copied-part", "path": obj.path, "part": number, "parts": total}), flush=True)
            self._guard_exclusive_writer()
            require(self.head(source_key) == obj.identity and self.head(key) is None,
                    f"identity changed before completion: {obj.path}")
            state["status"] = "completing"
            self.evidence.save()
            result = self.call("complete_multipart_upload", Key=key, UploadId=state["upload_id"],
                               MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]}
                                                          for p in state["parts"]]}, IfNoneMatch="*")
            require(result.get("ETag"), f"multipart completion has no success ETag: {obj.path}")
            identity = self.head(key)
            require(identity is not None and identity.etag == result["ETag"], f"completed object differs: {obj.path}")
            state.update(status="copied", identity=asdict(identity))
            self.evidence.save()
        return state

    def reconcile_part(self, path, conditional_evidence, *, authorization, exclusive_writer_confirmed,
                       expected_journal_sha256, expected_part_etag):
        """Observe one pending part in an owned upload without any provider mutation.

        The operator must exclude all other writers, including replicated journals.
        This observation cannot establish the original request outcome or content SHA.
        """
        require(exclusive_writer_confirmed, "explicit current exclusive-writer confirmation required")
        require(self.protection is PublicationProtection.EXCLUSIVE_WRITER,
                "part reconciliation requires exclusive-writer publication")
        self._require_exclusive_probe(conditional_evidence, authorization)
        original = read_small_file(self.evidence.path, 32 * 1024**2)
        require(digest(original) == expected_journal_sha256, "reviewed journal SHA-256 differs")
        require(json.loads(original) == self.evidence.value, "journal changed before reconciliation")
        value = self.evidence.value
        require(value["schema"] == "hfx-dataset-delivery-v1" and value["plan"] == self.plan,
                "delivery plan changed")
        require(value["status"] in {"refused", "checking", "partial-unverified"},
                "reconciliation requires an interrupted unverified delivery")
        protection = value["publication_protection"]
        expected = {"mode": self.protection.value,
                    "guarantee": "Operational exclusive-writer protection, not provider-enforced atomic destination no-overwrite.",
                    "decision_sha256": authorization.decision_sha256,
                    "failed_probe_sha256": authorization.failed_probe_sha256,
                    "authorization_binding_sha256": authorization.authorization_binding_sha256,
                    "scope": asdict(authorization.scope)}
        require(all(protection.get(key) == item for key, item in expected.items()),
                "publication protection or immutable authorization changed")
        candidates = [obj for obj in self.source.objects if obj.path == path]
        require(len(candidates) == 1, "reconciliation path must name one preserved object")
        obj = candidates[0]
        states = value["objects"]
        require(path in states and all(
            state["status"] in {"copied", "verified"} and not state.get("pending_part")
            for name, state in states.items() if name != path),
            "other uncertain object requires inspection")
        state = states[path]
        require(state["status"] == "copying" and isinstance(state.get("upload_id"), str)
                and state["upload_id"], "reconciliation requires the journal-owned copying upload")
        number = state.get("pending_part")
        total = (obj.identity.bytes + self.part_bytes - 1) // self.part_bytes
        require(type(number) is int and 1 <= number <= total <= 10000 and
                number == len(state["parts"]) + 1, "pending part is not the exact next source range")
        for index, part in enumerate(state["parts"], 1):
            require(type(part["PartNumber"]) is int and part["PartNumber"] == index and
                    type(part["Size"]) is int and part["Size"] == self.part_bytes and
                    isinstance(part["ETag"], str) and part["ETag"], "invalid preceding journal part")
        self._preflight()
        reservation_verification = self._verify_reservation()
        key = self.destination + path
        require(self.head(key) is None, "pending destination already exists; completion is uncertain")
        observed = self._listed_parts(key, state["upload_id"])
        require(len(observed) == number and observed[:-1] == state["parts"],
                "multipart preceding parts or additional part count differs")
        part = observed[-1]
        require(part["ETag"] == expected_part_etag, "reviewed pending part ETag differs")
        expected_size = min(self.part_bytes, obj.identity.bytes - (number - 1) * self.part_bytes)
        require(type(part["PartNumber"]) is int and part["PartNumber"] == number and
                type(part["Size"]) is int and part["Size"] == expected_size and
                isinstance(part["ETag"], str) and re.fullmatch(r'"[^"\s]+"', part["ETag"]),
                "observed pending part number, size or ETag is invalid")
        require(self._listed_parts(key, state["upload_id"]) == observed,
                "multipart parts changed during reconciliation")
        # Bracket ListParts with ownership/source checks. This cannot fence an external writer.
        self._check_destination_ownership()
        for source_obj in self.source.objects:
            require(self.head(self.source.prefix + source_obj.path) == source_obj.identity,
                    f"source changed during reconciliation: {source_obj.path}")
        self._verify_reservation()
        require(read_small_file(self.evidence.path, 32 * 1024**2) == original,
                "journal changed during reconciliation")
        self.budget.check()
        value.setdefault("part_recoveries", []).append({
            "path": path, "upload_id": state["upload_id"], "pending_part": number,
            "observed_parts": observed, "observed_at": stamp(),
            "original_journal": original.decode(), "original_journal_sha256": digest(original),
            "original_request_success_observed": False,
            "method": "owned-upload-list-parts-observation",
            "exclusive_writer_confirmed": True,
            "reservation_verification": reservation_verification,
            "integrity_requirement": "completed object requires full ordered range SHA-256 verification"})
        state["parts"] = observed
        del state["pending_part"]
        value["status"] = "partial-unverified"
        self.evidence.save()  # Original intent and reconciliation commit in one atomic replacement.

    def deliver(self, conditional_evidence, *, authorization=None):
        return delivery_attempt(self.evidence, self.budget,
                                lambda: self._deliver(conditional_evidence, authorization=authorization))

    def _require_exclusive_probe(self, probe, authorization):
        require(isinstance(authorization, ExclusiveWriterAuthorization),
                "exclusive-writer mode requires a bound operator decision")
        scope = PublicationScope(self.source.endpoint, self.source.region, self.source.bucket,
                                 self.source.prefix, self.destination)
        require(authorization.scope == scope, "exclusive-writer authorization scope differs")
        require(digest(canonical(probe)) == authorization.probe_content_sha256,
                "exclusive-writer failed probe content changed")
        require(probe["schema"] == "hfx-conditional-writes-probe-v1" and
                probe["status"] == "refused" and probe["endpoint"] == scope.endpoint and
                probe["region"] == scope.region and probe["bucket"] == scope.bucket and
                probe["last_failure"]["reason"] ==
                "provider ignored destination condition; promotion is prohibited",
                "exclusive-writer decision requires the recorded ignored-completion probe")
        put = probe["operations"]["PutObject"]
        require(put["condition"] == "IfNoneMatch:*" and put["initial_status"] == 200 and
                put["replacement_status"] == 412 and put["readback_sha256"] == put["initial_sha256"] and
                re.fullmatch(r"[a-f0-9]{64}", put["initial_sha256"]) and
                re.fullmatch(r"[a-f0-9]{64}", put["replacement_sha256"]) and
                put["initial_sha256"] != put["replacement_sha256"],
                "exclusive-writer reservation requires working conditional PutObject evidence")
        writes = [event for event in probe["events"] if event["operation"] == "put_object" and
                  event.get("key") == put["key"]]
        reads = [event for event in probe["events"] if event["operation"] == "full-readback" and
                 event.get("key") == put["key"]]
        require(len(writes) == 2 and all(event.get("if_none_match") == "*" for event in writes) and
                [event["response"]["ResponseMetadata"]["HTTPStatusCode"] for event in writes] == [200, 412] and
                len(reads) == 2 and all(event["sha256"] == put["initial_sha256"] and event["bytes"] > 0 for event in reads),
                "conditional PutObject probe events disagree")
        complete = probe["operations"]["CompleteMultipartUpload"]
        writes = [event for event in probe["events"] if event["operation"] == "complete_multipart_upload" and
                  event.get("key") == complete["key"]]
        require(complete["condition"] == "IfNoneMatch:*" and complete["initial_status"] == 200 and
                re.fullmatch(r"[a-f0-9]{64}", complete["initial_sha256"]) and
                re.fullmatch(r"[a-f0-9]{64}", complete["replacement_sha256"]) and
                complete["initial_sha256"] != complete["replacement_sha256"] and
                len(writes) == 2 and all(event.get("if_none_match") == "*" for event in writes) and
                [event["response"]["ResponseMetadata"]["HTTPStatusCode"] for event in writes] == [200, 200] and
                writes[0]["upload_id"] != writes[1]["upload_id"],
                "retained probe does not establish ignored multipart completion condition")
        require(put["key"] != complete["key"] and all(relative_path(key).startswith(
                "scratch/dataset-delivery-probes/") for key in (put["key"], complete["key"])),
                "failed probe keys differ from isolated probe scope")

    def _destination_uploads(self):
        uploads, marker, seen = set(), {}, set()
        while True:
            page = self.call("list_multipart_uploads", Prefix=self.destination, **marker)
            for item in page.get("Uploads", []):
                pair = (item["Key"], item["UploadId"])
                require(pair[0].startswith(self.destination) and pair not in uploads,
                        "invalid or duplicate destination multipart upload")
                uploads.add(pair)
            if not page.get("IsTruncated"):
                return uploads
            pair = (page.get("NextKeyMarker"), page.get("NextUploadIdMarker"))
            require(all(pair) and pair not in seen, "invalid multipart upload pagination")
            seen.add(pair)
            marker = {"KeyMarker": pair[0], "UploadIdMarker": pair[1]}

    def _check_destination_ownership(self):
        states = self.evidence.value["objects"]
        expected_uploads = {(self.destination + path, state["upload_id"]) for path, state in states.items()
                            if state["status"] == "copying" and "upload_id" in state}
        require(self._destination_uploads() == expected_uploads,
                "unexpected or missing destination multipart upload")
        actual = self.inventory(self.destination)
        expected = {self.destination + path: state["identity"]["bytes"] for path, state in states.items()
                    if state["status"] in {"copied", "verified"}}
        require(actual == expected, "unexpected or changed destination object inventory")
        for path, state in states.items():
            if state["status"] in {"copied", "verified"}:
                require(self.head(self.destination + path) == ObjectIdentity(**state["identity"]),
                        f"destination identity changed: {path}")

    def _reservation_payload(self):
        protection = self.evidence.value["publication_protection"]
        return canonical({"schema": "hfx-dataset-publication-ownership-v1",
                          "scope": protection["scope"], "operation_token": protection["operation_token"],
                          "plan_sha256": digest(canonical(self.plan)),
                          "authorization_binding_sha256": protection["authorization_binding_sha256"],
                          "decision_sha256": protection["decision_sha256"],
                          "failed_probe_sha256": protection["failed_probe_sha256"]})

    def _reserve_exclusive_writer(self):
        protection = self.evidence.value["publication_protection"]
        destination_scope = {key: protection["scope"][key] for key in
                             ("endpoint", "region", "bucket", "destination_prefix")}
        key = "scratch/dataset-publication-ownership/" + digest(canonical(destination_scope)) + "/owner.json"
        reservation = protection.get("reservation")
        if reservation is None:
            require(not self.evidence.value["objects"], "existing objects have no ownership reservation")
            self._check_destination_ownership()
            require(self.head(key) is None, "destination ownership reservation already exists")
            protection["operation_token"] = uuid.uuid4().hex
            payload = self._reservation_payload()
            reservation = {"status": "putting", "key": key, "sha256": digest(payload)}
            protection["reservation"] = reservation
            self.evidence.save()
            response = self.call("put_object", Key=key, Body=payload, ACL="private",
                                 ContentType="application/json", IfNoneMatch="*")
            identity = self.head(key)
            require(identity is not None and identity.etag == response.get("ETag"),
                    "ownership reservation write identity differs")
            reservation.update(status="owned", identity=asdict(identity))
            self.evidence.save()
        reservation["verification"] = self._verify_reservation()
        self.evidence.save()

    def _verify_reservation(self):
        """Read the existing ownership reservation without acquiring or changing it."""
        protection = self.evidence.value["publication_protection"]
        destination_scope = {key: protection["scope"][key] for key in
                             ("endpoint", "region", "bucket", "destination_prefix")}
        key = "scratch/dataset-publication-ownership/" + digest(canonical(destination_scope)) + "/owner.json"
        reservation = protection.get("reservation")
        require(reservation is not None, "reconciliation requires an existing owned reservation")
        require(reservation["status"] == "owned" and reservation["key"] == key and
                re.fullmatch(r"[a-f0-9]{32}", protection["operation_token"]),
                "uncertain or foreign ownership reservation requires inspection")
        payload = self._reservation_payload()
        require(reservation["sha256"] == digest(payload), "ownership reservation payload changed")
        identity = ObjectIdentity(**reservation["identity"])
        require(self.head(key) == identity, "ownership reservation identity changed")
        verification = self.verify("owner.json", digest(payload), len(payload), identity,
                                   prefix=key.removesuffix("owner.json"))
        return verification

    def _guard_exclusive_writer(self):
        if self.protection is PublicationProtection.EXCLUSIVE_WRITER:
            # Cooperating invocations share this conditional reservation. External
            # writers remain an operator exclusion; HEAD then complete has a race.
            self._reserve_exclusive_writer()
            self._check_destination_ownership()
            for obj in self.source.objects:
                require(self.head(self.source.prefix + obj.path) == obj.identity,
                        f"source changed before destination write: {obj.path}")

    def _require_provider_conditional(self, conditional_evidence):
        require(conditional_evidence["schema"] == "hfx-conditional-writes-probe-v1" and
                conditional_evidence["status"] == "verified" and
                conditional_evidence["endpoint"] == self.source.endpoint and
                conditional_evidence["bucket"] == self.source.bucket and
                conditional_evidence["region"] == self.source.region,
                "provider conditional-write evidence is required")
        require(set(conditional_evidence["operations"]) == {"PutObject", "CompleteMultipartUpload"},
                "both destination write operations must have probe evidence")
        for operation, probe in conditional_evidence["operations"].items():
            require(probe["condition"] == "IfNoneMatch:*" and
                    probe["initial_status"] == 200 and probe["replacement_status"] == 412 and
                    re.fullmatch(r"[a-f0-9]{64}", probe["initial_sha256"]) and
                    probe["readback_sha256"] == probe["initial_sha256"] and
                    probe["replacement_sha256"] != probe["initial_sha256"] and
                    re.fullmatch(r"[a-f0-9]{64}", probe["replacement_sha256"]) and
                    relative_path(probe["key"]).startswith("scratch/dataset-delivery-probes/"),
                    f"conditional-write enforcement was not established: {operation}")
            api = {"PutObject": "put_object", "CompleteMultipartUpload": "complete_multipart_upload"}[operation]
            writes = [event for event in conditional_evidence["events"]
                      if event["operation"] == api and event.get("key") == probe["key"]]
            require(len(writes) == 2 and
                    all(event.get("if_none_match") == "*" for event in writes) and
                    [event["response"]["ResponseMetadata"]["HTTPStatusCode"] for event in writes] == [200, 412],
                    f"probe operation records disagree: {operation}")
            reads = [event for event in conditional_evidence["events"]
                     if event["operation"] == "full-readback" and event.get("key") == probe["key"]]
            require(len(reads) == 2 and all(event["sha256"] == probe["initial_sha256"] and
                                           event["bytes"] > 0 for event in reads),
                    f"probe readback records disagree: {operation}")
            if operation == "CompleteMultipartUpload":
                require(writes[0]["upload_id"] != writes[1]["upload_id"] and
                        {entry["upload_id"] for entry in conditional_evidence["uploads"]
                         if entry["key"] == probe["key"]} == {event["upload_id"] for event in writes},
                        "probe must retain both distinct multipart upload IDs")
        require(conditional_evidence["operations"]["PutObject"]["key"] !=
                conditional_evidence["operations"]["CompleteMultipartUpload"]["key"],
                "probe operations must use distinct keys")
        observed = datetime.fromisoformat(conditional_evidence["observed_at"])
        require(observed.tzinfo is not None and
                0 <= (datetime.now(timezone.utc) - observed).total_seconds() <= 86400,
                "conditional-write probe must be from the last 24 hours")

    def _deliver(self, conditional_evidence, *, authorization=None):
        if self.protection is PublicationProtection.PROVIDER_CONDITIONAL:
            require(authorization is None, "exclusive-writer authorization requires explicit exclusive-writer mode")
            self._require_provider_conditional(conditional_evidence)
            protection = {"mode": self.protection.value,
                          "guarantee": "Provider-conditional destination writes with reviewed probe evidence."}
        else:
            self._require_exclusive_probe(conditional_evidence, authorization)
            protection = {"mode": self.protection.value,
                          "guarantee": "Operational exclusive-writer protection, not provider-enforced atomic destination no-overwrite.",
                          "decision_sha256": authorization.decision_sha256,
                          "failed_probe_sha256": authorization.failed_probe_sha256,
                          "authorization_binding_sha256": authorization.authorization_binding_sha256,
                          "scope": asdict(authorization.scope)}
        self._preflight()
        previous = self.evidence.value.get("publication_protection")
        if previous is None:
            require(self.protection is PublicationProtection.PROVIDER_CONDITIONAL or
                    not self.evidence.value["objects"], "existing objects have no exclusive-writer ownership evidence")
            self.evidence.value["publication_protection"] = protection
        else:
            require(all(previous.get(key) == value for key, value in protection.items()),
                    "publication protection or immutable authorization changed")
        self.evidence.save()
        print(json.dumps({"event": "publication-protection", "mode": protection["mode"],
                          "guarantee": protection["guarantee"]}), flush=True)
        if self.protection is PublicationProtection.EXCLUSIVE_WRITER:
            self._guard_exclusive_writer()
        evidence_hash = digest(canonical(conditional_evidence))
        history = self.evidence.value.setdefault("conditional_writes_history", [])
        if evidence_hash not in [item["sha256"] for item in history]:
            observed_at = (conditional_evidence["observed_at"] if self.protection is
                           PublicationProtection.PROVIDER_CONDITIONAL else
                           conditional_evidence["last_failure"]["observed_at"])
            history.append({"sha256": evidence_hash, "observed_at": observed_at,
                            "probe_status": conditional_evidence["status"]})
        self.evidence.value.update(status="partial-unverified", conditional_writes_evidence_sha256=evidence_hash)
        self.evidence.save()
        ordered = sorted(self.source.objects, key=lambda obj: (obj.path == "manifest.json", obj.path))
        for obj in ordered:
            if obj.path == "manifest.json":
                self.deliver_readme()
            state = self.copy(obj)
            identity = ObjectIdentity(**state["identity"])
            require(self.head(self.destination + obj.path) == identity, f"destination changed: {obj.path}")
            if state["status"] == "verified":
                require(state["verification"]["sha256"] == obj.sha256 and
                        state["verification"]["bytes"] == obj.identity.bytes and
                        state["verification"]["method"] == "full-ordered-range-stream-sha256",
                        f"verification receipt differs: {obj.path}")
                self.private_access(self.destination + obj.path)
            else:
                state["verification"] = self.verify(obj.path, obj.sha256, obj.identity.bytes, identity)
                state["status"] = "verified"
                self.evidence.save()
        self._preflight()  # Includes exact source and destination identities and policy checks.
        self._guard_exclusive_writer()
        require(set(self.inventory(self.destination)) ==
                {self.destination + obj.path for obj in ordered} | {self.destination + "README.md"},
                "final destination inventory differs")
        self.evidence.value.update(status="verified-dataset", completed_at=stamp(),
                                   final_bytes=sum(obj.identity.bytes for obj in ordered) + len(self.readme))
        self.evidence.save()

    def deliver_readme(self):
        path, key = "README.md", self.destination + "README.md"
        states = self.evidence.value["objects"]
        state = states.get(path)
        if state is None:
            self._guard_exclusive_writer()
            require(self.head(key) is None, "README already exists")
            state = {"status": "putting"}
            states[path] = state
            self.evidence.save()
            response = self.call("put_object", Key=key, Body=self.readme, ACL="private",
                                 ContentType="text/markdown; charset=utf-8", IfNoneMatch="*")
            identity = self.head(key)
            require(identity is not None and identity.etag == response.get("ETag"), "README write identity differs")
            state.update(status="copied", identity=asdict(identity))
            self.evidence.save()
        require(state["status"] in {"copied", "verified"}, "uncertain README write requires inspection")
        identity = ObjectIdentity(**state["identity"])
        require(self.head(key) == identity, "README destination changed")
        # Small README always read back, including resume.
        state["verification"] = self.verify(path, digest(self.readme), len(self.readme), identity)
        state["status"] = "verified"
        self.evidence.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "deliver", "reconcile-part"))
    parser.add_argument("--object-path", help="exact preserved object with one pending part")
    parser.add_argument("--expected-journal-sha256", help="reviewed raw interrupted delivery.json SHA-256")
    parser.add_argument("--expected-part-etag", help="reviewed pending part ETag, including quotes")
    parser.add_argument("--confirm-exclusive-writer", action="store_true",
                        help="confirm all other writers, including replicated journals, are excluded now")
    parser.add_argument("--source-inventory", type=Path, required=True)
    parser.add_argument("--destination-prefix", required=True)
    parser.add_argument("--readme", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--conditional-writes-evidence", type=Path)
    parser.add_argument("--publication-protection", choices=[mode.value for mode in PublicationProtection],
                        default=PublicationProtection.PROVIDER_CONDITIONAL.value)
    parser.add_argument("--exclusive-writer-decision", type=Path)
    parser.add_argument("--exclusive-writer-authorization-binding", type=Path)
    parser.add_argument("--max-seconds", type=int, default=3600)
    parser.add_argument("--max-read-bytes", type=int, required=True)
    parser.add_argument("--request-timeout-seconds", type=int, default=30)
    try:
        with command_evidence(parser) as (args, evidence):
            require(args.action == "reconcile-part" or
                    (args.object_path is None and not args.confirm_exclusive_writer and
                     args.expected_journal_sha256 is None and args.expected_part_etag is None),
                    "reconciliation options require reconcile-part")
            require(1 <= args.request_timeout_seconds <= 120, "request timeout must be 1..120 seconds")
            budget = TransferBudget(args.max_seconds, args.max_read_bytes)
            with bounded_invocation(budget):
                source = PreservedDataset.load(args.source_inventory)
                config = dict(connect_timeout=args.request_timeout_seconds, read_timeout=args.request_timeout_seconds,
                              retries={"total_max_attempts": 1}, s3={"addressing_style": "path"},
                              request_checksum_calculation="when_required", response_checksum_validation="when_required")
                storage = boto3.Session(profile_name=args.profile).client(
                    "s3", endpoint_url=source.endpoint, region_name=source.region, config=Config(**config))
                anonymous = boto3.client("s3", endpoint_url=source.endpoint, region_name=source.region,
                                         config=Config(signature_version=UNSIGNED, **config))
                try:
                    protection = PublicationProtection(args.publication_protection)
                    delivery = DatasetDelivery(storage, anonymous, source, args.destination_prefix,
                                               read_small_file(args.readme), evidence, budget, protection=protection)
                    if args.action == "plan":
                        require(args.exclusive_writer_decision is None and
                                args.exclusive_writer_authorization_binding is None,
                                "operator authorization inputs are for deliver, not read-only plan")
                        delivery._preflight()
                    else:
                        require(args.conditional_writes_evidence is not None, "conditional-write evidence file required")
                        probe_bytes = read_small_file(args.conditional_writes_evidence)
                        authorization = None
                        if protection is PublicationProtection.EXCLUSIVE_WRITER:
                            require(args.exclusive_writer_decision is not None and
                                    args.exclusive_writer_authorization_binding is not None,
                                    "exclusive-writer decision and authorization binding files required")
                            authorization = ExclusiveWriterAuthorization.load(
                                args.exclusive_writer_decision, args.exclusive_writer_authorization_binding, probe_bytes)
                        else:
                            require(args.exclusive_writer_decision is None and
                                    args.exclusive_writer_authorization_binding is None,
                                    "operator authorization requires explicit exclusive-writer mode")
                        if args.action == "reconcile-part":
                            delivery.reconcile_part(args.object_path, json.loads(probe_bytes),
                                                    authorization=authorization,
                                                    exclusive_writer_confirmed=args.confirm_exclusive_writer,
                                                    expected_journal_sha256=args.expected_journal_sha256,
                                                    expected_part_etag=args.expected_part_etag)
                        else:
                            delivery._deliver(json.loads(probe_bytes), authorization=authorization)
                finally:
                    storage.close()
                    anonymous.close()
            print(json.dumps({"status": evidence.value["status"], "evidence": str(evidence.path)}))
    except (Refusal, InvocationInterrupted, OSError, ValueError, KeyError, TypeError, BotoCoreError, ClientError) as error:
        # Never print provider exception text: it may include request/authentication details.
        message = str(error) if isinstance(error, (Refusal, InvocationInterrupted)) else type(error).__name__
        print(json.dumps({"status": "refused", "reason": message}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
