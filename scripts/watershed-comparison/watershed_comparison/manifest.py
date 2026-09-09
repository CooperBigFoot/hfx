"""observe : dataset identity -> bounded manifest bytes and read-time identity evidence."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .models import GRIT_URI, DatasetIdentity

MAX_MANIFEST_BYTES = 1024 * 1024


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("manifest redirects are not supported")


def _bounded_read(stream):
    content = stream.read(MAX_MANIFEST_BYTES + 1)
    if len(content) > MAX_MANIFEST_BYTES:
        raise ValueError("manifest exceeds 1 MiB observation bound")
    return content


def observe_manifest(dataset: DatasetIdentity, output: Path, phase: str, s3: dict):
    """Read and preserve bounded bytes before checking caller identity.

    These observations bracket session use. They do not pin the engine's own
    independent requests or establish immutable remote object versions.
    """
    uri = dataset.uri.rstrip("/") + "/manifest.json"
    source = urlsplit(dataset.uri)
    remote = {}
    if source.scheme == "s3":
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            endpoint_url=s3["AWS_ENDPOINT"],
            region_name=s3["AWS_REGION"],
            aws_access_key_id=s3["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=s3["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=s3.get("AWS_SESSION_TOKEN"),
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"total_max_attempts": 1},
                connect_timeout=15,
                read_timeout=60,
            ),
        )
        response = client.get_object(
            Bucket=source.netloc, Key=source.path.strip("/") + "/manifest.json"
        )
        with response["Body"] as stream:
            content = _bounded_read(stream)
        remote = {
            "etag": response.get("ETag"),
            "version_id": response.get("VersionId"),
            "last_modified": response["LastModified"].isoformat(),
        }
    elif source.scheme == "https":
        if dataset.uri != GRIT_URI:
            raise ValueError(
                "only the exact supported public dataset URI can be observed"
            )
        # No environment proxy, auth handler, redirects or AWS/session credentials.
        opener = build_opener(ProxyHandler({}), NoRedirect())
        with opener.open(
            Request(uri, headers={"Accept": "application/json"}), timeout=60
        ) as stream:
            content = _bounded_read(stream)
            remote = {
                "etag": stream.headers.get("ETag"),
                "last_modified": stream.headers.get("Last-Modified"),
            }
    elif not source.scheme:
        with (Path(dataset.uri) / "manifest.json").open("rb") as stream:
            content = _bounded_read(stream)
    else:
        raise ValueError("unsupported manifest source")
    with (output / f"manifest-{phase}.json").open("xb") as stream:
        stream.write(content)
    observed = {
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
        "source_uri": uri,
        "observed_at": datetime.now(UTC).isoformat(),
        **remote,
    }
    manifest = json.loads(content)
    if observed["sha256"] != dataset.manifest_sha256:
        raise ValueError("observed manifest checksum differs from dataset identity")
    if (
        manifest["fabric_name"] != dataset.fabric_name
        or manifest.get("fabric_version") != dataset.fabric_version
    ):
        raise ValueError("observed manifest fabric identity differs from request")
    return observed
