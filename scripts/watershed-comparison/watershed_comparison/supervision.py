"""sequential delineation : two dataset requests × resource bounds -> retained evidence."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil

from .models import GRIT_URI, DelineationRequest, ResourceLimits, sha256, write_json

GIB = 1024**3
PRIVATE_KEYS = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_ENDPOINT",
    "AWS_REGION",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST",
}


def isolated_environment(
    output: Path, credentials: dict[str, str] | None
) -> dict[str, str]:
    """Build a small allowlist; never inherit AWS, proxy, Python-path or user config."""
    home = output / "home"
    home.mkdir()
    cache = output / "cache"
    cache.mkdir()
    python_cache = output / "python-startup-cache"
    python_cache.mkdir()
    env = {
        "PATH": os.defpath,
        "HOME": str(home),
        "HFX_CACHE_DIR": str(cache),
        "PYTHONNOUSERSITE": "1",
        "PYTHONPYCACHEPREFIX": str(python_cache),
        "PYTHONUNBUFFERED": "1",
        "PROJ_NETWORK": "OFF",
        "LANG": "en_US.UTF-8",
        "TMPDIR": str(output),
        "MPLCONFIGDIR": str(home / "matplotlib"),
    }
    if credentials is not None:
        if set(credentials) - PRIVATE_KEYS:
            raise ValueError("unrecognized private credential configuration keys")
        required = {
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ENDPOINT",
            "AWS_REGION",
        }
        if not required <= credentials.keys() or any(
            not isinstance(v, str) or not v for v in credentials.values()
        ):
            raise ValueError("private S3 configuration is incomplete")
        from urllib.parse import urlsplit

        endpoint = urlsplit(credentials["AWS_ENDPOINT"])
        if (
            endpoint.scheme != "https"
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError("private S3 endpoint must be plain HTTPS")
        env.update(credentials)
        env["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "false"
    return env


def resource_sample(process, disk: Path, start: float) -> dict:
    """Process-tree RSS; system memory/swap/disk/network (network is host-wide)."""
    rss = 0
    if process is not None:
        try:
            processes = [psutil.Process(process.pid)]
            processes += processes[0].children(recursive=True)
            for member in processes:
                try:
                    rss += member.memory_info().rss
                except psutil.NoSuchProcess:
                    pass  # Exited member contributes no live resident memory.
        except psutil.NoSuchProcess:
            pass
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    network = psutil.net_io_counters()
    return {
        "elapsed_seconds": time.monotonic() - start,
        "rss_bytes": rss,
        "available_bytes": memory.available,
        "total_memory_bytes": memory.total,
        "swap_used_bytes": swap.used,
        "free_disk_bytes": psutil.disk_usage(disk).free,
        "host_network_received_bytes": network.bytes_recv,
        "host_network_sent_bytes": network.bytes_sent,
    }


def violation(sample: dict, initial: dict, limits: ResourceLimits) -> str | None:
    checks = [
        (sample["rss_bytes"] > limits.max_rss_gib * GIB, "process-tree RSS ceiling"),
        (
            sample["available_bytes"] < limits.min_available_gib * GIB,
            "available memory floor",
        ),
        (
            sample["swap_used_bytes"] - initial["swap_used_bytes"]
            > limits.max_swap_growth_gib * GIB,
            "swap growth ceiling",
        ),
        (sample["free_disk_bytes"] < limits.min_free_disk_gib * GIB, "free disk floor"),
        (sample["elapsed_seconds"] > limits.max_elapsed_seconds, "wallclock ceiling"),
    ]
    return next((name for failed, name in checks if failed), None)


def stop_process(process) -> None:
    """Terminate the entire detached group, including descendants of exited leader."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait(timeout=5)
        return
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        process.poll()  # Reap leader as soon as it exits; group can still live.
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            process.wait(timeout=5)
            return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass  # Group exited after the liveness check.
    process.wait(timeout=5)


def _execute_request(
    request: DelineationRequest,
    output: Path,
    credentials,
    limits: ResourceLimits,
    cancellation: list[int],
):
    output.mkdir()
    env = isolated_environment(output, credentials)
    request_path = output / "request.json"
    write_json(request_path, request.model_dump())
    started = time.monotonic()
    initial = resource_sample(None, output, started)
    write_json(
        output / "resource_preflight.json",
        {
            "sample": initial,
            "limits": limits.model_dump(),
            "network_guarantee": "host-wide cumulative counters; includes unrelated traffic; not object-store wire accounting",
            "rss_guarantee": "sampled process-tree RSS, shared pages may count more than once; no hard memory reservation",
        },
    )
    problem = violation(initial, initial, limits)
    if problem:
        raise RuntimeError(problem)
    process = None
    try:
        with (
            (output / "stderr.log").open("xb") as stderr,
            (output / "stdout.log").open("xb") as stdout,
            (output / "resources.jsonl").open("x") as resources,
        ):
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "watershed_comparison.cli",
                    "delineate",
                    str(request_path),
                    str(output / "watershed"),
                ],
                env=env,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            while True:
                if cancellation:
                    raise DelineationCancelled(cancellation[0])
                sample = resource_sample(process, output, started)
                resources.write(json.dumps(sample, allow_nan=False) + "\n")
                resources.flush()
                problem = violation(sample, initial, limits)
                if problem:
                    raise RuntimeError(problem)
                code = process.poll()
                if code is not None:
                    if code != 0:
                        raise RuntimeError(
                            "consumer process failed; inspect retained stage evidence"
                        )
                    break
                time.sleep(limits.sample_seconds)
        success = json.loads((output / "watershed/success.json").read_text())
        if sha256(output / "watershed/metadata.json") != success["metadata_sha256"]:
            raise ValueError("worker success receipt does not match metadata")
    finally:
        if process is not None:
            stop_process(process)


class DelineationCancelled(InterruptedError):
    """Raised after an OS cancellation signal, once the child can be reaped safely."""

    def __init__(self, signum: int):
        super().__init__("supervised delineation cancelled")
        self.signum = signum


def execute_request(
    request: DelineationRequest, output: Path, credentials, limits: ResourceLimits
):
    """Keep SIGTERM/SIGINT under supervision until the child group is reaped."""
    cancellation = []

    def request_cancellation(signum, frame):
        cancellation.append(signum)

    previous = {
        kind: signal.getsignal(kind) for kind in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        for kind in previous:
            signal.signal(kind, request_cancellation)
        try:
            _execute_request(request, output, credentials, limits, cancellation)
        except BaseException as exc:
            if output.is_dir():
                write_json(
                    output / "supervision_failure.json",
                    {
                        "exception_type": type(exc).__name__,
                        "signal": exc.signum
                        if isinstance(exc, DelineationCancelled)
                        else None,
                        "action": "stop and reap child group; request maintainer decision",
                    },
                )
            raise
    finally:
        for kind, handler in previous.items():
            signal.signal(kind, handler)
    if cancellation:
        write_json(
            output / "supervision_failure.json",
            {
                "exception_type": "DelineationCancelled",
                "signal": cancellation[0],
                "action": "completed child reaped; comparison cancelled; request maintainer decision",
            },
        )
        raise DelineationCancelled(cancellation[0])


def verify_delivery(receipt_path: Path, dataset) -> dict:
    """Require complete destination verification before any private session opens."""
    receipt = json.loads(receipt_path.read_text())
    if (
        receipt.get("schema") != "hfx-dataset-delivery-v1"
        or receipt.get("status") != "verified-dataset"
    ):
        raise ValueError("delivery receipt does not establish verified destination")
    plan = receipt["plan"]
    uri = f"s3://{plan['bucket']}/{plan['destination_prefix'].strip('/')}"
    if uri != dataset.uri.rstrip("/"):
        raise ValueError("delivery receipt destination differs from consumer request")
    manifest = receipt["objects"]["manifest.json"]["verification"]
    if manifest["sha256"] != dataset.manifest_sha256:
        raise ValueError("delivered manifest hash differs from consumer request")
    expected = {
        artifact["path"]: (artifact["sha256"], artifact["identity"]["bytes"])
        for artifact in plan["objects"]
    }
    if len(expected) != len(plan["objects"]) or "README.md" in expected:
        raise ValueError(
            "delivery plan object paths are duplicate or conflict with README"
        )
    expected["README.md"] = (plan["readme_sha256"], plan["readme_bytes"])
    if set(receipt["objects"]) != set(expected):
        raise ValueError("delivery receipt object inventory differs from complete plan")
    for path, (checksum, size) in expected.items():
        obj = receipt["objects"][path]
        actual = obj["verification"]
        if (
            obj["status"] != "verified"
            or actual["method"] != "full-ordered-range-stream-sha256"
            or actual["sha256"] != checksum
            or actual["bytes"] != size
            or obj["identity"]["bytes"] != size
        ):
            raise ValueError(
                "delivery receipt contains incomplete payload verification"
            )
    if receipt["final_bytes"] != sum(size for _, size in expected.values()):
        raise ValueError(
            "delivery receipt final byte total differs from complete inventory"
        )
    return {
        "path": str(receipt_path.resolve()),
        "sha256": sha256(receipt_path),
        "endpoint": plan["endpoint"],
        "region": plan["region"],
    }


def compare_sequentially(
    first: DelineationRequest,
    second: DelineationRequest,
    output: Path,
    credentials: dict,
    limits: ResourceLimits,
    delivery_receipt: Path,
):
    """Verified private dataset first, supported public dataset second; no retry."""
    if not first.dataset.uri.startswith("s3://"):
        raise ValueError("first dataset must use the verified private S3 destination")
    if second.dataset.uri != GRIT_URI:
        raise ValueError("second dataset must be the exact public GRIT endpoint")
    if (
        first.input_outlet != second.input_outlet
        or first.consumer != second.consumer
        or first.settings != second.settings
    ):
        raise ValueError(
            "both requests must use identical point, consumer identity and settings"
        )
    delivery = verify_delivery(delivery_receipt, first.dataset)
    if (
        credentials.get("AWS_ENDPOINT") != delivery["endpoint"]
        or credentials.get("AWS_REGION") != delivery["region"]
    ):
        raise ValueError(
            "private endpoint/region differs from verified delivery receipt"
        )
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "delivery_receipt_identity.json", delivery)
    write_json(
        output / "comparison_request.json",
        {
            "first": first.model_dump(),
            "second": second.model_dump(),
            "limits": limits.model_dump(),
        },
    )
    stage = "private_dataset"
    try:
        execute_request(first, output / "private", credentials, limits)
        stage = "public_dataset"
        execute_request(second, output / "public", None, limits)
        write_json(output / "success.json", {"datasets": ["private", "public"]})
    except BaseException as exc:
        write_json(
            output / "failure.json",
            {
                "stage": stage,
                "exception_type": type(exc).__name__,
                "action": "stop; obtain maintainer decision; no retry performed",
            },
        )
        raise
