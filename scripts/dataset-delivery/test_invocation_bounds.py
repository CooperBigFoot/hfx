"""Local real-boto HTTP interruption regressions, not provider capability evidence."""

from contextlib import ExitStack, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

import boto3
from botocore import UNSIGNED
from botocore.config import Config

import conditional_writes_probe as probe
import dataset_delivery as delivery
from test_dataset_delivery import Anonymous, Storage


class NetworkStorage(Storage):
    """Keep prerequisites local; route the normal GET to a real boto HTTP client."""

    def __init__(self, client, prerequisite_delay=0):
        super().__init__()
        self.client = client
        self.read_entered = False
        self.prerequisite_delay = prerequisite_delay

    def get_bucket_policy(self, **kwargs):
        if self.prerequisite_delay:
            threading.Event().wait(self.prerequisite_delay)
        return super().get_bucket_policy(**kwargs)

    def close(self):
        self.client.close()

    def list_multipart_uploads(self, **kwargs):
        return {"Uploads": [], "IsTruncated": False}

    def put_object(self, **kwargs):
        result = super().put_object(**kwargs)
        result["ResponseMetadata"] = {"HTTPStatusCode": 200}
        return result

    def get_object(self, **kwargs):
        self.read_entered = True
        return self.client.get_object(**kwargs)


class LocalAnonymous(Anonymous):
    def close(self):
        pass


@unittest.skipUnless(hasattr(signal, "setitimer"), "POSIX signal deadline required")
class InvocationBoundsTests(unittest.TestCase):
    def exercise(self, module, mode, interrupt, prerequisite_delay=0):
        stop = threading.Event()
        entered = threading.Event()
        sent = threading.Event()
        sent_at = []
        payload = probe.INITIAL
        storage = None

        class Response(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                entered.set()
                if mode == "headers" and stop.wait(6):
                    return
                key = self.path.split("/", 2)[2].split("?", 1)[0]
                identity = storage.objects[key][1]
                ranged = "Range" in self.headers
                self.send_response(206 if ranged else 200)
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("ETag", identity.etag)
                if ranged:
                    self.send_header("Content-Range", f"bytes 0-{len(payload)-1}/{len(payload)}")
                self.end_headers()
                try:
                    for byte in payload:
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()
                        # Regular bytes prevent an inactivity timeout from enforcing runtime.
                        if mode == "body" and stop.wait(0.15):
                            return
                except (BrokenPipeError, ConnectionResetError):
                    pass

        server = HTTPServer(("127.0.0.1", 0), Response)
        # Keep accepting throughout durable prerequisite work. A one-shot
        # handle_request timeout can silently stop before the first real GET.
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.05), daemon=True)
        client = boto3.client(
            "s3", endpoint_url=f"http://127.0.0.1:{server.server_port}",
            region_name="test-1", aws_access_key_id="local", aws_secret_access_key="local",
            config=Config(signature_version=UNSIGNED, read_timeout=10, connect_timeout=1,
                          retries={"total_max_attempts": 1},
                          response_checksum_validation="when_required",
                          s3={"addressing_style": "path"}),
        )
        storage = NetworkStorage(client, prerequisite_delay)
        identity = storage.add("scratch/preserved/manifest.json", payload)
        source = delivery.PreservedDataset(
            "https://local.invalid", "test-1", "private-bucket", "scratch/preserved/",
            (delivery.DatasetObject("manifest.json", delivery.digest(payload), identity),), "a" * 64,
        )
        # Allow fsync prerequisites on CI while the server remains blocked for
        # at least six seconds without the production interruption mechanism.
        seconds = 3 if interrupt == "deadline" else 10
        handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM)}
        old_timer = signal.getitimer(signal.ITIMER_REAL)

        def terminate():
            while not stop.is_set():
                if entered.wait(0.05):
                    if not stop.wait(0.1):
                        sent_at.append(time.monotonic())
                        sent.set()
                        os.kill(os.getpid(), signal.SIGTERM)
                    return

        sender = threading.Thread(target=terminate, daemon=True) if interrupt == "sigterm" else None
        stderr = io.StringIO()
        try:
            with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                base = Path(directory)
                readme = base / "README.md"
                readme.write_text("Private local fixture")
                argv = [module.__file__, "--source-inventory", str(base / "inventory.json"),
                        "--profile", "local-fixture", "--evidence-dir", str(base / "evidence")]
                if module is delivery:
                    argv += ["plan", "--destination-prefix", "hfx/test", "--readme", str(readme),
                             "--max-seconds", str(seconds), "--max-read-bytes", "4096"]
                else:
                    argv += ["--execute", "--max-seconds", str(seconds)]
                stack.enter_context(patch.object(module.PreservedDataset, "load", return_value=source))
                session = stack.enter_context(patch.object(module.boto3, "Session"))
                session.return_value.client.return_value = storage
                stack.enter_context(patch.object(module.boto3, "client", return_value=LocalAnonymous()))
                stack.enter_context(patch.object(sys, "argv", argv))
                stack.enter_context(redirect_stdout(io.StringIO()))
                stack.enter_context(redirect_stderr(stderr))
                thread.start()
                if sender:
                    sender.start()
                started = time.monotonic()
                result = module.main()
                elapsed = time.monotonic() - started
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL), old_timer)
                for sig, handler in handlers.items():
                    self.assertEqual(signal.getsignal(sig), handler)
                journal = base / "evidence" / "delivery.json"
                if module is probe:
                    self.assertTrue(journal.exists(), "probe must retain its in-progress receipt")
                if journal.exists():
                    receipt = json.loads(journal.read_text())
                    self.assertEqual(receipt["status"], "refused")
                    self.assertIn("cancelled" if sender else "runtime limit",
                                  receipt["last_failure"]["reason"])
            self.assertTrue(storage.read_entered, "must enter normal action's real boto GET")
            self.assertTrue(entered.is_set(), "local HTTP request must arrive")
            if sender:
                self.assertTrue(sent.is_set(), "must deliver actual SIGTERM during HTTP I/O")
            self.assertEqual(result, 1, stderr.getvalue())
            refusal = json.loads(stderr.getvalue().splitlines()[-1])
            self.assertEqual(refusal["status"], "refused")
            self.assertIn("cancelled" if sender else "runtime limit", refusal["reason"])
            # Cancellation latency starts at the actual signal, independently
            # of variable durable preflight time. Deadline remains total time.
            latency = started + elapsed - sent_at[0] if sender else elapsed
            self.assertLess(latency, 1.0 if sender else seconds + 0.75,
                            f"{module.__name__} {mode} {interrupt} blocked for {latency:.3f}s")
        finally:
            stop.set()
            try:
                if sender:
                    sender.join(timeout=2)
                    self.assertFalse(sender.is_alive(), "signal sender did not stop")
                if thread.ident is not None:
                    server.shutdown()
                    thread.join(timeout=2)
                    self.assertFalse(thread.is_alive(), "local HTTP server did not stop")
            finally:
                client.close()
                server.server_close()
                signal.setitimer(signal.ITIMER_REAL, 0)
                for sig, handler in handlers.items():
                    signal.signal(sig, handler)
                signal.setitimer(signal.ITIMER_REAL, *old_timer)

    def test_delivery_plan_read_only_preflight_deadline_interrupts_real_http(self):
        for mode in ("body", "headers"):
            with self.subTest(mode=mode):
                self.exercise(delivery, mode, "deadline")

    def test_probe_deadline_interrupts_real_http(self):
        for mode in ("body", "headers"):
            with self.subTest(mode=mode):
                self.exercise(probe, mode, "deadline")

    def test_delivery_plan_read_only_preflight_sigterm_interrupts_real_http(self):
        for mode in ("body", "headers"):
            with self.subTest(mode=mode):
                self.exercise(delivery, mode, "sigterm")

    def test_probe_listener_survives_slow_prerequisites(self):
        self.exercise(probe, "headers", "sigterm", prerequisite_delay=0.3)

    def test_probe_sigterm_interrupts_real_http(self):
        for mode in ("body", "headers"):
            with self.subTest(mode=mode):
                self.exercise(probe, mode, "sigterm")


if __name__ == "__main__":
    unittest.main()
