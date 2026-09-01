#!/usr/bin/env python3
"""Regression coverage for full-body public attribution verification."""

import importlib.util
import subprocess
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("authority", ROOT / "verify-authority.py")
AUTHORITY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUTHORITY)
HOSTED_SOURCE_REF = "50a7ef575e4481801712f17c4af22b85fdc31424"


def hosted_bytes(name):
    repository = ROOT.parents[1]
    return subprocess.run(
        ["git", "-C", str(repository), "show", f"{HOSTED_SOURCE_REF}:hosting/grit-hfx-v0.3.0/{name}"],
        check=True,
        capture_output=True,
    ).stdout


class PublicServer:
    def __init__(self, bodies):
        self.bodies = bodies
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = outer.bodies.get(self.path.lstrip("/"))
                if body is None:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_HEAD(self):
                metadata = {
                    "aux/d8/flow_dir.tif": (50686516478, '"bc48d1013cf6908fb44c325dd2ad10ab-1511"'),
                    "aux/d8/flow_acc.tif": (205069870081, '"49eab3942a26036aa49e72ea33a1b724-6112"'),
                }.get(self.path.lstrip("/"))
                if metadata is None:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(metadata[0]))
                self.send_header("ETag", metadata[1])
                self.end_headers()

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_port}/"

    def __exit__(self, exc_type, exc, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class PublicAttributionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.expected = {
            "manifest.json": (ROOT / "manifest.json").read_bytes(),
            "NOTICE": hosted_bytes("NOTICE"),
            "CITATION.txt": hosted_bytes("CITATION.txt"),
            "README.md": hosted_bytes("README.md"),
        }

    def test_each_attribution_object_drift_fails_public_verification(self):
        for name in ("NOTICE", "CITATION.txt", "README.md"):
            with self.subTest(name=name):
                bodies = dict(self.expected)
                bodies[name] += b"drift"
                with PublicServer(bodies) as base:
                    with self.assertRaises(AUTHORITY.VerificationError):
                        AUTHORITY.verify_public(ROOT, base)


if __name__ == "__main__":
    unittest.main()
