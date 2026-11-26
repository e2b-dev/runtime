#!/usr/bin/env python3
"""Exercise envd /init against reachable MMDS in private Linux namespaces.

Usage: python3 scripts/test-envd-mmds-mode.py /absolute/path/to/envd
Requires Python 3, Bubblewrap with user namespaces, and iproute2. No host network,
/run files, cgroups or credentials are changed. This is not a Firecracker VM test.
"""

import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request


METADATA_ADDRESS = "169.254.169.254"
TOKEN = "fixture-metadata-token"


def wait_for(predicate, message):
    deadline = time.monotonic() + 5
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(message)
        time.sleep(0.01)


def exercise(binary):
    parent_netns = os.environ["ENVD_MMDS_REPRO_PARENT_NETNS"]
    if os.readlink("/proc/self/ns/net") == parent_netns:
        raise RuntimeError("refusing to configure metadata outside a private network namespace")
    subprocess.run(["ip", "addr", "add", METADATA_ADDRESS + "/32", "dev", "lo"], check=True)
    lock = threading.Lock()
    requests = []
    epoch = "startup"

    class Metadata(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def respond(self, status, payload):
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_PUT(self):
            with lock:
                requests.append(self.path)
            if self.path != "/latest/api/token" or self.headers.get("X-metadata-token-ttl-seconds") != "60":
                self.respond(400, b"invalid metadata token request")
                return
            self.respond(200, TOKEN.encode())

        def do_GET(self):
            with lock:
                requests.append(self.path)
                current = epoch
            if self.path != "/" or self.headers.get("X-metadata-token") != TOKEN:
                self.respond(400, b"invalid metadata options request")
                return
            self.respond(200, json.dumps({"instanceID": "metadata-" + current, "envID": "metadata-template"}).encode())

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def envd_request(path, data=None):
        request = urllib.request.Request("http://127.0.0.1:49983" + path, data=data)
        with opener.open(request, timeout=1) as response:
            return response.status

    def request_count():
        with lock:
            return len(requests)

    identity = Path("/run/e2b/.E2B_SANDBOX_ID")
    template = Path("/run/e2b/.E2B_TEMPLATE_ID")

    def has_identity(value):
        return identity.exists() and identity.read_text() == value

    with http.server.HTTPServer((METADATA_ADDRESS, 80), Metadata) as metadata:
        thread = threading.Thread(target=metadata.serve_forever, daemon=True)
        thread.start()
        try:
            for standalone in (False, True):
                with lock:
                    requests.clear()
                    epoch = "startup"
                args = [binary, "-no-cgroups", "-port", "49983"]
                if standalone:
                    args.append("-isnotfc")
                with tempfile.TemporaryFile() as log:
                    process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
                    try:
                        def ready():
                            if process.poll() is not None:
                                raise AssertionError("envd exited before serving health")
                            try:
                                return envd_request("/health") == 204
                            except (urllib.error.URLError, TimeoutError):
                                return False

                        wait_for(ready, "envd health did not become available")
                        if standalone:
                            if request_count():
                                raise AssertionError("standalone startup contacted MMDS")
                            identity.write_text("explicit-instance")
                            template.write_text("explicit-template")
                        else:
                            wait_for(lambda: has_identity("metadata-startup"), "Firecracker mode did not fetch startup metadata")
                        before = request_count()
                        with lock:
                            epoch = "after-init"
                        status = envd_request("/init", json.dumps({"envVars": {"E2B_SANDBOX_ID": "explicit-instance"}}).encode())
                        if status != 204:
                            raise AssertionError("/init did not succeed")
                        if standalone:
                            # A reachable fixture catches polling rather than relying on absent logs.
                            deadline = time.monotonic() + 1
                            while time.monotonic() < deadline:
                                if request_count() != before:
                                    raise AssertionError("standalone /init contacted MMDS")
                                if process.poll() is not None:
                                    raise AssertionError("standalone envd exited after /init")
                                time.sleep(0.01)
                            if not has_identity("explicit-instance") or template.read_text() != "explicit-template":
                                raise AssertionError("standalone /init replaced explicit identity files")
                        else:
                            wait_for(lambda: has_identity("metadata-after-init"), "Firecracker mode did not refresh metadata after /init")
                            if request_count() < before + 2:
                                raise AssertionError("Firecracker refresh omitted the token/options exchange")
                        print(("standalone" if standalone else "Firecracker-mode") + " /init metadata regression: PASS", flush=True)
                    finally:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        if Path("/run/e2b").exists():
                            shutil.rmtree("/run/e2b")
        finally:
            metadata.shutdown()
            thread.join(timeout=5)


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--inside":
        exercise(sys.argv[2])
        return
    if len(sys.argv) != 2 or not sys.platform.startswith("linux"):
        raise SystemExit("usage (Linux): python3 scripts/test-envd-mmds-mode.py /absolute/path/to/envd")
    binary = Path(sys.argv[1]).resolve(strict=True)
    for command in ("bwrap", "ip"):
        if shutil.which(command) is None:
            raise SystemExit("required command unavailable: " + command)
    command = ["bwrap", "--unshare-all", "--die-with-parent", "--new-session", "--uid", "0", "--gid", "0",
               "--cap-add", "CAP_NET_ADMIN", "--cap-add", "CAP_NET_BIND_SERVICE", "--ro-bind", "/", "/",
               "--tmpfs", "/run", "--tmpfs", "/tmp", "--proc", "/proc", "--dev", "/dev",
               "--ro-bind", str(binary), "/tmp/envd", "--ro-bind", str(Path(__file__).resolve()), "/tmp/repro.py",
               "--chdir", "/tmp", sys.executable, "/tmp/repro.py", "--inside", "/tmp/envd"]
    env = {"PATH": os.environ["PATH"], "HOME": "/tmp", "TMPDIR": "/tmp",
           "ENVD_MMDS_REPRO_PARENT_NETNS": os.readlink("/proc/self/ns/net")}
    subprocess.run(command, env=env, check=True, timeout=30)


if __name__ == "__main__":
    main()
