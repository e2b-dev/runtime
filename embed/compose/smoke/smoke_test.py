#!/usr/bin/env python3
"""Host-side smoke test, run on the KVM host. Requires: pip install e2b==2.46.0 and the E2B_API_URL,
E2B_API_KEY, E2B_SANDBOX_URL variables shown by docker compose logs ready."""
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from e2b import Sandbox

PORT = 8080
REACH_RETRY_S = 0.5
REACH_TOTAL_S = 15.0

REQUIRED_FIX = (
    "FIX: export the E2B_API_URL, E2B_SANDBOX_URL and E2B_API_KEY values shown by docker compose logs ready"
)

for name in ("E2B_API_URL", "E2B_API_KEY", "E2B_SANDBOX_URL"):
    if not os.environ.get(name):
        print(f"smoke: {name} is required", file=sys.stderr)
        print(REQUIRED_FIX, file=sys.stderr)
        sys.exit(2)

SANDBOX_URL = os.environ["E2B_SANDBOX_URL"]
# Built once, before anything is created: a value that is not a URL is a
# configuration error to report now, not something to retry against for 15
# seconds and then blame on client-proxy. urljoin quietly yields a bare "/" for
# a value with no scheme, so the parts are what gets checked.
TARGET = urllib.parse.urljoin(SANDBOX_URL, "/")
_parts = urllib.parse.urlparse(TARGET)
if not _parts.scheme or not _parts.netloc:
    print(f"smoke: E2B_SANDBOX_URL is not a URL: {SANDBOX_URL}", file=sys.stderr)
    print(REQUIRED_FIX, file=sys.stderr)
    sys.exit(2)


def reach_port(sandbox_id):
    """Fetch the sandbox's exposed port through client-proxy.

    Returns None once it answers 200, or the last failure reason if it never
    does. get_host(port) returns {port}-{id}.e2b.app, which nothing here
    resolves; client-proxy routes on the two headers below instead, so that is
    the path this checks. The listener needs a moment to bind and client-proxy
    a moment to learn the sandbox, so this retries rather than sending one
    request. No attempt starts past the deadline and each one is bounded by the
    time left, so a request that never answers cannot stretch the 15 seconds
    this reports by more than one retry interval.
    """
    deadline = time.monotonic() + REACH_TOTAL_S
    last = "no attempt completed"
    while time.monotonic() < deadline:
        request = urllib.request.Request(
            TARGET,
            headers={"E2b-Sandbox-Id": sandbox_id, "E2b-Sandbox-Port": str(PORT)},
        )
        try:
            timeout = max(deadline - time.monotonic(), REACH_RETRY_S)
            with urllib.request.urlopen(request, timeout=timeout) as response:
                response.read()
                if response.status == 200:
                    return None
                last = f"status {response.status}"
        except urllib.error.HTTPError as err:
            # urllib raises for 4xx and 5xx rather than returning them.
            err.close()
            last = f"status {err.code}"
        except Exception as err:
            last = f"{type(err).__name__}: {err}"
        time.sleep(max(min(REACH_RETRY_S, deadline - time.monotonic()), 0))
    return last


sbx = None
listener = None
failed = False
port_unreachable = False
try:
    sbx = Sandbox.create("base", timeout=120)
    result = sbx.commands.run("echo hello")
    if result.exit_code != 0 or result.stdout.strip() != "hello":
        raise RuntimeError(f"unexpected command result: {result!r}")
    listener = sbx.commands.run(f"python3 -m http.server {PORT} --bind 0.0.0.0", background=True)
    reason = reach_port(sbx.sandbox_id)
    if reason is not None:
        print(
            f"smoke: port {PORT} was not reached through {SANDBOX_URL} within {REACH_TOTAL_S:g}s: {reason}",
            file=sys.stderr,
        )
        failed = True
        port_unreachable = True
    else:
        print(f"smoke: ok sandbox={sbx.sandbox_id} port {PORT} reached through {SANDBOX_URL}")
except Exception as err:
    print(f"smoke: failed: {err}", file=sys.stderr)
    failed = True
finally:
    # Ending the sandbox is part of what this test checks, so a kill failure is
    # still a failure: the exit status stays non-zero. It must read as a FIX
    # line though, never as a traceback out of the finally block. The FIX lines
    # are printed here, after the kill, so that when the test itself failed its
    # own FIX line is the last thing on stderr.
    # stdout is block-buffered when it is a pipe rather than a terminal, so the
    # "smoke: ok" line above would otherwise flush at exit — after these stderr
    # lines — and the FIX line would not be last in a combined log.
    sys.stdout.flush()
    if listener is not None:
        # Best effort, and never a failure of its own: this drops the local
        # stream carrying the listener's output. The command goes with the
        # sandbox.
        try:
            listener.disconnect()
        except Exception:
            pass
    kill_failed = False
    if sbx is not None:
        try:
            sbx.kill()
        except Exception as err:
            print(f"smoke: the sandbox could not be killed: {err}", file=sys.stderr)
            kill_failed = True
    if port_unreachable:
        print(
            "FIX: the exposed port could not be reached; read the client-proxy and orchestrator logs",
            file=sys.stderr,
        )
        sys.exit(1)
    if failed:
        print("FIX: run docker compose logs api orchestrator client-proxy", file=sys.stderr)
        sys.exit(1)
    if kill_failed:
        print(
            "FIX: the sandbox could not be killed; run docker compose logs api orchestrator",
            file=sys.stderr,
        )
        sys.exit(1)
