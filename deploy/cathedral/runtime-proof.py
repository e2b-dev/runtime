#!/usr/bin/env python3
"""One-sandbox Cathedral runtime durability proof. No customer/API admission."""

import argparse
import asyncio
from datetime import datetime, timezone
import json
import pathlib
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def request(opener, base, key, method, path, body=None, operation_key=None):
    headers = {"X-API-Key": key, "Accept": "application/json"}
    if operation_key:
        headers["Idempotency-Key"] = operation_key
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with opener.open(req, timeout=12) as response:
            raw = response.read(256 * 1024 + 1)
            status = response.status
    except urllib.error.HTTPError as error:
        # Do not read or log bodies: create/lookup can contain envd access tokens.
        return error.code, None
    except (urllib.error.URLError, TimeoutError, OSError):
        return None, None
    if len(raw) > 256 * 1024:
        return None, None
    try:
        return status, json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return status, None


def lookup_until(opener, base, key, path, wanted, deadline):
    last_status = None
    last = None
    while time.monotonic() < deadline:
        last_status, last = request(opener, base, key, "GET", path)
        if last_status == 200 and isinstance(last, dict) and wanted(last):
            return last
        time.sleep(1)
    return last if last_status == 200 and isinstance(last, dict) else None


def guest_detail_ok(detail, sandbox_id):
    if not isinstance(detail, dict) or detail.get("sandboxID") != sandbox_id or detail.get("state") != "running":
        return False
    lifecycle = detail.get("lifecycle")
    network = detail.get("network")
    if (not isinstance(lifecycle, dict) or lifecycle.get("autoResume") is not False or
            lifecycle.get("onTimeout") != "kill" or detail.get("allowInternetAccess") is not False or
            not isinstance(network, dict) or network.get("allowPublicTraffic") is not False):
        return False
    if not isinstance(detail.get("envdAccessToken"), str) or not detail["envdAccessToken"]:
        return False
    if not isinstance(detail.get("envdVersion"), str) or not detail["envdVersion"]:
        return False
    try:
        end_at = datetime.fromisoformat(detail["endAt"].replace("Z", "+00:00"))
    except (KeyError, AttributeError, ValueError):
        return False
    return end_at.tzinfo is not None and (end_at - datetime.now(timezone.utc)).total_seconds() >= 45


async def guest_roundtrip(detail, sandbox_id, api_url, sandbox_url, api_key):
    # Same direct constructor used by the Cathedral API adapter. It does not
    # call SDK connect or change the sandbox's lifetime.
    from e2b import AsyncSandbox
    from e2b.connection_config import ConnectionConfig
    from packaging.version import Version

    token = detail["envdAccessToken"]
    connection = ConnectionConfig(
        api_key=api_key, api_url=api_url, sandbox_url=sandbox_url,
        request_timeout=15.0, retries=0, debug=False,
        extra_sandbox_headers={
            "E2b-Sandbox-Id": sandbox_id,
            "E2b-Sandbox-Port": str(ConnectionConfig.envd_port),
            "X-Access-Token": token,
        },
    )
    sandbox = AsyncSandbox(
        sandbox_id=sandbox_id, sandbox_domain=detail.get("domain"),
        envd_version=Version(detail["envdVersion"]), envd_access_token=token,
        traffic_access_token=None, connection_config=connection,
    )
    command_state = "NOT_PROVEN"
    file_state = "NOT_PROVEN"
    try:
        result = await sandbox.commands.run(
            "printf 'cathedral-proof-command'", user="root", timeout=10.0, request_timeout=10.0,
        )
        command_state = "PASS" if result.exit_code == 0 and result.stdout == "cathedral-proof-command" else "FAIL"
    except Exception:
        pass
    path = "/tmp/cathedral-proof-" + uuid.uuid4().hex + ".txt"
    expected = b"cathedral-proof-file\n"
    try:
        await sandbox.files.write(path, expected, user="root", request_timeout=10.0)
        stream = await sandbox.files.read(
            path, format="stream", user="root", request_timeout=10.0, stream_idle_timeout=10.0,
        )
        actual = bytearray()
        async with stream:
            async for block in stream:
                actual.extend(bytes(block))
                if len(actual) > 128:
                    break
        file_state = "PASS" if bytes(actual) == expected else "FAIL"
    except Exception:
        pass
    return command_state, file_state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key-file", required=True, type=pathlib.Path)
    parser.add_argument("--template-id", required=True)
    parser.add_argument("--sandbox-url", default="http://127.0.0.1:3002")
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.api_url)
    if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
        parser.error("API URL must be an origin without credentials, path, query or fragment")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "localhost", "::1")
    ):
        parser.error("API URL must use HTTPS, except for host loopback HTTP")
    if not parsed.netloc or not args.template_id or "/" in args.template_id:
        parser.error("API URL and template ID are required")
    sandbox_url = urllib.parse.urlsplit(args.sandbox_url)
    if (sandbox_url.scheme != "https" and not
            (sandbox_url.scheme == "http" and sandbox_url.hostname in ("127.0.0.1", "localhost", "::1"))):
        parser.error("sandbox URL must use HTTPS, except for host loopback HTTP")
    if (not sandbox_url.netloc or sandbox_url.username or sandbox_url.password or sandbox_url.query or
            sandbox_url.fragment or sandbox_url.path not in ("", "/")):
        parser.error("sandbox URL must be an origin without credentials, path, query or fragment")
    try:
        key = args.api_key_file.read_text(encoding="utf-8").strip()
    except OSError:
        parser.error("API key file is not readable")
    if not key or "\n" in key or "\r" in key:
        parser.error("API key file must contain exactly one key")

    base = args.api_url.rstrip("/")
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context()))
    create_key = "cathedral-create-" + uuid.uuid4().hex
    delete_key = "cathedral-delete-" + uuid.uuid4().hex
    print("proof_scope=RUNTIME_ONLY", flush=True)
    print("create_key=" + create_key, flush=True)
    print("delete_key=" + delete_key, flush=True)

    cap_status, cap = request(opener, base, key, "GET", "/v1/cathedral/capabilities")
    required = ("durable_create_idempotency", "operation_lookup", "durable_lifecycle_operations",
                "safe_delete", "execution_identity")
    if cap_status != 200 or not isinstance(cap, dict) or cap.get("schema") != 1 or not all(cap.get(k) is True for k in required):
        print("capabilities=FAIL", flush=True)
        return 1
    print("capabilities=PASS", flush=True)

    body = {"templateID": args.template_id, "timeout": 120, "autoPause": False,
            "secure": True, "allow_internet_access": False,
            "network": {"allowPublicTraffic": False, "denyOut": ["0.0.0.0/0"]}}
    create_status, created = request(opener, base, key, "POST", "/sandboxes", body, create_key)
    create_path = "/v1/cathedral/operations/" + create_key
    op = lookup_until(opener, base, key, create_path,
                      lambda item: item.get("state") in ("ready", "failed"), time.monotonic() + 35)
    sandbox_id = op.get("sandbox_id") if isinstance(op, dict) else None
    if not isinstance(sandbox_id, str) or not sandbox_id:
        print("durable_create=NOT_PROVEN", flush=True)
        print("cleanup=NOT_PROVEN", flush=True)
        return 1
    print("sandbox_id=" + sandbox_id, flush=True)
    if op.get("state") != "ready" or not isinstance(op.get("sandbox"), dict):
        print("durable_create=FAIL", flush=True)
        print("cleanup=NOT_PROVEN", flush=True)
        return 1
    create_ok = (op["sandbox"].get("sandboxID") == sandbox_id and
                 (create_status != 201 or (isinstance(created, dict) and created.get("sandboxID") == sandbox_id)))
    if not create_ok:
        print("durable_create=FAIL", flush=True)
    else:
        print("durable_create=PASS", flush=True)
    # Replay only after the durable lookup says ready. It must return the same
    # ID and must not dispatch a second create.
    replay_status, replay = request(opener, base, key, "POST", "/sandboxes", body, create_key)
    replay_ok = (replay_status == 201 and isinstance(replay, dict) and replay.get("sandboxID") == sandbox_id)
    print("same_id_recovery=" + ("PASS" if replay_ok else "FAIL"), flush=True)

    encoded_id = urllib.parse.quote(sandbox_id, safe="")
    identity_status = None
    identity = None
    identity_deadline = time.monotonic() + 10
    while time.monotonic() < identity_deadline:
        identity_status, identity = request(opener, base, key, "GET", f"/v1/cathedral/sandboxes/{encoded_id}/identity")
        if identity_status == 200 and isinstance(identity, dict) and identity.get("state") == "running":
            break
        time.sleep(1)
    if (identity_status != 200 or not isinstance(identity, dict) or
            identity.get("sandbox_id") != sandbox_id or identity.get("state") != "running" or
            not isinstance(identity.get("execution_id"), str) or not identity["execution_id"]):
        print("execution_identity=NOT_PROVEN", flush=True)
        print("cleanup=NOT_PROVEN", flush=True)
        return 1
    execution_id = identity["execution_id"]
    print("execution_identity=PASS", flush=True)

    detail_status, detail = request(opener, base, key, "GET", f"/sandboxes/{encoded_id}")
    if detail_status == 200 and guest_detail_ok(detail, sandbox_id):
        try:
            command_state, file_state = asyncio.run(
                guest_roundtrip(detail, sandbox_id, base, args.sandbox_url.rstrip("/"), key)
            )
        except Exception:
            command_state = file_state = "NOT_PROVEN"
    else:
        command_state = file_state = "NOT_PROVEN"
    print("guest_command=" + command_state, flush=True)
    print("guest_file=" + file_state, flush=True)

    delete_path = f"/v1/cathedral/sandboxes/{encoded_id}/lifecycle-operations"
    delete_body = {"operation": "delete", "execution_id": execution_id}
    # One dispatch only. An ambiguous response is recovered by the durable key.
    request(opener, base, key, "POST", delete_path, delete_body, delete_key)
    delete_lookup = "/v1/cathedral/lifecycle-operations/" + delete_key
    deleted = lookup_until(opener, base, key, delete_lookup,
                           lambda item: item.get("state") in ("failed", "unknown") or
                           (item.get("state") == "completed" and item.get("cleanup_state") == "completed"),
                           time.monotonic() + 35)
    if (not isinstance(deleted, dict) or deleted.get("operation_key") != delete_key or
            deleted.get("sandbox_id") != sandbox_id or deleted.get("execution_id") != execution_id or
            deleted.get("state") != "completed" or deleted.get("cleanup_state") != "completed" or
            not deleted.get("execution_removed_at")):
        print("durable_delete=NOT_PROVEN", flush=True)
        print("cleanup=NOT_PROVEN", flush=True)
        return 1
    print("durable_delete=PASS", flush=True)
    print("cleanup=CONFIRMED", flush=True)
    return 0 if create_ok and replay_ok and command_state == file_state == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
