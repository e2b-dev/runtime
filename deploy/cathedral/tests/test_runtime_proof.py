import contextlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "runtime-proof.py"
spec = importlib.util.spec_from_file_location("runtime_proof", SCRIPT)
runtime_proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime_proof)


class RuntimeProofTest(unittest.TestCase):
    def test_one_create_replay_and_execution_bound_delete_without_secret_output(self):
        calls = []

        def fake_request(_opener, _base, key, method, path, body=None, operation_key=None):
            self.assertEqual(key, "secret-key")
            calls.append((method, path, body, operation_key))
            if path.endswith("/capabilities"):
                return 200, {"schema": 1, "durable_create_idempotency": True,
                             "operation_lookup": True, "durable_lifecycle_operations": True,
                             "safe_delete": True, "execution_identity": True}
            if path == "/sandboxes":
                return 201, {"sandboxID": "i-1", "envdAccessToken": "do-not-print"}
            if path.endswith("/identity"):
                return 200, {"sandbox_id": "i-1", "execution_id": "execution-1", "state": "running"}
            if path == "/sandboxes/i-1":
                return 200, {"sandboxID": "i-1", "state": "running", "allowInternetAccess": False,
                             "network": {"allowPublicTraffic": False},
                             "lifecycle": {"autoResume": False, "onTimeout": "kill"},
                             "envdAccessToken": "do-not-print", "envdVersion": "0.9.0",
                             "endAt": "2099-01-01T00:00:00Z"}
            if path.endswith("/lifecycle-operations"):
                return 201, {"state": "completed"}
            self.fail(f"unexpected request {method} {path}")

        def fake_lookup(_opener, _base, _key, path, _wanted, _deadline):
            if "/lifecycle-operations/" in path:
                return {"operation_key": path.rsplit("/", 1)[1], "sandbox_id": "i-1",
                        "execution_id": "execution-1", "state": "completed",
                        "cleanup_state": "completed", "execution_removed_at": "2026-09-22T00:00:00Z"}
            return {"state": "ready", "sandbox_id": "i-1",
                    "sandbox": {"sandboxID": "i-1", "envdAccessToken": "do-not-print"}}

        with tempfile.TemporaryDirectory() as directory:
            key_file = pathlib.Path(directory) / "key"
            key_file.write_text("secret-key\n")
            output = io.StringIO()
            with (mock.patch.object(sys, "argv", [str(SCRIPT), "--api-url", "http://127.0.0.1:3000",
                                                 "--api-key-file", str(key_file), "--template-id", "base"]),
                  mock.patch.object(runtime_proof, "request", fake_request),
                  mock.patch.object(runtime_proof, "lookup_until", fake_lookup),
                  mock.patch.object(runtime_proof, "guest_roundtrip", return_value=("FAIL", "NOT_PROVEN")) as guest,
                  contextlib.redirect_stdout(output)):
                self.assertEqual(runtime_proof.main(), 1)
                guest.assert_called_once()
        self.assertEqual(len([call for call in calls if call[:2] == ("POST", "/sandboxes")]), 2)
        create = next(call for call in calls if call[:2] == ("POST", "/sandboxes"))
        self.assertEqual(create[2]["timeout"], 120)
        self.assertIs(create[2]["secure"], True)
        self.assertIs(create[2]["allow_internet_access"], False)
        delete = next(call for call in calls if call[0] == "POST" and call[1].endswith("/lifecycle-operations"))
        self.assertEqual(delete[2], {"operation": "delete", "execution_id": "execution-1"})
        self.assertNotIn("secret-key", output.getvalue())
        self.assertNotIn("do-not-print", output.getvalue())
        self.assertIn("guest_command=FAIL", output.getvalue())
        self.assertIn("cleanup=CONFIRMED", output.getvalue())


if __name__ == "__main__":
    unittest.main()
