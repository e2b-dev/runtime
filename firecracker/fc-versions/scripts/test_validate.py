#!/usr/bin/env python3
"""Tests for validate.py"""

import json
import subprocess
from unittest.mock import patch, MagicMock

import pytest

from validate import (
    main,
    resolve_tag_to_commit,
    resolve_release_tag,
    check_ci_status,
    generate_build_matrix,
    get_existing_release_assets,
    check_artifacts_needed,
    gh_api,
)


# Test fixtures
SAMPLE_COMMIT_SHA = "abc123def456789012345678901234567890abcd"


def mock_run_command(responses: dict):
    """Create a mock for run_command that returns predefined responses."""
    def _mock(cmd: list[str], check: bool = True):
        key = " ".join(cmd)
        for pattern, response in responses.items():
            if pattern in key:
                result = MagicMock()
                result.returncode = response.get("returncode", 0)
                result.stdout = response.get("stdout", "")
                result.stderr = response.get("stderr", "")
                return result
        # Default: command not found
        result = MagicMock()
        result.returncode = 1
        result.stdout = ""
        result.stderr = "not found"
        return result
    return _mock


class TestResolveTagToCommit:
    """Tests for resolve_tag_to_commit function."""

    def test_tag_not_found(self):
        with patch("validate.gh_api", return_value=None):
            commit, error = resolve_tag_to_commit("v1.0.0")
            assert commit == ""
            assert "does not exist" in error

    def test_lightweight_tag(self):
        """Test resolving a lightweight tag (points directly to commit)."""
        with patch("validate.gh_api") as mock_api:
            # First call: get tag ref
            mock_api.side_effect = [
                {"object": {"sha": SAMPLE_COMMIT_SHA}},
                None,  # No tag object (lightweight tag)
            ]
            commit, error = resolve_tag_to_commit("v1.0.0")
            assert commit == SAMPLE_COMMIT_SHA
            assert error is None

    def test_annotated_tag(self):
        """Test resolving an annotated tag (points to tag object, then commit)."""
        tag_object_sha = "tag123456789"
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"object": {"sha": tag_object_sha}},  # Tag ref points to tag object
                {"object": {"sha": SAMPLE_COMMIT_SHA}},  # Tag object points to commit
            ]
            commit, error = resolve_tag_to_commit("v1.0.0")
            assert commit == SAMPLE_COMMIT_SHA
            assert error is None


class TestResolveReleaseTag:
    """Tests for resolve_release_tag function."""

    def test_lightweight_tag(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"object": {"sha": SAMPLE_COMMIT_SHA}},
                None,
            ]
            version_name, commit, error = resolve_release_tag("v1.14-0.1.0")
            assert error is None
            assert version_name == "v1.14-0.1.0"
            assert commit == SAMPLE_COMMIT_SHA

    def test_annotated_tag(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"object": {"sha": "tag123456789"}},
                {"object": {"sha": SAMPLE_COMMIT_SHA}},
            ]
            version_name, commit, error = resolve_release_tag("v1.14-0.1.0")
            assert error is None
            assert version_name == "v1.14-0.1.0"
            assert commit == SAMPLE_COMMIT_SHA

    def test_version_name_carries_no_commit_suffix(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"object": {"sha": SAMPLE_COMMIT_SHA}},
                None,
            ]
            version_name, _, _ = resolve_release_tag("v1.14-0.1.0")
            assert version_name == "v1.14-0.1.0"
            assert SAMPLE_COMMIT_SHA[:7] not in version_name

    def test_tag_does_not_exist(self):
        with patch("validate.gh_api", return_value=None):
            version_name, commit, error = resolve_release_tag("v1.14-0.1.0")
            assert version_name == ""
            assert commit == ""
            assert "does not exist" in error

    @pytest.mark.parametrize("tag", [
        "v1.14.1",
        "v1.14",
        "v1.14.1_abc1234",
        "1.14-0.1.0",
        "v1.14-0.1",
        "v1.14-0.1.0-rc1",
        "v01.14-0.1.0",
        "v1.14-01.0.0",
        "v1.14-0.1.00",
        "",
    ])
    def test_rejects_non_release_tag_without_calling_the_api(self, tag):
        with patch("validate.gh_api") as mock_api:
            version_name, commit, error = resolve_release_tag(tag)
            assert mock_api.call_count == 0
            assert version_name == ""
            assert commit == ""
            assert "is not a vX.Y-<e2b-semver> release tag" in error
            assert "never rebuilt" in error
            assert "release runbook" in error


class TestCheckCIStatus:
    """Tests for check_ci_status function."""

    def test_ci_success_via_status_api(self):
        """Test CI success detected via status API."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "success", "total_count": 1},
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is True
            assert "passed" in message

    def test_ci_success_via_check_runs(self):
        """Test CI success detected via check-runs API."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "unknown", "total_count": 0},
                {
                    "total_count": 2,
                    "check_runs": [
                        {"status": "completed", "conclusion": "success"},
                        {"status": "completed", "conclusion": "success"},
                    ],
                },
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is True

    def test_ci_failure_via_status_api(self):
        """Test CI failure detected via status API."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "failure", "total_count": 1},
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is False
            assert "failed" in message

    def test_ci_failure_via_check_runs(self):
        """Test CI failure detected via check-runs API."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "unknown", "total_count": 0},
                {
                    "total_count": 2,
                    "check_runs": [
                        {"status": "completed", "conclusion": "success"},
                        {"status": "completed", "conclusion": "failure"},
                    ],
                },
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is False

    def test_ci_pending_via_status_api(self):
        """Test CI pending detected via status API."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "pending", "total_count": 1},
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is False
            assert "still running" in message

    def test_ci_pending_via_check_runs(self):
        """Test CI pending detected via check-runs API."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "unknown", "total_count": 0},
                {
                    "total_count": 1,
                    "check_runs": [
                        {"status": "in_progress", "conclusion": None},
                    ],
                },
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is False
            assert "still running" in message

    def test_ci_no_checks_found(self):
        """Test when no CI checks are found (proceeds with warning)."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "unknown", "total_count": 0},
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is True
            assert "No CI checks found" in message

    def test_ci_skipped_checks_count_as_success(self):
        """Test that skipped checks count as success."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {"state": "unknown", "total_count": 0},
                {
                    "total_count": 2,
                    "check_runs": [
                        {"status": "completed", "conclusion": "success"},
                        {"status": "completed", "conclusion": "skipped"},
                    ],
                },
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is True

    def test_ci_ignored_cla_status_does_not_block(self):
        """cla-bot's verification/cla-signed status is permanently red on
        external-contributor backport branches; filtering it out should let
        the build proceed when it's the only failure."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {
                    "state": "failure",
                    "total_count": 2,
                    "statuses": [
                        {"context": "verification/cla-signed", "state": "error"},
                        {"context": "buildkite/firecracker", "state": "success"},
                    ],
                },
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is True
            assert "passed" in message

    def test_ci_ignored_cla_status_alone_falls_through(self):
        """If the CLA status is the only one and we filter it out, the
        rollup is empty → fall back to the no-checks 'proceed anyway' path."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {
                    "state": "failure",
                    "total_count": 1,
                    "statuses": [
                        {"context": "verification/cla-signed", "state": "error"},
                    ],
                },
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is True

    def test_ci_other_failure_still_blocks_when_cla_also_failed(self):
        """Real CI failure must still block even when the CLA status is also
        failing — the filter must not mask non-ignored failures."""
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                {
                    "state": "failure",
                    "total_count": 2,
                    "statuses": [
                        {"context": "verification/cla-signed", "state": "error"},
                        {"context": "buildkite/firecracker", "state": "failure"},
                    ],
                },
                {"total_count": 0, "check_runs": []},
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
            assert success is False
            assert "failed" in message


def _check_run(name, conclusion, status="completed", app="github-actions", suite=1):
    return {
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "app": {"slug": app},
        "check_suite": {"id": suite},
    }


def _suite_runs(event):
    return {"total_count": 1, "workflow_runs": [{"event": event}]}


NO_STATUSES = {"state": "unknown", "total_count": 0}


class TestCheckCIStatusPushOnly:
    """Actions check-runs not triggered by a push do not gate a release."""

    @pytest.mark.parametrize("event", ["schedule", "workflow_dispatch", "dynamic"])
    @pytest.mark.parametrize("status,conclusion", [("in_progress", None), ("completed", "failure")])
    def test_non_push_run_does_not_block_green_push_ci(self, event, status, conclusion):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [
                    _check_run("codeql", "success", suite=1),
                    _check_run("codeql", conclusion, status=status, suite=2),
                ]},
                _suite_runs("push"),
                _suite_runs(event),
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is True
        assert "passed" in message

    def test_failed_push_run_blocks(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [
                    _check_run("codeql", "failure", suite=1),
                    _check_run("monitor", "success", suite=2),
                ]},
                _suite_runs("push"),
                _suite_runs("schedule"),
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is False
        assert "failed" in message

    def test_check_run_from_another_app_is_not_filtered(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [
                    _check_run("codeql", "success", suite=1),
                    _check_run("bugbot", "failure", app="cursor", suite=2),
                ]},
                _suite_runs("push"),
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is False
        assert "failed" in message
        assert mock_api.call_count == 3

    @pytest.mark.parametrize("lookup", [None, {"total_count": 0, "workflow_runs": []}])
    def test_unresolved_suite_keeps_its_check_runs(self, lookup):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [
                    _check_run("codeql", "success", suite=1),
                    _check_run("scan", "failure", suite=2),
                ]},
                _suite_runs("push"),
                lookup,
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is False
        assert "failed" in message

    def test_each_suite_is_resolved_once(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [
                    _check_run("codeql (rust)", "success", suite=7),
                    _check_run("codeql (go)", "success", suite=7),
                    _check_run("osv", "success", suite=7),
                ]},
                _suite_runs("push"),
            ]
            success, _ = check_ci_status(SAMPLE_COMMIT_SHA, repo="owner/repo")
        assert success is True
        endpoints = [c.args[0] for c in mock_api.call_args_list]
        assert endpoints[1] == f"/repos/owner/repo/commits/{SAMPLE_COMMIT_SHA}/check-runs?per_page=100&page=1"
        assert endpoints[2:] == ["/repos/owner/repo/actions/runs?check_suite_id=7"]

    @pytest.mark.parametrize("lookup", [None, {"total_count": 0, "workflow_runs": []}])
    def test_unresolved_suite_is_reported(self, lookup, capsys):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [_check_run("scan", "success", suite=9)]},
                lookup,
            ]
            check_ci_status(SAMPLE_COMMIT_SHA)
        assert "::warning::Could not resolve the workflow event of check suite 9" in capsys.readouterr().err

    def test_push_failure_on_a_later_page_blocks(self):
        page_one = [_check_run(f"monitor-{i}", "success", suite=2) for i in range(100)]
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"total_count": 101, "check_runs": page_one},
                {"total_count": 101, "check_runs": [_check_run("codeql", "failure", suite=1)]},
                _suite_runs("schedule"),
                _suite_runs("push"),
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA, repo="owner/repo")
        assert success is False
        assert "failed" in message
        assert mock_api.call_args_list[2].args[0] == (
            f"/repos/owner/repo/commits/{SAMPLE_COMMIT_SHA}/check-runs?per_page=100&page=2"
        )

    def test_unreadable_later_page_refuses(self):
        page_one = [_check_run(f"monitor-{i}", "success", suite=2) for i in range(100)]
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"total_count": 101, "check_runs": page_one},
                None,
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is False
        assert "Could not list every check-run" in message

    def test_full_page_is_followed_without_a_reported_total(self):
        newest = [_check_run(f"monitor-{i}", "success", suite=2) for i in range(100)]
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": newest},
                {"check_runs": [_check_run("codeql", "failure", suite=1)]},
                _suite_runs("schedule"),
                _suite_runs("push"),
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is False
        assert "failed" in message

    def test_unreadable_first_page_reads_as_no_check_runs(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [{"state": "success", "total_count": 1}, None]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is True
        assert "passed" in message

    def test_only_non_push_runs_falls_back_to_no_checks_found(self):
        with patch("validate.gh_api") as mock_api:
            mock_api.side_effect = [
                NO_STATUSES,
                {"check_runs": [_check_run("monitor", "failure", suite=3)]},
                _suite_runs("schedule"),
            ]
            success, message = check_ci_status(SAMPLE_COMMIT_SHA)
        assert success is True
        assert "No CI checks found" in message


class TestGenerateBuildMatrix:
    """Tests for generate_build_matrix function."""

    def test_build_both_architectures(self):
        """Test generating matrix for both architectures."""
        matrix = generate_build_matrix(True, True)
        assert len(matrix["include"]) == 2
        archs = {item["arch"] for item in matrix["include"]}
        assert archs == {"amd64", "arm64"}

    def test_build_amd64_only(self):
        """Test generating matrix for amd64 only."""
        matrix = generate_build_matrix(True, False)
        assert len(matrix["include"]) == 1
        assert matrix["include"][0]["arch"] == "amd64"
        assert matrix["include"][0]["runner"] == "ubuntu-24.04"

    def test_build_arm64_only(self):
        """Test generating matrix for arm64 only."""
        matrix = generate_build_matrix(False, True)
        assert len(matrix["include"]) == 1
        assert matrix["include"][0]["arch"] == "arm64"
        assert matrix["include"][0]["runner"] == "ubuntu-24.04-arm"

    def test_build_neither_architecture(self):
        """Test generating empty matrix when no architectures requested."""
        matrix = generate_build_matrix(False, False)
        assert matrix == {"include": []}


class TestGetExistingReleaseAssets:
    """Tests for get_existing_release_assets function."""

    def test_no_github_repository_env(self):
        """Test returns empty set when GITHUB_REPOSITORY is not set."""
        with patch.dict("os.environ", {}, clear=True):
            assets = get_existing_release_assets("v1.0.0_abc1234")
            assert assets == set()

    def test_release_not_found(self):
        """Test returns empty set when release doesn't exist."""
        with patch.dict("os.environ", {"GITHUB_REPOSITORY": "owner/repo"}):
            with patch("validate.run_command") as mock_run:
                mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="release not found")
                assets = get_existing_release_assets("v1.0.0_abc1234")
                assert assets == set()

    def test_release_with_assets(self):
        """Test returns set of asset names when release exists."""
        with patch.dict("os.environ", {"GITHUB_REPOSITORY": "owner/repo"}):
            with patch("validate.run_command") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="firecracker-amd64\nfirecracker-arm64\nfirecracker\n",
                    stderr=""
                )
                assets = get_existing_release_assets("v1.0.0_abc1234")
                assert assets == {"firecracker-amd64", "firecracker-arm64", "firecracker"}

    def test_release_with_no_assets(self):
        """Test returns empty set when release exists but has no assets."""
        with patch.dict("os.environ", {"GITHUB_REPOSITORY": "owner/repo"}):
            with patch("validate.run_command") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
                assets = get_existing_release_assets("v1.0.0_abc1234")
                assert assets == set()


class TestCheckArtifactsNeeded:
    """Tests for check_artifacts_needed function."""

    def test_both_requested_neither_exists(self):
        """Test returns True when both requested and neither exists."""
        with patch("validate.get_existing_release_assets", return_value=set()):
            assert check_artifacts_needed("v1.0.0_abc1234", True, True) is True

    def test_both_requested_amd64_missing(self):
        """Test returns True when both requested but amd64 is missing."""
        with patch("validate.get_existing_release_assets", return_value={"firecracker-arm64"}):
            assert check_artifacts_needed("v1.0.0_abc1234", True, True) is True

    def test_both_requested_arm64_missing(self):
        """Test returns True when both requested but arm64 is missing."""
        with patch("validate.get_existing_release_assets", return_value={"firecracker-amd64"}):
            assert check_artifacts_needed("v1.0.0_abc1234", True, True) is True

    def test_both_requested_both_exist(self):
        """Test returns False when both requested and both prod + debug exist."""
        assets = {
            "firecracker-amd64", "firecracker-arm64",
            "firecracker-debug-amd64", "firecracker-debug-arm64",
        }
        with patch("validate.get_existing_release_assets", return_value=assets):
            assert check_artifacts_needed("v1.0.0_abc1234", True, True) is False

    def test_both_requested_prod_exists_debug_missing(self):
        """Test returns True when prod binaries exist but the debug ones do not."""
        with patch("validate.get_existing_release_assets", return_value={"firecracker-amd64", "firecracker-arm64"}):
            assert check_artifacts_needed("v1.0.0_abc1234", True, True) is True

    def test_amd64_only_exists(self):
        """Test returns False when only amd64 requested and its prod + debug exist."""
        with patch("validate.get_existing_release_assets", return_value={"firecracker-amd64", "firecracker-debug-amd64"}):
            assert check_artifacts_needed("v1.0.0_abc1234", True, False) is False

    def test_amd64_only_prod_exists_debug_missing(self):
        """Test returns True when amd64 prod exists but its debug binary does not."""
        with patch("validate.get_existing_release_assets", return_value={"firecracker-amd64"}):
            assert check_artifacts_needed("v1.0.0_abc1234", True, False) is True

    def test_amd64_only_missing(self):
        """Test returns True when only amd64 requested and it's missing."""
        with patch("validate.get_existing_release_assets", return_value=set()):
            assert check_artifacts_needed("v1.0.0_abc1234", True, False) is True

    def test_arm64_only_exists(self):
        """Test returns False when only arm64 requested and its prod + debug exist."""
        with patch("validate.get_existing_release_assets", return_value={"firecracker-arm64", "firecracker-debug-arm64"}):
            assert check_artifacts_needed("v1.0.0_abc1234", False, True) is False

    def test_arm64_only_missing(self):
        """Test returns True when only arm64 requested and it's missing."""
        with patch("validate.get_existing_release_assets", return_value=set()):
            assert check_artifacts_needed("v1.0.0_abc1234", False, True) is True

    def test_neither_requested(self):
        """Test returns False when no architectures are requested."""
        with patch("validate.get_existing_release_assets", return_value=set()):
            assert check_artifacts_needed("v1.0.0_abc1234", False, False) is False

class TestMain:
    """End-to-end tests through the real entry point."""

    def _run(self, argv, api_responses):
        outputs = {}
        with patch("sys.argv", ["validate.py"] + argv), \
             patch("validate.gh_api", side_effect=api_responses), \
             patch("validate.get_existing_release_assets", return_value=set()), \
             patch("validate.write_github_output", side_effect=lambda o: outputs.update(o)):
            rc = main()
        return rc, outputs

    def test_no_architectures_selected(self):
        rc, outputs = self._run(
            ["--tag", "v1.14-0.1.0", "--build-amd64", "false", "--build-arm64", "false"], []
        )
        assert rc == 1
        assert outputs == {}

    def test_single_arch_dispatch_outputs_verbatim_version_name(self):
        rc, outputs = self._run(
            ["--tag", "v1.14-0.1.0", "--build-arm64", "false"],
            [
                {"object": {"sha": SAMPLE_COMMIT_SHA, "type": "commit"}},  # tag ref
                None,  # annotated-tag dereference miss (lightweight tag)
                {"state": "success", "total_count": 1,
                 "statuses": [{"state": "success", "context": "ci"}]},
                {"check_runs": []},
            ],
        )
        assert rc == 0
        assert outputs["version_name"] == "v1.14-0.1.0"
        assert outputs["commit_hash"] == SAMPLE_COMMIT_SHA
        assert json.loads(outputs["build_matrix"]) == {"include": [{"arch": "amd64", "runner": "ubuntu-24.04"}]}
