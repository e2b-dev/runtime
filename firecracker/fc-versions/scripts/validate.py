#!/usr/bin/env python3
"""
Validation script for Firecracker release workflow.

This script validates the release tag, resolves it to the commit it pins,
checks CI status, and determines which architectures need to be built.
"""

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional


FIRECRACKER_REPO = os.environ.get("FIRECRACKER_REPO", "e2b-dev/e2b-firecracker")

# vX.Y-<e2b-semver>: the upstream minor line we track, then our own version of
# the patches carried on top of it. A release is named by a maintainer cutting
# this tag; nothing here composes one.
E2B_TAG_RE = re.compile(r"^v(0|[1-9]\d*)\.(0|[1-9]\d*)-(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def gh_api(endpoint: str) -> Optional[dict]:
    """Call the GitHub API using the gh CLI."""
    # Route firecracker-repo calls through the App token when present, so
    # they don't inherit GH_TOKEN, which is scoped to the current repo.
    env = os.environ.copy()
    firecracker_token = env.get("FIRECRACKER_GH_TOKEN")
    if firecracker_token:
        env["GH_TOKEN"] = firecracker_token
    result = subprocess.run(
        ["gh", "api", endpoint],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def resolve_tag_to_commit(tag: str, repo: str = FIRECRACKER_REPO) -> tuple[str, Optional[str]]:
    """
    Resolve a tag to its commit hash.

    Returns (commit_hash, error_message).
    """
    data = gh_api(f"repos/{repo}/git/ref/tags/{tag}")
    if not data:
        return "", f"Tag {tag} does not exist in {repo} repository"

    commit_hash = data["object"]["sha"]

    # Handle annotated tags (need to dereference to get commit SHA)
    tag_object = gh_api(f"repos/{repo}/git/tags/{commit_hash}")
    if tag_object and "object" in tag_object:
        commit_hash = tag_object["object"]["sha"]

    return commit_hash, None


def resolve_release_tag(tag: str, repo: str = FIRECRACKER_REPO) -> tuple[str, str, Optional[str]]:
    """
    Validate a release tag and resolve the commit it pins.

    Returns (version_name, commit_hash, error_message).
    """
    if not E2B_TAG_RE.match(tag):
        return "", "", (
            f"Tag {tag!r} is not a vX.Y-<e2b-semver> release tag (for example "
            "v1.14-0.1.0). Legacy {tag}_{sha} releases are never rebuilt; "
            "see the release runbook in the fc-versions README."
        )

    commit_hash, error = resolve_tag_to_commit(tag, repo)
    if error:
        return "", "", error

    return tag, commit_hash, None


# IGNORED_STATUS_CONTEXTS lists legacy commit-status contexts that should not
# block a release build even when failing. Keep the set tiny and well-justified.
#
# verification/cla-signed: cla-bot fails on the upstream firecracker fork
# whenever a backport branch carries commits authored by upstream maintainers
# we don't have a CLA for (e.g. ilstam, ShadowCurse, JackThomson2). Those
# contributors won't ever sign our CLA, so the status is permanently red on
# every direct-mem / hint backport branch — we still want to ship those builds.
IGNORED_STATUS_CONTEXTS = frozenset({"verification/cla-signed"})

# IGNORED_CHECK_NAMES is the equivalent for the Checks API (apps that file a
# check-run rather than a legacy status). Empty today; mirror IGNORED_STATUS_CONTEXTS
# if a check-run-based bot ever ends up in the same situation.
IGNORED_CHECK_NAMES = frozenset()

# The gate asks whether the CI that ran when the commit landed passed. A GitHub
# Actions check-run counts only when its workflow run was triggered by a push.
# Scheduled and Dependabot runs also attach check-runs to the head of the default
# branch, which is often the commit a release tag points at, and a dispatched
# run attaches them to the head of the ref it ran on. A re-scan that is still
# running or that broke for an infrastructure reason must not block an unchanged
# commit. Check-runs from other apps are kept as they are.
ACTIONS_APP_SLUG = "github-actions"
GATING_ACTIONS_EVENT = "push"


def _actions_suite_event(suite_id, repo: str) -> Optional[str]:
    """Return the event that triggered the workflow run behind a check suite,
    or None when it cannot be resolved."""
    response = gh_api(f"/repos/{repo}/actions/runs?check_suite_id={suite_id}")
    runs = (response or {}).get("workflow_runs") or []
    if not runs:
        return None
    return runs[0].get("event")


def _drop_non_push_actions_runs(check_runs: list[dict], repo: str) -> tuple[list[dict], list[str]]:
    """Drop Actions check-runs whose workflow run was not triggered by a push.

    Each check suite is resolved once. A suite whose event cannot be resolved
    keeps its check-runs, so a failed lookup never loosens the gate.

    Returns (kept_check_runs, dropped_names).
    """
    events: dict = {}
    kept, dropped = [], []
    for cr in check_runs:
        suite_id = (cr.get("check_suite") or {}).get("id")
        if (cr.get("app") or {}).get("slug") != ACTIONS_APP_SLUG or suite_id is None:
            kept.append(cr)
            continue
        if suite_id not in events:
            events[suite_id] = _actions_suite_event(suite_id, repo)
            if events[suite_id] is None:
                print(
                    f"::warning::Could not resolve the workflow event of check suite "
                    f"{suite_id}; counting its check-runs",
                    file=sys.stderr,
                )
        event = events[suite_id]
        if event is None or event == GATING_ACTIONS_EVENT:
            kept.append(cr)
        else:
            dropped.append(f"{cr.get('name')} ({event})")
    return kept, dropped


# The Checks API's maximum page size.
CHECK_RUNS_PER_PAGE = 100


def _list_check_runs(commit_hash: str, repo: str) -> Optional[list[dict]]:
    """Return every check-run the API lists for the commit, reading all pages.

    The API lists the newest check-runs first, and scheduled workflows keep
    adding them to an unchanged commit, so the first page alone can miss the
    push-triggered ones. Paging stops at the first short page, which needs no
    trust in the reported total. A failed first page yields no
    check-runs, as a single call always did; a failed later page returns None.
    """
    check_runs: list[dict] = []
    page = 1
    while True:
        response = gh_api(
            f"/repos/{repo}/commits/{commit_hash}/check-runs?per_page={CHECK_RUNS_PER_PAGE}&page={page}"
        )
        if not response:
            return check_runs if page == 1 else None
        batch = response.get("check_runs") or []
        check_runs.extend(batch)
        if len(batch) < CHECK_RUNS_PER_PAGE:
            return check_runs
        page += 1


def _rollup_status(statuses: list[dict]) -> tuple[str, int]:
    """Compute (state, count) over the statuses list, mirroring how GitHub's
    combined-status endpoint rolls up: any failure → failure, else any pending
    → pending, else any success → success, else unknown.
    """
    if not statuses:
        return "unknown", 0
    states = {s.get("state") for s in statuses}
    if "failure" in states or "error" in states:
        return "failure", len(statuses)
    if "pending" in states:
        return "pending", len(statuses)
    if "success" in states:
        return "success", len(statuses)
    return "unknown", len(statuses)


def check_ci_status(commit_hash: str, repo: str = FIRECRACKER_REPO) -> tuple[bool, str]:
    """
    Check CI status for a commit.

    Returns (success, message).
    """
    # Check commit status API. Filter out IGNORED_STATUS_CONTEXTS and recompute
    # the rollup so a single permanently-red status (e.g. cla-bot on
    # external-contributor backport branches) doesn't block release builds.
    status_response = gh_api(f"/repos/{repo}/commits/{commit_hash}/status")
    if not status_response:
        status_response = {"state": "unknown", "total_count": 0, "statuses": []}

    raw_statuses = status_response.get("statuses", []) or []
    ignored_status_contexts = [
        s.get("context") for s in raw_statuses
        if s.get("context") in IGNORED_STATUS_CONTEXTS
    ]
    filtered_statuses = [
        s for s in raw_statuses
        if s.get("context") not in IGNORED_STATUS_CONTEXTS
    ]
    if ignored_status_contexts:
        status, status_count = _rollup_status(filtered_statuses)
        print(
            f"Status API: ignoring contexts {sorted(set(ignored_status_contexts))} "
            f"→ rollup state={status}, count={status_count}",
            file=sys.stderr,
        )
    else:
        status = status_response.get("state", "unknown")
        status_count = status_response.get("total_count", 0)
        print(f"Status API: state={status}, count={status_count}", file=sys.stderr)

    # Check check-runs API. Same filter for IGNORED_CHECK_NAMES, then drop
    # Actions check-runs not triggered by a push.
    raw_check_runs = _list_check_runs(commit_hash, repo)
    if raw_check_runs is None:
        return False, f"Could not list every check-run for commit {commit_hash} - refusing to build"
    ignored_check_names = [
        cr.get("name") for cr in raw_check_runs
        if cr.get("name") in IGNORED_CHECK_NAMES
    ]
    check_runs = [
        cr for cr in raw_check_runs
        if cr.get("name") not in IGNORED_CHECK_NAMES
    ]
    check_runs, non_push_check_names = _drop_non_push_actions_runs(check_runs, repo)
    if non_push_check_names:
        print(
            f"Check-runs API: ignoring Actions runs not triggered by a push "
            f"{sorted(set(non_push_check_names))}",
            file=sys.stderr,
        )
    check_count = len(check_runs)

    # Determine check conclusion
    if check_count == 0:
        check_conclusion = "no_checks"
    elif any(cr.get("status") in ("in_progress", "queued") for cr in check_runs):
        check_conclusion = "pending"
    elif any(cr.get("conclusion") in ("failure", "cancelled", "timed_out") for cr in check_runs):
        check_conclusion = "failure"
    elif all(cr.get("conclusion") in ("success", "skipped", "neutral") for cr in check_runs):
        check_conclusion = "success"
    else:
        check_conclusion = "unknown"

    if ignored_check_names:
        print(
            f"Check-runs API: ignoring {sorted(set(ignored_check_names))} "
            f"→ conclusion={check_conclusion}, count={check_count}",
            file=sys.stderr,
        )
    else:
        print(f"Check-runs API: conclusion={check_conclusion}, count={check_count}", file=sys.stderr)

    if status == "failure" or check_conclusion == "failure":
        return False, f"CI failed for commit {commit_hash} - refusing to build"

    if check_conclusion == "pending" or (status == "pending" and status_count > 0):
        return False, f"CI is still running for commit {commit_hash} - refusing to build"

    if status == "success" or check_conclusion == "success":
        return True, f"CI passed for commit {commit_hash}"

    if status_count == 0 and check_count == 0:
        print(f"::warning::No CI checks found for commit {commit_hash} - proceeding anyway", file=sys.stderr)
        return True, f"No CI checks found for commit {commit_hash} - proceeding anyway"

    print(f"::warning::Could not definitively verify CI status - proceeding anyway", file=sys.stderr)
    return True, f"Could not definitively verify CI status (status={status}, check_conclusion={check_conclusion}) - proceeding anyway"


def get_existing_release_assets(version_name: str) -> set[str]:
    """
    Get the set of existing asset names for a release.

    Returns empty set if release doesn't exist.
    """
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not repo:
        return set()

    result = run_command(
        ["gh", "release", "view", version_name, "--json", "assets", "-q", ".assets[].name"],
        check=False
    )
    if result.returncode != 0:
        return set()

    return set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()


def check_artifacts_needed(version_name: str, build_amd64: bool, build_arm64: bool) -> bool:
    """
    Check if any requested architectures are missing an artifact from the release.

    Returns True if at least one artifact needs to be built and uploaded. Mirrors
    the build job's skip-check: a release needs both the prod binary
    (firecracker-<arch>) and the gdb-enabled debug binary (firecracker-debug-<arch>),
    so a release that has the prod binary but not the debug one still has new
    artifacts to publish.
    """
    existing_assets = get_existing_release_assets(version_name)

    archs = []
    if build_amd64:
        archs.append("amd64")
    if build_arm64:
        archs.append("arm64")

    for arch in archs:
        if f"firecracker-{arch}" not in existing_assets:
            return True
        if f"firecracker-debug-{arch}" not in existing_assets:
            return True

    return False


def generate_build_matrix(build_amd64: bool, build_arm64: bool) -> dict:
    """
    Generate build matrix for all requested architectures.

    Build and deploy jobs always run; individual steps check for existing artifacts.
    """
    include = []
    if build_amd64:
        include.append({"arch": "amd64", "runner": "ubuntu-24.04"})
    if build_arm64:
        include.append({"arch": "arm64", "runner": "ubuntu-24.04-arm"})

    return {"include": include}


def write_github_output(outputs: dict[str, str]) -> None:
    """Write outputs to GITHUB_OUTPUT file."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a") as f:
            for key, value in outputs.items():
                f.write(f"{key}={value}\n")
    else:
        # For local testing, print to stdout
        for key, value in outputs.items():
            print(f"{key}={value}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Firecracker release inputs")
    parser.add_argument("--tag", required=True, help="Release tag to build (e.g., v1.14-0.1.0)")
    parser.add_argument("--build-amd64", type=lambda x: x.lower() == "true", default=True,
                        help="Build for amd64 architecture")
    parser.add_argument("--build-arm64", type=lambda x: x.lower() == "true", default=True,
                        help="Build for arm64 architecture")

    args = parser.parse_args()

    # Step 1: Validate inputs
    if not args.build_amd64 and not args.build_arm64:
        print("::error::At least one architecture must be selected", file=sys.stderr)
        return 1

    # Step 2: Resolve the release tag to the commit it pins
    print(f"Resolving tag {args.tag}...", file=sys.stderr)
    version_name, commit_hash, error = resolve_release_tag(args.tag)
    if error:
        print(f"::error::{error}", file=sys.stderr)
        return 1

    print(f"Full commit hash: {commit_hash}", file=sys.stderr)
    print(f"Version name: {version_name}", file=sys.stderr)

    # Step 3: Check CI status
    print(f"Checking CI status for commit {commit_hash}...", file=sys.stderr)
    ci_ok, ci_message = check_ci_status(commit_hash)
    if not ci_ok:
        print(f"::error::{ci_message}", file=sys.stderr)
        return 1
    print(ci_message, file=sys.stderr)

    # Step 4: Generate build matrix for all requested architectures
    build_matrix = generate_build_matrix(args.build_amd64, args.build_arm64)

    print(f"Build matrix: {json.dumps(build_matrix)}", file=sys.stderr)

    # Step 5: Check if any artifacts need to be built
    has_new_artifacts = check_artifacts_needed(version_name, args.build_amd64, args.build_arm64)
    print(f"Has new artifacts to build: {has_new_artifacts}", file=sys.stderr)

    # Write outputs
    write_github_output({
        "commit_hash": commit_hash,
        "version_name": version_name,
        "build_matrix": json.dumps(build_matrix),
        "has_new_artifacts": str(has_new_artifacts).lower(),
    })

    return 0


if __name__ == "__main__":
    sys.exit(main())
