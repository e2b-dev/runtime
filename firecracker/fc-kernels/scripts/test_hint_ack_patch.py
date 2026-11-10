"""Check the hint-ACK dependency added by every pinned carried patch.

This unit test executes only the added validation code, not a model of upstream
validate(), and refuses a hunk that deletes code it cannot exercise. The bump
and kernel PR builds check full patches against real source.
"""

import re
import subprocess
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent
PATCH_NAME = "0001-virtio_balloon-Support-wait-on-ACK-for-hinting.patch"
DRIVER = "drivers/virtio/virtio_balloon.c"


def pinned_patches(root):
    patches = set()
    for line in (root / "kernel_versions.txt").read_text(encoding="utf-8").splitlines():
        version = line.partition("#")[0].strip()
        if not version:
            continue
        # Flavours also apply their base version's patches.
        for name in (version.split("-", 1)[0], version):
            patch = root / "patches" / name / PATCH_NAME
            if patch.is_file():
                patches.add(patch)
    return sorted(patches)


def validation_additions(patch):
    diff = patch.read_text(encoding="utf-8")
    driver = diff.split(f"diff --git a/{DRIVER} b/{DRIVER}\n", 1)[1]
    driver = driver.split("diff --git ", 1)[0]
    hunks = re.findall(
        r"^@@ [^\n]*virtballoon_validate[^\n]*\n(.*?)(?=^@@ |\Z)",
        driver, re.MULTILINE | re.DOTALL,
    )
    assert len(hunks) == 1, f"expected one validation hunk in {patch}"
    lines = hunks[0].splitlines(keepends=True)
    assert not any(line.startswith("-") for line in lines), (
        f"{patch} deletes upstream validation; additions alone cannot test that change"
    )
    return "".join(line[1:] for line in lines
                   if line.startswith("+"))


PATCHES = pinned_patches(SCRIPTS.parent)
assert PATCHES, "no pinned hint-ACK patch found"


def test_patch_selection_follows_bumps_and_flavours(tmp_path):
    patches = {}
    for version in ("1.2.3", "1.2.4", "1.2.4-android"):
        patch = tmp_path / "patches" / version / PATCH_NAME
        patch.parent.mkdir(parents=True)
        patch.touch()
        patches[version] = patch
    pins = tmp_path / "kernel_versions.txt"
    pins.write_text("1.2.3\n", encoding="utf-8")
    assert pinned_patches(tmp_path) == [patches["1.2.3"]]

    pins.write_text("1.2.3\n1.2.4-android\n", encoding="utf-8")
    assert set(pinned_patches(tmp_path)) == set(patches.values())

    pins.write_text("1.2.4-android\n", encoding="utf-8")
    assert set(pinned_patches(tmp_path)) == {patches["1.2.4"], patches["1.2.4-android"]}
    assert patches["1.2.3"].is_file()


def test_patch_selection_ignores_comments(tmp_path):
    for version in ("1.2.3", "1.2.4"):
        patch = tmp_path / "patches" / version / PATCH_NAME
        patch.parent.mkdir(parents=True)
        patch.touch()
    (tmp_path / "kernel_versions.txt").write_text(
        "# 1.2.3\n\n \t1.2.4  # retired 1.2.3\n", encoding="utf-8",
    )
    assert pinned_patches(tmp_path) == [tmp_path / "patches" / "1.2.4" / PATCH_NAME]


def test_validation_rejects_upstream_deletions(tmp_path):
    patch = tmp_path / PATCH_NAME
    patch.write_text(
        f"diff --git a/{DRIVER} b/{DRIVER}\n"
        "@@ -1,2 +1,2 @@ static int virtballoon_validate(struct virtio_device *vdev)\n"
        "-\t__virtio_clear_bit(vdev, VIRTIO_F_ACCESS_PLATFORM);\n"
        "+\t__virtio_clear_bit(vdev, VIRTIO_BALLOON_F_HINT_WAIT_ON_ACK);\n"
        " \treturn 0;\n", encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="upstream validation"):
        validation_additions(patch)


@pytest.mark.parametrize("patch", PATCHES, ids=lambda patch: patch.parent.name)
def test_hint_ack_requires_free_page_hint(patch, tmp_path):
    (tmp_path / "hint_ack_validation.inc").write_text(
        validation_additions(patch), encoding="utf-8",
    )
    binary = tmp_path / "hint_ack_validation"
    subprocess.run(
        ["cc", "-std=c99", "-Wall", "-Wextra", "-Werror",
         "-I", str(tmp_path), str(SCRIPTS / "hint_ack_validation_test.c"),
         "-o", str(binary)],
        check=True,
    )
    subprocess.run([str(binary)], check=True)
