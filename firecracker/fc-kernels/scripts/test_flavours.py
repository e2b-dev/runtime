"""Check flavour fragments against the configs built under them."""

from pathlib import Path

import pytest

from flavour_fragment import fragment, load, lost

ROOT = Path(__file__).resolve().parents[1]
ARCHES = ("x86_64", "arm64")
VERSIONS = [
    version
    for line in (ROOT / "kernel_versions.txt").read_text(encoding="utf-8").splitlines()
    if (version := line.partition("#")[0].strip())
]
FLAVOURED = [
    (config, ROOT / "flavours" / config.parent.name / f"{config.stem.partition('-')[2]}.config")
    for arch in ARCHES
    for config in sorted((ROOT / "configs" / arch).glob("*-*.config"))
]


def flavoured_id(pair):
    return str(pair[0].relative_to(ROOT))


@pytest.mark.parametrize("pair", FLAVOURED, ids=flavoured_id)
def test_flavoured_config_keeps_its_fragment(pair):
    config, flavour = pair
    assert flavour.is_file(), f"{config.name} has no {flavour.relative_to(ROOT)}"
    missing = lost(
        load(flavour.read_text(encoding="utf-8")),
        load(config.read_text(encoding="utf-8")),
    )
    assert not missing, f"{config.relative_to(ROOT)} lost flavour lines: {missing}"


@pytest.mark.parametrize("arch", ARCHES)
@pytest.mark.parametrize("version", VERSIONS)
def test_every_pin_has_a_config_on_every_arch(version, arch):
    # make build and make build-arm64 both read every pin.
    config = ROOT / "configs" / arch / f"{version}.config"
    assert config.is_file(), (
        f"{config.relative_to(ROOT)} is missing: resolve it with "
        f"CONFIG_ONLY=1 ./build.sh {version} {arch} on an {arch} machine"
    )


def test_fragment_is_what_the_flavour_changed():
    seed = load(
        "CONFIG_A=y\n"
        "# CONFIG_B is not set\n"
        "CONFIG_C=y\n"
        "CONFIG_D=\"x\"\n"
        "CONFIG_E=y\n"
        "CONFIG_CC_VERSION_TEXT=\"gcc 13\"\n"
        "CONFIG_BUILD_SALT=\"1.2.3-amzn2023.x86_64\"\n"
    )
    flavour = load(
        "CONFIG_A=y\n"
        "CONFIG_B=y\n"
        "# CONFIG_C is not set\n"
        "CONFIG_D=\"y\"\n"
        "CONFIG_F=y\n"
        "CONFIG_CC_VERSION_TEXT=\"gcc 14\"\n"
        "CONFIG_BUILD_SALT=\"1.2.3-android-amzn2023.x86_64\"\n"
    )
    assert fragment(seed, flavour) == [
        "CONFIG_B=y",
        "# CONFIG_C is not set",
        'CONFIG_D="y"',
        "# CONFIG_E is not set",
        "CONFIG_F=y",
    ]


def test_lost_reads_a_left_out_symbol_as_n():
    wanted = load("# CONFIG_A is not set\nCONFIG_B=y\nCONFIG_C=\"x\"\n")
    resolved = load("CONFIG_C=\"y\"\n# CONFIG_B is not set\n")
    assert lost(wanted, resolved) == ["CONFIG_B=y", 'CONFIG_C="x"']
