from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parents[1]
VERSIONS = [
    version
    for line in (ROOT / "kernel_versions.txt").read_text(encoding="utf-8").splitlines()
    if (version := line.partition("#")[0].strip())
]
CONFIGS = [
    ROOT / "configs" / arch / f"{version}.config"
    for arch in ("x86_64", "arm64")
    for version in VERSIONS
] + sorted((ROOT / "configs").glob("*.config"))


@pytest.mark.parametrize("config", [p for p in CONFIGS if p.stem == "6.1.189"], ids=lambda p: str(p.relative_to(ROOT)))
def test_service_ipvs_is_built_in(config):
    lines = config.read_text(encoding="utf-8").splitlines()
    for symbol in (
        "CONFIG_IP_VS",
        "CONFIG_IP_VS_IPV6",
        "CONFIG_IP_VS_PROTO_TCP",
        "CONFIG_IP_VS_PROTO_UDP",
        "CONFIG_IP_VS_RR",
        "CONFIG_IP_VS_WRR",
        "CONFIG_IP_VS_LC",
        "CONFIG_IP_VS_WLC",
        "CONFIG_IP_VS_FO",
        "CONFIG_IP_VS_OVF",
        "CONFIG_IP_VS_DH",
        "CONFIG_IP_VS_SH",
        "CONFIG_IP_VS_MH",
        "CONFIG_IP_VS_SED",
        "CONFIG_IP_VS_NQ",
        "CONFIG_IP_VS_TWOS",
        "CONFIG_IP_VS_NFCT",
        "CONFIG_NETFILTER_XT_MATCH_IPVS",
        "CONFIG_NETFILTER_XT_MARK",
        "CONFIG_NETFILTER_XT_SET",
        "CONFIG_NETFILTER_XT_MATCH_COMMENT",
        "CONFIG_NETFILTER_XT_MATCH_CONNTRACK",
        "CONFIG_NETFILTER_XT_MATCH_ADDRTYPE",
        "CONFIG_NETFILTER_XT_MATCH_PHYSDEV",
        "CONFIG_NETFILTER_XT_TARGET_MASQUERADE",
        "CONFIG_NF_CONNTRACK",
        "CONFIG_NF_CT_NETLINK",
        "CONFIG_IP_SET",
        "CONFIG_IP_SET_BITMAP_PORT",
        "CONFIG_IP_SET_HASH_IP",
        "CONFIG_IP_SET_HASH_IPPORT",
        "CONFIG_IP_SET_HASH_IPPORTIP",
        "CONFIG_IP_SET_HASH_IPPORTNET",
        "CONFIG_DUMMY",
        "CONFIG_IPV6",
        "CONFIG_NF_DEFRAG_IPV6",
        "CONFIG_IP6_NF_IPTABLES",
        "CONFIG_IP6_NF_FILTER",
        "CONFIG_IP6_NF_NAT",
        "CONFIG_IP_NF_IPTABLES",
        "CONFIG_IP_NF_FILTER",
        "CONFIG_IP_NF_NAT",
        "CONFIG_IP_NF_TARGET_REJECT",
        "CONFIG_IP6_NF_TARGET_REJECT",
        "CONFIG_PROC_FS",
        "CONFIG_SYSCTL",
        "CONFIG_NET_NS",
        "CONFIG_NETFILTER",
        "CONFIG_NF_NAT",
        "CONFIG_NETFILTER_XTABLES",
    ):
        assert f"{symbol}=y" in lines, f"{config}: Service IPVS requires built-in {symbol}"


@pytest.mark.parametrize("config", [p for p in CONFIGS if p.stem != "6.1.189"], ids=lambda p: str(p.relative_to(ROOT)))
def test_older_kernels_do_not_enable_ipvs(config):
    text = config.read_text(encoding="utf-8")
    assert not re.search(r"^CONFIG_IP_VS(?:_[A-Z0-9_]+)?=[ym]$", text, re.MULTILINE), f"{config}: IPVS requires Linux 6.1.189"


@pytest.mark.parametrize("config", [p for p in CONFIGS if p.stem == "6.1.189"], ids=lambda p: str(p.relative_to(ROOT)))
def test_excluded_protocol_and_schedulers_stay_disabled(config):
    text = config.read_text(encoding="utf-8")
    assert not re.search(r"^CONFIG_\w*SCTP\w*=[ym]$", text, re.MULTILINE), f"{config}: SCTP is excluded"
    assert not re.search(r"^CONFIG_IP_VS_LBLC(?:R)?=[ym]$", text, re.MULTILINE), f"{config}: LBLC and LBLCR are excluded"
