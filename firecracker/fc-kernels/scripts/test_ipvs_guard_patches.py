"""Execute the carried guard predicates and check their patch call sites.

These tests do not reproduce device-down races, skb copying, sync wire parsing,
or kernel lifetime. Full patch application, kernel builds and real
consumers remain necessary. Like test_hint_ack_patch, execute the added C itself.
"""

import re
import subprocess
from pathlib import Path

import pytest

PATCHES = Path(__file__).resolve().parents[1] / "patches" / "6.1.189"


def patch(number):
    paths = list(PATCHES.glob(f"{number:04d}-*.patch"))
    assert len(paths) == 1
    return paths[0].read_text(encoding="utf-8")


def additions(number):
    return "\n".join(line[1:] for line in patch(number).splitlines()
                     if line.startswith("+") and not line.startswith("+++"))


def added_function(number, name):
    matches = re.findall(r"static bool " + name + r"\([^)]*\)\n\{.*?^\}",
                         additions(number), re.MULTILINE | re.DOTALL)
    assert len(matches) == 1
    return matches[0]


def guards():
    route = added_function(2, "rt_dev_is_down")
    sync = added_function(4, "ip_vs_sync_validate_flags")
    ihl = re.findall(r"if \(cih->ihl \* 4 != ciph->len - ciph->off\)\n\s*return false;", additions(3))
    assert len(ihl) == 1
    return [route, sync, ihl[0]]


def program(parts):
    route, sync, ihl = parts
    return r"""
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
typedef uint32_t u32;
#define IP_VS_CONN_F_NO_CPORT 0x0800
#define IP_VS_CONN_F_TEMPLATE 0x1000
#define IP_VS_CONN_F_ONE_PACKET 0x2000
struct net_device { bool running; };
#define netif_running(dev) ((dev)->running)
struct header { unsigned ihl; };
struct parsed { unsigned len, off; };
""" + route + "\n" + sync + "\n" + r"""
static bool check_ihl(struct header *cih, struct parsed *ciph) {
""" + ihl + r"""
    return true;
}
#define CHECK(value) do { if (!(value)) return 1; } while (0)
int main(void) {
    struct net_device up = {true}, down = {false};
    CHECK(!rt_dev_is_down(NULL));
    CHECK(!rt_dev_is_down(&up));
    CHECK(rt_dev_is_down(&down));
    for (u32 flags = 0; flags < 65536; flags++) {
        bool expected = !(flags & IP_VS_CONN_F_ONE_PACKET) &&
            (flags & (IP_VS_CONN_F_NO_CPORT | IP_VS_CONN_F_TEMPLATE)) !=
                     (IP_VS_CONN_F_NO_CPORT | IP_VS_CONN_F_TEMPLATE);
        CHECK(ip_vs_sync_validate_flags(flags) == expected);
    }
    CHECK(ip_vs_sync_validate_flags(0x80000000));
    for (unsigned old = 5; old <= 15; old++)
        for (unsigned offset = 0; offset <= 60; offset += 20)
            for (unsigned now = 0; now <= 15; now++) {
                struct header cih = {now};
                struct parsed ciph = {old * 4 + offset, offset};
                CHECK(check_ihl(&cih, &ciph) == (now == old));
            }
    return 0;
}
"""


def execute(parts, tmp_path):
    source = tmp_path / "guards.c"
    binary = tmp_path / "guards"
    source.write_text(program(parts), encoding="utf-8")
    subprocess.run(["cc", "-std=c99", "-Wall", "-Wextra", "-Werror",
                    str(source), "-o", str(binary)], check=True)
    return subprocess.run([str(binary)], check=False).returncode


def test_carried_predicates(tmp_path):
    assert execute(guards(), tmp_path) == 0


@pytest.mark.parametrize("which", range(3), ids=["device", "sync", "ihl"])
def test_bypassing_each_predicate_fails(which, tmp_path):
    parts = guards()
    if which == 0:
        old, new = "dev && !netif_running(dev)", "dev && false"
    elif which == 1:
        old, new = "if (flags & IP_VS_CONN_F_ONE_PACKET)", "if ((flags & IP_VS_CONN_F_ONE_PACKET) && false)"
    elif which == 2:
        old, new = "cih->ihl * 4 != ciph->len - ciph->off", "cih->ihl * 4 != ciph->len - ciph->off && false"
    assert parts[which].count(old) == 1
    parts[which] = parts[which].replace(old, new)
    assert execute(parts, tmp_path) == 1


def test_patch_call_sites():
    # Static wiring checks supplement the predicate execution; not a race proof.
    assert additions(2).count("if (!rt_dev_is_down(dst_dev_rcu(&rt->dst)))") == 2
    assert "rt->dst.dev" not in additions(2)
    assert additions(3).count("if (!ip_vs_nat_icmp(") == 2
    assert additions(4).count("if (!ip_vs_sync_validate_flags(flags))") == 2
    v0 = patch(4).split("static void ip_vs_process_message_v0", 1)[1]
    assert v0.index("p += SIMPLE_CONN_SIZE") < v0.index("if (!ip_vs_sync_validate_flags(flags))")
    assert v0.index("p += FULL_CONN_SIZE") < v0.index("if (!ip_vs_sync_validate_flags(flags))")
