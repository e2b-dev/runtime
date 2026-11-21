"""Print what a flavour changes in its seed config, as Kconfig lines.

usage: python3 flavour_fragment.py SEED_CONFIG FLAVOUR_CONFIG

Every symbol whose value differs between the config a flavour was seeded from
and the flavour's resolved config, with the flavour's value. A symbol a config
leaves out is n, as Kconfig reads it. Toolchain probes and the build salt are
left out: they describe the compiler that resolved the file and the tag it was
resolved against, not the flavour, and every stem's config carries its own.
This is how flavours/<arch>/<flavour>.config is derived.
"""

import re
import sys
from pathlib import Path

ASSIGN = re.compile(r"^(CONFIG_[A-Za-z0-9_]+)=(.*)$")
UNSET = re.compile(r"^# (CONFIG_[A-Za-z0-9_]+) is not set$")
TOOLCHAIN = re.compile(r"^CONFIG_(BUILD_SALT|(CC|AS|LD|GCC|CLANG|LLD|PAHOLE|RUSTC)_[A-Z0-9_]+)$")


def load(text):
    """Symbol to value for every assignment in a config; "is not set" is n."""
    values = {}
    for line in text.splitlines():
        if match := ASSIGN.match(line):
            values[match[1]] = match[2]
        elif match := UNSET.match(line):
            values[match[1]] = "n"
    return values


def line(symbol, value):
    return f"# {symbol} is not set" if value == "n" else f"{symbol}={value}"


def fragment(seed, flavour):
    lines = []
    for symbol in sorted(seed.keys() | flavour.keys()):
        if TOOLCHAIN.match(symbol):
            continue
        value = flavour.get(symbol, "n")
        if seed.get(symbol, "n") != value:
            lines.append(line(symbol, value))
    return lines


def lost(fragment_values, config):
    """The fragment's lines a resolved config does not carry, as build.sh checks them."""
    return [
        line(symbol, value)
        for symbol, value in sorted(fragment_values.items())
        if config.get(symbol, "n") != value
    ]


def main():
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    seed, flavour = (load(Path(path).read_text(encoding="utf-8")) for path in sys.argv[1:])
    lines = fragment(seed, flavour)
    if not lines:
        raise SystemExit("flavour_fragment.py: the flavour changes nothing")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
