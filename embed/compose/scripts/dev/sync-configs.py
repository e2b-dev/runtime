#!/usr/bin/env python3
"""Copy compose/config/clickhouse/config.xml, compose/config/vector/vector.toml
and compose/config/otel/otel-collector.yaml into their inline `configs:` blocks
in compose/compose.yaml and stamp each consuming service with the file's
SHA-256 (CLICKHOUSE_CONFIG_SHA256, VECTOR_CONFIG_SHA256, OTEL_CONFIG_SHA256).

The inline copies are what the two-file install ships; the checksums are what
make Compose recreate the container when a config changes, because Compose
does not hash `configs:` content. tests/inline-configs.bats and
tests/config-hashes.bats fail until this has been run after editing a config.
Developer tool only; it is not baked into any image. Run: make sync-configs
"""
import hashlib
import pathlib
import re
import sys

# The package root: the compose files sit under compose/ and the Kubernetes
# copies under kubernetes/, so every path below is written from there.
ROOT = pathlib.Path(__file__).resolve().parents[3]
COMPOSE = ROOT / "compose/compose.yaml"
CONFIGS = {
    "clickhouse-config": ("compose/config/clickhouse/config.xml", "CLICKHOUSE_CONFIG_SHA256"),
    "vector-config": ("compose/config/vector/vector.toml", "VECTOR_CONFIG_SHA256"),
    "otel-collector-config": ("compose/config/otel/otel-collector.yaml", "OTEL_CONFIG_SHA256"),
}

# The Kubernetes copies are derived, not identical: the pod shares the node's
# network namespace, so every listener that compose keeps behind a 127.0.0.1
# port mapping must bind loopback itself, and Vector reaches ClickHouse on
# loopback instead of the compose service name. Regenerate with
# `make sync-configs` after editing any source; tests/kubernetes.bats
# compares the checked-in copies with `--print-k8s`.
K8S_CONFIGS = {
    "compose/config/clickhouse/config.xml": ("kubernetes/config/clickhouse-config.xml", (
        ("<listen_host>0.0.0.0</listen_host>", "<listen_host>127.0.0.1</listen_host>"),
    )),
    "compose/config/vector/vector.toml": ("kubernetes/config/vector.toml", (
        ('address = "0.0.0.0:44313"', 'address = "127.0.0.1:44313"'),
        # The log listener also moves off Kubernetes' default NodePort range
        # (30000-32767): a NodePort allocated 30006 would DNAT the pod's own
        # loopback POSTs away from Vector.
        ('address = "0.0.0.0:30006"', 'address = "127.0.0.1:20006"'),
        ('endpoint = "http://clickhouse:8123"', 'endpoint = "http://127.0.0.1:8123"'),
    )),
    # The collector already runs on the host network under compose, binding
    # loopback and reaching ClickHouse there, so its copy is the source as is.
    "compose/config/otel/otel-collector.yaml": ("kubernetes/config/otel-collector.yaml", ()),
}


def derive_k8s(text: str, substitutions) -> str:
    for old, new in substitutions:
        if text.count(old) != 1:
            raise SystemExit(f"sync-configs: expected exactly one {old!r} in the source config")
        text = text.replace(old, new)
    return text


def indent(text: str) -> str:
    lines = text.rstrip("\n").split("\n")
    return "".join((("      " + line) if line.strip() else "") + "\n" for line in lines)


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--print-k8s":
        dst, substitutions = K8S_CONFIGS[sys.argv[2]]
        sys.stdout.write(derive_k8s((ROOT / sys.argv[2]).read_text(), substitutions))
        return 0

    yaml = COMPOSE.read_text()
    for name, (rel, env_var) in CONFIGS.items():
        raw = (ROOT / rel).read_bytes()
        content = raw.decode()
        if "$" in content:
            # Compose interpolates `$` in compose.yaml, so a literal one would have
            # to be doubled in the inline copy and the byte-for-byte test would fail.
            print(f"sync-configs: {rel} contains '$'; rewrite it without one", file=sys.stderr)
            return 1
        indicator = "|" if content.endswith("\n") else "|-"
        block = re.compile(rf"(  {re.escape(name)}:\n    content: )\|-?\n((?:      .*\n|\n)*)")
        if not block.search(yaml):
            print(f"sync-configs: no inline block for {name} in compose/compose.yaml", file=sys.stderr)
            return 1
        yaml = block.sub(lambda m: m.group(1) + indicator + "\n" + indent(content), yaml, count=1)
        digest = hashlib.sha256(raw).hexdigest()
        yaml, n = re.subn(rf"({env_var}: )[0-9a-f]{{64}}", rf"\g<1>{digest}", yaml)
        if n != 1:
            print(f"sync-configs: expected one {env_var} stamp in compose/compose.yaml, found {n}", file=sys.stderr)
            return 1
        print(f"sync-configs: {name} <- {rel} ({digest[:12]})")

    for rel, (dst, substitutions) in K8S_CONFIGS.items():
        (ROOT / dst).write_text(derive_k8s((ROOT / rel).read_text(), substitutions))
        print(f"sync-configs: {dst} <- {rel} ({'loopback binds' if substitutions else 'as is'})")

    COMPOSE.write_text(yaml)
    return 0


if __name__ == "__main__":
    sys.exit(main())
