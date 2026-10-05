#!/usr/bin/env python3
"""Print a copy of config/vector/vector.toml that reads events from stdin and
writes the sandbox_logs rows to stdout as JSON, one per line, so a fixture can
be replayed through the real transforms, compiled exactly as Vector compiles
them in the stack (each remap on its own, with no knowledge of the others'
types). Dropped events print nothing. Everything between the source and the
sink, the route included, is the shipped configuration."""
import pathlib
import re
import sys

toml = (pathlib.Path(__file__).resolve().parents[2] / "config/vector/vector.toml").read_text()


def replace_table(text, header, body):
    # A TOML table runs from its header to the next top-level [table] header.
    pattern = re.compile(r"^\[" + re.escape(header) + r"\]\n.*?(?=^\[[a-z_]+\.|\Z)", re.S | re.M)
    if not pattern.search(text):
        sys.exit(f"[{header}] not found in vector.toml")
    return pattern.sub(body.rstrip("\n") + "\n\n", text, count=1)


toml = replace_table(toml, "sources.http_server", '[sources.http_server]\ntype = "stdin"\ndecoding.codec = "json"\n')
toml = replace_table(
    toml,
    "sinks.clickhouse_sandbox_logs",
    '[sinks.clickhouse_sandbox_logs]\ntype = "console"\ninputs = [ "sandbox_logs_rows" ]\ntarget = "stdout"\nencoding.codec = "json"\n',
)
# The console sink emits an event only after the remap ran; nothing else may print to stdout.
sys.stdout.write(toml)
