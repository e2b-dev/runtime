{{- /*gotype:github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs.templateModel*/ -}}
{{- if .EnvdMemoryProtection -}}
{{ .WriteFile "/etc/systemd/system/system.slice.d/10-e2b-envd.conf" 0o644 }}

# cgroup v2 grants a cgroup the smaller of its own memory.min request and its
# share of the parent's grant, level by level up to the root. envd.service
# requests protection but sits in system.slice, which requests none, so envd's
# grant is zero and its pages are reclaimed like any other under pressure.
# Requesting the same amount on the slice grants envd's request in full.
# The slice asks for no more than envd does: the kernel hands a slice's
# unclaimed grant to its children in proportion to their usage, so a larger
# request here would shield the other system daemons too, at the guest's
# expense. What can reach them is bounded by the request minus envd's use.
[Slice]
MemoryMin={{ .EnvdMemoryMinMiB }}M
MemoryLow={{ .EnvdMemoryLowMiB }}M
{{- end -}}
