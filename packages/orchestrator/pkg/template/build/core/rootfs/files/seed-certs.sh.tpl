{{- /*gotype:github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs.templateModel*/ -}}
{{ .WriteFile "usr/local/bin/e2b-seed-certs" 0o755 }}

#!/bin/sh
# Seeds the tmpfs-backed /etc/ssl/certs before envd starts — shared by
# envd.service (systemd) and /etc/init.d/envd (OpenRC). See envd.service.tpl
# for the full rationale (why a tar, why a bind mount, the egress-CA contract).
#
# Every failure path here WARNS and continues deliberately: a sandbox with a
# degraded trust store is recoverable (envd's POST /init reinstalls the egress
# CA), a sandbox whose envd never starts is not.

# The copies DEREFERENCE symlinks (-L; the build-time tar already packs with
# -h): some images ship the /etc/ssl/certs bundle as symlinks into a read-only
# store, and envd's egress-proxy CA install rewrites
# /etc/ssl/certs/ca-certificates.crt by reading it and renaming a new copy over
# it — the tmpfs copy must be the real bundle, not a copied symlink whose
# target can be immutable or missing.
if ! mountpoint -q /etc/ssl/certs; then
    mkdir -p /run/e2b/certs
    if [ -f /usr/local/share/e2b/ssl-certs.tar ]; then
        if ! tar -C /run/e2b/certs -xf /usr/local/share/e2b/ssl-certs.tar; then
            echo "e2b-seed-certs: ssl-certs.tar extraction failed; seeding from the live cert dir instead" >&2
            cp -aL /etc/ssl/certs/. /run/e2b/certs/
        fi
    else
        # Only expected during the base-layer boot, before finalize packs the tar.
        echo "e2b-seed-certs: ssl-certs.tar not packed yet; seeding from the live cert dir"
        cp -aL /etc/ssl/certs/. /run/e2b/certs/
    fi
    if ! mount -o bind /run/e2b/certs /etc/ssl/certs; then
        echo "e2b-seed-certs: bind mount failed; envd runs with the image's certs as-is" >&2
    fi
fi

if [ ! -s /etc/ssl/certs/ca-certificates.crt ]; then
    if command -v update-ca-certificates >/dev/null 2>&1; then
        update-ca-certificates
    elif command -v update-ca-trust >/dev/null 2>&1; then
        # RHEL family and Arch refresh with update-ca-trust. Arch's extract emits
        # the Debian-named bundle itself; RHEL keeps it under /etc/pki, and the
        # symlink provisioning made was dereferenced into this tmpfs, so copy it.
        update-ca-trust extract
        if [ ! -s /etc/ssl/certs/ca-certificates.crt ] && [ -s /etc/pki/tls/certs/ca-bundle.crt ]; then
            cp -L /etc/pki/tls/certs/ca-bundle.crt /etc/ssl/certs/ca-certificates.crt
        fi
    else
        # Provisioning guarantees the bundle on every supported family;
        # reaching this means the image diverged after the build.
        echo "e2b-seed-certs: CA bundle missing and no CA refresh tool on this image; TLS trust will be degraded" >&2
    fi
fi

exit 0
