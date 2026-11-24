//go:build linux

package sandboxtools

import (
	"context"
	"fmt"

	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// CertBundlePath is the tar that e2b-seed-certs (envd.service's ExecStartPre)
// extracts over /etc/ssl/certs on every cold boot of the guest.
const CertBundlePath = "/usr/local/share/e2b/ssl-certs.tar"

// PackCertBundleTar packs /etc/ssl/certs into CertBundlePath. -h dereferences
// the hash-named symlinks so the tar carries the cert contents rather than
// links into a rootfs the next boot fetches lazily. The finalize phase appends
// it to its trust-store refresh; build steps preserve links through PackCertBundle.
var PackCertBundleTar = packCertBundleTar(true)

// Intermediate layers preserve links, including dangling links that a later
// step's trust-store refresh can clean up. Their targets live in the same
// filesystem layer. Only finalize dereferences them for fast cold-boot reads.
func packCertBundleTar(dereference bool) string {
	flags := "-cf"
	if dereference {
		flags = "-chf"
	}

	return "mkdir -p /usr/local/share/e2b\ntar -C /etc/ssl/certs " + flags + " " + CertBundlePath + " .\n"
}

// packStepCertBundleCmd packs the trust store at the end of a build step.
// /etc/ssl/certs is a tmpfs seeded from CertBundlePath on every cold boot, so
// what a step writes under it (a CA dropped in directly, the keystore
// ca-certificates-java generates) reaches the next cold boot only through the
// tar. The first uncached step after a cache hit and the finalize phase both
// cold-boot, and without this pack a cached layer's writes under /etc/ssl/certs
// were gone for them. A guest without the directory has nothing to pack.
var packStepCertBundleCmd = `set -e
[ -d /etc/ssl/certs ] || exit 0
` + packCertBundleTar(false)

// PackCertBundle runs packStepCertBundleCmd in the guest as root.
func PackCertBundle(ctx context.Context, proxy *proxy.SandboxProxy, userLogger logger.Logger, sandboxID string) error {
	err := RunCommandWithLogger(ctx, proxy, userLogger, zapcore.InfoLevel, "certs", sandboxID, packStepCertBundleCmd, metadata.Context{User: "root"})
	if err != nil {
		return fmt.Errorf("error packing CA cert bundle: %w", err)
	}

	return nil
}
