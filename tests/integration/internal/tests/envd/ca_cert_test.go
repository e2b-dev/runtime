package envd

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

// generateSelfSignedCA produces a PEM-encoded self-signed CA certificate
// in-memory. Each call produces a unique certificate so tests can distinguish
// certA from certB without any shared state.
func generateSelfSignedCA(t *testing.T, cn string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err, "generate key")

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err, "create certificate")

	var buf bytes.Buffer
	require.NoError(t, pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der}))

	return buf.String()
}

// TestCACertBundleOnTmpfs verifies that /etc/ssl/certs is bind-mounted from
// tmpfs as set up by the envd.service ExecStartPre script. Without this mount
// atomic cert rotation would fail with EXDEV and writes would hit NBD.
func TestCACertBundleOnTmpfs(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	client := setup.GetEnvdClient(t, t.Context())

	out, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client,
		"findmnt", "-T", "/etc/ssl/certs", "-o", "SOURCE,FSTYPE", "--noheadings")
	require.NoError(t, err)

	assert.Contains(t, out, "tmpfs", "/etc/ssl/certs should be on tmpfs")
}

// TestCACertPersistsThroughUpdateCACertificates checks the platform assumption
// envd's CA installer is built on: that a cert file envd persists in the local
// certificate directory is picked up when update-ca-certificates rebuilds the
// trust bundle from its source directories. envd persists every injected cert
// there for exactly that reason, so if the template's tooling or layout ever
// stopped honouring the directory, injected certs would silently vanish on the
// next rebuild.
//
// It deliberately exercises the guest, not envd: the cert is written straight to
// the path rather than through /init, which is how the orchestrator delivers it
// in production but is not reachable through the sandbox URL. envd's own side —
// that it installs, replaces and persists certs by identity — is covered by
// TestInstallCACerts_* in packages/envd/internal/host.
func TestCACertPersistsThroughUpdateCACertificates(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	client := setup.GetEnvdClient(t, t.Context())

	certPEM := generateSelfSignedCA(t, "e2b-test-ca-persist")

	// envd's caCertDir (packages/envd/internal/host/cacerts.go), which this module
	// cannot import; change it there, change it here. The name must stay outside
	// envd's managed pattern (e2b-<sha256>.crt): envd's background cleanup deletes
	// managed files it was not asked to install, and when that ran between the
	// upload and the rebuild the cert was gone and the test failed.
	const caCertPath = "/usr/local/share/ca-certificates/e2b-test-ca-persist.crt"

	utils.UploadFileAs(t, t.Context(), sbx, client, caCertPath, certPEM, "root")

	// Rebuild the bundle from source directories. envd's startup reconcile can
	// rename an older bundle snapshot over a rebuild it overlapped with, and stops
	// once its pending request clears; a guest that ignored the directory never
	// passes.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		err := utils.ExecCommandAsRoot(t, t.Context(), sbx, client, "update-ca-certificates")
		if !assert.NoError(c, err, "update-ca-certificates should succeed") {
			return
		}
		bundle, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client,
			"cat", "/etc/ssl/certs/ca-certificates.crt")
		if !assert.NoError(c, err, "read CA bundle") {
			return
		}
		assert.Contains(c, bundle, strings.TrimRight(certPEM, "\n"),
			"a cert in envd's local certificate directory should survive an update-ca-certificates rebuild")
	}, 30*time.Second, time.Second)
}
