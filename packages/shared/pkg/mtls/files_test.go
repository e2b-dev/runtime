package mtls

import (
	"encoding/pem"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

func TestNewFilesLoadsAConsistentSet(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)

	bundle := fx.files.Bundle()
	require.NotNil(t, bundle)
	assert.Equal(t, fx.leaf.Cert.SerialNumber, bundle.Certificate.Leaf.SerialNumber)
	assert.Len(t, bundle.Certificate.Certificate, 2, "leaf and intermediate")
	require.Len(t, bundle.RootCerts, 1)
	assert.Equal(t, fx.root.Cert.SerialNumber, bundle.RootCerts[0].SerialNumber)
	assert.False(t, bundle.LoadedAt.IsZero())
	assert.Contains(t, logMessages(fx.logs), "mtls: certificate files loaded")
}

func TestNewFilesStartsWithoutFilesAndLoadsLater(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := FileConfig{
		CertFile: filepath.Join(dir, "tls.crt"),
		KeyFile:  filepath.Join(dir, "tls.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}
	log, logs := testLogger(t)

	files := NewFiles(t.Context(), cfg, WithReloadInterval(time.Hour), WithFilesLogger(log))
	assert.Nil(t, files.Bundle(), "the process starts with nothing loaded")
	assert.Contains(t, logMessages(logs), "mtls: no certificate loaded; TLS handshakes fail until the files are fixed")

	root := mtlstest.NewRootCA(t, "root")
	leaf := root.Leaf(t, mtlstest.LeafSpec{SPIFFEID: serverID})
	mtlstest.Dir{Dir: dir, CertFile: cfg.CertFile, KeyFile: cfg.KeyFile, CAFile: cfg.CAFile}.Rewrite(t, leaf, root.BundlePEM())

	require.NoError(t, files.Reload(t.Context()))
	require.NotNil(t, files.Bundle())
	assert.Equal(t, leaf.Cert.SerialNumber, files.Bundle().Certificate.Leaf.SerialNumber)
}

func TestFilesKeepsLastGoodSetWhenKeyDoesNotMatch(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	renewed := fx.peerLeaf(t, serverID)

	// A reload that straddles the kubelet's directory swap sees the new
	// certificate beside the old key.
	mtlstest.Write(t, fx.dir.CertFile, renewed.CertPEM)
	err := fx.files.Reload(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "private key does not match public key")
	assert.Equal(t, fx.leaf.Cert.SerialNumber, fx.files.Bundle().Certificate.Leaf.SerialNumber, "the previous set keeps serving")
	assert.Contains(t, logMessages(fx.logs), "mtls: certificate reload failed; keeping the previous set")

	// The next reload sees a consistent pair.
	mtlstest.Write(t, fx.dir.KeyFile, renewed.KeyPEM)
	require.NoError(t, fx.files.Reload(t.Context()))
	assert.Equal(t, renewed.Cert.SerialNumber, fx.files.Bundle().Certificate.Leaf.SerialNumber)
}

// corruptPEMBlock has a valid header and footer around a body pem.Decode
// cannot decode, which pem.Decode skips rather than reports.
const corruptPEMBlock = "-----BEGIN CERTIFICATE-----\n!!!! this body is not base64 !!!!\n-----END CERTIFICATE-----\n"

func TestParseTrustBundleRejectsASkippedBlock(t *testing.T) {
	t.Parallel()

	root := mtlstest.NewRootCA(t, "root")
	other := mtlstest.NewRootCA(t, "other")
	keyPEM := root.Leaf(t, mtlstest.LeafSpec{SPIFFEID: serverID}).KeyPEM

	tests := []struct {
		name   string
		bundle []byte
	}{
		{name: "empty", bundle: nil},
		{name: "text only", bundle: []byte("not a certificate")},
		{name: "a corrupt block after a good one", bundle: concat(root.PEM, []byte(corruptPEMBlock))},
		{name: "a corrupt block before a good one", bundle: concat([]byte(corruptPEMBlock), root.PEM)},
		{name: "a key block among the certificates", bundle: concat(root.PEM, keyPEM)},
		{name: "two roots and a corrupt tail", bundle: concat(root.PEM, other.PEM, []byte(corruptPEMBlock))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := parseTrustBundle(tt.bundle)
			require.ErrorIs(t, err, ErrInvalidBundle)
		})
	}

	pool, certs, err := parseTrustBundle(concat(root.PEM, other.PEM))
	require.NoError(t, err)
	assert.Len(t, certs, 2)
	assert.NotNil(t, pool)
}

func TestFilesRejectsACorruptBundleAndKeepsThePreviousRoots(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	previous := fx.files.Bundle()

	mtlstest.Write(t, fx.dir.CAFile, concat(fx.root.PEM, []byte(corruptPEMBlock)))
	require.ErrorIs(t, fx.files.Reload(t.Context()), ErrInvalidBundle)
	assert.Same(t, previous, fx.files.Bundle())
}

func TestFilesRejectsACertificateFileWithASkippedBlock(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	previous := fx.files.Bundle()

	mtlstest.Write(t, fx.dir.CertFile, concat(fx.leaf.CertPEM, []byte(corruptPEMBlock)))
	require.ErrorIs(t, fx.files.Reload(t.Context()), ErrInvalidBundle)
	assert.Same(t, previous, fx.files.Bundle())
}

func TestFilesReloadUnchangedKeepsTheSameBundle(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	first := fx.files.Bundle()

	require.NoError(t, fx.files.Reload(t.Context()))
	assert.Same(t, first, fx.files.Bundle(), "unchanged files do not produce a new set")
}

func TestFilesStartReloadsOnTheInterval(t *testing.T) {
	t.Parallel()

	root := mtlstest.NewRootCA(t, "root")
	leaf := root.Leaf(t, mtlstest.LeafSpec{SPIFFEID: serverID})
	dir := mtlstest.WriteFiles(t, leaf, root.BundlePEM())
	log, _ := testLogger(t)
	files := NewFiles(t.Context(),
		FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile},
		WithReloadInterval(20*time.Millisecond), WithFilesLogger(log))
	files.Start(t.Context())

	renewed := root.Leaf(t, mtlstest.LeafSpec{SPIFFEID: serverID})
	dir.Rewrite(t, renewed, root.BundlePEM())

	require.Eventually(t, func() bool {
		return files.Bundle().Certificate.Leaf.SerialNumber.Cmp(renewed.Cert.SerialNumber) == 0
	}, 5*time.Second, 10*time.Millisecond, "the renewal is picked up without a restart")
}

//nolint:paralleltest // t.Setenv cannot be used from a parallel test
func TestFileConfigFromEnv(t *testing.T) {
	t.Setenv(CertFileEnv, "")
	t.Setenv(KeyFileEnv, "")
	t.Setenv(CAFileEnv, "")
	assert.Equal(t, FileConfig{CertFile: DefaultCertFile, KeyFile: DefaultKeyFile, CAFile: DefaultCAFile}, FileConfigFromEnv())

	t.Setenv(CertFileEnv, "/srv/tls/a.crt")
	t.Setenv(KeyFileEnv, "/srv/tls/a.key")
	t.Setenv(CAFileEnv, "/srv/trust/ca.crt")
	assert.Equal(t, FileConfig{CertFile: "/srv/tls/a.crt", KeyFile: "/srv/tls/a.key", CAFile: "/srv/trust/ca.crt"}, FileConfigFromEnv())
}

// concat joins byte slices into a fresh slice so test inputs never alias a fixture.
func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}

	return out
}

func TestFilesRejectsAnIntermediateThatDoesNotParse(t *testing.T) {
	t.Parallel()

	fx := newFixture(t)
	previous := fx.files.Bundle()

	// tls.X509KeyPair parses only the leaf and keeps the rest of the chain as
	// bytes, so a block that decodes but is not a certificate gets past it.
	notACert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not a certificate")})
	mtlstest.Write(t, fx.dir.CertFile, concat(fx.leaf.CertPEM, notACert))
	require.ErrorIs(t, fx.files.Reload(t.Context()), ErrInvalidBundle)
	assert.Same(t, previous, fx.files.Bundle())
}

func TestNewFilesIgnoresANonPositiveReloadInterval(t *testing.T) {
	t.Parallel()

	log, _ := testLogger(t)
	for _, interval := range []time.Duration{0, -time.Second} {
		// A ticker would panic on it inside Start's goroutine.
		files := NewFiles(t.Context(), FileConfig{}, WithReloadInterval(interval), WithFilesLogger(log))
		assert.Equal(t, DefaultReloadInterval, files.interval)
	}
}
