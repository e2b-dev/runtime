package mtlstest

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testID = "spiffe://cluster-a.example.internal/ns/platform/sa/server"

func TestLeafChainsToTheRootThroughTheIntermediate(t *testing.T) {
	t.Parallel()

	root := NewRootCA(t, "root")
	inter := root.Intermediate(t, "intermediate")
	leaf := inter.Leaf(t, LeafSpec{SPIFFEID: testID, DNSNames: []string{"server.platform.svc.cluster.local"}})

	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(inter.BundlePEM()))
	intermediates := x509.NewCertPool()
	intermediates.AddCert(inter.Cert)

	chains, err := leaf.Cert.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		DNSName:       "server.platform.svc.cluster.local",
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	require.NoError(t, err)
	require.Len(t, chains, 1)
	assert.Len(t, chains[0], 3, "leaf, intermediate, root")

	require.Len(t, leaf.Cert.URIs, 1)
	want, err := url.Parse(testID)
	require.NoError(t, err)
	assert.Equal(t, want.String(), leaf.Cert.URIs[0].String())

	assert.Len(t, leaf.TLS.Certificate, 2, "the certificate file carries the leaf and the intermediate")
	assert.NotNil(t, leaf.TLS.Leaf)
}

func TestLeafSpecControlsSANsUsagesAndValidity(t *testing.T) {
	t.Parallel()

	root := NewRootCA(t, "root")
	expiredAt := time.Now().Add(-time.Minute)

	noSAN := root.Leaf(t, LeafSpec{URIs: []string{}})
	assert.Empty(t, noSAN.Cert.URIs)

	twoSANs := root.Leaf(t, LeafSpec{URIs: []string{testID, "spiffe://cluster-a.example.internal/ns/platform/sa/other"}})
	assert.Len(t, twoSANs.Cert.URIs, 2)

	serverOnly := root.Leaf(t, LeafSpec{SPIFFEID: testID, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	assert.Equal(t, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, serverOnly.Cert.ExtKeyUsage)

	expired := root.Leaf(t, LeafSpec{SPIFFEID: testID, NotBefore: expiredAt.Add(-time.Hour), NotAfter: expiredAt})
	assert.True(t, expired.Cert.NotAfter.Before(time.Now()))

	// An expired leaf still loads as a key pair; validity is a verification concern.
	_, err := tls.X509KeyPair(expired.CertPEM, expired.KeyPEM)
	require.NoError(t, err)
}

func TestWriteFilesAndRewrite(t *testing.T) {
	t.Parallel()

	root := NewRootCA(t, "root")
	first := root.Leaf(t, LeafSpec{SPIFFEID: testID})
	second := root.Leaf(t, LeafSpec{SPIFFEID: testID})

	dir := WriteFiles(t, first, root.BundlePEM())
	cert, err := tls.LoadX509KeyPair(dir.CertFile, dir.KeyFile)
	require.NoError(t, err)
	assert.Equal(t, first.Cert.SerialNumber, cert.Leaf.SerialNumber)

	bundle, err := os.ReadFile(dir.CAFile)
	require.NoError(t, err)
	assert.Equal(t, root.BundlePEM(), bundle)

	dir.Rewrite(t, second, root.BundlePEM())
	cert, err = tls.LoadX509KeyPair(dir.CertFile, dir.KeyFile)
	require.NoError(t, err)
	assert.Equal(t, second.Cert.SerialNumber, cert.Leaf.SerialNumber)

	Write(t, dir.CAFile, []byte("not a certificate"))
	bundle, err = os.ReadFile(dir.CAFile)
	require.NoError(t, err)
	assert.Equal(t, "not a certificate", string(bundle))
}
