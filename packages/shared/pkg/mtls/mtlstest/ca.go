// Package mtlstest mints throwaway certificate hierarchies for tests of the
// mtls package and of the services that use it: a root, an intermediate and
// leaves carrying a SPIFFE URI SAN, all ECDSA P-256, optionally written to a
// temporary directory laid out the way the chart mounts them.
//
// It imports only the standard library so that any module's tests can use it.
package mtlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a signing certificate: a root, or an intermediate that remembers the
// root it chains to.
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
	// PEM is the certificate alone.
	PEM []byte

	parent *CA
	root   *CA
}

// NewRootCA mints a self-signed root valid from a day ago for a day.
func NewRootCA(tb testing.TB, name string) *CA {
	tb.Helper()

	key := newKey(tb)
	template := &x509.Certificate{
		SerialNumber:          serial(tb),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-24 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	cert := sign(tb, template, template, &key.PublicKey, key)

	ca := &CA{Cert: cert, Key: key, PEM: certPEM(cert)}
	ca.root = ca

	return ca
}

// Intermediate mints a CA signed by ca with path length zero, the shape a
// cert-manager issuer produces. It lists no extended key usage, and so
// restricts none, unless usages are given.
func (ca *CA) Intermediate(tb testing.TB, name string, usages ...x509.ExtKeyUsage) *CA {
	tb.Helper()

	key := newKey(tb)
	template := &x509.Certificate{
		SerialNumber:          serial(tb),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(12 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           usages,
	}
	cert := sign(tb, template, ca.Cert, &key.PublicKey, ca.Key)

	return &CA{Cert: cert, Key: key, PEM: certPEM(cert), parent: ca, root: ca.root}
}

// BundlePEM is the trust bundle a workload under this CA mounts: the root alone.
func (ca *CA) BundlePEM() []byte {
	return ca.root.PEM
}

// LeafSpec describes the certificate to mint. The zero value is a leaf for
// client and server use, valid from an hour ago for an hour, with no names.
type LeafSpec struct {
	// SPIFFEID becomes the one URI SAN when URIs is nil.
	SPIFFEID string
	// URIs, when non-nil, replaces SPIFFEID so a test can mint zero or several URI SANs.
	URIs []string
	// DNSNames are the names a client may dial the server by.
	DNSNames []string
	// ExtKeyUsage defaults to client and server authentication.
	ExtKeyUsage []x509.ExtKeyUsage
	// NotBefore and NotAfter default to an hour either side of now.
	NotBefore time.Time
	NotAfter  time.Time
}

// Leaf is a minted end-entity certificate with its key.
type Leaf struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
	// CertPEM holds the leaf followed by the intermediates up to, not
	// including, the root: what a certificate file carries.
	CertPEM []byte
	KeyPEM  []byte
	// TLS is CertPEM and KeyPEM parsed, with Leaf set.
	TLS tls.Certificate
}

// Leaf mints an end-entity certificate signed by ca.
func (ca *CA) Leaf(tb testing.TB, spec LeafSpec) *Leaf {
	tb.Helper()

	key := newKey(tb)
	template := &x509.Certificate{
		SerialNumber: serial(tb),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    spec.NotBefore,
		NotAfter:     spec.NotAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  spec.ExtKeyUsage,
		DNSNames:     spec.DNSNames,
	}
	if template.NotBefore.IsZero() {
		template.NotBefore = time.Now().Add(-time.Hour)
	}
	if template.NotAfter.IsZero() {
		template.NotAfter = time.Now().Add(time.Hour)
	}
	if template.ExtKeyUsage == nil {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}
	}

	uris := spec.URIs
	if uris == nil && spec.SPIFFEID != "" {
		uris = []string{spec.SPIFFEID}
	}
	for _, raw := range uris {
		u, err := url.Parse(raw)
		if err != nil {
			tb.Fatalf("mtlstest: parse URI SAN %q: %v", raw, err)
		}
		template.URIs = append(template.URIs, u)
	}

	cert := sign(tb, template, ca.Cert, &key.PublicKey, ca.Key)

	chain := certPEM(cert)
	for c := ca; c.parent != nil; c = c.parent {
		chain = append(chain, c.PEM...)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		tb.Fatalf("mtlstest: marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	tlsCert, err := tls.X509KeyPair(chain, keyPEM)
	if err != nil {
		tb.Fatalf("mtlstest: key pair: %v", err)
	}

	return &Leaf{Cert: cert, Key: key, CertPEM: chain, KeyPEM: keyPEM, TLS: tlsCert}
}

// Dir is a directory holding tls.crt, tls.key and ca.crt.
type Dir struct {
	Dir      string
	CertFile string
	KeyFile  string
	CAFile   string
}

// WriteFiles writes leaf and bundle under a fresh tb.TempDir().
func WriteFiles(tb testing.TB, leaf *Leaf, bundlePEM []byte) Dir {
	tb.Helper()

	dir := tb.TempDir()
	d := Dir{
		Dir:      dir,
		CertFile: filepath.Join(dir, "tls.crt"),
		KeyFile:  filepath.Join(dir, "tls.key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}
	d.Rewrite(tb, leaf, bundlePEM)

	return d
}

// Rewrite replaces the three files, each through a rename, the way the
// kubelet swaps a projected directory.
func (d Dir) Rewrite(tb testing.TB, leaf *Leaf, bundlePEM []byte) {
	tb.Helper()

	Write(tb, d.CertFile, leaf.CertPEM)
	Write(tb, d.KeyFile, leaf.KeyPEM)
	Write(tb, d.CAFile, bundlePEM)
}

// Write replaces one file with data through a rename, for malformed-input tests.
func Write(tb testing.TB, path string, data []byte) {
	tb.Helper()

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		tb.Fatalf("mtlstest: create temp file: %v", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tb.Fatalf("mtlstest: write %s: %v", path, err)
	}
	if err := tmp.Close(); err != nil {
		tb.Fatalf("mtlstest: close %s: %v", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		tb.Fatalf("mtlstest: rename into %s: %v", path, err)
	}
}

func newKey(tb testing.TB) *ecdsa.PrivateKey {
	tb.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatalf("mtlstest: generate key: %v", err)
	}

	return key
}

func serial(tb testing.TB) *big.Int {
	tb.Helper()

	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		tb.Fatalf("mtlstest: serial number: %v", err)
	}

	return n
}

func sign(tb testing.TB, template, parent *x509.Certificate, pub *ecdsa.PublicKey, signer *ecdsa.PrivateKey) *x509.Certificate {
	tb.Helper()

	der, err := x509.CreateCertificate(rand.Reader, template, parent, pub, signer)
	if err != nil {
		tb.Fatalf("mtlstest: create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		tb.Fatalf("mtlstest: parse certificate: %v", err)
	}

	return cert
}

func certPEM(cert *x509.Certificate) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}
