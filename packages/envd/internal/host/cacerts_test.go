//go:build unix

package host

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/cabundle"
)

// newTestCA returns a PEM self-signed CA certificate with a fresh key, so two
// calls with the same subject give distinct certificates.
func newTestCA(t *testing.T, cn string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
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
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

type testTrust struct {
	bundlePath string
	certDir    string
	system     []string // certificates envd did not install
	userFile   string   // a user's own certificate in certDir
}

// newTestTrust lays out a system bundle holding two unrelated roots and a
// local certificate directory with one user root.
func newTestTrust(t *testing.T) testTrust {
	t.Helper()

	dir := t.TempDir()
	tr := testTrust{
		bundlePath: filepath.Join(dir, "certs", "ca-certificates.crt"),
		certDir:    filepath.Join(dir, "local"),
		system:     []string{newTestCA(t, "system root"), newTestCA(t, "user root")},
	}
	tr.userFile = filepath.Join(tr.certDir, "user.crt")
	require.NoError(t, os.MkdirAll(filepath.Dir(tr.bundlePath), 0o755))
	require.NoError(t, os.MkdirAll(tr.certDir, 0o755))
	require.NoError(t, os.WriteFile(tr.bundlePath, []byte("# comment kept\n"+strings.Join(tr.system, "")), 0o644))
	require.NoError(t, os.WriteFile(tr.userFile, []byte(tr.system[1]), 0o644))

	return tr
}

// installer starts an installer, as a freshly started envd would.
func (tr testTrust) installer() *CACertInstaller {
	logger := zerolog.Nop()

	c := NewCACertInstaller(&logger)
	c.statePath = filepath.Join(filepath.Dir(tr.bundlePath), "state.json")

	return c
}

// update runs the part of Install that completes before /init returns.
func (tr testTrust) update(t *testing.T, c *CACertInstaller, certs ...string) {
	t.Helper()

	parsed, err := cabundle.Parse(strings.Join(certs, ""))
	require.NoError(t, err)
	require.NoError(t, c.updateBundle(t.Context(), parsed, tr.bundlePath))
}

// install runs both halves of Install synchronously.
func (tr testTrust) install(t *testing.T, c *CACertInstaller, certs ...string) {
	t.Helper()

	tr.update(t, c, certs...)
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
}

// derCounts counts the certificates in data by DER, decoded with the standard
// library and keyed by subject and fingerprint.
func derCounts(t *testing.T, data []byte) map[string]int {
	t.Helper()

	counts := map[string]int{}
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			require.Empty(t, bytes.TrimSpace(data), "bundle has undecodable content")

			return counts
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		sum := sha256.Sum256(cert.Raw)
		counts[fmt.Sprintf("%s %x", cert.Subject.CommonName, sum[:6])]++
	}
}

// ders returns each distinct certificate in certs once, keyed as derCounts.
func ders(t *testing.T, certs ...string) map[string]int {
	t.Helper()

	counts := derCounts(t, []byte(strings.Join(certs, "")))
	for key := range counts {
		counts[key] = 1
	}

	return counts
}

// requireBundle checks that the bundle holds the system roots and want once
// each and nothing else, and that readers other than root can load it.
func (tr testTrust) requireBundle(t *testing.T, want ...string) {
	t.Helper()

	bundle, err := os.ReadFile(tr.bundlePath)
	require.NoError(t, err)

	assert.Equal(t, ders(t, append(want, tr.system...)...), derCounts(t, bundle))
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(bundle))
	for _, certPEM := range want {
		block, _ := pem.Decode([]byte(certPEM))
		cert, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		_, err = cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
		require.NoError(t, err)
	}
	info, err := os.Stat(tr.bundlePath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}

// requireTrust checks the bundle and that certDir persists exactly want, one
// certificate per file, beside the user's untouched file.
func (tr testTrust) requireTrust(t *testing.T, want ...string) {
	t.Helper()

	tr.requireBundle(t, want...)

	var persisted []byte
	entries, err := os.ReadDir(tr.certDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") || entry.Name() == "user.crt" {
			continue
		}
		info, err := entry.Info()
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o644), info.Mode().Perm(), entry.Name())
		require.True(t, strings.HasPrefix(entry.Name(), "e2b-") && strings.HasSuffix(entry.Name(), ".crt"), entry.Name())
		data, err := os.ReadFile(filepath.Join(tr.certDir, entry.Name()))
		require.NoError(t, err)
		require.Len(t, derCounts(t, data), 1, "one certificate per file")
		persisted = append(persisted, data...)
	}
	assert.Equal(t, ders(t, want...), derCounts(t, persisted))

	user, err := os.ReadFile(tr.userFile)
	require.NoError(t, err)
	assert.Equal(t, tr.system[1], string(user))
}

// reformat gives a certificate CRLF line endings, short lines and surrounding
// blank lines without changing its DER.
func reformat(cert string) string {
	block, _ := pem.Decode([]byte(cert))
	block.Headers = map[string]string{"Comment": "test certificate"}
	lines := strings.Split(strings.TrimSpace(string(pem.EncodeToMemory(block))), "\n")
	var out []string
	for _, line := range lines {
		if strings.HasPrefix(line, "-----") || len(line) < 32 {
			out = append(out, line)

			continue
		}
		out = append(out, line[:32], line[32:])
	}

	return "\r\n\r\n" + strings.Join(out, "\r\n") + "\r\n\r\n"
}

func TestInstallCACerts_OverlappingSets(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	w, w2 := newTestCA(t, "worker"), newTestCA(t, "worker")
	r0, r1 := newTestCA(t, "added root 0"), newTestCA(t, "added root 1")

	for _, step := range []struct {
		name  string
		certs []string
	}{
		{"worker only", []string{reformat(w)}},
		{"add first root", []string{w, r0}},
		{"add second root", []string{w, r0, r1}},
		{"retire first root", []string{w, r1}},
		{"replace worker CA with the same subject", []string{w2, r1}},
		{"same set reordered, reformatted and repeated", []string{reformat(r1), w2, r1}},
	} {
		t.Log(step.name)
		tr.update(t, c, step.certs...)
		// Final identity assertions apply after deferred cleanup.
		require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
		tr.requireTrust(t, step.certs...)
	}
}

// The unchanged path must not rewrite the bundle once an install completed.
func TestInstallCACerts_UnchangedSkipsIO(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	w := newTestCA(t, "worker")

	tr.install(t, c, w)
	before, err := os.Stat(tr.bundlePath)
	require.NoError(t, err)

	require.NoError(t, c.install(t.Context(), reformat(w), tr.bundlePath))

	after, err := os.Stat(tr.bundlePath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(before, after), "bundle was replaced")
}

func TestInstallCACerts_PreexistingSuppliedRootIsOwned(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b := newTestCA(t, "preexisting E2B root"), newTestCA(t, "replacement root")
	require.NoError(t, os.WriteFile(tr.bundlePath, []byte(strings.TrimSuffix(strings.Join(tr.system, "")+reformat(a)+a, "\n")), 0o644))
	customerSource := filepath.Join(tr.certDir, "customer-copy.crt")
	require.NoError(t, os.WriteFile(customerSource, []byte(a), 0o644))
	tr.install(t, c, a)
	tr.requireBundle(t, a)
	// Ownership survives restart, even when the certificate predated this process.
	tr.install(t, tr.installer(), b)
	tr.requireBundle(t, b)
	source, err := os.ReadFile(customerSource)
	require.NoError(t, err)
	require.Equal(t, a, string(source), "customer source files are not managed by envd")
}

func TestInstallCACerts_PersistenceFailureDoesNotBlockTrust(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	w, r0 := newTestCA(t, "retired root"), newTestCA(t, "replacement root")
	tr.install(t, c, w)
	require.NoError(t, os.Rename(tr.certDir, tr.certDir+".away"))
	require.NoError(t, os.Symlink(tr.certDir+".missing", tr.certDir))
	tr.update(t, c, r0)
	pending, err := os.ReadFile(c.statePath)
	require.NoError(t, err)
	require.Error(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireBundle(t, r0)
	after, err := os.ReadFile(c.statePath)
	require.NoError(t, err)
	require.Equal(t, pending, after, "retain ownership until source cleanup succeeds")
	require.NoError(t, os.Remove(tr.certDir))
	require.NoError(t, os.Rename(tr.certDir+".away", tr.certDir))
	// A restart must retry the source cleanup using the retained ownership.
	require.NoError(t, tr.installer().persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireTrust(t, r0)
}

func TestInstallCACerts_PublishesReplacement(t *testing.T) {
	t.Parallel()
	for _, symlink := range []bool{false, true} {
		t.Run(fmt.Sprintf("symlink=%t", symlink), func(t *testing.T) {
			t.Parallel()
			tr := newTestTrust(t)
			before, err := os.ReadFile(tr.bundlePath)
			require.NoError(t, err)
			target := tr.bundlePath + ".target"
			if symlink {
				require.NoError(t, os.Rename(tr.bundlePath, target))
				require.NoError(t, os.Symlink(target, tr.bundlePath))
			}
			reader, err := os.Open(tr.bundlePath)
			require.NoError(t, err)
			defer reader.Close()
			a := newTestCA(t, "A")
			c := tr.installer()
			tr.update(t, c, a)
			tr.requireBundle(t, a)
			old, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, before, old, "an existing reader must retain the complete old bundle")
			if symlink {
				tr.install(t, c, newTestCA(t, "B"))
				original, err := os.ReadFile(target)
				require.NoError(t, err)
				require.Equal(t, before, original, "never modify the original symlink target")
			}
		})
	}
}

func TestInstallCACerts_RepairsInterruptedAppendBeforeInitReturns(t *testing.T) {
	t.Parallel()
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			t.Parallel()
			tr := newTestTrust(t)
			c := tr.installer()
			a, b := newTestCA(t, "A"), newTestCA(t, "B")
			tr.install(t, c, a)
			before, err := os.ReadFile(tr.bundlePath)
			require.NoError(t, err)
			require.NoError(t, os.Remove(tr.bundlePath))
			require.Error(t, c.install(t.Context(), b, tr.bundlePath))
			// An older installer could leave a partial append after recording intent.
			require.NoError(t, os.WriteFile(tr.bundlePath, append(before, b[:len(b)/2]...), 0o644))
			if restart {
				c = tr.installer()
			}
			require.NoError(t, c.install(t.Context(), b, tr.bundlePath))
			tr.requireBundle(t, b) // No background cleanup is needed to make init's trust usable.
		})
	}
}

func TestInstallCACerts_ReplacesPersistedFiles(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	w, r := newTestCA(t, "worker"), newTestCA(t, "root")
	require.NoError(t, os.WriteFile(filepath.Join(tr.certDir, legacyCACertFile), []byte(w), 0o644))
	userPath := filepath.Join(tr.certDir, "e2b-user.crt")
	require.NoError(t, os.WriteFile(userPath, []byte(tr.system[1]), 0o644))
	c := tr.installer()
	tr.install(t, c, r)
	require.NoFileExists(t, filepath.Join(tr.certDir, legacyCACertFile))
	data, err := os.ReadFile(userPath)
	require.NoError(t, err)
	assert.Equal(t, tr.system[1], string(data))
}

func TestInstallCACerts_RejectsInvalidBundle(t *testing.T) {
	t.Parallel()
	w := newTestCA(t, "worker")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	malformed := "-----BEGIN CERTIFICATE-----\n!!! invalid !!!\n-----END CERTIFICATE-----\n"

	for name, bundle := range map[string]string{
		"not a certificate":       "-----BEGIN CERTIFICATE-----\nMIIBcTCCARegAwIBAgIUTestCertA==\n-----END CERTIFICATE-----\n",
		"malformed block first":   malformed + w,
		"malformed block between": w + malformed + w,
		"unterminated block":      "-----BEGIN CERTIFICATE-----\nMIIB\n" + w,
		"text between blocks":     w + "junk\n" + w,
		"truncated":               w + w[:len(w)/2],
		"private key":             w + string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})),
		"no certificates":         "\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tr := newTestTrust(t)
			before, err := os.ReadFile(tr.bundlePath)
			require.NoError(t, err)

			require.Error(t, tr.installer().install(t.Context(), bundle, tr.bundlePath))

			after, err := os.ReadFile(tr.bundlePath)
			require.NoError(t, err)
			assert.Equal(t, string(before), string(after))
		})
	}
}

// Install through its background goroutine while readers load the bundle and
// resumes repeat the same set.
func TestInstallCACerts_ConcurrentInstallAndReaders(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); c.run(ctx, tr.bundlePath, tr.certDir) }()
	t.Cleanup(func() { cancel(); <-done })
	w, r0, r1 := newTestCA(t, "worker"), newTestCA(t, "added root 0"), newTestCA(t, "added root 1")

	var stop atomic.Bool
	var readers sync.WaitGroup
	t.Cleanup(func() { stop.Store(true); readers.Wait() })
	for range 4 {
		readers.Go(func() {
			for !stop.Load() {
				data, err := os.ReadFile(tr.bundlePath)
				if err != nil {
					t.Error(err)

					return
				}
				if end := bytes.LastIndex(data, []byte("-----END CERTIFICATE-----\n")); !assert.Empty(t, bytes.TrimSpace(data[end+26:]), "partial bundle observed") {
					return
				}
			}
		})
	}

	require.NoError(t, c.install(t.Context(), w+r0, tr.bundlePath))
	var resumes sync.WaitGroup
	for range 10 {
		resumes.Go(func() {
			assert.NoError(t, c.install(t.Context(), w+r1, tr.bundlePath))
		})
	}
	resumes.Wait()

	block, _ := pem.Decode([]byte(r1))
	require.Eventually(t, func() bool {
		if !c.mu.TryAcquire(1) {
			return false
		}
		defer c.mu.Release(1)

		data, err := os.ReadFile(filepath.Join(tr.certDir, fmt.Sprintf("e2b-%x.crt", sha256.Sum256(block.Bytes))))

		return !c.pending && err == nil && string(data) == r1
	}, 5*time.Second, time.Millisecond)
	stop.Store(true)
	readers.Wait()
	tr.requireTrust(t, w, r1)
}

// A canceled acquire while a previous install's cleanup still holds mu must
// return ErrCAInstallInProgress, not a raw context error.
func TestInstallCACerts_CanceledCtxUnderContention(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()

	// Simulate the background persist still holding mu.
	require.NoError(t, c.mu.Acquire(t.Context(), 1))
	defer c.mu.Release(1)

	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	err := c.install(canceledCtx, newTestCA(t, "worker"), tr.bundlePath)
	require.ErrorIs(t, err, ErrCAInstallInProgress)
	require.ErrorIs(t, err, context.Canceled)
}

func TestInstallCACerts_RetainsLegacyDirectoryTrust(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	w, r := newTestCA(t, "legacy CA"), newTestCA(t, "additional CA")
	legacy := filepath.Join(tr.certDir, legacyCACertFile)
	require.NoError(t, os.WriteFile(legacy, []byte(w), 0o644))
	pemLink := filepath.Join(filepath.Dir(tr.bundlePath), "e2b-ca.pem")
	require.NoError(t, os.Symlink(legacy, pemLink))
	hashLink := filepath.Join(filepath.Dir(tr.bundlePath), "subject-hash.0")
	require.NoError(t, os.Symlink("e2b-ca.pem", hashLink))
	c := tr.installer()
	tr.install(t, c, w, r)
	data, err := os.ReadFile(hashLink)
	require.NoError(t, err)
	assert.Equal(t, w, string(data))
	tr.requireBundle(t, w, r)
	tr.install(t, c, r)
	require.NoFileExists(t, legacy)
	tr.requireTrust(t, r)
}

func TestInstallCACerts_NormalizesPreexistingRequestedRoot(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b := newTestCA(t, "preexisting E2B root"), newTestCA(t, "injected root")
	// The supplied root is normalized; the unrelated customer root keeps its headers.
	initial := strings.TrimRight(reformat(a)+tr.system[0]+reformat(tr.system[1]), "\r\n")
	require.NoError(t, os.WriteFile(tr.bundlePath, []byte(initial), 0o644))
	block, _ := pem.Decode([]byte(a))
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	verify := func(trusted bool) {
		t.Helper()
		content, err := os.ReadFile(tr.bundlePath)
		require.NoError(t, err)
		roots := x509.NewCertPool()
		require.True(t, roots.AppendCertsFromPEM(content))
		_, err = cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
		if trusted {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	tr.update(t, c, a, b)
	verify(true) // The foreground append is already usable by Go clients.
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	verify(true)
	tr.requireTrust(t, a, b)
	content, err := os.ReadFile(tr.bundlePath)
	require.NoError(t, err)
	for len(content) > 0 {
		current, rest := pem.Decode(content)
		require.NotNil(t, current)
		if bytes.Equal(current.Bytes, block.Bytes) {
			require.Empty(t, current.Headers)
		} else {
			unrelated, _ := pem.Decode([]byte(tr.system[1]))
			if bytes.Equal(current.Bytes, unrelated.Bytes) {
				require.Equal(t, map[string]string{"Comment": "test certificate"}, current.Headers)
			}
		}
		content = rest
	}
	tr.install(t, c, b)
	verify(false) // Supplying A makes it E2B-owned, even when it already existed.
	tr.requireTrust(t, b)
}

func TestInstallCACerts_RestartRetiresOwnedRoots(t *testing.T) {
	t.Parallel()
	for _, persisted := range []bool{false, true} {
		for _, reinitialize := range []bool{false, true} {
			t.Run(fmt.Sprintf("persisted=%t/reinitialize=%t", persisted, reinitialize), func(t *testing.T) {
				t.Parallel()
				tr := newTestTrust(t)
				a, b := newTestCA(t, "A"), newTestCA(t, "B")
				c := tr.installer()
				if persisted {
					tr.install(t, c, a)
				} else {
					tr.update(t, c, a)
				}

				c = tr.installer()
				if reinitialize {
					tr.install(t, c, a)
				}
				tr.install(t, c, b)
				tr.requireTrust(t, b) // A retires, and unrelated system/customer roots survive.
			})
		}
	}
}

func TestInstallCACerts_RestartBetweenOwnershipAndBundleWrites(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	a, b := newTestCA(t, "A"), newTestCA(t, "B")
	before, err := os.ReadFile(tr.bundlePath)
	require.NoError(t, err)
	c := tr.installer()
	tr.update(t, c, a)
	// Reproduce a crash after recording ownership but before publishing the bundle.
	require.NoError(t, os.WriteFile(tr.bundlePath, before, 0o644))
	c = tr.installer()
	tr.install(t, c, a)
	tr.install(t, c, b)
	tr.requireTrust(t, b)

	tr.update(t, c, a)
	owned, err := os.ReadFile(c.statePath)
	require.NoError(t, err)
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	// Reproduce a crash after cleanup but before retiring the ownership record.
	require.NoError(t, os.WriteFile(c.statePath, owned, 0o644))
	tr.install(t, tr.installer(), b)
	tr.requireTrust(t, b)
}

func TestInstallCACerts_OwnershipFailureDoesNotPublish(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	before, err := os.ReadFile(tr.bundlePath)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(c.statePath, 0o755))
	certs, err := cabundle.Parse(newTestCA(t, "A"))
	require.NoError(t, err)
	err = c.updateBundle(t.Context(), certs, tr.bundlePath)
	require.ErrorContains(t, err, "read CA state")
	after, err := os.ReadFile(tr.bundlePath)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

// A new process must finish persisted intent without receiving another init.
func TestInstallCACerts_StartupRecovery(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"before-append", "partial-append", "after-append", "after-cleanup"} {
		t.Run(boundary, func(t *testing.T) {
			t.Parallel()
			tr := newTestTrust(t)
			a, b := newTestCA(t, "A"), newTestCA(t, "B")
			c := tr.installer()
			tr.install(t, c, a)
			before, err := os.ReadFile(tr.bundlePath)
			require.NoError(t, err)
			tr.update(t, c, b)
			state, err := os.ReadFile(c.statePath)
			require.NoError(t, err)
			switch boundary {
			case "before-append":
				require.NoError(t, os.WriteFile(tr.bundlePath, before, 0o644))
			case "partial-append":
				require.NoError(t, os.WriteFile(tr.bundlePath, append(before, b[:len(b)/2]...), 0o644))
			case "after-cleanup":
				require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
				require.NoError(t, os.WriteFile(c.statePath, state, 0o600))
			}
			restarted := tr.installer()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() { defer close(done); restarted.run(ctx, tr.bundlePath, tr.certDir) }()
			t.Cleanup(func() { cancel(); <-done })
			require.Eventually(t, func() bool {
				if !restarted.mu.TryAcquire(1) {
					return false
				}
				defer restarted.mu.Release(1)

				return restarted.loaded && !restarted.pending
			}, time.Second, time.Millisecond)
			tr.requireTrust(t, b)
			saved, err := os.ReadFile(c.statePath)
			require.NoError(t, err)
			var final caState
			require.NoError(t, json.Unmarshal(saved, &final))
			parsed, err := cabundle.Parse(b)
			require.NoError(t, err)
			require.Equal(t, []string{parsed[0].Fingerprint}, final.Owned)
		})
	}
}
