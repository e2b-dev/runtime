//go:build unix

package host

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/cabundle"
)

func TestLazyCleanupSerializesOverlappingUpdates(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b, d := newTestCA(t, "A"), newTestCA(t, "B"), newTestCA(t, "D")
	tr.update(t, c, a)
	tr.update(t, c, a, b)
	tr.update(t, c, b, d)
	data, err := os.ReadFile(tr.bundlePath)
	require.NoError(t, err)
	// No background job has run: new roots are usable and retired A still exists.
	require.Contains(t, string(data), a)
	require.Contains(t, string(data), b)
	require.Contains(t, string(data), d)
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireTrust(t, b, d)
	tr.install(t, c, d)
	tr.requireTrust(t, d) // A/B retire, unrelated system roots stay.
}

func TestLazyCleanupKeepsRootAddedExternallyBetweenAppends(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b, u := newTestCA(t, "A"), newTestCA(t, "B"), newTestCA(t, "external")
	tr.update(t, c, a)
	f, err := os.OpenFile(tr.bundlePath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString(u)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	tr.system = append(tr.system, u)
	tr.update(t, c, b)
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.install(t, c, b)
	tr.requireTrust(t, b)
}

func TestLazyCleanupFailureCanRetryWithoutLosingOwnership(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b := newTestCA(t, "A"), newTestCA(t, "B")
	tr.update(t, c, a)
	tr.update(t, c, b)
	require.NoError(t, os.Rename(tr.bundlePath, tr.bundlePath+".saved"))
	require.Error(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	require.NoError(t, os.Rename(tr.bundlePath+".saved", tr.bundlePath))
	tr.update(t, c, b)
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireTrust(t, b)
}

func TestLazyCleanupRetiresAllPreexistingCopiesOfSuppliedRoot(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b := newTestCA(t, "A"), newTestCA(t, "B")
	require.NoError(t, os.WriteFile(tr.bundlePath, []byte(strings.Join(tr.system, "")+reformat(a)+a), 0o644))
	tr.update(t, c, a)
	tr.update(t, c, b)
	require.NoError(t, c.persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireTrust(t, b)
}

func TestLazyCleanupRetriesIntentAfterPublicationFailure(t *testing.T) {
	t.Parallel()
	tr := newTestTrust(t)
	c := tr.installer()
	a, b := newTestCA(t, "A"), newTestCA(t, "B")
	tr.update(t, c, a)
	require.NoError(t, os.Rename(tr.bundlePath, tr.bundlePath+".saved"))
	parsed, err := cabundle.Parse(b)
	require.NoError(t, err)
	err = c.updateBundle(t.Context(), parsed, tr.bundlePath)
	require.Error(t, err)
	require.NoError(t, os.Rename(tr.bundlePath+".saved", tr.bundlePath))
	require.NoError(t, tr.installer().persist(t.Context(), tr.bundlePath, tr.certDir))
	tr.requireTrust(t, b)
}
