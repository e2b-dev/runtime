// Package testutils holds helpers shared by the orchestrator's tests.
package testutils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// SocketDir returns an empty directory for Unix sockets, removed when the test
// ends. t.TempDir names its directory after the test, so a socket path inside
// it can exceed the 108-byte sun_path limit and fail to bind.
func SocketDir(tb testing.TB) string {
	tb.Helper()

	dir, err := os.MkdirTemp("", "sock") //nolint:usetesting // t.TempDir's test-name prefix can overflow sun_path
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = os.RemoveAll(dir) })

	return dir
}
