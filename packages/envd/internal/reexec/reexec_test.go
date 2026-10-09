package reexec_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/reexec"
)

// record writes its second argument to the file named by its first, or fails on "fail".
var record = reexec.Define("reexec-test-record", func(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: PATH VALUE")
	}
	if args[1] == "fail" {
		return errors.New("asked to fail")
	}

	return os.WriteFile(args[0], []byte(args[1]), 0o644)
})

// The test binary stands in for envd, so it dispatches helpers the same way. A child
// that Main lets through would run this whole suite again, children included; with
// noFallthrough set it exits instead, so a broken Main fails a test rather than forking
// without bound.
const noFallthrough = "REEXEC_TEST_NO_FALLTHROUGH"

func init() {
	reexec.Main()
	if os.Getenv(noFallthrough) != "" {
		fmt.Fprintln(os.Stderr, "Main fell through to the test binary's main")
		os.Exit(2)
	}
}

func TestExecRunsTheHelperInAChild(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, record.Exec(path, "hello").Run())

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(b))
}

func TestExecMarksTheChildAsAHelper(t *testing.T) {
	t.Parallel()

	assert.Contains(t, record.Exec("x", "y").Env, "E2B_REEXEC_HELPER=reexec-test-record")
}

func TestHelperErrorFailsTheChildWithTheMessage(t *testing.T) {
	t.Parallel()

	var stderr strings.Builder
	cmd := record.Exec("/dev/null", "fail")
	cmd.Stderr = &stderr

	var exitErr *exec.ExitError
	require.ErrorAs(t, cmd.Run(), &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.Equal(t, "asked to fail\n", stderr.String())
}

// A child Exec started must never fall through to the program's own main, which could
// call Exec again.
func TestMarkedChildWithoutItsHelperExitsInstead(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(t.Context(), os.Args[0], "no-such-helper")
	cmd.Env = append(os.Environ(), "E2B_REEXEC_HELPER=no-such-helper", noFallthrough+"=1")
	var stderr strings.Builder
	cmd.Stderr = &stderr

	var exitErr *exec.ExitError
	require.ErrorAs(t, cmd.Run(), &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.Contains(t, stderr.String(), `no such helper`)
}

func TestDefineRejectsNamesThatCannotDispatch(t *testing.T) {
	t.Parallel()

	noop := func([]string) error { return nil }
	assert.Panics(t, func() { reexec.Define("", noop) })
	assert.Panics(t, func() { reexec.Define("-version", noop) })
	assert.Panics(t, func() { reexec.Define("reexec-test-record", noop) }, "a second definition would silently shadow the first")
}
