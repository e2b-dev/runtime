package resumebuild

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSkipEnvdWaitConflicts(t *testing.T) {
	t.Parallel()

	require.NoError(t, skipEnvdWaitConflicts(false, true, true, true), "without the skip nothing is refused")
	require.NoError(t, skipEnvdWaitConflicts(true, false, false, false), "a bare resume or cold boot may skip the wait")
	require.Error(t, skipEnvdWaitConflicts(true, true, false, false), "-cmd needs envd")
	require.Error(t, skipEnvdWaitConflicts(true, false, true, false), "a pause needs envd")
	require.Error(t, skipEnvdWaitConflicts(true, false, false, true), "-shell needs envd")
}
