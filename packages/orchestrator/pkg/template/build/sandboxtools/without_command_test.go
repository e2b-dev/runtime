package sandboxtools

import (
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithoutCommandMasksEnvdsText(t *testing.T) {
	t.Parallel()

	const command = `git clone https://x-access-token:secret-token@github.com/org/repo`
	// envd names the whole command line, on Start or on the stream.
	fromEnvd := connect.NewWireError(connect.CodeResourceExhausted,
		errors.New("error starting process '/bin/bash -l -c "+command+"': fork: resource temporarily unavailable"))
	err := withoutCommand(fromEnvd, command)
	assert.NotContains(t, err.Error(), "secret-token")
	assert.Equal(t, "resource_exhausted: error starting process '/bin/bash -l -c <command>': fork: resource temporarily unavailable", err.Error())

	other := errors.New("dial tcp: connection refused")
	require.ErrorIs(t, withoutCommand(other, command), other)
	require.ErrorIs(t, withoutCommand(fromEnvd, ""), fromEnvd)
}
