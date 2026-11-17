package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:paralleltest // uses t.Setenv, which is incompatible with t.Parallel
func TestProxyPort(t *testing.T) {
	t.Setenv("PROXY_PORT", "")
	require.Equal(t, uint16(5007), proxyPort(), "unset")

	t.Setenv("PROXY_PORT", "6007")
	require.Equal(t, uint16(6007), proxyPort(), "valid override")

	t.Setenv("PROXY_PORT", "not-a-port")
	require.Equal(t, uint16(5007), proxyPort(), "invalid value falls back to the default")
}
