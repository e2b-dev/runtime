package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvVars_StoreUserRejectsInternal(t *testing.T) {
	t.Parallel()
	e := NewEnvVars()

	e.Store("E2B_SANDBOX_ID", "sbx-1")
	assert.False(t, e.StoreUser("E2B_SANDBOX_ID", "spoofed"))

	v, _ := e.Load("E2B_SANDBOX_ID")
	assert.Equal(t, "sbx-1", v)
}

func TestEnvVars_StorePromotesUserToInternal(t *testing.T) {
	t.Parallel()
	e := NewEnvVars()

	require.True(t, e.StoreUser("FOO", "user"))
	e.Store("FOO", "internal")

	assert.False(t, e.StoreUser("FOO", "user-again"))
	v, _ := e.Load("FOO")
	assert.Equal(t, "internal", v)
}

func TestEnvVars_ReplaceUserVars(t *testing.T) {
	t.Parallel()
	e := NewEnvVars()

	e.Store("E2B_SANDBOX", "true")
	e.Store("E2B_SANDBOX_ID", "sbx-1")
	require.True(t, e.StoreUser("FOO", "1"))
	require.True(t, e.StoreUser("BAR", "2"))

	e.ReplaceUserVars(map[string]string{
		"BAR":            "two",
		"BAZ":            "three",
		"E2B_SANDBOX_ID": "spoofed",
	})

	assert.Equal(t, map[string]string{
		"E2B_SANDBOX":    "true",
		"E2B_SANDBOX_ID": "sbx-1",
		"BAR":            "two",
		"BAZ":            "three",
	}, e.All())

	assert.True(t, e.StoreUser("BAZ", "four"))
	v, _ := e.Load("BAZ")
	assert.Equal(t, "four", v)

	assert.False(t, e.StoreUser("E2B_SANDBOX_ID", "spoofed-again"))
}

func TestEnvVars_ReplaceUserVarsClearsOmittedKeys(t *testing.T) {
	t.Parallel()
	e := NewEnvVars()

	e.Store("E2B_SANDBOX", "true")
	require.True(t, e.StoreUser("FOO", "1"))
	require.True(t, e.StoreUser("BAR", "2"))

	e.ReplaceUserVars(map[string]string{"FOO": "1"})

	assert.Equal(t, map[string]string{
		"E2B_SANDBOX": "true",
		"FOO":         "1",
	}, e.All())
}

func TestEnvVars_UserVars(t *testing.T) {
	t.Parallel()
	e := NewEnvVars()

	e.Store("E2B_SANDBOX", "true")
	require.True(t, e.StoreUser("FOO", "1"))
	require.True(t, e.StoreUser("EMPTY", ""))

	got := e.UserVars()
	assert.Equal(t, map[string]string{"FOO": "1", "EMPTY": ""}, got)

	got["FOO"] = "changed"
	v, _ := e.Load("FOO")
	assert.Equal(t, "1", v, "the result is a copy")
}
