//go:build linux

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPUTemplateOverride(t *testing.T) {
	t.Parallel()

	for raw, want := range map[string]string{
		`{"kvm_capabilities":["!122"]}`: `{"template":{"kvm_capabilities":["!122"]}}`,
		`{}`:                            `{"template":{}}`,
		"null\n":                        `{"template":{}}`,
		"":                              `{"template":{}}`,
	} {
		got, err := cpuTemplateOverride([]byte(raw))
		require.NoError(t, err, raw)
		assert.JSONEq(t, want, got.JSONString(), raw)
	}

	_, err := cpuTemplateOverride([]byte(`{"kvm_capabilities":`))
	require.Error(t, err)
}
