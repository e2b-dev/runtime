//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeTemplateEnvVars(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		tmpl map[string]string
		user map[string]string
		want map[string]string
	}{
		"no template, no user":    {nil, nil, nil},
		"no template, user only":  {nil, map[string]string{"A": "1"}, map[string]string{"A": "1"}},
		"template only":           {map[string]string{"PYTHONPATH": "/lib"}, nil, map[string]string{"PYTHONPATH": "/lib"}},
		"disjoint merged":         {map[string]string{"P": "/lib"}, map[string]string{"K": "v"}, map[string]string{"P": "/lib", "K": "v"}},
		"user overrides template": {map[string]string{"L": "info", "P": "/lib"}, map[string]string{"L": "debug"}, map[string]string{"L": "debug", "P": "/lib"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, mergeTemplateEnvVars(tc.tmpl, tc.user))
		})
	}
}

// the template map must not be mutated (it is shared template metadata).
func TestMergeTemplateEnvVars_DoesNotMutateTemplate(t *testing.T) {
	t.Parallel()
	tmpl := map[string]string{"P": "/lib"}
	_ = mergeTemplateEnvVars(tmpl, map[string]string{"P": "/override", "K": "v"})
	assert.Equal(t, map[string]string{"P": "/lib"}, tmpl, "template metadata map must be left unchanged")
}
