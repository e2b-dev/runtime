//go:build linux

package template

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

// A reboot masks the metadata with what it applied, so the next pause stores that.
func TestMaskTemplateWithMetadata(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{Template: metadata.TemplateMetadata{BuildID: "build-1"}}
	masked := NewMaskTemplate(nil, WithMetadata(meta))

	got, err := masked.Metadata()
	require.NoError(t, err)
	assert.Equal(t, meta, got)
}
