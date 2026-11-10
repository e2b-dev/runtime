package sandbox

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// The execution span is a new trace root, so its prefetch attributes can only
// be joined back to a create or resume through the ids it carries itself.
func TestStartExecutionSpan_CarriesSandboxIdentity(t *testing.T) {
	t.Parallel()

	runtime := sandboxtypes.RuntimeMetadata{
		SandboxID:  "sbx-exec-span",
		TemplateID: "tpl-exec-span",
		BuildID:    "build-exec-span",
	}

	_, span := startExecutionSpan(t.Context(), runtime)
	span.End()

	got := map[attribute.Key]string{}
	for _, sp := range testSpanRecorder.Ended() {
		if sp.Name() != "execute sandbox" {
			continue
		}
		for _, kv := range sp.Attributes() {
			if kv.Key == "sandbox.id" && kv.Value.AsString() != runtime.SandboxID {
				continue
			}
			got[kv.Key] = kv.Value.AsString()
		}
		if got["sandbox.id"] == runtime.SandboxID {
			break
		}
	}

	require.Equal(t, runtime.SandboxID, got["sandbox.id"])
	require.Equal(t, runtime.TemplateID, got["template.id"])
	require.Equal(t, runtime.BuildID, got["build.id"])
}
