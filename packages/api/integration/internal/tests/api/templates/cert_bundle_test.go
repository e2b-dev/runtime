package api_templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
)

func TestTemplateBuildRUNCertPackFailure(t *testing.T) {
	t.Parallel()

	var messages []string
	outcome := runTemplateBuild(t, "test-cert-pack-failure", api.TemplateBuildStartV2{
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{{
			Type:  "RUN",
			Force: new(true),
			// The RUN succeeds, but tar cannot replace a directory with its archive.
			Args: new([]string{"rm -f /usr/local/share/e2b/ssl-certs.tar && mkdir -p /usr/local/share/e2b/ssl-certs.tar", "root"}),
		}}),
	}, func(alias string, entry api.BuildLogEntry) {
		messages = append(messages, entry.Message)
		defaultBuildLogHandler(t)(alias, entry)
	})

	require.False(t, outcome.ready)
	require.NotNil(t, outcome.step)
	require.Equal(t, "1", *outcome.step, "pack errors must retain their build step")
	require.Contains(t, outcome.reason, "error packing CA cert bundle")
	// Status is polled at Info level: users must see tar's cause without debug logs.
	logs := strings.Join(messages, "\n")
	require.Contains(t, logs, "[certs] [stderr]")
	require.Contains(t, logs, "Is a directory")
}
