package analyticscollector

import (
	"net/http"
	"testing"

	"github.com/posthog/posthog-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationFromUserAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		userAgent   string
		wantName    string
		wantVersion string
		wantOK      bool
	}{
		{
			name:        "CLI via JS SDK",
			userAgent:   "e2b-js-sdk/1.2.3 e2b-cli/1.0.5",
			wantName:    "e2b-cli",
			wantVersion: "1.0.5",
			wantOK:      true,
		},
		{
			name:        "CLI with command attribution picks the integration",
			userAgent:   "e2b-js-sdk/1.2.3 e2b-cli/1.0.5 e2b-cli-command/sandbox.list",
			wantName:    "e2b-cli",
			wantVersion: "1.0.5",
			wantOK:      true,
		},
		{
			name:        "code interpreter via Python SDK",
			userAgent:   "e2b-python-sdk/2.0.0 e2b-code-interpreter/0.1.0",
			wantName:    "e2b-code-interpreter",
			wantVersion: "0.1.0",
			wantOK:      true,
		},
		{
			name:      "plain SDK without integration",
			userAgent: "e2b-js-sdk/1.2.3",
			wantOK:    false,
		},
		{
			name:      "browser user agent is not an integration",
			userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
			wantOK:    false,
		},
		{
			name:      "empty user agent",
			userAgent: "",
			wantOK:    false,
		},
		{
			name:      "token without version is skipped",
			userAgent: "e2b-js-sdk/1.2.3 e2b-cli/",
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, version, ok := integrationFromUserAgent(tt.userAgent)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantName, name)
			assert.Equal(t, tt.wantVersion, version)
		})
	}
}

func TestGetPackageToPosthogPropertiesUserAgent(t *testing.T) {
	t.Parallel()

	p := &PosthogClient{}

	t.Run("integration traffic", func(t *testing.T) {
		t.Parallel()

		header := http.Header{}
		header.Set("User-Agent", "e2b-js-sdk/1.2.3 e2b-cli/1.0.5")

		properties := p.GetPackageToPosthogProperties(&header)
		assert.Equal(t, "e2b-js-sdk/1.2.3 e2b-cli/1.0.5", properties["user_agent"])
		assert.Equal(t, "e2b-cli", properties["integration"])
		assert.Equal(t, "1.0.5", properties["integration_version"])
	})

	t.Run("no user agent", func(t *testing.T) {
		t.Parallel()

		header := http.Header{}

		properties := p.GetPackageToPosthogProperties(&header)
		assert.NotContains(t, properties, "user_agent")
		assert.NotContains(t, properties, "integration")
		assert.NotContains(t, properties, "integration_version")
	})

	t.Run("plain SDK traffic has no integration", func(t *testing.T) {
		t.Parallel()

		header := http.Header{}
		header.Set("User-Agent", "e2b-python-sdk/2.0.0")

		properties := p.GetPackageToPosthogProperties(&header)
		assert.Equal(t, "e2b-python-sdk/2.0.0", properties["user_agent"])
		assert.NotContains(t, properties, "integration")
		assert.NotContains(t, properties, "integration_version")
	})
}

// embeds posthog.Client so unimplemented methods are never reached
type captureRecorder struct {
	posthog.Client

	messages []posthog.Message
}

func (c *captureRecorder) Enqueue(msg posthog.Message) error {
	c.messages = append(c.messages, msg)

	return nil
}

func (c *captureRecorder) capture(t *testing.T) posthog.Capture {
	t.Helper()

	require.Len(t, c.messages, 1)
	capture, ok := c.messages[0].(posthog.Capture)
	require.True(t, ok)

	return capture
}

func TestTeamEventCarriesTeamID(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := &PosthogClient{client: recorder}

	p.CreateAnalyticsTeamEvent(t.Context(), "team-uuid", "created_instance", posthog.NewProperties().Set("environment", "tpl"))

	capture := recorder.capture(t)
	assert.Equal(t, "team-uuid", capture.Properties[teamIDKey])
	assert.Equal(t, "team-uuid", capture.Groups[teamGroup])
	assert.Equal(t, "tpl", capture.Properties["environment"])
	assert.Equal(t, infraVersion, capture.Properties[infraVersionKey])
}
