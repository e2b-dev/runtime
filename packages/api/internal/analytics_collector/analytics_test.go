package analyticscollector

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

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
	// enqueueErr is returned by the next Enqueue instead of recording it.
	enqueueErr error
	// enqueueDelay holds every Enqueue open, so concurrent callers overlap.
	enqueueDelay time.Duration
	// onEnqueue runs inside Enqueue before the message is recorded, standing
	// in for work that overlaps the enqueue.
	onEnqueue func(posthog.Message)
}

func (c *captureRecorder) Enqueue(msg posthog.Message) error {
	if c.enqueueErr != nil {
		err := c.enqueueErr
		c.enqueueErr = nil

		return err
	}

	time.Sleep(c.enqueueDelay)

	if c.onEnqueue != nil {
		c.onEnqueue(msg)
	}

	c.messages = append(c.messages, msg)

	return nil
}

func (c *captureRecorder) Close() error {
	return nil
}

func (c *captureRecorder) capture(t *testing.T) posthog.Capture {
	t.Helper()

	require.Len(t, c.messages, 1)
	capture, ok := c.messages[0].(posthog.Capture)
	require.True(t, ok)

	return capture
}

// newTestPosthogClient wraps a recording client and closes it when the test
// ends.
func newTestPosthogClient(t *testing.T, client posthog.Client, identifyTTL time.Duration) *PosthogClient {
	t.Helper()

	p := newPosthogClient(client, identifyTTL)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	return p
}

func groupIdentify(t *testing.T, msg posthog.Message) posthog.GroupIdentify {
	t.Helper()

	identify, ok := msg.(posthog.GroupIdentify)
	require.True(t, ok)

	return identify
}

func TestTeamEventCarriesTeamID(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)

	p.CreateAnalyticsTeamEvent(t.Context(), "team-uuid", "created_instance", posthog.NewProperties().Set("environment", "tpl"))

	capture := recorder.capture(t)
	assert.Equal(t, "team-uuid", capture.Properties[teamIDKey])
	assert.Equal(t, "team-uuid", capture.Groups[teamGroup])
	assert.Equal(t, "tpl", capture.Properties["environment"])
	assert.Equal(t, infraVersion, capture.Properties[infraVersionKey])
}

func TestIdentifyAnalyticsTeamSendsOncePerTeam(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	p.IdentifyAnalyticsTeam(t.Context(), "team-b", "Team B")
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")

	require.Len(t, recorder.messages, 2)

	first := groupIdentify(t, recorder.messages[0])
	assert.Equal(t, teamGroup, first.Type)
	assert.Equal(t, "team-a", first.Key)
	// The whole property set is pinned: a property that varies per team must
	// also join the cached value the dedupe compares.
	assert.Equal(t, posthog.Properties{placeholderProperty: true, "name": "Team A"}, first.Properties)

	assert.Equal(t, "team-b", groupIdentify(t, recorder.messages[1]).Key)
}

func TestIdentifyAnalyticsTeamResendsOnRename(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Old name")
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "New name")
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "New name")

	require.Len(t, recorder.messages, 2)
	assert.Equal(t, "Old name", groupIdentify(t, recorder.messages[0]).Properties["name"])
	assert.Equal(t, "New name", groupIdentify(t, recorder.messages[1]).Properties["name"])
}

func TestIdentifyAnalyticsTeamRefreshesActiveTeamAfterTTL(t *testing.T) {
	t.Parallel()

	const ttl = 20 * time.Millisecond

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, ttl)

	// Keep the team active across several TTLs. Hits must not extend the
	// entry, so the identify goes out again once a TTL has lapsed.
	start := time.Now()
	for time.Since(start) < 3*ttl {
		p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
		time.Sleep(ttl / 5)
	}

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")

	assert.GreaterOrEqual(t, len(recorder.messages), 2)
}

func TestIdentifyAnalyticsTeamRetriesAfterEnqueueError(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{enqueueErr: errors.New("posthog unavailable")}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	require.Empty(t, recorder.messages)

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	require.Len(t, recorder.messages, 1)
}

func TestIdentifyAnalyticsTeamResendsAfterFailedUpload(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)
	callback := identifyFailures{identifiedTeams: p.identifiedTeams}

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	require.Len(t, recorder.messages, 1)

	// A failed capture upload does not forget the team.
	capture := posthog.Capture{DistinctId: placeholderTeamGroupUser, Event: "created_instance"}
	callback.Failure(capture.APIfy(), errors.New("upload failed"))
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	require.Len(t, recorder.messages, 1)

	// A failed identify upload does.
	callback.Failure(groupIdentify(t, recorder.messages[0]).APIfy(), errors.New("upload failed"))
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	require.Len(t, recorder.messages, 2)
}

func TestIdentifyAnalyticsTeamRecordsBeforeUploadFailureCallback(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)
	callback := identifyFailures{identifiedTeams: p.identifiedTeams}

	// posthog-go can report the upload failure from its own goroutine as soon
	// as Enqueue hands the message off. The entry must already be recorded so
	// that Delete is not overtaken by a later Set.
	recorder.onEnqueue = func(msg posthog.Message) {
		callback.Failure(groupIdentify(t, msg).APIfy(), errors.New("upload failed"))
	}

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
	recorder.onEnqueue = nil
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")

	require.Len(t, recorder.messages, 2)
}

func TestIdentifyAnalyticsTeamCachedNameMatchesLastEnqueued(t *testing.T) {
	t.Parallel()

	recorder := &captureRecorder{}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)

	// A rename lands while the identify for the old name is still being
	// enqueued. The overlapping caller must not write the new name to the
	// cache ahead of the old identify, or PostHog would keep the old name
	// until the TTL lapses while the cache claims the new one.
	recorder.onEnqueue = func(posthog.Message) {
		recorder.onEnqueue = nil
		p.IdentifyAnalyticsTeam(t.Context(), "team-a", "New name")
	}

	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Old name")
	p.IdentifyAnalyticsTeam(t.Context(), "team-a", "New name")

	last := groupIdentify(t, recorder.messages[len(recorder.messages)-1])
	assert.Equal(t, "New name", last.Properties["name"])
}

func TestIdentifyAnalyticsTeamSendsOnceUnderConcurrentMisses(t *testing.T) {
	t.Parallel()

	// Every goroutine misses the fast path while the first enqueue is still
	// open; only the caller holding the in-flight marker may send.
	recorder := &captureRecorder{enqueueDelay: 20 * time.Millisecond}
	p := newTestPosthogClient(t, recorder, teamIdentifyTTL)

	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			p.IdentifyAnalyticsTeam(t.Context(), "team-a", "Team A")
		})
	}

	wg.Wait()

	require.Len(t, recorder.messages, 1)
}
