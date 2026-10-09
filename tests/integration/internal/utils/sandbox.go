package utils

import (
	"context"
	"maps"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

// MaxConcurrentSandboxes gates parallel sandbox creation in tests. The
// integration test team is granted extra capacity in seed.go (base tier 20 +
// addon 200 = 220), so this can stay well below that ceiling.
const MaxConcurrentSandboxes = 100

// sandboxSemaphore gates sandbox creation so parallel tests don't exceed the
// tier's concurrent instance limit. A slot is acquired before creation and
// released after teardown in t.Cleanup.
var sandboxSemaphore = make(chan struct{}, MaxConcurrentSandboxes)

// AcquireSandboxSlot blocks until a sandbox slot is available and
// returns an idempotent release function. The slot is also released
// automatically via t.Cleanup, so callers that tie the sandbox
// lifetime to the test can simply ignore the return value. Callers
// that tear down sandboxes inline (loops, sequential creates) can
// call the returned function early to free the slot sooner.
func AcquireSandboxSlot(t *testing.T) func() {
	t.Helper()
	sandboxSemaphore <- struct{}{}
	release := sync.OnceFunc(func() { <-sandboxSemaphore })
	t.Cleanup(release)

	return release
}

type SandboxConfig struct {
	templateID          string
	metadata            api.SandboxMetadata
	timeout             int32
	autoPause           bool
	autoPauseMemory     *bool
	autoResume          *api.SandboxAutoResumeConfig
	network             *api.SandboxNetworkConfig
	allowInternetAccess *bool
}

type SandboxOption func(config *SandboxConfig)

func WithMetadata(metadata api.SandboxMetadata) SandboxOption {
	return func(config *SandboxConfig) {
		maps.Copy(config.metadata, metadata)
	}
}

func WithoutAnyMetadata() SandboxOption {
	return func(config *SandboxConfig) {
		config.metadata = make(map[string]string)
	}
}

func WithTimeout(timeout int32) SandboxOption {
	return func(config *SandboxConfig) {
		config.timeout = timeout
	}
}

func WithAutoPause(autoPause bool) SandboxOption {
	return func(config *SandboxConfig) {
		config.autoPause = autoPause
	}
}

// WithAutoPauseMemory controls the snapshot kind taken when the sandbox
// auto-pauses on timeout. false requests a filesystem-only auto-pause.
func WithAutoPauseMemory(memory bool) SandboxOption {
	return func(config *SandboxConfig) {
		config.autoPauseMemory = &memory
	}
}

func WithAutoResume(enabled bool) SandboxOption {
	return func(config *SandboxConfig) {
		config.autoResume = &api.SandboxAutoResumeConfig{Enabled: enabled}
	}
}

func WithNetwork(network *api.SandboxNetworkConfig) SandboxOption {
	return func(config *SandboxConfig) {
		config.network = network
	}
}

func WithAllowInternetAccess(allow bool) SandboxOption {
	return func(config *SandboxConfig) {
		config.allowInternetAccess = &allow
	}
}

func WithTemplateID(templateID string) SandboxOption {
	return func(config *SandboxConfig) {
		config.templateID = templateID
	}
}

// SetupSandboxWithCleanup creates a new sandbox and returns its data.
// It acquires a semaphore slot to stay within the tier's concurrent instance
// limit; the slot is released after the sandbox is torn down in t.Cleanup.
func SetupSandboxWithCleanup(t *testing.T, c *api.ClientWithResponses, options ...SandboxOption) *api.Sandbox {
	t.Helper()

	release := AcquireSandboxSlot(t)

	// t.Context() doesn't work with go vet, so we use our own context
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	config := SandboxConfig{
		timeout: 30, // default timeout
		metadata: api.SandboxMetadata{
			"sandboxType": "test",
		},
	}

	for _, option := range options {
		option(&config)
	}

	templateID := config.templateID
	if templateID == "" {
		templateID = setup.SandboxTemplateID
	}

	for range 10 { // retry up to 10 times, but only in case of 429
		createSandboxResponse, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
			TemplateID:          templateID,
			Timeout:             &config.timeout,
			Metadata:            &config.metadata,
			AutoPause:           &config.autoPause,
			AutoPauseMemory:     config.autoPauseMemory,
			AutoResume:          config.autoResume,
			Network:             config.network,
			AllowInternetAccess: config.allowInternetAccess,
		}, setup.WithAPIKey())
		require.NoError(t, err)

		if createSandboxResponse.StatusCode() == http.StatusTooManyRequests {
			t.Logf("Sandbox creation failed with status code %d, retrying...", createSandboxResponse.StatusCode())
			time.Sleep(time.Second * 5)

			continue
		}

		if createSandboxResponse.StatusCode() != http.StatusCreated {
			t.Logf("Sandbox creation failed status=%d body=%s", createSandboxResponse.StatusCode(), string(createSandboxResponse.Body))
			t.Logf("Sandbox creation=%+v", *createSandboxResponse)
		}

		require.Equal(t, http.StatusCreated, createSandboxResponse.StatusCode())
		sbx := createSandboxResponse.JSON201
		require.NotNil(t, sbx)

		t.Cleanup(func() {
			TeardownSandbox(t, c, sbx.SandboxID)
			release()
		})

		return sbx
	}

	// Release the slot since we never created a sandbox.
	release()

	t.Logf("Sandbox creation failed after 10 retries")
	t.FailNow()

	return nil
}

// TeardownSandbox kills the sandbox with the given ID
func TeardownSandbox(t *testing.T, c *api.ClientWithResponses, sandboxID string) {
	t.Helper()

	ctx := context.WithoutCancel(t.Context())

	killSandboxResponse, err := c.DeleteSandboxesSandboxIDWithResponse(ctx, sandboxID, setup.WithAPIKey())
	require.NoError(t, err)

	assert.Contains(t, []int{http.StatusNoContent, http.StatusNotFound}, killSandboxResponse.StatusCode())
}
