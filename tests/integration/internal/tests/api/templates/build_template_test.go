package api_templates

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

const (
	EnableDebugLogs = false

	ForceBaseBuild = false
	BuildTimeout   = 5 * time.Minute
)

type BuildLogHandler func(alias string, entry api.BuildLogEntry)

// PreBuildFunc is called after the template build is requested but before it is started.
// It receives the templateID, allowing callers to upload files or perform other setup.
type PreBuildFunc func(tb testing.TB, ctx context.Context, templateID string)

// buildOutcome is how a build ended: ready, or failed with the reason the API
// reported for it.
type buildOutcome struct {
	ready  bool
	reason string
}

func buildTemplate(
	tb testing.TB,
	templateName string,
	data api.TemplateBuildStartV2,
	logHandler BuildLogHandler,
	preBuildFns ...PreBuildFunc,
) bool {
	tb.Helper()

	outcome := runTemplateBuild(tb, templateName, data, logHandler, preBuildFns...)
	if !outcome.ready {
		tb.Fatalf("Build failed: %v", outcome.reason)

		return false
	}

	return true
}

// runTemplateBuild requests, starts and follows a build, reporting how it ended
// instead of failing the test — so a case can assert on a build that must fail.
func runTemplateBuild(
	tb testing.TB,
	templateName string,
	data api.TemplateBuildStartV2,
	logHandler BuildLogHandler,
	preBuildFns ...PreBuildFunc,
) buildOutcome {
	tb.Helper()

	ctx, cancel := context.WithTimeout(tb.Context(), BuildTimeout)
	defer cancel()

	c := setup.GetAPIClient()

	// Request build
	resp, err := c.PostV3TemplatesWithResponse(ctx, api.TemplateBuildRequestV3{
		Name:     new(templateName),
		CpuCount: new(api.CPUCount(2)),
		MemoryMB: new(api.MemoryMB(1024)),
	}, setup.WithAPIKey(), setup.WithTestsUserAgent())
	require.NoError(tb, err)
	require.Equal(tb, http.StatusAccepted, resp.StatusCode())
	require.NotNil(tb, resp.JSON202)

	// Run pre-build hooks (e.g. file uploads for COPY steps)
	for _, fn := range preBuildFns {
		fn(tb, ctx, resp.JSON202.TemplateID)
	}

	// Start build
	startResp, err := c.PostV2TemplatesTemplateIDBuildsBuildIDWithResponse(
		ctx,
		resp.JSON202.TemplateID,
		resp.JSON202.BuildID,
		data,
		setup.WithAPIKey(),
		setup.WithTestsUserAgent(),
	)
	require.NoError(tb, err)
	require.Equal(tb, http.StatusAccepted, startResp.StatusCode())

	logLevel := api.LogLevelInfo
	if EnableDebugLogs {
		logLevel = api.LogLevelDebug
	}

	// Check build status
	offset := 0
	for {
		statusResp, err := c.GetTemplatesTemplateIDBuildsBuildIDStatusWithResponse(
			ctx,
			resp.JSON202.TemplateID,
			resp.JSON202.BuildID,
			&api.GetTemplatesTemplateIDBuildsBuildIDStatusParams{
				LogsOffset: new(int32(offset)),
				Level:      &logLevel,
			},
			setup.WithAPIKey(),
			setup.WithTestsUserAgent(),
		)
		require.NoError(tb, err)
		require.Equal(tb, http.StatusOK, statusResp.StatusCode(), string(statusResp.Body))
		require.NotNil(tb, statusResp.JSON200, string(statusResp.Body))

		offset += len(statusResp.JSON200.LogEntries)
		for _, entry := range statusResp.JSON200.LogEntries {
			logHandler(templateName, entry)
		}

		switch statusResp.JSON200.Status {
		case api.TemplateBuildStatusReady:
			tb.Log("Build completed successfully")

			return buildOutcome{ready: true}
		case api.TemplateBuildStatusError:
			return buildOutcome{reason: safe(statusResp.JSON200.Reason).Message}
		}

		time.Sleep(time.Second)
	}
}

func safe[T any](item *T) T {
	if item != nil {
		return *item
	}
	var t T

	return t
}

func defaultBuildLogHandler(tb testing.TB) BuildLogHandler {
	tb.Helper()

	return func(alias string, entry api.BuildLogEntry) {
		tb.Logf("%s: [%s] %s", alias, entry.Level, entry.Message)
	}
}

func TestTemplateBuildRUN(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		templateName string
		buildConfig  api.TemplateBuildStartV2
	}{
		{
			name:         "Single RUN command",
			templateName: "test-ubuntu-run",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "RUN",
						Force: new(true),
						Args:  new([]string{"echo 'Hello, World!'"}),
					},
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, buildTemplate(t, tc.templateName, tc.buildConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildENV(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		templateName string
		buildConfig  api.TemplateBuildStartV2
	}{
		{
			name:         "ENV variable persistence",
			templateName: "test-ubuntu-env",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"ENV_VAR", "Hello, World!"}),
					},
					{
						Type: "RUN",
						Args: new([]string{": \"${ENV_VAR:?ENV_VAR is not set or empty}\"; echo \"$ENV_VAR\""}),
					},
				}),
			},
		},
		{
			name:         "ENV variable persistence for start command",
			templateName: "test-ubuntu-env-start",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"ENV_VAR", "Hello, World!"}),
					},
				}),
				StartCmd: new(": \"${ENV_VAR:?ENV_VAR is not set or empty}\"; echo \"$ENV_VAR\""),
				ReadyCmd: new("sleep 5"),
			},
		},
		{
			name:         "ENV variable recursive",
			templateName: "test-ubuntu-env-recursive",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"PATH", "${PATH}:/my/path"}),
					},
					{
						Type: "RUN",
						Args: new([]string{"[[ \"$PATH\" == *:/my/path* ]] || exit 1"}),
					},
				}),
			},
		},
		{
			name:         "ENV with PEM-style dashes",
			templateName: "test-ubuntu-env-pem-dashes",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"PEM_HEADER", "-----BEGIN DATA-----"}),
					},
					{
						Type: "RUN",
						Args: new([]string{"[[ \"$PEM_HEADER\" == \"-----BEGIN DATA-----\" ]] || exit 1"}),
					},
				}),
			},
		},
		{
			name:         "ENV with double quotes",
			templateName: "test-ubuntu-env-quotes",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"QUOTED_VAR", `say "hello"`}),
					},
					{
						Type: "RUN",
						Args: new([]string{`[[ "$QUOTED_VAR" == 'say "hello"' ]] || exit 1`}),
					},
				}),
			},
		},
		{
			name:         "ENV with single quotes",
			templateName: "test-ubuntu-env-single-quotes",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"SINGLE_QUOTED", "it's working"}),
					},
					{
						Type: "RUN",
						Args: new([]string{`[[ "$SINGLE_QUOTED" == "it's working" ]] || exit 1`}),
					},
				}),
			},
		},
		{
			name:         "ENV with backslashes",
			templateName: "test-ubuntu-env-backslash",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"PATH_VAR", `path\to\file`}),
					},
					{
						Type: "RUN",
						Args: new([]string{`[[ "$PATH_VAR" == 'path\to\file' ]] || exit 1`}),
					},
				}),
			},
		},
		{
			name:         "ENV with multiline value",
			templateName: "test-ubuntu-env-multiline",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"MULTILINE", "line1\nline2\nline3"}),
					},
					{
						Type: "RUN",
						Args: new([]string{`[[ $(echo "$MULTILINE" | wc -l) -eq 3 ]] || exit 1`}),
					},
				}),
			},
		},
		{
			name:         "ENV blocks command substitution with backticks",
			templateName: "test-ubuntu-env-backticks",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"LITERAL_CMD", "`echo pwned`"}),
					},
					{
						Type: "RUN",
						Args: new([]string{`[[ "$LITERAL_CMD" == '` + "`echo pwned`" + `' ]] || exit 1`}),
					},
				}),
			},
		},
		{
			name:         "ENV blocks command substitution with dollar paren",
			templateName: "test-ubuntu-env-dollarparen",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"LITERAL_CMD2", "$(echo pwned)"}),
					},
					{
						Type: "RUN",
						Args: new([]string{`[[ "$LITERAL_CMD2" == '$(echo pwned)' ]] || exit 1`}),
					},
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, buildTemplate(t, tc.templateName, tc.buildConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildWORKDIR(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		templateName string
		buildConfig  api.TemplateBuildStartV2
	}{
		{
			name:         "WORKDIR persistence",
			templateName: "test-ubuntu-workdir-persistence",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "WORKDIR",
						Force: new(true),
						Args:  new([]string{"/app"}),
					},
					{
						Type: "RUN",
						Args: new([]string{"[[ \"$(pwd)\" == \"/app\" ]] || exit 1"}),
					},
				}),
			},
		},
		{
			name:         "WORKDIR persistence in start command",
			templateName: "test-ubuntu-workdir-start",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "WORKDIR",
						Force: new(true),
						Args:  new([]string{"/app"}),
					},
				}),
				StartCmd: new("[[ \"$(pwd)\" == \"/app\" ]] || exit 1"),
				ReadyCmd: new("sleep 5"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, buildTemplate(t, tc.templateName, tc.buildConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildCache(t *testing.T) {
	t.Parallel()

	alias := "test-ubuntu-cache"
	template := api.TemplateBuildStartV2{
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{
			{
				Type: "ENV",
				Args: new([]string{"ENV_VAR", "Hello, World!"}),
			},
			{
				Type: "RUN",
				Args: new([]string{": \"${ENV_VAR:?ENV_VAR is not set or empty}\"; echo \"$ENV_VAR\""}),
			},
		}),
	}

	assert.True(t, buildTemplate(t, alias, template, defaultBuildLogHandler(t)))

	messages := make([]string, 0)
	assert.True(t, buildTemplate(t, alias, template, func(alias string, entry api.BuildLogEntry) {
		messages = append(messages, entry.Message)
		defaultBuildLogHandler(t)(alias, entry)
	}))
	assert.Condition(t, func() bool {
		for _, msg := range messages {
			if strings.Contains(msg, "CACHED [builder 1/2] ENV ENV_VAR Hello, World!") {
				return true
			}
		}

		return false
	}, "Expected to contain cached ENV layer")
}

func TestTemplateBuildFromTemplate(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		baseTemplate    string
		baseConfig      api.TemplateBuildStartV2
		derivedTemplate string
		derivedConfig   api.TemplateBuildStartV2
	}{
		{
			name:         "Basic fromTemplate functionality",
			baseTemplate: "test-ubuntu-base-template",
			baseConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"BASE_VAR", "base_value"}),
					},
					{
						Type:  "WORKDIR",
						Force: new(true),
						Args:  new([]string{"/app"}),
					},
				}),
			},
			derivedTemplate: "test-ubuntu-derived-template",
			derivedConfig: api.TemplateBuildStartV2{
				Force:        new(true),
				FromTemplate: new("test-ubuntu-base-template"),
				Steps: new([]api.TemplateStep{
					{
						Type: "ENV",
						Args: new([]string{"DERIVED_VAR", "derived_value"}),
					},
					{
						Type: "RUN",
						Args: new([]string{": \"${BASE_VAR:?BASE_VAR not inherited}\"; [[ \"$BASE_VAR\" == \"base_value\" ]] || exit 1; [[ \"$(pwd)\" == \"/app\" ]] || exit 2; [[ \"$DERIVED_VAR\" == \"derived_value\" ]] || exit 3; echo 'Inheritance verification passed'"}),
					},
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// First build the base template
			assert.True(t, buildTemplate(t, tc.baseTemplate, tc.baseConfig, defaultBuildLogHandler(t)))

			// Then build the derived template from the base template
			assert.True(t, buildTemplate(t, tc.derivedTemplate, tc.derivedConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildFromTemplateCommandOverride(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		baseTemplate    string
		baseConfig      api.TemplateBuildStartV2
		derivedTemplate string
		derivedConfig   api.TemplateBuildStartV2
	}{
		{
			name:         "Start command override in derived template",
			baseTemplate: "test-ubuntu-base-override-start",
			baseConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"BASE_VAR", "base_value"}),
					},
					{
						Type:  "WORKDIR",
						Force: new(true),
						Args:  new([]string{"/app/base"}),
					},
				}),
				// Base start command - fails if override_check.txt exists (meaning it's running in derived context)
				StartCmd: new("[[ ! -f /override_check.txt ]] || exit 97; echo 'base_command_executed'"),
				ReadyCmd: new("sleep 5"),
			},
			derivedTemplate: "test-ubuntu-derived-override-start",
			derivedConfig: api.TemplateBuildStartV2{
				Force:        new(true),
				FromTemplate: new("test-ubuntu-base-override-start"),
				Steps: new([]api.TemplateStep{
					{
						Type: "ENV",
						Args: new([]string{"DERIVED_VAR", "derived_value"}),
					},
					{
						Type: "WORKDIR",
						Args: new([]string{"/app/derived"}),
					},
					{
						Type: "RUN",
						Args: new([]string{"echo 'override_expected' > /override_check.txt", "root"}),
					},
				}),
				// Override the base start command - simple success proves override worked
				StartCmd: new("exit 0"),
				ReadyCmd: new("sleep 5"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// First build the base template
			assert.True(t, buildTemplate(t, tc.baseTemplate, tc.baseConfig, defaultBuildLogHandler(t)))

			// Then build the derived template from the base template
			assert.True(t, buildTemplate(t, tc.derivedTemplate, tc.derivedConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildFromTemplateInheritance(t *testing.T) {
	t.Parallel()

	baseTemplate := "test-ubuntu-inheritance-base"
	derivedTemplate := "test-ubuntu-inheritance-derived"

	// Base template with ENV and WORKDIR settings
	baseConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{
			{
				Type:  "ENV",
				Force: new(true),
				Args:  new([]string{"BASE_ENV", "inherited_value"}),
			},
			{
				Type:  "ENV",
				Force: new(true),
				Args:  new([]string{"OVERRIDE_VAR", "base_value"}),
			},
			{
				Type:  "WORKDIR",
				Force: new(true),
				Args:  new([]string{"/base/workdir"}),
			},
		}),
	}

	// Derived template that tests inheritance and override
	derivedConfig := api.TemplateBuildStartV2{
		Force:        new(true),
		FromTemplate: new(baseTemplate),
		Steps: new([]api.TemplateStep{
			{
				Type: "ENV",
				Args: new([]string{"OVERRIDE_VAR", "derived_value"}),
			},
			{
				Type: "RUN",
				Args: new([]string{
					// Test ENV inheritance
					": \"${BASE_ENV:?BASE_ENV is not set or empty}\"; " +
						"[[ \"$BASE_ENV\" == \"inherited_value\" ]] || exit 1; " +
						// Test ENV override
						"[[ \"$OVERRIDE_VAR\" == \"derived_value\" ]] || exit 2; " +
						// Test WORKDIR inheritance
						"[[ \"$(pwd)\" == \"/base/workdir\" ]] || exit 3; " +
						"echo 'All inheritance tests passed'",
				}),
			},
		}),
	}

	// First build the base template
	assert.True(t, buildTemplate(t, baseTemplate, baseConfig, defaultBuildLogHandler(t)))

	// Then build the derived template from the base template
	assert.True(t, buildTemplate(t, derivedTemplate, derivedConfig, defaultBuildLogHandler(t)))
}

func TestTemplateBuildFromTemplateStartCommand(t *testing.T) {
	t.Parallel()

	baseTemplate := "test-ubuntu-start-base"
	derivedTemplate := "test-ubuntu-start-derived"

	// Base template with ENV and WORKDIR for start command inheritance
	baseConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{
			{
				Type:  "ENV",
				Force: new(true),
				Args:  new([]string{"START_ENV", "start_value"}),
			},
			{
				Type:  "WORKDIR",
				Force: new(true),
				Args:  new([]string{"/start/workdir"}),
			},
		}),
	}

	// Derived template with start command that tests ENV and WORKDIR inheritance
	derivedConfig := api.TemplateBuildStartV2{
		Force:        new(true),
		FromTemplate: new(baseTemplate),
		StartCmd: new(
			// Test ENV inheritance in start command
			": \"${START_ENV:?START_ENV is not set or empty}\"; " +
				"[[ \"$START_ENV\" == \"start_value\" ]] || exit 1; " +
				// Test WORKDIR inheritance in start command
				"[[ \"$(pwd)\" == \"/start/workdir\" ]] || exit 2; " +
				"echo 'Start command inheritance tests passed'",
		),
		ReadyCmd: new("sleep 5"),
	}

	// First build the base template
	assert.True(t, buildTemplate(t, baseTemplate, baseConfig, defaultBuildLogHandler(t)))

	// Then build the derived template from the base template
	assert.True(t, buildTemplate(t, derivedTemplate, derivedConfig, defaultBuildLogHandler(t)))
}

func TestTemplateBuildFromTemplateBaseCommandsInheritance(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name            string
		baseTemplate    string
		baseConfig      api.TemplateBuildStartV2
		derivedTemplate string
		derivedConfig   api.TemplateBuildStartV2
	}{
		{
			name:         "Start command inherited from base template uses original base context",
			baseTemplate: "test-ubuntu-base-with-start",
			baseConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"BASE_VAR", "base_value"}),
					},
					{
						Type:  "WORKDIR",
						Force: new(true),
						Args:  new([]string{"/app/base"}),
					},
				}),
				// Start command runs with base template context (not derived modifications)
				StartCmd: new(": \"${BASE_VAR:?BASE_VAR not set}\"; [[ \"$BASE_VAR\" == \"base_value\" ]] || exit 1; [[ \"$(pwd)\" == \"/app/base\" ]] || exit 2; echo \"Base start command runs with original base context\""),
				ReadyCmd: new("sleep 5"),
			},
			derivedTemplate: "test-ubuntu-derived-with-inheritance",
			derivedConfig: api.TemplateBuildStartV2{
				Force:        new(true),
				FromTemplate: new("test-ubuntu-base-with-start"),
				Steps: new([]api.TemplateStep{
					{
						Type: "WORKDIR",
						Args: new([]string{"/app/derived"}), // Override base workdir
					},
				}),
				// No StartCmd/ReadyCmd - inherit from base, runs with original base context
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// First build the base template
			assert.True(t, buildTemplate(t, tc.baseTemplate, tc.baseConfig, defaultBuildLogHandler(t)))

			// Then build the derived template from the base template
			assert.True(t, buildTemplate(t, tc.derivedTemplate, tc.derivedConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildFromTemplateLayered(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                 string
		baseTemplate         string
		baseConfig           api.TemplateBuildStartV2
		intermediateTemplate string
		intermediateConfig   api.TemplateBuildStartV2
		finalTemplate        string
		finalConfig          api.TemplateBuildStartV2
	}{
		{
			name:         "Three-level template inheritance with ENV accumulation",
			baseTemplate: "test-ubuntu-layered-base",
			baseConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"LEVEL", "base"}),
					},
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"BASE_VAR", "base_value"}),
					},
				}),
			},
			intermediateTemplate: "test-ubuntu-layered-intermediate",
			intermediateConfig: api.TemplateBuildStartV2{
				Force:        new(true),
				FromTemplate: new("test-ubuntu-layered-base"),
				Steps: new([]api.TemplateStep{
					{
						Type: "ENV",
						Args: new([]string{"LEVEL", "intermediate"}),
					},
					{
						Type: "ENV",
						Args: new([]string{"INTERMEDIATE_VAR", "intermediate_value"}),
					},
				}),
			},
			finalTemplate: "test-ubuntu-layered-final",
			finalConfig: api.TemplateBuildStartV2{
				Force:        new(true),
				FromTemplate: new("test-ubuntu-layered-intermediate"),
				Steps: new([]api.TemplateStep{
					{
						Type: "ENV",
						Args: new([]string{"LEVEL", "final"}),
					},
					{
						Type: "ENV",
						Args: new([]string{"FINAL_VAR", "final_value"}),
					},
					{
						Type: "RUN",
						Args: new([]string{
							"[[ \"$LEVEL\" == \"final\" ]] || exit 1; " +
								"[[ \"$BASE_VAR\" == \"base_value\" ]] || exit 2; " +
								"[[ \"$INTERMEDIATE_VAR\" == \"intermediate_value\" ]] || exit 3; " +
								"[[ \"$FINAL_VAR\" == \"final_value\" ]] || exit 4",
						}),
					},
				}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Build base template
			assert.True(t, buildTemplate(t, tc.baseTemplate, tc.baseConfig, defaultBuildLogHandler(t)))

			// Build intermediate template from base
			assert.True(t, buildTemplate(t, tc.intermediateTemplate, tc.intermediateConfig, defaultBuildLogHandler(t)))

			// Build final template from intermediate
			assert.True(t, buildTemplate(t, tc.finalTemplate, tc.finalConfig, defaultBuildLogHandler(t)))
		})
	}
}

func TestTemplateBuildStartReadyCommandExecution(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		templateName string
		buildConfig  api.TemplateBuildStartV2
		expectedLogs []string
	}{
		{
			name:         "Start and Ready commands are executed",
			templateName: "test-ubuntu-start-ready-execution",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "RUN",
						Force: new(true),
						Args:  new([]string{"echo 'Setting up template'"}),
					},
				}),
				StartCmd: new("echo 'Hello, World!'"),
				ReadyCmd: new("sleep 2"),
			},
			expectedLogs: []string{
				"Running start command",
				"[start] [stdout]: Hello, World!",
				"Waiting for template to be ready: sleep 2",
				"Template is ready",
			},
		},
		{
			name:         "Complex Start and Ready commands with environment variables",
			templateName: "test-ubuntu-complex-start-ready",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:22.04"),
				Steps: new([]api.TemplateStep{
					{
						Type:  "ENV",
						Force: new(true),
						Args:  new([]string{"TEST_VAR", "test_value"}),
					},
				}),
				StartCmd: new("echo \"Starting with TEST_VAR=$TEST_VAR\"; echo 'Initialization complete'"),
				ReadyCmd: new("echo 'Checking readiness...'; sleep 1; echo 'Ready check complete'"),
			},
			expectedLogs: []string{
				"Running start command",
				"[start] [stdout]: Starting with TEST_VAR=test_value",
				"[start] [stdout]: Initialization complete",
				"Waiting for template to be ready",
				"Template is ready",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Collect all log messages
			var logMessages []string
			logHandler := func(alias string, entry api.BuildLogEntry) {
				logMessages = append(logMessages, entry.Message)
				defaultBuildLogHandler(t)(alias, entry)
			}

			// Build the template
			assert.True(t, buildTemplate(t, tc.templateName, tc.buildConfig, logHandler))

			// Verify expected log messages appear
			for _, expectedLog := range tc.expectedLogs {
				found := false
				for _, msg := range logMessages {
					if strings.Contains(msg, expectedLog) {
						found = true

						break
					}
				}
				assert.True(t, found, "Expected log message not found: %s", expectedLog)
			}

			// Additional verification: ensure commands were executed in the right order
			runningStartIdx := -1
			waitingForReadyIdx := -1
			templateReadyIdx := -1

			for i, msg := range logMessages {
				if strings.Contains(msg, "Running start command") {
					runningStartIdx = i
				}
				if strings.Contains(msg, "Waiting for template to be ready") {
					waitingForReadyIdx = i
				}
				if strings.Contains(msg, "Template is ready") {
					templateReadyIdx = i
				}
			}

			// Verify order: start command -> waiting for ready -> template ready
			assert.Greater(t, waitingForReadyIdx, runningStartIdx, "Ready command should run after start command")
			assert.Greater(t, templateReadyIdx, waitingForReadyIdx, "Template ready should come after waiting for ready")
		})
	}
}

func TestTemplateBuildWithDifferentSourceImages(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		templateName string
		buildConfig  api.TemplateBuildStartV2
		expectedLogs []string
	}{
		{
			name:         "Test with Ubuntu 24.04 base image",
			templateName: "test-ubuntu-24-04-source",
			buildConfig: api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new("ubuntu:24.04"),
				Steps:     new([]api.TemplateStep{}),
				StartCmd:  new("echo 'Initialization complete'"),
				ReadyCmd:  new("echo 'Checking readiness...'; sleep 1; echo 'Ready check complete'"),
			},
			expectedLogs: []string{
				"Running start command",
				"[start] [stdout]: Initialization complete",
				"Waiting for template to be ready",
				"Template is ready",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Collect all log messages
			var logMessages []string
			logHandler := func(alias string, entry api.BuildLogEntry) {
				logMessages = append(logMessages, entry.Message)
				defaultBuildLogHandler(t)(alias, entry)
			}

			// Build the template
			assert.True(t, buildTemplate(t, tc.templateName, tc.buildConfig, logHandler))

			// Verify expected log messages appear
			for _, expectedLog := range tc.expectedLogs {
				found := false
				for _, msg := range logMessages {
					if strings.Contains(msg, expectedLog) {
						found = true

						break
					}
				}
				assert.True(t, found, "Expected log message not found: %s", expectedLog)
			}
		})
	}
}

func TestTemplateBuildInstalledPackagesAvailable(t *testing.T) {
	t.Parallel()

	// Test that packages installed by provision.sh are available during template build
	packages := []string{
		"systemd",
		"systemd-sysv",
		"openssh-server",
		"sudo",
		"chrony",
		"socat",
		"curl",
		"ca-certificates",
		"fuse3",
		"iptables",
		"git",
		"less",
		"nftables",
		"iputils-ping",
		"jq",
		"iproute2",
	}

	steps := make([]api.TemplateStep, 0, len(packages))
	for _, pkg := range packages {
		steps = append(steps, api.TemplateStep{
			Type: "RUN",
			Args: new([]string{
				"dpkg-query -W -f='${Status}' " + pkg + " | grep -q 'install ok installed'",
			}),
		})
	}

	buildConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("ubuntu:22.04"),
		Steps:     new(steps),
	}

	assert.True(t, buildTemplate(t, "test-ubuntu-packages-available", buildConfig, defaultBuildLogHandler(t)))
}

func createTarWithFile(tb testing.TB, fileName string, content string) []byte {
	tb.Helper()

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	err := tw.WriteHeader(&tar.Header{
		Name: fileName,
		Mode: 0o644,
		Size: int64(len(content)),
	})
	require.NoError(tb, err)

	_, err = tw.Write([]byte(content))
	require.NoError(tb, err)
	require.NoError(tb, tw.Close())
	require.NoError(tb, gw.Close())

	return buf.Bytes()
}

func computeSHA256Hex(data []byte) string {
	h := sha256.Sum256(data)

	return fmt.Sprintf("%x", h[:])
}

// uploadFileForTemplate gets a signed upload URL for the given template and hash,
// then uploads the data to that URL.
func uploadFileForTemplate(
	tb testing.TB,
	ctx context.Context,
	templateID string,
	hash string,
	data []byte,
) {
	tb.Helper()

	c := setup.GetAPIClient()

	// Get the signed upload URL
	resp, err := c.GetTemplatesTemplateIDFilesHashWithResponse(
		ctx,
		templateID,
		hash,
		setup.WithAPIKey(),
		setup.WithTestsUserAgent(),
	)
	require.NoError(tb, err)
	require.Equal(tb, http.StatusCreated, resp.StatusCode(), string(resp.Body))
	require.NotNil(tb, resp.JSON201)
	require.NotNil(tb, resp.JSON201.Url, "Expected a signed upload URL")

	// Upload the tar data to the signed URL
	uploadReq, err := http.NewRequestWithContext(ctx, http.MethodPut, *resp.JSON201.Url, bytes.NewReader(data))
	require.NoError(tb, err)
	uploadReq.Header.Set("Content-Type", "application/octet-stream")
	// Request headers the API returned must go on the PUT (Azure needs x-ms-blob-type).
	if resp.JSON201.Headers != nil {
		for name, value := range *resp.JSON201.Headers {
			uploadReq.Header.Set(name, value)
		}
	}

	uploadResp, err := http.DefaultClient.Do(uploadReq)
	require.NoError(tb, err)
	defer uploadResp.Body.Close()
	io.ReadAll(uploadResp.Body)

	require.Less(tb, uploadResp.StatusCode, 300, "Upload failed with status %d", uploadResp.StatusCode)
}

func TestTemplateBuildCOPY(t *testing.T) {
	t.Parallel()

	fileContent := "Hello from COPY!"
	tarData := createTarWithFile(t, "hello.txt", fileContent)
	filesHash := computeSHA256Hex(tarData)

	buildConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("ubuntu:24.04"),
		Steps: new([]api.TemplateStep{
			{
				Type:      "COPY",
				Force:     new(true),
				Args:      new([]string{".", "/app/"}),
				FilesHash: new(filesHash),
			},
			{
				Type: "RUN",
				Args: new([]string{
					fmt.Sprintf(`cat /app/hello.txt | grep '%s'`, fileContent),
				}),
			},
		}),
	}

	assert.True(t, buildTemplate(t, "test-ubuntu-copy", buildConfig, defaultBuildLogHandler(t),
		func(tb testing.TB, ctx context.Context, templateID string) {
			tb.Helper()
			uploadFileForTemplate(tb, ctx, templateID, filesHash, tarData)
		},
	))
}

func TestTemplateBuildFuseConfiguration(t *testing.T) {
	t.Parallel()

	// Test that FUSE is configured to allow non-root users:
	// - /etc/tmpfiles.d/fuse.conf sets /dev/fuse permissions at boot
	// - /dev/fuse has actual permissions 666
	buildConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{
			{
				Type: "RUN",
				Args: new([]string{
					"grep -q 'z /dev/fuse 0666 root root -' /etc/tmpfiles.d/fuse.conf",
				}),
			},
			{
				Type: "RUN",
				Args: new([]string{
					"echo \"Checking /dev/fuse permissions:\"; ls -la /dev/fuse; stat -c 'mode=%a owner=%U group=%G' /dev/fuse; test $(stat -c %a /dev/fuse) = '666'",
				}),
			},
		}),
	}

	assert.True(t, buildTemplate(t, "test-ubuntu-fuse-config", buildConfig, defaultBuildLogHandler(t)))
}
