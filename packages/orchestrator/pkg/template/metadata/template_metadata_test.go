package metadata

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeserialize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		input          string
		expectedResult Template
		expectedError  string
	}{
		{
			name:  "Valid current version template with all fields",
			input: `{"version": 2, "template": {"build_id": "build123", "kernel_version": "5.10", "firecracker_version": "1.0"}, "context": {"user": "testuser", "workdir": "/app", "env_vars": {"KEY": "value"}}, "start": {"start_command": "npm start", "ready_command": "echo ready", "context": {"user": "root"}}, "from_image": "ubuntu:20.04"}`,
			expectedResult: Template{
				Version: 2,
				Template: TemplateMetadata{
					BuildID:            "build123",
					KernelVersion:      "5.10",
					FirecrackerVersion: "1.0",
				},
				Context: Context{
					User:    "testuser",
					WorkDir: new("/app"),
					EnvVars: map[string]string{"KEY": "value"},
				},
				Start: &Start{
					StartCmd: "npm start",
					ReadyCmd: "echo ready",
					Context: Context{
						User: "root",
					},
				},
				FromImage: new("ubuntu:20.04"),
			},
		},
		{
			name:  "Valid current version template with from_template",
			input: `{"version": 2, "template": {"build_id": "build456", "kernel_version": "5.10", "firecracker_version": "1.0"}, "context": {"user": "testuser"}, "from_template": {"alias": "base-template", "build_id": "base-build-123"}}`,
			expectedResult: Template{
				Version: 2,
				Template: TemplateMetadata{
					BuildID:            "build456",
					KernelVersion:      "5.10",
					FirecrackerVersion: "1.0",
				},
				Context: Context{
					User: "testuser",
				},
				FromTemplate: &FromTemplate{
					Alias:   "base-template",
					BuildID: "base-build-123",
				},
			},
		},
		{
			name:  "Valid current version template minimal fields",
			input: `{"version": 2, "template": {"build_id": "build789", "kernel_version": "5.10", "firecracker_version": "1.0"}, "context": {}}`,
			expectedResult: Template{
				Version: 2,
				Template: TemplateMetadata{
					BuildID:            "build789",
					KernelVersion:      "5.10",
					FirecrackerVersion: "1.0",
				},
				Context: Context{},
			},
		},
		{
			name:  "Deprecated version 1",
			input: `{"version": 1, "some": "data"}`,
			expectedResult: Template{
				Version: DeprecatedVersion,
			},
		},
		{
			name:  "Version less than deprecated (0)",
			input: `{"version": 0, "some": "data"}`,
			expectedResult: Template{
				Version: DeprecatedVersion,
			},
		},
		{
			name:  "Version as string (should be treated as deprecated)",
			input: `{"version": "1", "some": "data"}`,
			expectedResult: Template{
				Version: DeprecatedVersion,
			},
		},
		{
			name:  "No version field",
			input: `{"some": "data"}`,
			expectedResult: Template{
				Version: DeprecatedVersion,
			},
		},
		{
			name:          "Invalid JSON",
			input:         `{"version": 2, "template": {invalid json`,
			expectedError: "error unmarshaling template version",
		},
		{
			name:          "Empty input",
			input:         "",
			expectedError: "error unmarshaling template version",
		},
		{
			name:          "Valid version but invalid template structure",
			input:         `{"version": 2, "template": "invalid_template_structure"}`,
			expectedError: "error unmarshaling template metadata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := strings.NewReader(tt.input)
			result, err := deserialize(reader)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedResult.Version, result.Version)
			assert.Equal(t, tt.expectedResult.Template.BuildID, result.Template.BuildID)
			assert.Equal(t, tt.expectedResult.Template.KernelVersion, result.Template.KernelVersion)
			assert.Equal(t, tt.expectedResult.Template.FirecrackerVersion, result.Template.FirecrackerVersion)
			assert.Equal(t, tt.expectedResult.Context.User, result.Context.User)
			assert.Equal(t, tt.expectedResult.Context.WorkDir, result.Context.WorkDir)
			assert.Equal(t, tt.expectedResult.Context.EnvVars, result.Context.EnvVars)
			assert.Equal(t, tt.expectedResult.Start, result.Start)
			assert.Equal(t, tt.expectedResult.FromImage, result.FromImage)
			assert.Equal(t, tt.expectedResult.FromTemplate, result.FromTemplate)
		})
	}
}

func TestDeserialize_ReadError(t *testing.T) {
	t.Parallel()
	// Create a reader that always returns an error
	errorReader := &errorReader{err: io.ErrUnexpectedEOF}

	_, err := deserialize(errorReader)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "error reading template metadata")
}

func TestDeserialize_VersionEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		input           string
		expectedVersion uint64
	}{
		{
			name:            "Version exactly equals deprecated version",
			input:           `{"version": 1}`,
			expectedVersion: DeprecatedVersion,
		},
		{
			name:            "Version as float 1.0",
			input:           `{"version": 1.0}`,
			expectedVersion: DeprecatedVersion,
		},

		{
			name:            "Version as negative number",
			input:           `{"version": -1}`,
			expectedVersion: DeprecatedVersion,
		},
		{
			name:            "Version as null",
			input:           `{"version": null}`,
			expectedVersion: DeprecatedVersion,
		},
		{
			name:            "Version as boolean",
			input:           `{"version": true}`,
			expectedVersion: DeprecatedVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := strings.NewReader(tt.input)
			result, err := deserialize(reader)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedVersion, result.Version)
		})
	}
}

// errorReader is a test helper that always returns an error when read
type errorReader struct {
	err error
}

func (er *errorReader) Read([]byte) (n int, err error) {
	return 0, er.err
}

func TestIsFilesystemOnly(t *testing.T) {
	t.Parallel()

	assert.True(t, Template{FilesystemOnly: true}.IsFilesystemOnly())
	assert.False(t, Template{FilesystemOnly: false}.IsFilesystemOnly(), "memory snapshot")
	assert.False(t, Template{}.IsFilesystemOnly(), "zero value is a memory snapshot")
}

// A filesystem-only snapshot taken from a V1 template must survive a metadata
// round-trip with the flag intact. deserialize() strips every field from a
// version<=DeprecatedVersion snapshot, so MarkFilesystemOnly upgrades the
// version; without it the flag is lost and the orchestrator wrongly dispatches
// to a memory resume that hard-fails (the fs-only pause uploaded no memfile).
func TestMarkFilesystemOnly_V1SnapshotRoundTrips(t *testing.T) {
	t.Parallel()

	v1 := V1TemplateVersion()
	require.Equal(t, uint64(DeprecatedVersion), v1.Version, "V1 fallback starts at the deprecated version")

	fsOnly := v1.MarkFilesystemOnly(true)
	assert.True(t, fsOnly.FilesystemOnly)
	assert.Equal(t, uint64(FilesystemOnlyVersion), fsOnly.Version,
		"a filesystem-only snapshot must be upgraded to the pinned FilesystemOnlyVersion, past DeprecatedVersion")
	assert.Greater(t, uint64(FilesystemOnlyVersion), uint64(DeprecatedVersion),
		"FilesystemOnlyVersion must clear the deserialize() strip threshold")

	// The flag survives serialize -> deserialize.
	reader, err := serialize(fsOnly)
	require.NoError(t, err)
	got, err := deserialize(reader)
	require.NoError(t, err)
	assert.True(t, got.IsFilesystemOnly(),
		"filesystem-only flag must survive a V1-derived snapshot round-trip")

	// Guard documenting the hazard the upgrade avoids: a version<=Deprecated
	// snapshot still loses the flag on deserialize.
	rawReader, err := serialize(Template{Version: DeprecatedVersion, FilesystemOnly: true})
	require.NoError(t, err)
	raw, err := deserialize(rawReader)
	require.NoError(t, err)
	assert.False(t, raw.IsFilesystemOnly(),
		"version<=DeprecatedVersion strips filesystem_only — exactly why MarkFilesystemOnly upgrades")

	// Clearing the flag must not change the version.
	cleared := V1TemplateVersion().MarkFilesystemOnly(false)
	assert.False(t, cleared.FilesystemOnly)
	assert.Equal(t, uint64(DeprecatedVersion), cleared.Version, "clearing must not upgrade the version")
}

// Pre-existing snapshots have no "filesystem_only" key in metadata.json; it must
// decode to false (a memory snapshot) so they resume normally without migration.
func TestFilesystemOnly_BackwardCompatAndOmitempty(t *testing.T) {
	t.Parallel()

	var legacy Template
	require.NoError(t, json.Unmarshal([]byte(`{"version":1}`), &legacy))
	assert.False(t, legacy.IsFilesystemOnly())

	// A memory snapshot must not emit the key (omitempty), so its metadata.json
	// stays identical to today's.
	memJSON, err := json.Marshal(Template{Version: 1})
	require.NoError(t, err)
	assert.NotContains(t, string(memJSON), "filesystem_only")

	// A filesystem-only snapshot round-trips to true.
	fsJSON, err := json.Marshal(Template{Version: 1, FilesystemOnly: true})
	require.NoError(t, err)
	assert.Contains(t, string(fsJSON), `"filesystem_only":true`)

	var back Template
	require.NoError(t, json.Unmarshal(fsJSON, &back))
	assert.True(t, back.IsFilesystemOnly())
}

func TestIsFsQuiesced(t *testing.T) {
	t.Parallel()

	assert.True(t, Template{FsQuiesced: true}.IsFsQuiesced())
	assert.False(t, Template{}.IsFsQuiesced(), "zero value is not quiesced")
}

// fs_quiesced must survive a metadata round-trip on a filesystem-only snapshot
// without needing its own version bump: MarkFilesystemOnly already lifts the
// version past deserialize()'s strip threshold. A legacy/unmarked snapshot must
// deserialize as not-quiesced — the safe "not eligible" default that makes it
// wait for a future freezing pause.
func TestMarkFsQuiesced_SurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	// A frozen fs-only snapshot taken from a V1 template: both flags survive.
	frozen := V1TemplateVersion().MarkFilesystemOnly(true).MarkFsQuiesced(true)
	r, err := serialize(frozen)
	require.NoError(t, err)
	got, err := deserialize(r)
	require.NoError(t, err)
	assert.True(t, got.IsFilesystemOnly())
	assert.True(t, got.IsFsQuiesced(), "fs_quiesced must survive alongside filesystem_only")

	// A fs-only-but-sync-fallback snapshot round-trips as not quiesced.
	syncOnly := V1TemplateVersion().MarkFilesystemOnly(true).MarkFsQuiesced(false)
	r2, err := serialize(syncOnly)
	require.NoError(t, err)
	g2, err := deserialize(r2)
	require.NoError(t, err)
	assert.True(t, g2.IsFilesystemOnly())
	assert.False(t, g2.IsFsQuiesced(), "a sync-fallback fs-only snapshot must not be quiesced")

	// A legacy fs-only snapshot without the field defaults to not-quiesced.
	legacy := Template{Version: FilesystemOnlyVersion, FilesystemOnly: true}
	r3, err := serialize(legacy)
	require.NoError(t, err)
	g3, err := deserialize(r3)
	require.NoError(t, err)
	assert.False(t, g3.IsFsQuiesced(), "a snapshot without fs_quiesced is not eligible (safe default)")
}

// The envd_unresponsive mark survives a metadata round-trip on a current
// snapshot, is cleared by a later pause whose agent answered, and is absent
// (false) on every snapshot from before the field. It never lifts the version:
// a V1 memory lineage keeps its rootfs path and loses the mark on disk, which
// the API's own copy of the mark covers.
func TestMarkEnvdUnresponsive_RoundTripsWithoutLiftingTheVersion(t *testing.T) {
	t.Parallel()

	marked := Template{Version: CurrentVersion}.MarkEnvdUnresponsive(true)
	assert.True(t, marked.IsEnvdUnresponsive())
	assert.Equal(t, uint64(CurrentVersion), marked.Version)
	r, err := serialize(marked)
	require.NoError(t, err)
	got, err := deserialize(r)
	require.NoError(t, err)
	assert.True(t, got.IsEnvdUnresponsive(), "the mark must survive a round-trip")
	assert.False(t, got.IsFilesystemOnly(), "the mark rides on a memory snapshot")

	cleared := marked.MarkEnvdUnresponsive(false)
	assert.False(t, cleared.IsEnvdUnresponsive(), "a pause whose agent answered clears the mark")

	legacy, err := deserialize(strings.NewReader(`{"version": 2, "template": {"build_id": "b"}}`))
	require.NoError(t, err)
	assert.False(t, legacy.IsEnvdUnresponsive(), "a snapshot without the field is not marked")

	v1 := V1TemplateVersion().MarkEnvdUnresponsive(true)
	assert.Equal(t, uint64(DeprecatedVersion), v1.Version, "the mark must not lift a V1 lineage's version")
	r1, err := serialize(v1)
	require.NoError(t, err)
	g1, err := deserialize(r1)
	require.NoError(t, err)
	assert.False(t, g1.IsEnvdUnresponsive(), "a V1 lineage strips the mark, by design")

	// The prefetch rewrite of the same snapshot's metadata keeps the mark; a
	// new lineage derived from it does not inherit it.
	assert.True(t, marked.WithPrefetch(nil).IsEnvdUnresponsive())
	assert.False(t, marked.SameVersionTemplate(TemplateMetadata{BuildID: "next"}).IsEnvdUnresponsive())
}
