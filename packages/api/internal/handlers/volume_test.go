package handlers

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestIsValidVolumeName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		volume   string
		expected bool
	}{
		{
			name:     "valid name",
			volume:   "my-volume_123",
			expected: true,
		},
		{
			name:     "valid name with only numbers",
			volume:   "123456",
			expected: true,
		},
		{
			name:     "valid name with only letters",
			volume:   "myvolume",
			expected: true,
		},
		{
			name:     "valid name with hyphens",
			volume:   "my-volume",
			expected: true,
		},
		{
			name:     "valid name with underscores",
			volume:   "my_volume",
			expected: true,
		},
		{
			name:     "invalid name with space",
			volume:   "my volume",
			expected: false,
		},
		{
			name:     "invalid name with special character",
			volume:   "my-volume!",
			expected: false,
		},
		{
			name:     "invalid name with @",
			volume:   "my@volume",
			expected: false,
		},
		{
			name:     "empty name",
			volume:   "",
			expected: false,
		},
		{
			name:     "invalid name with leading dot",
			volume:   ".my-volume",
			expected: false,
		},
		{
			name:     "invalid name with trailing dot",
			volume:   "my-volume.",
			expected: false,
		},
		{
			name:     "invalid name with slash",
			volume:   "my/volume",
			expected: false,
		},
		{
			name:     "invalid name with backslash",
			volume:   "my\\volume",
			expected: false,
		},
		{
			name:     "invalid name with colon",
			volume:   "my:volume",
			expected: false,
		},
		{
			name:     "invalid name with asterisk",
			volume:   "my*volume",
			expected: false,
		},
		{
			name:     "invalid name with question mark",
			volume:   "my?volume",
			expected: false,
		},
		{
			name:     "invalid name with double quote",
			volume:   "my\"volume",
			expected: false,
		},
		{
			name:     "invalid name with less than",
			volume:   "my<volume",
			expected: false,
		},
		{
			name:     "invalid name with greater than",
			volume:   "my>volume",
			expected: false,
		},
		{
			name:     "invalid name with pipe",
			volume:   "my|volume",
			expected: false,
		},
		{
			name:     "invalid name with semicolon",
			volume:   "my;volume",
			expected: false,
		},
		{
			name:     "invalid name with comma",
			volume:   "my,volume",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			isValid := isValidVolumeName(tt.volume)
			assert.Equal(t, tt.expected, isValid)
		})
	}
}

// TestGetVolumeType covers the precedence paths that don't need a cluster;
// APIStore.orchestrator is a concrete *orchestrator.Orchestrator with no test
// constructor, so the node-derived resolution is not covered here.
func TestGetVolumeType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// flagValue is the LaunchDarkly default-persistent-volume-type
		// override; empty leaves the flag unset.
		flagValue        string
		regionVolumeType map[string]string
		defaultType      string
		expected         string
	}{
		{
			name:        "no region map falls back to global default",
			defaultType: "global-type",
			expected:    "global-type",
		},
		{
			// The orchestrator is only nil in tests, but a missing cluster
			// view must not be what decides whether a volume can be created.
			name:             "no orchestrator falls back to global default",
			regionVolumeType: map[string]string{"us-west3": "zonalfilestore-us-west3"},
			defaultType:      "global-type",
			expected:         "global-type",
		},
		{
			name:             "feature flag wins over region map and global default",
			flagValue:        "flag-type",
			regionVolumeType: map[string]string{"us-west3": "zonalfilestore-us-west3"},
			defaultType:      "global-type",
			expected:         "flag-type",
		},
		{
			name:     "no default configured at all",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Each subtest gets its own datasource/client so parallel runs
			// don't race on the shared flag value.
			td := ldtestdata.DataSource()
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() {
				assert.NoError(t, ff.Close(context.WithoutCancel(t.Context())))
			})

			if tt.flagValue != "" {
				td.Update(td.Flag(featureflags.DefaultPersistentVolumeType.Key()).
					ValueForAll(ldvalue.String(tt.flagValue)))
			}

			store := &APIStore{
				featureFlags: ff,
				config: cfg.Config{
					DefaultPersistentVolumeType:         tt.defaultType,
					DefaultPersistentVolumeTypeByRegion: tt.regionVolumeType,
				},
			}
			team := &types.Team{Team: &dbtypes.Team{ID: uuid.New()}}

			assert.Equal(t, tt.expected, store.getVolumeType(t.Context(), team))
		})
	}
}

func TestHasAllLabels(t *testing.T) {
	t.Parallel()

	labels := map[string]struct{}{"gpu": {}, "highmem": {}, "region=us-west3": {}}

	assert.True(t, hasAllLabels(labels, nil))
	assert.True(t, hasAllLabels(labels, []string{"gpu"}))
	assert.True(t, hasAllLabels(labels, []string{"gpu", "highmem"}))
	assert.False(t, hasAllLabels(labels, []string{"gpu", "default"}))
	assert.False(t, hasAllLabels(map[string]struct{}{}, []string{"default"}))
}
