//go:build linux

package sandbox

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// A snapshot without an MMDS store keeps its envd's token: a restore under the
// same token (a same-ID resume) starts, one under another (a fork, a snapshot
// template) is refused before it starts, and every snapshot with a store, which
// authorizes the replacement through MMDS, starts as before.
func TestRefuseEnvdTokenReplacement(t *testing.T) {
	t.Parallel()

	storeless := metadata.Template{GuestOS: "firmware"}.WithEnvdToken(true, keys.HashAccessToken("token-1"))

	for name, tc := range map[string]struct {
		meta    metadata.Template
		token   *string
		skip    bool
		refused bool
	}{
		"same-id resume":                    {storeless, new("token-1"), false, false},
		"fork under a new token":            {storeless, new("token-2"), false, true},
		"fork without a token":              {storeless, nil, false, true},
		"envd holds no token":               {storeless.WithEnvdToken(true, keys.HashAccessToken("")), new("token-2"), false, false},
		"holder unrecorded":                 {storeless.WithEnvdToken(true, ""), new("token-2"), false, false},
		"legacy store-less build, no field": {metadata.Template{GuestOS: "firmware"}, new("token-2"), false, false},
		"store-bearing build":               {storeless.WithEnvdToken(false, keys.HashAccessToken("token-1")), new("token-2"), false, false},
		"fork restored without /init":       {storeless, new("token-2"), true, false},
		"default-profile build":             {metadata.Template{}, new("token-2"), false, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := refuseEnvdTokenReplacement(tc.meta, tc.token, !tc.skip)
			if tc.refused {
				require.ErrorIs(t, err, ErrEnvdTokenNotReplaceable)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// A pause records the token only for a microVM without a store: the one this
// life's /init gave envd, or the lineage's when this life sent none.
func TestEnvdTokenAtPause(t *testing.T) {
	t.Parallel()

	noStore, hash := envdTokenAtPause(false, true, new("token-1"), "recorded")
	assert.False(t, noStore, "a microVM with a store records nothing")
	assert.Empty(t, hash)

	noStore, hash = envdTokenAtPause(true, true, new("token-1"), "")
	assert.True(t, noStore)
	assert.Equal(t, keys.HashAccessToken("token-1"), hash, "the legacy build's first pause learns the holder")

	_, hash = envdTokenAtPause(true, true, nil, "recorded")
	assert.Equal(t, keys.HashAccessToken(""), hash, "an /init without a token leaves envd holding none")

	_, hash = envdTokenAtPause(true, false, new("token-1"), "recorded")
	assert.Equal(t, "recorded", hash, "a life without /init leaves envd holding the lineage's token")
}

// The legacy store-less build end to end through its metadata: its first
// secured create tolerates the missing store, the pause records the holder,
// and the snapshot then resumes under the same token and refuses a fork.
func TestStorelessLineageAcrossAPause(t *testing.T) {
	t.Parallel()

	legacy := metadata.Template{Version: metadata.CurrentVersion, GuestOS: "firmware"}
	require.NoError(t, refuseEnvdTokenReplacement(legacy, new("token-1"), true), "a build without the field is restored as before")

	paused := legacy.SameVersionTemplate(metadata.TemplateMetadata{BuildID: "paused"}).
		WithEnvdToken(envdTokenAtPause(true, true, new("token-1"), legacy.EnvdAccessTokenHash))
	raw, err := json.Marshal(paused)
	require.NoError(t, err)
	var stored metadata.Template
	require.NoError(t, json.Unmarshal(raw, &stored))

	require.NoError(t, refuseEnvdTokenReplacement(stored, new("token-1"), true), "same-id resume")
	require.ErrorIs(t, refuseEnvdTokenReplacement(stored, new("token-2"), true), ErrEnvdTokenNotReplaceable, "fork")

	child := stored.SameVersionTemplate(metadata.TemplateMetadata{BuildID: "child"})
	require.ErrorIs(t, refuseEnvdTokenReplacement(child, new("token-2"), true), ErrEnvdTokenNotReplaceable, "the next pause's metadata keeps the holder")
}

// The refusal comes before any resource of the resume is set up: the template
// panics on any access past its metadata.
func TestResumeRefusesAnEnvdTokenReplacementBeforeResourceInitialization(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{GuestOS: "firmware"}.WithEnvdToken(true, keys.HashAccessToken("token-1"))
	config := NewConfig(Config{Vcpu: 2, Envd: EnvdMetadata{AccessToken: new("token-2")}})

	_, err := (&Factory{}).ResumeSandbox(
		t.Context(),
		capacityOnlyTemplate{meta: meta},
		config,
		sandboxtypes.RuntimeMetadata{},
		time.Time{},
		time.Time{},
		nil,
	)
	require.ErrorIs(t, err, ErrEnvdTokenNotReplaceable)
}
