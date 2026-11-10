package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

func TestBaseLayerFromKeepsTheDefaultBehaviour(t *testing.T) {
	t.Parallel()

	src := metadata.FromTemplate{Alias: "parent", BuildID: "parent"}
	parent := metadata.Template{Version: metadata.CurrentVersion, RootfsDriveID: "root", Context: metadata.Context{User: "user"}}.WithEnvdToken(true, "hash")

	base := baseLayerFrom(parent, src, guest.TemplateBuild{})
	assert.Equal(t, parent.BasedOn(src), base)
	assert.Empty(t, base.RootfsDriveID, "a booted layer names its own devices")
	assert.False(t, base.NoMmdsStore, "a booted layer has an MMDS store")
	assert.Empty(t, base.EnvdAccessTokenHash)
	assert.Nil(t, base.Context.WorkDir)
}

func TestBaseLayerFromCarriesAResumedParent(t *testing.T) {
	t.Parallel()

	src := metadata.FromTemplate{Alias: "parent", BuildID: "parent"}
	tb := guest.TemplateBuild{AlwaysResume: true, DefaultUser: "root", DefaultWorkDir: "/work"}

	parent := metadata.Template{Version: metadata.CurrentVersion, GuestOS: "resuming", RootfsDriveID: "root", NetIfaceID: "net0", RootfsFile: "disk.img"}.WithEnvdToken(true, "hash")
	base := baseLayerFrom(parent, src, tb)
	assert.True(t, base.NoMmdsStore, "the resumed memory image keeps its storeless envd")
	assert.Equal(t, "hash", base.EnvdAccessTokenHash, "and the token that envd holds")
	assert.Equal(t, "resuming", base.GuestOS)
	assert.Equal(t, "root", base.RootfsDriveID, "every layer resumes the parent's memory image and its device names")
	assert.Equal(t, "net0", base.NetIfaceID)
	assert.Equal(t, "disk.img", base.RootfsFile)
	assert.Equal(t, "root", base.Context.User, "an empty context gets the profile's user")
	require.NotNil(t, base.Context.WorkDir)
	assert.Equal(t, "/work", *base.Context.WorkDir, "and its working directory")
	assert.Equal(t, &src, base.FromTemplate)

	set := metadata.Template{Version: metadata.CurrentVersion, GuestOS: "resuming", Context: metadata.Context{User: "builder", WorkDir: new("/src")}}
	base = baseLayerFrom(set, src, tb)
	assert.Equal(t, "builder", base.Context.User, "a parent's own user stands")
	assert.Equal(t, "/src", *base.Context.WorkDir)
}
