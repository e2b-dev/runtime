//go:build unix

package filesystem

import (
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	fsmodel "github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
)

func TestStatOwnership(t *testing.T) {
	t.Parallel()

	u, err := user.Current()
	require.NoError(t, err)
	group, err := user.LookupGroupId(u.Gid)
	require.NoError(t, err)

	testFile := filepath.Join(t.TempDir(), "file.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("Hello, World!"), 0o644))
	linkedFile := filepath.Join(t.TempDir(), "linked-file.txt")
	require.NoError(t, os.Symlink(testFile, linkedFile))

	svc := mockService()

	for _, path := range []string{testFile, linkedFile} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			resp, err := svc.Stat(authn.SetInfo(t.Context(), u), connect.NewRequest(&filesystem.StatRequest{Path: path}))
			require.NoError(t, err)
			entry := resp.Msg.GetEntry()
			assert.Equal(t, u.Username, entry.GetOwner())
			assert.Equal(t, group.Name, entry.GetGroup())
			assert.Equal(t, uint32(0o644), entry.GetMode())
		})
	}
}

func TestGetFileOwnership_CurrentUser(t *testing.T) {
	t.Parallel()

	cur, err := user.Current()
	if err != nil {
		t.Skipf("unable to determine current user: %v", err)
	}

	expectedOwner := cur.Uid
	if u, err := user.LookupId(cur.Uid); err == nil {
		expectedOwner = u.Username
	}

	expectedGroup := cur.Gid
	if g, err := user.LookupGroupId(cur.Gid); err == nil {
		expectedGroup = g.Name
	}

	uid64, err := strconv.ParseUint(cur.Uid, 10, 32)
	require.NoError(t, err)
	gid64, err := strconv.ParseUint(cur.Gid, 10, 32)
	require.NoError(t, err)

	info := fsmodel.EntryInfo{
		UID: uint32(uid64),
		GID: uint32(gid64),
	}

	owner, group := getFileOwnership(info)
	assert.Equal(t, expectedOwner, owner)
	assert.Equal(t, expectedGroup, group)
}
