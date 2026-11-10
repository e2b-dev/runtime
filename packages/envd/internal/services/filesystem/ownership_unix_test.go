//go:build unix

package filesystem

import (
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
)

func skipWithoutNumericOwner(*testing.T) {}

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
