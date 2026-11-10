package api

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postMultipart uploads one part per file, in order, each named by its path.
func postMultipart(t *testing.T, files [][2]string) int {
	t.Helper()

	api, currentUser := newComposeTestAPI(t)
	var body bytes.Buffer
	mp := multipart.NewWriter(&body)
	for _, f := range files {
		part, err := mp.CreateFormFile("file", f[0])
		require.NoError(t, err)
		_, err = part.Write([]byte(f[1]))
		require.NoError(t, err)
	}
	require.NoError(t, mp.Close())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files", &body)
	req.Header.Set("Content-Type", mp.FormDataContentType())
	w := httptest.NewRecorder()
	api.PostFiles(w, req, PostFilesParams{Username: &currentUser.Username})

	return w.Code
}

// A part naming a file this request already wrote through another name is
// refused before it truncates that file.
func TestPostFiles_MultipartRefusesAnotherNameForAWrittenFile(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		link     func(oldname, newname string) error
		optional bool
	}{
		{name: "hard link", link: os.Link},
		// An account without the privilege to create one skips it.
		{name: "symbolic link", link: os.Symlink, optional: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			orig := filepath.Join(dir, "orig.txt")
			alias := filepath.Join(dir, "alias.txt")
			require.NoError(t, os.WriteFile(orig, []byte("old"), 0o644))
			err := tc.link(orig, alias)
			if err != nil && tc.optional {
				t.Skipf("cannot create a %s here: %v", tc.name, err)
			}
			require.NoError(t, err)

			code := postMultipart(t, [][2]string{{orig, "first"}, {alias, "second"}})

			assert.Equal(t, http.StatusBadRequest, code)
			got, err := os.ReadFile(orig)
			require.NoError(t, err)
			assert.Equal(t, "first", string(got), "the second part truncated the first one's file")
		})
	}
}

// Distinct files in one request are all written.
func TestPostFiles_MultipartWritesDistinctFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	a, b := filepath.Join(dir, "a.txt"), filepath.Join(dir, "b.txt")

	require.Equal(t, http.StatusOK, postMultipart(t, [][2]string{{a, "first"}, {b, "second"}}))
	for path, want := range map[string]string{a: "first", b: "second"} {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, want, string(got))
	}
}
