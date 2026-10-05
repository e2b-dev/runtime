package storage

import (
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeHMAC(t *testing.T) {
	t.Parallel()

	t.Run("Deterministic", func(t *testing.T) {
		t.Parallel()

		key := []byte("test-key")
		path := "templates/abc/layer.tar"
		expires := int64(1700000000)

		h1 := ComputeUploadHMAC(key, path, expires)
		h2 := ComputeUploadHMAC(key, path, expires)

		assert.Equal(t, h1, h2)
		assert.NotEmpty(t, h1)
	})

	t.Run("DifferentInputs", func(t *testing.T) {
		t.Parallel()

		key := []byte("test-key")
		expires := int64(1700000000)

		h1 := ComputeUploadHMAC(key, "path-a", expires)
		h2 := ComputeUploadHMAC(key, "path-b", expires)

		assert.NotEqual(t, h1, h2, "different paths should produce different HMACs")

		h3 := ComputeUploadHMAC(key, "path-a", expires+1)

		assert.NotEqual(t, h1, h3, "different expires should produce different HMACs")

		h4 := ComputeUploadHMAC([]byte("other-key"), "path-a", expires)

		assert.NotEqual(t, h1, h4, "different keys should produce different HMACs")
	})
}

func TestValidateUploadToken(t *testing.T) {
	t.Parallel()

	validKey := []byte("secret")
	validPath := "file.txt"
	validExpiresSec := time.Now().Add(5 * time.Minute).Unix()
	validToken := ComputeUploadHMAC(validKey, validPath, validExpiresSec)

	expiredExpiresSec := time.Now().Add(-1 * time.Minute).Unix()
	expiredToken := ComputeUploadHMAC(validKey, validPath, expiredExpiresSec)

	tests := []struct {
		name    string
		key     []byte
		path    string
		expires int64
		token   string
		want    bool
	}{
		{
			name:    "Valid",
			key:     validKey,
			path:    validPath,
			expires: validExpiresSec,
			token:   validToken,
			want:    true,
		},
		{
			name:    "Expired",
			key:     validKey,
			path:    validPath,
			expires: expiredExpiresSec,
			token:   expiredToken,
			want:    false,
		},
		{
			name:    "WrongToken",
			key:     validKey,
			path:    validPath,
			expires: validExpiresSec,
			token:   "bad-token",
			want:    false,
		},
		{
			name:    "WrongPath",
			key:     validKey,
			path:    "tampered.txt",
			expires: validExpiresSec,
			token:   validToken,
			want:    false,
		},
		{
			name:    "WrongExpires",
			key:     validKey,
			path:    validPath,
			expires: validExpiresSec + 10,
			token:   validToken,
			want:    false,
		},
		{
			name:    "WrongKey",
			key:     []byte("wrong-key"),
			path:    validPath,
			expires: validExpiresSec,
			token:   validToken,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ValidateUploadToken(tt.key, tt.path, tt.expires, tt.token)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUploadSignedURL(t *testing.T) {
	t.Parallel()

	t.Run("NotConfigured", func(t *testing.T) {
		t.Parallel()

		p := newTempProvider(t)
		// uploadURL and hmacKey are unset by default.

		_, err := p.UploadSignedURL(t.Context(), "file.txt", 5*time.Minute)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no local upload endpoint configured")
	})

	t.Run("Configured", func(t *testing.T) {
		t.Parallel()

		p := newTempProvider(t)
		hmacKey := []byte("test-key-32-bytes-long-enough!!")
		p.uploadURL = "http://localhost:5008"
		p.hmacKey = hmacKey

		signedURL, err := p.UploadSignedURL(t.Context(), "templates/abc/layer.tar", 5*time.Minute)
		require.NoError(t, err)
		assert.Nil(t, signedURL.Headers)

		// Parse the URL and verify structure.
		u, err := url.Parse(signedURL.URL)
		require.NoError(t, err)

		assert.Equal(t, "http", u.Scheme)
		assert.Equal(t, "localhost:5008", u.Host)
		assert.Equal(t, "/upload", u.Path)

		q := u.Query()
		assert.Equal(t, "templates/abc/layer.tar", q.Get("path"))
		assert.NotEmpty(t, q.Get("expires"))
		assert.NotEmpty(t, q.Get("token"))

		// Verify the token is valid.
		expires, err := strconv.ParseInt(q.Get("expires"), 10, 64)
		require.NoError(t, err)
		assert.True(t, ValidateUploadToken(hmacKey, q.Get("path"), expires, q.Get("token")))

		// Verify expires is in the future.
		assert.Greater(t, expires, time.Now().Unix())
	})

	t.Run("PathWithSpecialChars", func(t *testing.T) {
		t.Parallel()

		p := newTempProvider(t)
		p.uploadURL = "http://localhost:5008"
		p.hmacKey = []byte("key")

		signedURL, err := p.UploadSignedURL(t.Context(), "path with spaces/file name.tar", 5*time.Minute)
		require.NoError(t, err)

		u, err := url.Parse(signedURL.URL)
		require.NoError(t, err)

		// Query().Get() returns the decoded value.
		assert.Equal(t, "path with spaces/file name.tar", u.Query().Get("path"))
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		p := newTempProvider(t)
		hmacKey := []byte("round-trip-key-for-testing-12345")
		p.uploadURL = "http://localhost:5008"
		p.hmacKey = hmacKey

		paths := []string{
			"simple.txt",
			"nested/dir/file.tar.gz",
			"path with spaces/file.txt",
			"special-chars/file+name=value&other.bin",
		}

		for _, path := range paths {
			t.Run(path, func(t *testing.T) {
				t.Parallel()

				signedURL, err := p.UploadSignedURL(t.Context(), path, 10*time.Minute)
				require.NoError(t, err)

				// Parse the signed URL as a client would receive it.
				u, err := url.Parse(signedURL.URL)
				require.NoError(t, err)

				q := u.Query()
				gotPath := q.Get("path")
				gotExpires, err := strconv.ParseInt(q.Get("expires"), 10, 64)
				require.NoError(t, err)
				gotToken := q.Get("token")

				// The path should survive URL encoding round-trip.
				assert.Equal(t, path, gotPath)

				// The token should validate with the same key.
				assert.True(t, ValidateUploadToken(hmacKey, gotPath, gotExpires, gotToken),
					"token generated by UploadSignedURL should validate correctly")
			})
		}
	})
}
