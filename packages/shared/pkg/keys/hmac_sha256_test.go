package keys

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHMACSha256Hashing_ValidHash(t *testing.T) {
	t.Parallel()
	key := []byte("test-key")
	hasher := NewHMACSHA256Hashing(key)
	content := []byte("hello world")
	expectedHash := "18c4b268f0bbf8471eda56af3e70b1d4613d734dc538b4940b59931c412a1591"
	actualHash, err := hasher.Hash(content)
	require.NoError(t, err)

	assert.Equal(t, expectedHash, actualHash)
}

func TestHMACSha256Hashing_EmptyContent(t *testing.T) {
	t.Parallel()
	key := []byte("test-key")
	hasher := NewHMACSHA256Hashing(key)
	content := []byte("")
	expectedHash := "2711cc23e9ab1b8a9bc0fe991238da92671624a9ebdaf1c1abec06e7e9a14f9b"
	actualHash, err := hasher.Hash(content)
	require.NoError(t, err)

	assert.Equal(t, expectedHash, actualHash)
}

func TestHMACSha256Hashing_DifferentKey(t *testing.T) {
	t.Parallel()
	key := []byte("test-key")
	hasher := NewHMACSHA256Hashing(key)
	differentKeyHasher := NewHMACSHA256Hashing([]byte("different-key"))
	content := []byte("hello world")

	hashWithOriginalKey, err := hasher.Hash(content)
	require.NoError(t, err)

	hashWithDifferentKey, err := differentKeyHasher.Hash(content)
	require.NoError(t, err)

	assert.NotEqual(t, hashWithOriginalKey, hashWithDifferentKey, "hashes with different keys should not match")
}

func TestHMACSha256Hashing_IdenticalResult(t *testing.T) {
	t.Parallel()
	key := []byte("placeholder-hashing-key")
	content := []byte("test content for hashing")

	mac := hmac.New(sha256.New, key)
	mac.Write(content)
	expectedResult := hex.EncodeToString(mac.Sum(nil))

	hasher := NewHMACSHA256Hashing(key)
	actualResult, err := hasher.Hash(content)
	require.NoError(t, err)

	assert.Equal(t, expectedResult, actualResult)
}
