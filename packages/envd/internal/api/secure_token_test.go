package api

import (
	"sync"
	"testing"

	"github.com/awnumar/memguard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecureTokenSetAndEquals(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}

	// Initially not set
	assert.False(t, st.IsSet(), "token should not be set initially")
	assert.False(t, st.Equals("any-token"), "equals should return false when not set")

	// Set token
	err := st.Set([]byte("test-token"))
	require.NoError(t, err)
	assert.True(t, st.IsSet(), "token should be set after Set()")
	assert.True(t, st.Equals("test-token"), "equals should return true for correct token")
	assert.False(t, st.Equals("wrong-token"), "equals should return false for wrong token")
	assert.False(t, st.Equals(""), "equals should return false for empty token")
}

func TestSecureTokenReplace(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}

	// Set initial token
	err := st.Set([]byte("first-token"))
	require.NoError(t, err)
	assert.True(t, st.Equals("first-token"))

	// Replace with new token (old one should be destroyed)
	err = st.Set([]byte("second-token"))
	require.NoError(t, err)
	assert.True(t, st.Equals("second-token"), "should match new token")
	assert.False(t, st.Equals("first-token"), "should not match old token")
}

func TestSecureTokenDestroy(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}

	// Set and then destroy
	err := st.Set([]byte("test-token"))
	require.NoError(t, err)
	assert.True(t, st.IsSet())

	st.Destroy()
	assert.False(t, st.IsSet(), "token should not be set after Destroy()")
	assert.False(t, st.Equals("test-token"), "equals should return false after Destroy()")

	// Destroy on already destroyed should be safe
	st.Destroy()
	assert.False(t, st.IsSet())

	// Nil receiver should be safe
	var nilToken *SecureToken
	assert.False(t, nilToken.IsSet(), "nil receiver should return false for IsSet()")
	assert.False(t, nilToken.Equals("anything"), "nil receiver should return false for Equals()")
	assert.False(t, nilToken.EqualsSecure(st), "nil receiver should return false for EqualsSecure()")
	nilToken.Destroy() // should not panic

	_, err = nilToken.Bytes()
	require.ErrorIs(t, err, ErrTokenNotSet, "nil receiver should return ErrTokenNotSet for Bytes()")
}

func TestSecureTokenBytes(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}

	// Bytes should return error when not set
	_, err := st.Bytes()
	require.ErrorIs(t, err, ErrTokenNotSet)

	// Set token and get bytes
	err = st.Set([]byte("test-token"))
	require.NoError(t, err)

	bytes, err := st.Bytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("test-token"), bytes)

	// Zero out the bytes (as caller should do)
	memguard.WipeBytes(bytes)

	// Original should still be intact
	assert.True(t, st.Equals("test-token"), "original token should still work after zeroing copy")

	// After destroy, bytes should fail
	st.Destroy()
	_, err = st.Bytes()
	assert.ErrorIs(t, err, ErrTokenNotSet)
}

func TestSecureTokenConcurrentAccess(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}
	err := st.Set([]byte("initial-token"))
	require.NoError(t, err)

	var wg sync.WaitGroup
	const numGoroutines = 100

	// Concurrent reads
	for range numGoroutines {
		wg.Go(func() {
			st.IsSet()
			st.Equals("initial-token")
		})
	}

	// Concurrent writes
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			st.Set([]byte("token-" + string(rune('a'+idx))))
		}(i)
	}

	wg.Wait()

	// Should still be in a valid state
	assert.True(t, st.IsSet())
}

func TestSecureTokenEmptyToken(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}

	// Setting empty token should return an error
	err := st.Set([]byte{})
	require.ErrorIs(t, err, ErrTokenEmpty)
	assert.False(t, st.IsSet(), "token should not be set after empty token error")

	// Setting nil should also return an error
	err = st.Set(nil)
	require.ErrorIs(t, err, ErrTokenEmpty)
	assert.False(t, st.IsSet(), "token should not be set after nil token error")
}

func TestSecureTokenEmptyTokenDoesNotClearExisting(t *testing.T) {
	t.Parallel()

	st := &SecureToken{}

	// Set a valid token first
	err := st.Set([]byte("valid-token"))
	require.NoError(t, err)
	assert.True(t, st.IsSet())

	// Attempting to set empty token should fail and preserve existing token
	err = st.Set([]byte{})
	require.ErrorIs(t, err, ErrTokenEmpty)
	assert.True(t, st.IsSet(), "existing token should be preserved after empty token error")
	assert.True(t, st.Equals("valid-token"), "existing token value should be unchanged")
}

func TestSecureTokenUnmarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("unmarshals valid JSON string", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.UnmarshalJSON([]byte(`"my-secret-token"`))
		require.NoError(t, err)
		assert.True(t, st.IsSet())
		assert.True(t, st.Equals("my-secret-token"))
	})

	t.Run("returns error for empty string", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.UnmarshalJSON([]byte(`""`))
		require.ErrorIs(t, err, ErrTokenEmpty)
		assert.False(t, st.IsSet())
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.UnmarshalJSON([]byte(`not-valid-json`))
		require.Error(t, err)
		assert.False(t, st.IsSet())
	})

	t.Run("replaces existing token", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.Set([]byte("old-token"))
		require.NoError(t, err)

		err = st.UnmarshalJSON([]byte(`"new-token"`))
		require.NoError(t, err)
		assert.True(t, st.Equals("new-token"))
		assert.False(t, st.Equals("old-token"))
	})

	t.Run("wipes input buffer after parsing", func(t *testing.T) {
		t.Parallel()
		// Create a buffer with a known token
		input := []byte(`"secret-token-12345"`)
		original := make([]byte, len(input))
		copy(original, input)

		st := &SecureToken{}
		err := st.UnmarshalJSON(input)
		require.NoError(t, err)

		// Verify the token was stored correctly
		assert.True(t, st.Equals("secret-token-12345"))

		// Verify the input buffer was wiped (all zeros)
		for i, b := range input {
			assert.Equal(t, byte(0), b, "byte at position %d should be zero, got %d", i, b)
		}
	})

	t.Run("wipes input buffer on error", func(t *testing.T) {
		t.Parallel()
		// Create a buffer with an empty token (will error)
		input := []byte(`""`)

		st := &SecureToken{}
		err := st.UnmarshalJSON(input)
		require.Error(t, err)

		// Verify the input buffer was still wiped
		for i, b := range input {
			assert.Equal(t, byte(0), b, "byte at position %d should be zero, got %d", i, b)
		}
	})

	t.Run("rejects escape sequences", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.UnmarshalJSON([]byte(`"token\nwith\nnewlines"`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "escape sequence")
		assert.False(t, st.IsSet())
	})
}

func TestSecureTokenSetWipesInput(t *testing.T) {
	t.Parallel()

	t.Run("wipes input buffer after storing", func(t *testing.T) {
		t.Parallel()
		// Create a buffer with a known token
		input := []byte("my-secret-token")
		original := make([]byte, len(input))
		copy(original, input)

		st := &SecureToken{}
		err := st.Set(input)
		require.NoError(t, err)

		// Verify the token was stored correctly
		assert.True(t, st.Equals("my-secret-token"))

		// Verify the input buffer was wiped (all zeros)
		for i, b := range input {
			assert.Equal(t, byte(0), b, "byte at position %d should be zero, got %d", i, b)
		}
	})
}

func TestSecureTokenTakeFrom(t *testing.T) {
	t.Parallel()

	t.Run("transfers token from source to destination", func(t *testing.T) {
		t.Parallel()
		src := &SecureToken{}
		err := src.Set([]byte("source-token"))
		require.NoError(t, err)

		dst := &SecureToken{}
		dst.TakeFrom(src)

		assert.True(t, dst.IsSet())
		assert.True(t, dst.Equals("source-token"))
		assert.False(t, src.IsSet(), "source should be empty after transfer")
	})

	t.Run("replaces existing destination token", func(t *testing.T) {
		t.Parallel()
		src := &SecureToken{}
		err := src.Set([]byte("new-token"))
		require.NoError(t, err)

		dst := &SecureToken{}
		err = dst.Set([]byte("old-token"))
		require.NoError(t, err)

		dst.TakeFrom(src)

		assert.True(t, dst.Equals("new-token"))
		assert.False(t, dst.Equals("old-token"))
		assert.False(t, src.IsSet())
	})

	t.Run("handles nil source", func(t *testing.T) {
		t.Parallel()
		dst := &SecureToken{}
		err := dst.Set([]byte("existing-token"))
		require.NoError(t, err)

		dst.TakeFrom(nil)

		assert.True(t, dst.IsSet(), "destination should be unchanged with nil source")
		assert.True(t, dst.Equals("existing-token"))
	})

	t.Run("handles empty source", func(t *testing.T) {
		t.Parallel()
		src := &SecureToken{}
		dst := &SecureToken{}
		err := dst.Set([]byte("existing-token"))
		require.NoError(t, err)

		dst.TakeFrom(src)

		assert.False(t, dst.IsSet(), "destination should be cleared when source is empty")
	})

	t.Run("self-transfer is no-op and does not deadlock", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.Set([]byte("token"))
		require.NoError(t, err)

		st.TakeFrom(st)

		assert.True(t, st.IsSet(), "token should remain set after self-transfer")
		assert.True(t, st.Equals("token"), "token value should be unchanged")
	})
}

func TestSecureTokenEqualsSecure(t *testing.T) {
	t.Parallel()

	t.Run("returns true for matching tokens", func(t *testing.T) {
		t.Parallel()
		st1 := &SecureToken{}
		err := st1.Set([]byte("same-token"))
		require.NoError(t, err)

		st2 := &SecureToken{}
		err = st2.Set([]byte("same-token"))
		require.NoError(t, err)

		assert.True(t, st1.EqualsSecure(st2))
		assert.True(t, st2.EqualsSecure(st1))
	})

	t.Run("concurrent TakeFrom and EqualsSecure do not deadlock", func(t *testing.T) {
		t.Parallel()
		// This test verifies the fix for the lock ordering deadlock bug.

		const iterations = 100

		for range iterations {
			a := &SecureToken{}
			err := a.Set([]byte("token-a"))
			require.NoError(t, err)

			b := &SecureToken{}
			err = b.Set([]byte("token-b"))
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(2)

			// Goroutine 1: a.TakeFrom(b)
			go func() {
				defer wg.Done()
				a.TakeFrom(b)
			}()

			// Goroutine 2: b.EqualsSecure(a)
			go func() {
				defer wg.Done()
				b.EqualsSecure(a)
			}()

			wg.Wait()
			// Every live token holds locked memory, so each pair is freed before the next.
			a.Destroy()
			b.Destroy()
		}
	})

	t.Run("returns false for different tokens", func(t *testing.T) {
		t.Parallel()
		st1 := &SecureToken{}
		err := st1.Set([]byte("token-a"))
		require.NoError(t, err)

		st2 := &SecureToken{}
		err = st2.Set([]byte("token-b"))
		require.NoError(t, err)

		assert.False(t, st1.EqualsSecure(st2))
	})

	t.Run("returns false when comparing with nil", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.Set([]byte("token"))
		require.NoError(t, err)

		assert.False(t, st.EqualsSecure(nil))
	})

	t.Run("returns false when other is not set", func(t *testing.T) {
		t.Parallel()
		st1 := &SecureToken{}
		err := st1.Set([]byte("token"))
		require.NoError(t, err)

		st2 := &SecureToken{}

		assert.False(t, st1.EqualsSecure(st2))
	})

	t.Run("returns false when self is not set", func(t *testing.T) {
		t.Parallel()
		st1 := &SecureToken{}

		st2 := &SecureToken{}
		err := st2.Set([]byte("token"))
		require.NoError(t, err)

		assert.False(t, st1.EqualsSecure(st2))
	})

	t.Run("self-comparison returns true when set", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}
		err := st.Set([]byte("token"))
		require.NoError(t, err)

		assert.True(t, st.EqualsSecure(st), "self-comparison should return true and not deadlock")
	})

	t.Run("self-comparison returns false when not set", func(t *testing.T) {
		t.Parallel()
		st := &SecureToken{}

		assert.False(t, st.EqualsSecure(st), "self-comparison on unset token should return false")
	})
}
