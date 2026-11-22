package authservice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

type countingIdentityLookup struct {
	mu      sync.Mutex
	calls   atomic.Int32
	results map[string]struct {
		id  uuid.UUID
		err error
	}
}

func (l *countingIdentityLookup) GetUserIdentity(_ context.Context, iss, sub string) (uuid.UUID, error) {
	l.calls.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	res, ok := l.results[iss+"|"+sub]
	if !ok {
		return uuid.Nil, auth.ErrIdentityNotFound
	}

	return res.id, res.err
}

func TestCachingIdentityLookup_CachesSuccess(t *testing.T) {
	t.Parallel()

	userID := uuid.New()
	base := &countingIdentityLookup{results: map[string]struct {
		id  uuid.UUID
		err error
	}{
		"iss|sub": {id: userID},
	}}
	lookup := newCachingIdentityLookup(base)

	for range 5 {
		got, err := lookup.GetUserIdentity(t.Context(), "iss", "sub")
		require.NoError(t, err)
		require.Equal(t, userID, got)
	}

	require.Equal(t, int32(1), base.calls.Load(), "underlying calls")
}

func TestCachingIdentityLookup_DoesNotCacheNotFound(t *testing.T) {
	t.Parallel()

	base := &countingIdentityLookup{results: map[string]struct {
		id  uuid.UUID
		err error
	}{}}
	lookup := newCachingIdentityLookup(base)

	for range 3 {
		_, err := lookup.GetUserIdentity(t.Context(), "iss", "sub")
		require.ErrorIs(t, err, auth.ErrIdentityNotFound)
	}

	require.Equal(t, int32(3), base.calls.Load(), "underlying calls (no negative caching)")
}

func TestCachingIdentityLookup_DoesNotCacheError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("transient db error")
	base := &countingIdentityLookup{results: map[string]struct {
		id  uuid.UUID
		err error
	}{
		"iss|sub": {err: sentinel},
	}}
	lookup := newCachingIdentityLookup(base)

	for range 3 {
		_, err := lookup.GetUserIdentity(t.Context(), "iss", "sub")
		require.ErrorIs(t, err, sentinel)
	}

	require.Equal(t, int32(3), base.calls.Load(), "underlying calls (no error caching)")
}

func TestCachingIdentityLookup_SeparateKeysPerIssuerAndSubject(t *testing.T) {
	t.Parallel()

	a, b := uuid.New(), uuid.New()
	base := &countingIdentityLookup{results: map[string]struct {
		id  uuid.UUID
		err error
	}{
		"iss-a|sub": {id: a},
		"iss-b|sub": {id: b},
	}}
	lookup := newCachingIdentityLookup(base)

	got, err := lookup.GetUserIdentity(t.Context(), "iss-a", "sub")
	require.NoError(t, err, "iss-a")
	require.Equal(t, a, got, "iss-a")

	got, err = lookup.GetUserIdentity(t.Context(), "iss-b", "sub")
	require.NoError(t, err, "iss-b")
	require.Equal(t, b, got, "iss-b")

	require.Equal(t, int32(2), base.calls.Load(), "underlying calls")
}
