package utils

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromiseSuccess(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (int, error) {
		return 42, nil
	})

	value, err := p.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, value)
}

func TestPromiseError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("test error")
	p := NewPromise(func() (int, error) {
		return 0, expectedErr
	})

	value, err := p.Wait(t.Context())
	require.ErrorIs(t, err, expectedErr)
	assert.Equal(t, 0, value)
}

func TestPromiseDelayedResult(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (string, error) {
		time.Sleep(50 * time.Millisecond)

		return "delayed", nil
	})

	value, err := p.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "delayed", value)
}

func TestPromiseContextCancelled(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (int, error) {
		time.Sleep(1 * time.Second)

		return 42, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err := p.Wait(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPromiseMultipleWaiters(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (int, error) {
		time.Sleep(50 * time.Millisecond)

		return 42, nil
	})

	var wg sync.WaitGroup
	results := make([]int, 5)
	errs := make([]error, 5)

	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = p.Wait(t.Context())
		}(i)
	}

	wg.Wait()

	for i := range 5 {
		require.NoError(t, errs[i])
		assert.Equal(t, 42, results[i])
	}
}

func TestPromiseDone(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (int, error) {
		time.Sleep(50 * time.Millisecond)

		return 42, nil
	})

	select {
	case <-p.Done():
		require.Fail(t, "promise should not be done yet")
	default:
		// expected
	}

	<-p.Done()

	value, err := p.Result()
	require.NoError(t, err)
	assert.Equal(t, 42, value)
}

func TestPromiseResultBeforeResolve(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (int, error) {
		time.Sleep(100 * time.Millisecond)

		return 42, nil
	})

	_, err := p.Result()
	assert.ErrorAs(t, err, &NotSetError{})
}

func TestPromiseResultAfterResolve(t *testing.T) {
	t.Parallel()

	p := NewPromise(func() (int, error) {
		return 42, nil
	})

	<-p.Done()

	value, err := p.Result()
	require.NoError(t, err)
	assert.Equal(t, 42, value)

	// Multiple calls should return the same result
	value2, err2 := p.Result()
	require.NoError(t, err2)
	assert.Equal(t, 42, value2)
}
