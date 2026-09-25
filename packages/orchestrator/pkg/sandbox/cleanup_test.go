//go:build linux

package sandbox

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCleanupRunAndAddRace(t *testing.T) {
	t.Parallel()

	// This test verifies that a cleanup function added concurrently with Run()
	// is always executed exactly once — either by Run() or inline by Add().
	// Before the fix, there was a race window where hasRun was set before
	// acquiring the lock, causing Add() to see hasRun=false, wait for the lock,
	// then append f after Run() had already finished iterating the list.
	// The result was that f was never executed.
	for i := 0; i < 1000; i++ {
		c := NewCleanup()
		ctx := context.Background()

		var executed atomic.Int32

		// Pre-add a function so the cleanup list is non-empty.
		c.Add(ctx, func(_ context.Context) error {
			return nil
		})

		var wg sync.WaitGroup
		wg.Add(2)

		// goroutine A: Run cleanup
		go func() {
			defer wg.Done()
			c.Run(ctx)
		}()

		// goroutine B: Add a new cleanup function concurrently
		go func() {
			defer wg.Done()
			c.Add(ctx, func(_ context.Context) error {
				executed.Add(1)
				return nil
			})
		}()

		wg.Wait()

		if n := executed.Load(); n != 1 {
			t.Fatalf("iteration %d: cleanup function executed %d times, want exactly 1", i, n)
		}
	}
}

func TestCleanupAddAfterRun(t *testing.T) {
	t.Parallel()

	c := NewCleanup()
	ctx := context.Background()

	c.Run(ctx)

	var executed bool
	c.Add(ctx, func(_ context.Context) error {
		executed = true
		return nil
	})

	if !executed {
		t.Fatal("cleanup function added after Run() should be executed immediately")
	}
}

func TestCleanupPriorityRunAndAddRace(t *testing.T) {
	t.Parallel()

	for i := 0; i < 1000; i++ {
		c := NewCleanup()
		ctx := context.Background()

		var executed atomic.Int32

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			c.Run(ctx)
		}()

		go func() {
			defer wg.Done()
			c.AddPriority(ctx, func(_ context.Context) error {
				executed.Add(1)
				return nil
			})
		}()

		wg.Wait()

		if n := executed.Load(); n != 1 {
			t.Fatalf("iteration %d: priority cleanup function executed %d times, want exactly 1", i, n)
		}
	}
}
