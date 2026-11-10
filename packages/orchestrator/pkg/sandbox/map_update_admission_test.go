//go:build linux

package sandbox

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type updateStoppingSubscriber struct {
	insertRecorder

	onStopping func(*Sandbox)
}

func (s *updateStoppingSubscriber) OnStopping(_ context.Context, sbx *Sandbox) {
	s.onStopping(sbx)
}

func TestMapStoppingJoinsUpdatesBeforeNotification(t *testing.T) {
	t.Parallel()

	for _, reserved := range []bool{false, true} {
		name := "delete_or_pause"
		if reserved {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				m := NewSandboxesMap()
				sbx := testMapSandbox(t, "update-owner")
				require.NoError(t, m.MarkRunning(t.Context(), sbx))
				entered, release := make(chan struct{}), make(chan struct{})
				finish := sync.OnceFunc(func() { close(release) })
				defer finish()
				var returned atomic.Bool
				updateDone := make(chan error, 1)
				go func() {
					updateDone <- sbx.RunUpdate(func() error {
						close(entered)
						<-release
						// MarkStopping must not hold the registry or live-map lock
						// while joining an update that still uses those indexes.
						_, _ = m.Get(sbx.Runtime.SandboxID)
						r, err := m.Reserve("unrelated-sandbox")
						if err != nil {
							return err
						}
						r.Release()
						returned.Store(true)

						return nil
					})
				}()
				<-entered
				notified := make(chan struct{})
				m.Subscribe(&updateStoppingSubscriber{onStopping: func(got *Sandbox) {
					require.Same(t, sbx, got)
					assert.True(t, returned.Load(), "subscriber began teardown under an admitted update")
					close(notified)
				}})
				stopDone := make(chan error, 1)
				go func() {
					switch {
					case reserved:
						r, err := m.MarkStoppingReserved(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID)
						if r != nil {
							r.Release()
						}
						stopDone <- err
					case !m.MarkStopping(t.Context(), sbx.Runtime.SandboxID, sbx.LifecycleID):
						stopDone <- ErrSandboxAlreadyRunning
					default:
						stopDone <- nil
					}
				}()
				synctest.Wait()
				_, live := m.Get(sbx.Runtime.SandboxID)
				require.False(t, live)
				select {
				case <-notified:
					t.Error("OnStopping ran before the active update returned")
				default:
				}
				select {
				case err := <-stopDone:
					t.Errorf("stopping returned before the active update: %v", err)
					stopDone <- err
				default:
				}
				lateCalls := 0
				lateDone := make(chan error, 1)
				go func() {
					lateDone <- sbx.RunUpdate(func() error {
						lateCalls++

						return nil
					})
				}()
				finish()
				require.NoError(t, <-updateDone)
				require.NoError(t, <-stopDone)
				require.ErrorIs(t, <-lateDone, ErrSandboxUpdateClosed)
				require.Zero(t, lateCalls)
				<-notified
			})
		})
	}
}

func TestMapStaleStoppingDoesNotCloseLiveUpdates(t *testing.T) {
	t.Parallel()

	m := NewSandboxesMap()
	sbx := testMapSandbox(t, "current-owner")
	require.NoError(t, m.MarkRunning(t.Context(), sbx))
	require.False(t, m.MarkStopping(t.Context(), sbx.Runtime.SandboxID, "old-owner"))
	_, err := m.MarkStoppingReserved(t.Context(), sbx.Runtime.SandboxID, "old-owner")
	require.Error(t, err)
	called := false
	require.NoError(t, sbx.RunUpdate(func() error {
		called = true

		return nil
	}))
	require.True(t, called)
}
