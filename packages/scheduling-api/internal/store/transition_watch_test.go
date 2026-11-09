package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransitionPollDelay(t *testing.T) {
	t.Parallel()

	for age, want := range map[time.Duration]time.Duration{
		-time.Hour:               25 * time.Millisecond,
		0:                        25 * time.Millisecond,
		100*time.Millisecond - 1: 25 * time.Millisecond,
		100 * time.Millisecond:   50 * time.Millisecond,
		500*time.Millisecond - 1: 50 * time.Millisecond,
		500 * time.Millisecond:   100 * time.Millisecond,
		time.Second - 1:          100 * time.Millisecond,
		time.Second:              200 * time.Millisecond,
		2*time.Second - 1:        200 * time.Millisecond,
		2 * time.Second:          500 * time.Millisecond,
		time.Hour:                500 * time.Millisecond,
	} {
		assert.Equal(t, want, transitionPollDelay(age), "age %s", age)
	}
}

// Callers on one store share the reads; three stores on one database read
// three times, once each.
func TestWaitSharesReads(t *testing.T) {
	t.Parallel()

	t.Run("first read", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicas := make([]*Store, 3)
		pools := make([]*pgxpool.Pool, 3)
		for i := range replicas {
			pools[i] = secondPool(t, pool)
			replicas[i] = New(pool, pools[i])
			replicas[i].pollDelay = fixedDelay(time.Hour)
		}

		lock := lockRuns(t, pool)
		const callers = 100
		results := make([]<-chan waitOutcome, callers)
		for i := range callers {
			results[i] = waitAsync(t.Context(), replicas[i%3], project, actor, tid, time.Microsecond)
		}
		require.Eventually(t, func() bool {
			total := 0
			for _, replica := range replicas {
				subscribers := watchSubscribers(replica)
				if len(subscribers) != 1 {
					return false
				}
				total += subscribers[watchKey{project, actor, tid}]
			}

			return total == callers
		}, 10*time.Second, time.Millisecond, "every caller joined the watch of its store")
		require.NoError(t, lock.Commit(t.Context()))

		for _, result := range results {
			got := <-result
			require.NoError(t, got.err)
			require.Equal(t, WaitResult{Outcome: WaitWaiting}, got.result, "the caller waited for the first read")
		}
		for i, replica := range replicas {
			require.Equal(t, int64(1), pools[i].Stat().AcquireCount(), "one read on replica %d", i)
			require.Empty(t, watchSubscribers(replica))
		}
	})
	t.Run("polls", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)
		const delay = 10 * time.Millisecond
		replica.pollDelay = fixedDelay(delay)

		const callers = 100
		started := time.Now()
		results := make([]<-chan waitOutcome, callers)
		for i := range callers {
			results[i] = waitAsync(t.Context(), replica, project, actor, tid, 300*time.Millisecond)
		}
		for _, result := range results {
			got := <-result
			require.NoError(t, got.err)
			require.Equal(t, WaitResult{Outcome: WaitWaiting}, got.result)
		}
		elapsed := time.Since(started)

		reads := replicaPool.Stat().AcquireCount()
		t.Logf("%d callers, %d reads in %s", callers, reads, elapsed)
		require.GreaterOrEqual(t, reads, int64(2), "the watch polled")
		require.LessOrEqual(t, reads, int64(elapsed/delay)+1, "each read is followed by the poll delay")
		require.Less(t, reads, int64(callers))
		require.Empty(t, watchSubscribers(replica))
	})
	t.Run("keys", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)
		replica.pollDelay = fixedDelay(time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		holder := waitAsync(ctx, replica, project, actor, tid, 5*time.Second)
		requireObserved(t, replica, watchKey{project, actor, tid})

		got, err := replica.WaitForTransition(t.Context(), uuid.New(), actor, tid, time.Second)
		require.NoError(t, err)
		require.Equal(t, WaitResult{Outcome: WaitNotFound}, got, "another project does not join the watch")
		got, err = replica.WaitForTransition(t.Context(), project, actor, uuid.New(), time.Second)
		require.NoError(t, err)
		require.Equal(t, WaitResult{Outcome: WaitUnknown}, got, "another transition does not join the watch")
		require.Equal(t, int64(3), replicaPool.Stat().AcquireCount(), "each key reads on its own")

		cancel()
		require.ErrorIs(t, (<-holder).err, context.Canceled)
		require.Empty(t, watchSubscribers(replica))
	})
}

func TestWaitCallersAreIndependent(t *testing.T) {
	t.Parallel()

	t.Run("budget and cancel", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)
		replica.pollDelay = fixedDelay(5 * time.Millisecond)
		key := watchKey{project, actor, tid}

		creatorCtx, cancelCreator := context.WithCancel(t.Context())
		creator := waitAsync(creatorCtx, replica, project, actor, tid, 5*time.Second)
		requireObserved(t, replica, key)
		replica.watchMu.Lock()
		watch := replica.watches[key]
		replica.watchMu.Unlock()
		other := waitAsync(t.Context(), replica, project, actor, tid, 5*time.Second)
		requireSubscribers(t, replica, key, 2)

		short := <-waitAsync(t.Context(), replica, project, actor, tid, time.Microsecond)
		require.NoError(t, short.err)
		require.Equal(t, WaitResult{Outcome: WaitWaiting}, short.result)
		requireSubscribers(t, replica, key, 2)

		cancelCreator()
		require.ErrorIs(t, (<-creator).err, context.Canceled)
		requireSubscribers(t, replica, key, 1)
		replica.watchMu.Lock()
		require.Same(t, watch, replica.watches[key], "the watch outlives the caller that started it")
		replica.watchMu.Unlock()
		require.Empty(t, other)

		finishTransition(t, st, project, actor, tid, nil, FinishTransitionFinished)
		got := <-other
		require.NoError(t, got.err)
		require.Equal(t, WaitResult{Outcome: WaitFinished}, got.result)
		require.Empty(t, watchSubscribers(replica))
	})
	t.Run("last caller stops the read", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)

		lock := lockRuns(t, pool)
		ctx, cancel := context.WithCancel(t.Context())
		waiter := waitAsync(ctx, replica, project, actor, tid, 5*time.Second)
		require.Eventually(t, func() bool { return replicaPool.Stat().AcquiredConns() == 1 }, 5*time.Second, time.Millisecond,
			"the read waits for the lock")

		cancel()
		select {
		case got := <-waiter:
			require.ErrorIs(t, got.err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("the caller did not return on cancellation")
		}
		require.Empty(t, watchSubscribers(replica), "the watch returned")
		// pgx sends a cancel request and waits for the server to close the
		// connection, which can wait for the lock.
		require.NoError(t, lock.Rollback(t.Context()))
		require.Eventually(t, func() bool { return replicaPool.Stat().AcquiredConns() == 0 }, 5*time.Second, time.Millisecond,
			"the pool closes the connection of the cancelled read")
		require.Equal(t, int64(1), replicaPool.Stat().AcquireCount(), "no read after the last caller")
	})
	t.Run("deadline", func(t *testing.T) {
		t.Parallel()
		st, _ := newStore(t)
		project, actor, tid := activeTransition(t, st)
		holder := waitAsync(t.Context(), st, project, actor, tid, 5*time.Second)
		requireObserved(t, st, watchKey{project, actor, tid})

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := st.WaitForTransition(ctx, project, actor, tid, 5*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Empty(t, holder)
		finishTransition(t, st, project, actor, tid, nil, FinishTransitionFinished)
		require.Equal(t, WaitResult{Outcome: WaitFinished}, (<-holder).result)
	})
}

func TestWaitResultsAreNotCached(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, actor, first := activeTransition(t, st)
	replica := New(pool, secondPool(t, pool))

	const callers = 10
	results := make([]<-chan waitOutcome, callers)
	for i := range callers {
		results[i] = waitAsync(t.Context(), replica, project, actor, first, 5*time.Second)
	}
	requireSubscribers(t, replica, watchKey{project, actor, first}, callers)
	restore(t, st, project, actor, first, time.Second, RestoreRestored)
	for _, result := range results {
		got := <-result
		require.NoError(t, got.err)
		require.Equal(t, WaitResult{Outcome: WaitRestored}, got.result)
	}
	require.Empty(t, watchSubscribers(replica))

	second := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: second, Action: ActionKill}, BeginStarted)
	require.Equal(t, WaitResult{Outcome: WaitUnknown}, waitFor(t, replica, project, actor, first, time.Microsecond),
		"a new transition replaced the restored one")
	require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, replica, project, actor, second, time.Microsecond))

	failure := &Failure{Message: "node unreachable", Code: 14}
	for i := range callers {
		results[i] = waitAsync(t.Context(), replica, project, actor, second, 5*time.Second)
	}
	requireSubscribers(t, replica, watchKey{project, actor, second}, callers)
	finishTransition(t, st, project, actor, second, failure, FinishTransitionFinished)
	for _, result := range results {
		got := <-result
		require.NoError(t, got.err)
		require.Equal(t, WaitResult{Outcome: WaitFinished, Failure: failure}, got.result)
	}
	require.Empty(t, watchSubscribers(replica))
}

func TestWaitFirstRead(t *testing.T) {
	t.Parallel()

	t.Run("short budget", func(t *testing.T) {
		t.Parallel()
		st, _ := newStore(t)
		project, actor, tid := activeTransition(t, st)

		require.Equal(t, WaitResult{Outcome: WaitUnknown}, waitFor(t, st, project, actor, uuid.New(), time.Microsecond))
		require.Equal(t, WaitResult{Outcome: WaitNotFound}, waitFor(t, st, uuid.New(), actor, tid, time.Microsecond))
		require.Equal(t, WaitResult{Outcome: WaitNotFound}, waitFor(t, st, project, uuid.New(), tid, time.Microsecond))
		require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, st, project, actor, tid, time.Microsecond))
	})
	t.Run("budget passes before the first read", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)

		lock := lockRuns(t, pool)
		const callers = 10
		results := make([]<-chan waitOutcome, callers)
		for i := range callers {
			results[i] = waitAsync(t.Context(), replica, project, actor, tid, time.Microsecond)
		}
		requireSubscribers(t, replica, watchKey{project, actor, tid}, callers)
		_, err := lock.Exec(t.Context(), `UPDATE public.scheduling_actor_run SET state = 'gone', gone_at = now(), failure_message = 'gone' WHERE actor_id = $1`, actor)
		require.NoError(t, err)
		require.NoError(t, lock.Commit(t.Context()))
		for _, result := range results {
			got := <-result
			require.NoError(t, got.err)
			require.Equal(t, WaitResult{Outcome: WaitFinished, Failure: &Failure{Message: "gone"}}, got.result)
		}
		require.Equal(t, int64(1), replicaPool.Stat().AcquireCount())
	})
	t.Run("budget after the first read", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)
		replica.pollDelay = fixedDelay(time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		holder := waitAsync(ctx, replica, project, actor, tid, 5*time.Second)
		requireObserved(t, replica, watchKey{project, actor, tid})

		for range 100 {
			require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, replica, project, actor, tid, time.Microsecond))
		}
		require.Equal(t, int64(1), replicaPool.Stat().AcquireCount(), "callers returned the last read")

		cancel()
		require.ErrorIs(t, (<-holder).err, context.Canceled)
	})
	t.Run("read error", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, tid := activeTransition(t, st)
		replica := New(pool, secondPool(t, pool))

		lock := lockRuns(t, pool)
		const callers = 10
		results := make([]<-chan waitOutcome, callers)
		for i := range callers {
			results[i] = waitAsync(t.Context(), replica, project, actor, tid, 5*time.Second)
		}
		requireSubscribers(t, replica, watchKey{project, actor, tid}, callers)
		_, err := lock.Exec(t.Context(), `ALTER TABLE public.scheduling_actor_run RENAME TO scheduling_actor_run_moved`)
		require.NoError(t, err)
		require.NoError(t, lock.Commit(t.Context()))
		for _, result := range results {
			got := <-result
			require.Error(t, got.err)
			require.NotErrorIs(t, got.err, context.Canceled)
			require.NotErrorIs(t, got.err, context.DeadlineExceeded)
		}
		require.Empty(t, watchSubscribers(replica))

		_, err = pool.Exec(t.Context(), `ALTER TABLE public.scheduling_actor_run_moved RENAME TO scheduling_actor_run`)
		require.NoError(t, err)
		require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, replica, project, actor, tid, time.Microsecond),
			"the next call reads again")
	})
}

// A caller that comes while the last caller of a watch stops it waits for the
// watch to return, so that the key never has two reads at a time.
func TestWaitAfterStoppingWatch(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, actor, tid := activeTransition(t, st)
	replicaPool := secondPool(t, pool)
	replica := New(pool, replicaPool)
	key := watchKey{project, actor, tid}
	stopping := &transitionWatch{exited: make(chan struct{}), stopping: true}
	replica.watches[key] = stopping

	ctx, cancel := context.WithCancel(t.Context())
	cancelled := waitAsync(ctx, replica, project, actor, tid, time.Microsecond)
	waiter := waitAsync(t.Context(), replica, project, actor, tid, time.Microsecond)
	cancel()
	require.ErrorIs(t, (<-cancelled).err, context.Canceled)
	require.Empty(t, waiter)
	require.Equal(t, int64(0), replicaPool.Stat().AcquireCount(), "no read while the old watch stops")

	replica.watchMu.Lock()
	delete(replica.watches, key)
	replica.watchMu.Unlock()
	close(stopping.exited)
	got := <-waiter
	require.NoError(t, got.err)
	require.Equal(t, WaitResult{Outcome: WaitWaiting}, got.result)
	require.Equal(t, int64(1), replicaPool.Stat().AcquireCount())
	require.Empty(t, watchSubscribers(replica))
}

// The poll delay follows the transition age that the database reports, so
// that a caller that calls again does not restart the fast polls.
func TestWaitPollDelayFollowsTransitionAge(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, actor, tid := activeTransition(t, st)
	ages := make(chan time.Duration, 10)
	st.pollDelay = func(age time.Duration) time.Duration {
		ages <- age

		return time.Hour
	}

	require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, st, project, actor, tid, time.Microsecond))
	age := <-ages
	require.GreaterOrEqual(t, age, time.Duration(0))
	require.Less(t, age, time.Minute)

	_, err := pool.Exec(t.Context(), `UPDATE public.scheduling_actor_run SET transition_at = now() - interval '10 seconds' WHERE actor_id = $1`, actor)
	require.NoError(t, err)
	for range 2 {
		require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, st, project, actor, tid, time.Microsecond))
		age := <-ages
		require.GreaterOrEqual(t, age, 10*time.Second)
		require.Equal(t, 500*time.Millisecond, transitionPollDelay(age))
	}
}

// Callers join and leave the watches of a few transitions while the
// transitions end and new ones begin. Every call returns, and no watch stays.
func TestWaitJoinLeaveRace(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	replica := New(pool, secondPool(t, pool))
	replica.pollDelay = fixedDelay(time.Millisecond)

	type target struct {
		project, actor uuid.UUID
		mu             sync.Mutex
		tid            uuid.UUID
	}
	targets := make([]*target, 3)
	for i := range targets {
		project, actor, tid := activeTransition(t, st)
		targets[i] = &target{project: project, actor: actor, tid: tid}
	}

	stop := make(chan struct{})
	churnErr := make(chan error, 1)
	go func() {
		churnErr <- func() error {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return nil
				case <-time.After(5 * time.Millisecond):
				}
				tg := targets[i%len(targets)]
				tg.mu.Lock()
				tid := tg.tid
				tg.mu.Unlock()
				if _, err := st.RestoreRunning(t.Context(), tg.project, tg.actor, tid, 0); err != nil {
					return err
				}
				next := uuid.New()
				result, err := st.BeginTransition(t.Context(), TransitionRequest{ProjectID: tg.project, ActorID: tg.actor, TransitionID: next, Action: ActionPause})
				if err != nil {
					return err
				}
				if result.Outcome != BeginStarted {
					return fmt.Errorf("begin: outcome %d", result.Outcome)
				}
				tg.mu.Lock()
				tg.tid = next
				tg.mu.Unlock()
			}
		}()
	}()

	const callers, calls = 50, 40
	outcomes := make(chan waitOutcome, callers*calls)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			for range calls {
				tg := targets[rand.IntN(len(targets))]
				tg.mu.Lock()
				tid := tg.tid
				tg.mu.Unlock()
				ctx, cancel := context.WithTimeout(t.Context(), time.Duration(rand.IntN(4000))*time.Microsecond)
				result, err := replica.WaitForTransition(ctx, tg.project, tg.actor, tid, time.Duration(rand.IntN(3000))*time.Microsecond)
				cancel()
				outcomes <- waitOutcome{result: result, err: err}
			}
		})
	}
	wg.Wait()
	close(stop)
	require.NoError(t, <-churnErr)
	close(outcomes)
	counts := map[WaitOutcome]int{}
	for got := range outcomes {
		if got.err != nil {
			require.ErrorIs(t, got.err, context.DeadlineExceeded)
			counts[0]++

			continue
		}
		require.Contains(t, []WaitOutcome{WaitWaiting, WaitRestored, WaitUnknown}, got.result.Outcome)
		counts[got.result.Outcome]++
	}
	t.Logf("outcomes (0 is a deadline): %v", counts)
	require.Empty(t, watchSubscribers(replica))
}

type waitOutcome struct {
	result WaitResult
	err    error
}

func waitAsync(ctx context.Context, st *Store, project, actor, tid uuid.UUID, maxWait time.Duration) <-chan waitOutcome {
	done := make(chan waitOutcome, 1)
	go func() {
		result, err := st.WaitForTransition(ctx, project, actor, tid, maxWait)
		done <- waitOutcome{result: result, err: err}
	}()

	return done
}

func activeTransition(t *testing.T, st *Store) (uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()

	project, record, _ := runningActor(t, st, uuid.New(), strict)
	tid := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: record.ActorID, TransitionID: tid, Action: ActionPause}, BeginStarted)

	return project, record.ActorID, tid
}

func fixedDelay(d time.Duration) func(time.Duration) time.Duration {
	return func(time.Duration) time.Duration { return d }
}

func watchSubscribers(st *Store) map[watchKey]int {
	st.watchMu.Lock()
	defer st.watchMu.Unlock()

	subscribers := map[watchKey]int{}
	for key, w := range st.watches {
		subscribers[key] = w.subscribers
	}

	return subscribers
}

func requireSubscribers(t *testing.T, st *Store, key watchKey, want int) {
	t.Helper()

	require.Eventually(t, func() bool { return watchSubscribers(st)[key] == want }, 10*time.Second, time.Millisecond,
		"%d callers wait", want)
}

// requireObserved waits until the watch of key has read the transition as
// active.
func requireObserved(t *testing.T, st *Store, key watchKey) {
	t.Helper()

	require.Eventually(t, func() bool {
		st.watchMu.Lock()
		w := st.watches[key]
		st.watchMu.Unlock()
		if w == nil {
			return false
		}
		select {
		case <-w.observed:
			return true
		default:
			return false
		}
	}, 10*time.Second, time.Millisecond)
}

// lockRuns locks scheduling_actor_run until the returned transaction ends, so
// that reads of it wait.
func lockRuns(t *testing.T, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) })
	_, err = tx.Exec(t.Context(), `LOCK TABLE public.scheduling_actor_run IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	return tx
}
