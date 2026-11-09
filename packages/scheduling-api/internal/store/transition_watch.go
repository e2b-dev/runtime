package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// watchReadTimeout bounds one read of a watch, independent of the callers.
const watchReadTimeout = 5 * time.Second

func transitionPollDelay(age time.Duration) time.Duration {
	switch {
	case age < 100*time.Millisecond:
		return 25 * time.Millisecond
	case age < 500*time.Millisecond:
		return 50 * time.Millisecond
	case age < time.Second:
		return 100 * time.Millisecond
	case age < 2*time.Second:
		return 200 * time.Millisecond
	default:
		return 500 * time.Millisecond
	}
}

type watchKey struct {
	projectID    uuid.UUID
	actorID      uuid.UUID
	transitionID uuid.UUID
}

// transitionWatch reads one transition for all callers of WaitForTransition
// on this Store that wait for it. One goroutine reads, the callers wait on
// the channels.
type transitionWatch struct {
	cancel context.CancelFunc
	// Closed after the first read that found the transition active.
	observed chan struct{}
	// Closed after result and err are set: the transition ended, or a read
	// failed.
	done   chan struct{}
	result WaitResult
	err    error
	// Closed when the goroutine has returned and the watch is no longer in
	// Store.watches.
	exited chan struct{}

	// Guarded by Store.watchMu.
	subscribers int
	stopping    bool
}

// WaitForTransition waits up to maxWait for the transition to end. The
// callers that wait for the same transition on this Store share one watch,
// which reads the run at once and then after transitionPollDelay of the
// transition age. A caller does not start a read of its own: when maxWait
// passes, it returns WaitWaiting from the last read, which can lag by a poll
// delay plus the read time. Before the first read has ended, the caller waits for it even
// after maxWait. The watch holds no connection between reads and stops when
// its last caller returns.
func (s *Store) WaitForTransition(ctx context.Context, projectID, actorID, transitionID uuid.UUID, maxWait time.Duration) (WaitResult, error) {
	return telemetry.Observe1(ctx, tracer, "store.WaitForTransition", func(ctx context.Context) (WaitResult, error) {
		if err := ctx.Err(); err != nil {
			return WaitResult{}, fmt.Errorf("wait for transition %s: %w", actorID, err)
		}
		budget := time.NewTimer(maxWait)
		defer budget.Stop()

		w, err := s.joinWatch(ctx, watchKey{projectID: projectID, actorID: actorID, transitionID: transitionID})
		if err != nil {
			return WaitResult{}, fmt.Errorf("wait for transition %s: %w", actorID, err)
		}
		defer s.leaveWatch(w)

		observed := w.observed
		expired := false
		for {
			select {
			case <-w.done:
				return w.outcome(actorID)
			case <-ctx.Done():
				return WaitResult{}, fmt.Errorf("wait for transition %s: %w", actorID, ctx.Err())
			case <-observed:
				observed = nil
			case <-budget.C:
				expired = true
			}
			if expired && observed == nil {
				select {
				case <-w.done:
					return w.outcome(actorID)
				default:
					return WaitResult{Outcome: WaitWaiting}, nil
				}
			}
		}
	})
}

func (w *transitionWatch) outcome(actorID uuid.UUID) (WaitResult, error) {
	if w.err != nil {
		return WaitResult{}, fmt.Errorf("wait for transition %s: %w", actorID, w.err)
	}

	return w.result, nil
}

// joinWatch adds the caller to the watch of key, and starts the watch when
// there is none. A watch that is stopping is not joined: joinWatch waits for
// it to return, so that a key has at most one read at a time.
func (s *Store) joinWatch(ctx context.Context, key watchKey) (*transitionWatch, error) {
	for {
		s.watchMu.Lock()
		w, ok := s.watches[key]
		if !ok {
			watchCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			w = &transitionWatch{
				cancel:      cancel,
				observed:    make(chan struct{}),
				done:        make(chan struct{}),
				exited:      make(chan struct{}),
				subscribers: 1,
			}
			s.watches[key] = w
			s.watchMu.Unlock()
			go s.runWatch(watchCtx, key, w)

			return w, nil
		}
		if !w.stopping {
			w.subscribers++
			s.watchMu.Unlock()

			return w, nil
		}
		s.watchMu.Unlock()

		select {
		case <-w.exited:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// leaveWatch removes the caller from the watch. The last caller stops the
// watch, and returns after its goroutine has returned.
func (s *Store) leaveWatch(w *transitionWatch) {
	s.watchMu.Lock()
	w.subscribers--
	last := w.subscribers == 0
	if last {
		w.stopping = true
	}
	s.watchMu.Unlock()

	if last {
		w.cancel()
		<-w.exited
	}
}

func (s *Store) runWatch(ctx context.Context, key watchKey, w *transitionWatch) {
	defer func() {
		s.watchMu.Lock()
		if s.watches[key] == w {
			delete(s.watches, key)
		}
		s.watchMu.Unlock()
		close(w.exited)
	}()

	observed := false
	for {
		read, err := s.readTransition(ctx, key)
		if ctx.Err() != nil {
			return
		}
		if err != nil || read.result.Outcome != WaitWaiting {
			// The watch leaves the map before callers see the result, so a
			// later caller reads the run again.
			s.watchMu.Lock()
			if s.watches[key] == w {
				delete(s.watches, key)
			}
			w.result, w.err = read.result, err
			close(w.done)
			s.watchMu.Unlock()

			return
		}
		if !observed {
			observed = true
			close(w.observed)
		}

		timer := time.NewTimer(s.pollDelay(read.age))
		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

type transitionRead struct {
	result WaitResult
	// Set only when the transition is active.
	age time.Duration
}

// readTransition starts a new trace for each read, linked to the call that
// started the watch: the watch can outlive that call.
func (s *Store) readTransition(ctx context.Context, key watchKey) (transitionRead, error) {
	return telemetry.Observe1(ctx, tracer, "store.WatchTransition", func(ctx context.Context) (transitionRead, error) {
		ctx, cancel := context.WithTimeout(ctx, watchReadTimeout)
		defer cancel()

		run, err := s.reads.WatchTransition(ctx, queries.WatchTransitionParams{ActorID: key.actorID, ProjectID: key.projectID})
		state := run.State
		switch {
		case isNoRows(err):
			return transitionRead{result: WaitResult{Outcome: WaitNotFound}}, nil
		case err != nil:
			return transitionRead{}, fmt.Errorf("read: %w", err)
		case run.TransitionID == nil || *run.TransitionID != key.transitionID:
			return transitionRead{result: WaitResult{Outcome: WaitUnknown}}, nil
		case state == StateGone:
			result := WaitResult{Outcome: WaitFinished}
			if run.FailureMessage != nil {
				result.Failure = &Failure{Message: *run.FailureMessage}
				if run.FailureCode != nil {
					result.Failure.Code = *run.FailureCode
				}
			}

			return transitionRead{result: result}, nil
		case state == StateRunning && TransitionAction(*run.TransitionAction) == ActionPause:
			return transitionRead{result: WaitResult{Outcome: WaitRestored}}, nil
		case state != StatePausing && state != StateKilling && state != StateSnapshotting:
			return transitionRead{}, fmt.Errorf("transition %s in state %s", key.transitionID, state)
		}

		return transitionRead{
			result: WaitResult{Outcome: WaitWaiting},
			age:    time.Duration(run.TransitionAgeUs) * time.Microsecond,
		}, nil
	}, trace.WithNewRoot(), trace.WithLinks(trace.LinkFromContext(ctx)))
}
