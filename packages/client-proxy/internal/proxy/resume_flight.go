package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// resumeAttemptCeiling bounds one resume call: the API sweeps a pending
// reservation after this long, so a longer attempt could no longer be answered.
const resumeAttemptCeiling = 90 * time.Second

// maxDetachedFlights bounds the resume calls a process keeps running after
// every caller of theirs has left. The call under way completes, so a client
// whose timeout is shorter than the resume finds the sandbox awake when it
// comes back, and a caller arriving meanwhile joins it; no further attempt
// starts for nobody, and a flight between attempts ends with its last
// caller. Beyond this many such calls the last caller's departure cancels
// the call instead.
const maxDetachedFlights = 64

const (
	resumeRetryInitialBackoff = 250 * time.Millisecond
	resumeRetryBackoffFactor  = 1.5
	resumeRetryBackoffJitter  = 0.2
	resumeRetryBackoffCap     = 2 * time.Second
	// resumeRetryAttemptFloor is the least useful attempt duration: a
	// retry starts only while now + floor <= deadline, so the budget is
	// never spent on an attempt too short to be answered.
	resumeRetryAttemptFloor = 2 * time.Second
	// flagOffWaitBound is the most of an attempt a caller that may not park
	// waits out: an attempt ending within it is about to answer either way,
	// while a longer wait would hold callers the flag was turned off to
	// release, without a lot slot to bound them.
	flagOffWaitBound = resumeRetryAttemptFloor + 500*time.Millisecond
)

// errAttemptDeadline and errFlightClosed are the causes the flight gives its
// contexts, so an attempt that ended can tell which end it hit. Neither is
// rendered; the page for either says to retry and the log carries the text.
var (
	errAttemptDeadline = errors.New("resume attempt deadline exceeded")
	errFlightClosed    = errors.New("client proxy is shutting down")
	errFlightAbandoned = errors.New("every caller of the resume left")
	// errBudgetFloor is a retry that had less than the floor of its budget
	// left when it could have started.
	errBudgetFloor = errors.New("less than the attempt floor left")
)

// reasonParkingLotFull is the capacity reason for a request shed because the
// lot was full; the rejected counter counts it, so its log line stays quiet.
const reasonParkingLotFull = "parking lot full"

const (
	resumeFlightRoleLeader = "leader"
	resumeFlightRoleJoiner = "joiner"
	// resumeFlightRoleNone is a caller that neither led nor joined a flight,
	// so that leader counts flights started.
	resumeFlightRoleNone = "none"

	resumeFlightResultServed   = "served"
	resumeFlightResultError    = "error"
	resumeFlightResultCanceled = "canceled"
)

type resumeFlight struct {
	key      resumeFlightKey
	done     chan struct{}
	retrying chan struct{}
	cancel   context.CancelCauseFunc
	// waiters, parked, attempting and detached are guarded by
	// resumeFlights.mu. parked counts the waiters that took a lot slot; a
	// retry starts only while one is attached. attempting is set while a
	// resume call is under way and until the flight either publishes its
	// answer or decides to go on. detached marks a flight inside an attempt
	// whose waiters have all left, counted against maxDetachedFlights.
	// changed is closed and replaced under the mutex when parked rises from
	// zero, waiters falls to it or an attempt ends, so the flight's waits
	// wake.
	waiters    int
	parked     int
	attempting bool
	attempts   int // attempts begun, so a wait on one ends when it does
	// attemptEnds is when the attempt under way reaches the ceiling, on
	// the flight's clock.
	attemptEnds time.Time
	detached    bool
	changed     chan struct{}
	// capAttempt and capDeadline are set, under the mutex, while a retry
	// attempt is under way: once the flight is detached the attempt is cut
	// at the budget's end (capStop ends a pending cut when a caller joins).
	capAttempt  context.CancelCauseFunc
	capDeadline time.Time
	capStop     func()
	firstErr    error
	// route is the answer every waiter of the flight receives, shared and
	// never modified.
	route *catalog.SandboxInfo
	err   error
}

// resumeFlightKey names the callers that may share one resume: the API
// validates the tokens for the port the request names, so callers with
// different credentials get their own answer.
type resumeFlightKey struct {
	sandboxId   string
	sandboxPort uint64
	credentials [sha256.Size]byte
}

func newResumeFlightKey(sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) resumeFlightKey {
	digest := sha256.New()
	writeLengthPrefixed(digest, trafficAccessToken)
	writeLengthPrefixed(digest, envdAccessToken)

	key := resumeFlightKey{sandboxId: sandboxId, sandboxPort: sandboxPort}
	digest.Sum(key.credentials[:0])

	return key
}

func writeLengthPrefixed(h hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	h.Write(length[:])
	h.Write([]byte(value))
}

// resumeBudgetExhaustedError marks a retryable failure the flight stopped
// retrying because its budget ran out; the status it wraps still decides the
// response.
type resumeBudgetExhaustedError struct {
	err error
}

func (e *resumeBudgetExhaustedError) Error() string {
	return "resume retry budget exhausted: " + e.err.Error()
}

func (e *resumeBudgetExhaustedError) Unwrap() error {
	return e.err
}

// endedResumeError marks a retryable failure the flight stopped retrying
// because the proxy shut down or every caller left. The status it wraps
// still decides the response, and a wait Close ended counts as canceled, not
// as an error.
func endedResumeError(ctx context.Context, lastErr error) error {
	cause := context.Cause(ctx)
	if cause == nil {
		cause = errFlightAbandoned
	}

	return fmt.Errorf("%w: %w", cause, lastErr)
}

// resumeStatus returns the status the API sent with a resume error. It reads
// through wrapping, unlike status.FromError, which rebuilds a wrapped error's
// message from the whole error text.
func resumeStatus(err error) (*status.Status, bool) {
	var withStatus interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &withStatus) {
		return nil, false
	}

	st := withStatus.GRPCStatus()
	if st == nil {
		return nil, false
	}

	return st, true
}

// localResumeError reports an error the flight produced itself rather than
// received: an attempt deadline, Close, or a token it could not obtain.
func localResumeError(err error) bool {
	return errors.Is(err, errAttemptDeadline) || errors.Is(err, errFlightClosed) || errors.Is(err, errResumeToken)
}

// controlPlaneAnswer reports a status that outranks the end of the attempt
// that received it: anything but the DeadlineExceeded and Canceled the gRPC
// client makes of an ended context, unless the status carries the API's
// placement code, which only the API attaches.
func controlPlaneAnswer(err error) bool {
	st, ok := resumeStatus(err)
	if !ok {
		return false
	}
	if code, _ := resumeRefusalCode(st); code != "" {
		return true
	}

	return st.Code() != codes.DeadlineExceeded && st.Code() != codes.Canceled
}

// permanentResumeRefusal reports a status whose detail from the API names a
// placement refusal no retry helps (a feature no orchestrator in the cluster
// supports). An Unavailable without such a detail, or with a capacity or
// no-compatible-node code, is a shortage that clears or the control plane
// being unreachable, and is retried.
func permanentResumeRefusal(st *status.Status) bool {
	_, permanent := resumeRefusalCode(st)

	return permanent
}

// resumeErrorInfo returns the detail the API attaches to a resume failure,
// or nil when the status carries none.
func resumeErrorInfo(st *status.Status) *errdetails.ErrorInfo {
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetDomain() == proxygrpc.ResumeErrorDomain {
			return info
		}
	}

	return nil
}

// resumeRefusalCode returns the API's error code carried on the status, if
// any, and whether it names a refusal no retry helps.
func resumeRefusalCode(st *status.Status) (string, bool) {
	info := resumeErrorInfo(st)
	if info == nil {
		return "", false
	}

	return info.GetReason(), proxygrpc.PermanentPlacementRefusal(info.GetReason())
}

// resumeHTTPStatus returns the HTTP status the API answered the failure
// with, carried on its detail, or zero when the status carries none or one
// that is not an error.
func resumeHTTPStatus(st *status.Status) int {
	info := resumeErrorInfo(st)
	if info == nil {
		return 0
	}
	httpStatus, err := strconv.Atoi(info.GetMetadata()[proxygrpc.ResumeErrorHTTPStatusKey])
	if err != nil || httpStatus < http.StatusBadRequest || httpStatus > 599 {
		return 0
	}

	return httpStatus
}

// retryableResumeError reports a failure the flight retries: a shortage or
// an unreachable control plane (Unavailable without a permanent code), a
// sandbox still pausing, or a placement the API's carried code says to
// retry (a timeout, a create that failed on the node), whatever gRPC code
// its HTTP status mapped to.
func retryableResumeError(err error) bool {
	st, ok := resumeStatus(err)
	if !ok {
		return false
	}
	if code, _ := resumeRefusalCode(st); proxygrpc.RetryablePlacementFailure(code) {
		return true
	}

	switch st.Code() {
	case codes.Unavailable:
		return !permanentResumeRefusal(st)
	case codes.FailedPrecondition:
		return st.Message() == proxygrpc.SandboxStillTransitioningMessage
	default:
		return false
	}
}

// resumeFlights runs at most one Resume call per sandbox at a time and hands
// its result to every caller that arrived while that call was in flight.
type resumeFlights struct {
	resumer        SandboxResumer
	featureFlags   *featureflags.Client
	attemptCeiling time.Duration
	sleep          func(ctx context.Context, d time.Duration) bool
	now            func() time.Time
	after          func(d time.Duration) (<-chan time.Time, func())
	flightCounter  metric.Int64Counter
	lot            *parkingLot

	mu          sync.Mutex
	flights     map[resumeFlightKey]*resumeFlight
	closed      bool
	detached    int // flights running on with no waiter, at most maxDetached
	maxDetached int
	running     sync.WaitGroup // one per run goroutine; Close waits for them
}

// ResumeCloser stops the resume flights that outlive the proxy's requests
// and waits for them to end.
type ResumeCloser interface {
	Close(ctx context.Context) error
}

func newResumeFlights(meter metric.Meter, resumer SandboxResumer, featureFlags *featureflags.Client) (*resumeFlights, error) {
	flightCounter, err := telemetry.GetCounter(meter, telemetry.ClientProxyResumeFlightCounterName)
	if err != nil {
		return nil, fmt.Errorf("error registering client proxy resume flight metric (%s): %w", telemetry.ClientProxyResumeFlightCounterName, err)
	}

	lot, err := newParkingLot(meter)
	if err != nil {
		return nil, err
	}

	return &resumeFlights{
		resumer:        resumer,
		featureFlags:   featureFlags,
		attemptCeiling: resumeAttemptCeiling,
		sleep:          sleepUnlessDone,
		now:            time.Now,
		after:          timerAfter,
		flightCounter:  flightCounter,
		lot:            lot,
		flights:        make(map[resumeFlightKey]*resumeFlight),
		maxDetached:    maxDetachedFlights,
	}, nil
}

func (r *resumeFlights) Resume(ctx context.Context, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	if err := ctx.Err(); err != nil {
		// A caller already gone would lead a flight nobody waits on and
		// make the control plane resume for it; it joins nothing instead.
		r.countCaller(ctx, resumeFlightRoleNone, resumeFlightResultCanceled)

		return nil, err
	}

	key := newResumeFlightKey(sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		// A caller rejected after Close joined nothing, whatever the
		// registry still holds.
		r.countCaller(ctx, resumeFlightRoleNone, resumeFlightResultCanceled)

		return nil, reverseproxy.NewErrSandboxCapacity(sandboxId, reverseproxy.PresenceUnknown, errFlightClosed.Error())
	}
	f, ok := r.flights[key]
	if ok {
		f.waiters++
		r.releaseDetached(f)
		r.mu.Unlock()
		defer r.detach(f)
		logger.L().Debug(ctx, "catalog miss, attempting resume", logger.WithSandboxID(sandboxId))

		return r.await(ctx, f, resumeFlightRoleJoiner, sandboxId)
	}
	logger.L().Info(ctx, "catalog miss, attempting resume", logger.WithSandboxID(sandboxId))

	// Detached from the leader's request: a client that disconnects
	// mid-resume must not fail the callers that joined its flight, and the
	// call under way completes for the caller that comes back (detach).
	flightCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	f = &resumeFlight{
		key:      key,
		done:     make(chan struct{}),
		retrying: make(chan struct{}),
		cancel:   cancel,
		waiters:  1,
		changed:  make(chan struct{}),
	}
	r.flights[key] = f
	// Counted under the mutex, so Close, which marks the registry closed
	// under it, waits for every goroutine started before that.
	r.running.Add(1)
	r.mu.Unlock()
	defer r.detach(f)

	budget := time.Duration(r.featureFlags.IntFlag(ctx, featureflags.ClientProxyResumeParkingBudgetMs)) * time.Millisecond
	go r.run(flightCtx, f, budget, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)

	return r.await(ctx, f, resumeFlightRoleLeader, sandboxId)
}

// Close cancels every flight still running and waits for the flights to
// end, so that none is still reading the feature flags or calling the
// resumer when those are closed after it; the wait ends with the context.
// A Resume afterwards is answered without an attempt.
func (r *resumeFlights) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closed = true
	for _, f := range r.flights {
		f.cancel(errFlightClosed)
	}
	r.mu.Unlock()

	ended := make(chan struct{})
	go func() {
		r.running.Wait()
		close(ended)
	}()

	select {
	case <-ended:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("resume flights still running: %w", context.Cause(ctx))
	}
}

// detach removes one waiter. When the last one leaves, a flight between
// attempts ends at once, a flight inside an attempt runs that attempt to
// its end and stays in the registry for a caller that arrives meanwhile,
// and a flight beyond the detached bound is cancelled and unregistered so a
// later caller starts afresh (its call winds down while that caller's new
// one starts; the bound is the price of that overlap).
func (r *resumeFlights) detach(f *resumeFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()

	f.waiters--
	if f.waiters > 0 {
		return
	}
	f.notify()

	// Registered means still running and joinable; a flight that ended or
	// was unregistered runs nothing on.
	if r.flights[f.key] != f {
		return
	}
	if !f.attempting || r.detached >= r.maxDetached {
		delete(r.flights, f.key)
		f.cancel(errFlightAbandoned)

		return
	}
	f.detached = true
	r.detached++
	r.capDetached(f)
}

// capDetached cuts the retry attempt under way at the budget's end once no
// caller waits on it, so a callerless retry holds its detached slot for the
// budget left rather than for the ceiling; called under resumeFlights.mu.
// The cut is checked against the flight's state when the timer fires, so a
// caller that joined meanwhile keeps the attempt.
func (r *resumeFlights) capDetached(f *resumeFlight) {
	if f.capAttempt == nil || f.capStop != nil {
		return
	}
	timer, stop := r.after(f.capDeadline.Sub(r.now()))
	stopped := make(chan struct{})
	f.capStop = func() {
		stop()
		close(stopped)
	}
	go func() {
		select {
		case <-timer:
			r.mu.Lock()
			if f.detached && f.capAttempt != nil {
				f.capAttempt(errAttemptDeadline)
			}
			r.mu.Unlock()
		case <-stopped:
		}
	}()
}

// armCap registers the retry attempt's cut under the mutex and applies it
// at once to a flight already detached; disarmCap withdraws it when the
// attempt ends.
func (r *resumeFlights) armCap(f *resumeFlight, cancel context.CancelCauseFunc, deadline time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	f.capAttempt, f.capDeadline = cancel, deadline
	if f.detached {
		r.capDetached(f)
	}
}

func (r *resumeFlights) disarmCap(f *resumeFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.uncapDetached(f)
	f.capAttempt = nil
}

// uncapDetached ends a pending cut; called under resumeFlights.mu.
func (r *resumeFlights) uncapDetached(f *resumeFlight) {
	if f.capStop != nil {
		f.capStop()
		f.capStop = nil
	}
}

// beginAttempt marks the flight as inside a resume call, in the same
// critical section as the checks that allow it, so a detach or a parked
// waiter's departure sees either state whole. No waiter at all unregisters
// the flight and reports errFlightAbandoned. A retry needs a parked waiter
// and at least the floor of the budget left, both read in the same critical
// section that marks the attempt: an attached waiter that has not decided
// does not stand in for a parked one, so the retry waits here until one
// parks, until the budget has less than the floor left (errBudgetFloor), or
// until the flight ends (every waiter gone, or Close), in which case the
// flight's cause is reported.
func (r *resumeFlights) beginAttempt(ctx context.Context, f *resumeFlight, retry bool, deadline time.Time) error {
	for {
		r.mu.Lock()
		if f.waiters == 0 {
			r.unregister(f)
			r.mu.Unlock()

			return errFlightAbandoned
		}
		// Exactly the floor left still starts, as the check before the
		// backoff promised.
		floor := deadline.Add(-resumeRetryAttemptFloor).Sub(r.now())
		if retry && floor < 0 {
			r.mu.Unlock()

			return errBudgetFloor
		}
		if !retry || f.parked > 0 {
			f.attempting = true
			f.attempts++
			f.attemptEnds = r.now().Add(r.attemptCeiling)
			r.mu.Unlock()

			return nil
		}
		changed := f.changed
		r.mu.Unlock()

		floorTimer, stop := r.after(floor)
		select {
		case <-changed:
			stop()
		case <-floorTimer:
			return errBudgetFloor
		case <-ctx.Done():
			stop()

			return context.Cause(ctx)
		}
	}
}

// timerAfter is the production after: a timer channel and its stop.
func timerAfter(d time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(d)

	return timer.C, func() { timer.Stop() }
}

// endAttempt clears the mark once the flight has decided to go on after a
// failed attempt, wakes the waits on it, and reports whether a waiter
// remains; with none the flight is unregistered and its detached slot
// released, so the caller ends it rather than retry for nobody. An attempt
// whose outcome ends the flight keeps the mark until end publishes the
// answer with it, so a caller woken by its clearing finds the answer.
func (r *resumeFlights) endAttempt(f *resumeFlight) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	f.attempting = false
	f.notify()
	if f.waiters > 0 {
		return true
	}
	r.unregister(f)

	return false
}

// unregister takes the flight out of the registry and releases its detached
// slot; called under resumeFlights.mu.
func (r *resumeFlights) unregister(f *resumeFlight) {
	if r.flights[f.key] == f {
		delete(r.flights, f.key)
	}
	r.releaseDetached(f)
}

// releaseDetached gives back the flight's detached slot, if it holds one;
// called under resumeFlights.mu.
func (r *resumeFlights) releaseDetached(f *resumeFlight) {
	if f.detached {
		f.detached = false
		r.detached--
		r.uncapDetached(f)
	}
}

// notify wakes the flight's waits; called under resumeFlights.mu.
func (f *resumeFlight) notify() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// park records a waiter that took a lot slot and wakes the flight if it is
// waiting for one.
func (r *resumeFlights) park(f *resumeFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()

	f.parked++
	if f.parked == 1 {
		f.notify()
	}
}

func (r *resumeFlights) unpark(f *resumeFlight) {
	r.mu.Lock()
	f.parked--
	r.mu.Unlock()
}

func (r *resumeFlights) run(ctx context.Context, f *resumeFlight, budget time.Duration, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) {
	defer r.running.Done()
	defer f.cancel(nil)
	defer r.end(f)
	// Off the request goroutine there is no net/http to recover a panic;
	// one here ends this flight with the capacity page, not the process.
	defer func() {
		if p := recover(); p != nil {
			logger.L().Error(ctx, "resume flight panicked", zap.Any("panic", p), zap.Stack("stack"), logger.WithSandboxID(sandboxId))
			f.route, f.err = nil, fmt.Errorf("%w: resume flight panicked: %v", reverseproxy.NewErrSandboxCapacity(sandboxId, reverseproxy.PresenceUnknown, "resume flight panicked"), p)
		}
	}()

	// The leader attached before this goroutine started and was still
	// there when Resume began; it may have left since, and a flight nobody
	// waits on makes no attempt (beginAttempt).
	f.route, f.err = r.resume(ctx, f, budget, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
}

// end unregisters the flight, releases its detached slot and publishes its
// result, all under the mutex, so detach and a joiner see one state. The
// attempt mark is cleared too, for an attempt a panic ended.
func (r *resumeFlights) end(f *resumeFlight) {
	r.mu.Lock()
	defer r.mu.Unlock()

	f.attempting = false
	r.unregister(f)
	close(f.done)
}

// resume retries a retryable failure until the budget, measured from the
// first such failure, runs out. Every attempt runs under the ceiling while a
// caller waits on it; the budget decides whether a retry starts (none starts
// with less than the floor left, without a parked waiter, once every waiter
// has left, or once the registry is closed; a backoff that would leave less
// than the floor is not slept) and cuts a retry whose callers have all left
// at its end (capDetached). The flight reads no flag of its own: each
// waiter decides at the first retryable failure whether it parks, and the
// flight retries for as long as a parked one is attached (beginAttempt), so
// a leader whose flag is off leaves without ending the retries for a joiner
// whose flag is on, and a waiter that has not decided yet holds no retry. A
// status from the control plane always wins. A local error (the attempt
// deadline, Close, a token the attempt could not obtain) before any
// retryable failure answers with the unavailable page; after one, Close
// answers with the last retryable error, and the deadline and the token are
// retried while the budget allows, the last retryable error answering once
// it does not. The loop is its own rather than the shared retry
// package's because it departs from it in three ways: the budget runs from
// the first retryable failure rather than from the call, no attempt starts
// with less than the floor left or without a parked waiter, and the loop
// stops once every waiter has left.
func (r *resumeFlights) resume(ctx context.Context, f *resumeFlight, budget time.Duration, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	backoff := resumeRetryInitialBackoff
	var deadline time.Time
	var lastErr error

	for {
		route, err := r.attempt(ctx, f, lastErr != nil, deadline, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
		switch {
		case err == nil:
			return route, nil
		case errors.Is(err, errBudgetFloor):
			return nil, &resumeBudgetExhaustedError{err: lastErr}
		case errors.Is(err, errFlightAbandoned):
			// Nobody is left to read an answer.
			return nil, err
		case localResumeError(err) && lastErr == nil:
			// The cause stays in the chain: the page is the capacity
			// page either way, but a Close counts as canceled, not as
			// an error.
			return nil, fmt.Errorf("%w: %w", reverseproxy.NewErrSandboxCapacity(sandboxId, reverseproxy.PresenceUnknown, err.Error()), err)
		case errors.Is(err, errFlightClosed):
			return nil, endedResumeError(ctx, lastErr)
		case localResumeError(err):
			// After a retryable failure the attempt deadline and a token
			// the attempt could not obtain are retried within the budget;
			// the answer stays the control plane's last status, and the
			// token's cause goes on record here.
			if errors.Is(err, errResumeToken) {
				logger.L().Warn(ctx, "resume token unavailable, retrying", zap.Error(err), logger.WithSandboxID(sandboxId))
			}
			if !r.now().Before(deadline) {
				return nil, &resumeBudgetExhaustedError{err: lastErr}
			}
		case !retryableResumeError(err):
			return nil, err
		default:
			if lastErr == nil {
				deadline = r.now().Add(budget)
				f.firstErr = err
				close(f.retrying)
			}
			lastErr = err
		}

		// A failure nobody waits for ends the flight.
		if !r.endAttempt(f) {
			return nil, errFlightAbandoned
		}

		// The budget is over once the backoff would leave less than the
		// floor: the attempt after it could not start, so the waiters are
		// answered now rather than after a sleep for nothing.
		pause := jitteredBackoff(backoff)
		if deadline.Sub(r.now())-pause < resumeRetryAttemptFloor {
			return nil, &resumeBudgetExhaustedError{err: lastErr}
		}

		if !r.sleep(ctx, pause) {
			return nil, endedResumeError(ctx, lastErr)
		}
		backoff = min(time.Duration(float64(backoff)*resumeRetryBackoffFactor), resumeRetryBackoffCap)
	}
}

func sleepUnlessDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func jitteredBackoff(backoff time.Duration) time.Duration {
	spread := 1 + resumeRetryBackoffJitter*(2*rand.Float64()-1) //nolint:gosec // jitter, not a secret

	return time.Duration(float64(backoff) * spread)
}

// attempt runs one resume call, or none when no waiter is left to start it
// for. Every attempt runs under the ceiling; a retry waits for a parked
// waiter first and does not start with less than the floor of the budget
// left, but once started it is not cut by the budget while a caller waits:
// a resume that needs longer than what is left of it still completes for
// the waiters, where a cut would have ended every retry the same way. Only
// a retry every caller of which has left is cut at the budget's end, so it
// does not hold a detached slot for the ceiling. A status the control
// plane sent is its answer even when the attempt context ended meanwhile;
// any other error after that end came from the end itself, and the
// context's cause says which: the attempt deadline, Close, or the last
// caller leaving. The attempt mark stays set on return: resume clears it
// (endAttempt) when it goes on, end when it publishes.
func (r *resumeFlights) attempt(ctx context.Context, f *resumeFlight, retry bool, deadline time.Time, sandboxId string, sandboxPort uint64, trafficAccessToken string, envdAccessToken string) (*catalog.SandboxInfo, error) {
	if err := r.beginAttempt(ctx, f, retry, deadline); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeoutCause(ctx, r.attemptCeiling, errAttemptDeadline)
	defer cancel()
	if retry {
		var cancelCause context.CancelCauseFunc
		ctx, cancelCause = context.WithCancelCause(ctx)
		defer cancelCause(nil)
		r.armCap(f, cancelCause, deadline)
		defer r.disarmCap(f)
	}

	route, err := r.resumer.Resume(ctx, sandboxId, sandboxPort, trafficAccessToken, envdAccessToken)
	if err == nil {
		return route, nil
	}

	if cause := context.Cause(ctx); cause != nil && !controlPlaneAnswer(err) {
		return nil, cause
	}

	return nil, err
}

// await holds nothing until the flight reports its first retryable failure;
// only then does the caller take a lot slot or get shed.
func (r *resumeFlights) await(ctx context.Context, f *resumeFlight, role string, sandboxId string) (*catalog.SandboxInfo, error) {
	select {
	case <-f.done:
		return r.finish(ctx, f, role)
	case <-ctx.Done():
		r.countCaller(ctx, role, resumeFlightResultCanceled)

		return nil, ctx.Err()
	case <-f.retrying:
	}

	// Both channels may be closed by now; a ready result is served
	// without taking a slot.
	if f.ready() {
		return r.finish(ctx, f, role)
	}

	// Each caller decides for itself whether it may park; the flight
	// keeps retrying for those that may. A caller that may not takes the
	// first error, however old, unless the attempt under way is about to
	// end, in which case it waits for that answer.
	if !r.featureFlags.BoolFlag(ctx, featureflags.ClientProxyResumeParkingFlag) {
		if !r.awaitAttempt(ctx, f) {
			r.countCaller(ctx, role, resumeFlightResultCanceled)

			return nil, ctx.Err()
		}
		if f.ready() {
			return r.finish(ctx, f, role)
		}
		r.countCaller(ctx, role, resumeFlightResultError)

		return nil, f.firstErr
	}

	if !r.lot.tryEnter(r.featureFlags.IntFlag(ctx, featureflags.ClientProxyResumeParkingLotSize)) {
		if f.ready() {
			return r.finish(ctx, f, role)
		}

		r.lot.reject(ctx)
		r.countCaller(ctx, role, resumeFlightResultError)

		// No Retry-After: a fixed delay brings the shed clients back in one wave while the lot is still full.
		return nil, reverseproxy.NewErrSandboxShed(sandboxId, reasonParkingLotFull)
	}

	r.park(f)
	defer r.unpark(f)

	parkedAt := time.Now()
	select {
	case <-f.done:
		r.lot.leave(ctx, parkedAt, parkingOutcome(f.err))

		return r.finish(ctx, f, role)
	case <-ctx.Done():
		r.lot.leave(ctx, parkedAt, parkingOutcomeCanceled)
		r.countCaller(ctx, role, resumeFlightResultCanceled)

		return nil, ctx.Err()
	}
}

// awaitAttempt waits for the resume call the flight is inside, if any and
// if it ends within flagOffWaitBound, and reports false when the caller's
// context ended first. The mark is cleared under the mutex either with a
// wake (the flight goes on) or with the answer (the flight ends), so a
// caller that returns true finds one or the other; the attempt is told by
// its number, so a retry that began before the caller woke is not waited
// for in turn.
func (r *resumeFlights) awaitAttempt(ctx context.Context, f *resumeFlight) bool {
	r.mu.Lock()
	attempt := f.attempts
	longToRun := f.attemptEnds.Sub(r.now()) > flagOffWaitBound
	r.mu.Unlock()
	if longToRun {
		return true
	}

	for {
		r.mu.Lock()
		if !f.attempting || f.attempts != attempt {
			r.mu.Unlock()

			return true
		}
		changed := f.changed
		r.mu.Unlock()

		select {
		case <-changed:
		case <-f.done:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

func (f *resumeFlight) ready() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// finish hands the flight's answer to one caller and counts it; a flight
// Close ended counts as canceled, as its parked waits do.
func (r *resumeFlights) finish(ctx context.Context, f *resumeFlight, role string) (*catalog.SandboxInfo, error) {
	result := resumeFlightResultServed
	switch {
	case f.err == nil:
	case errors.Is(f.err, errFlightClosed):
		result = resumeFlightResultCanceled
	default:
		result = resumeFlightResultError
	}
	r.countCaller(ctx, role, result)

	return f.route, f.err
}

func (r *resumeFlights) countCaller(ctx context.Context, role string, result string) {
	r.flightCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("role", role),
		attribute.String("result", result),
	))
}
