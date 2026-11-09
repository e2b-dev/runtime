package logger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap/zapcore"
)

// LogRoute is the resolved destination for a single log write. It is populated
// by a LogRouteResolver so that this package stays decoupled from featureflags
// (which imports this package).
type LogRoute struct {
	// PrimaryURL is the synchronous, success-controlling destination.
	PrimaryURL string
	// ShadowURLs are best-effort, fire-and-forget destinations.
	ShadowURLs []string
	// Timeout bounds each individual request.
	Timeout time.Duration
	// MaxInflightShadowWrites bounds concurrent best-effort shadow writes.
	MaxInflightShadowWrites int64
}

// LogRouteResolver resolves the current log route for a write. It is called on
// each log line, so it must be cheap and non-blocking.
type LogRouteResolver func(ctx context.Context) LogRoute

const defaultMaxInflightShadowWrites = 1024

var (
	logWriterMeter      = otel.Meter("github.com/e2b-dev/infra/packages/shared/pkg/logger")
	logWriterWriteCount = mustLogWriterCounter(
		"log_writer_write_count",
		"Number of log writer HTTP attempts by route and result",
	)
	logWriterShadowInflight = mustLogWriterUpDownCounter(
		"log_writer_shadow_inflight",
		"Current number of in-flight best-effort shadow log writes",
	)
)

func mustLogWriterCounter(name, description string) metric.Int64Counter {
	counter, err := logWriterMeter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

func mustLogWriterUpDownCounter(name, description string) metric.Int64UpDownCounter {
	counter, err := logWriterMeter.Int64UpDownCounter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

type HTTPWriter struct {
	ctx        context.Context //nolint:containedctx // todo: fix the interface so this can be removed
	url        string
	httpClient *http.Client

	// resolve, when non-nil, dynamically selects the destination(s) per write.
	// When nil the writer always sends to url (legacy behavior).
	resolve LogRouteResolver

	// shadowInflight bounds concurrent shadow writes (best-effort, non-blocking
	// acquire; dropped when the resolved route limit is reached).
	shadowInflight atomic.Int64

	wgLock sync.Mutex
	wg     *sync.WaitGroup

	routeFailures routeFailureLog
}

// routeFailureLogInterval is the minimum time between two stderr reports of
// dropped lines. zap calls Write once per entry, so without it a down
// collector would print one stderr line per log entry.
const routeFailureLogInterval = 10 * time.Second

// routeFailureLog folds the routing failures of every Write into at most one
// report per routeFailureLogInterval. The per-line failure metric still counts
// every drop, including ones not yet reported here.
type routeFailureLog struct {
	mu         sync.Mutex
	lastReport time.Time
	lines      int
	bytes      int
}

// record adds one Write's failures and returns the report to print, if one is due.
func (r *routeFailureLog) record(now time.Time, lines, bytes int, lastErr error) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lines += lines
	r.bytes += bytes
	if !r.lastReport.IsZero() && now.Sub(r.lastReport) < routeFailureLogInterval {
		return "", false
	}

	msg := fmt.Sprintf("dropped %d log line(s) (%d bytes) while routing since the last report; last error: %v", r.lines, r.bytes, lastErr)
	r.lastReport, r.lines, r.bytes = now, 0, 0

	return msg, true
}

func NewHTTPWriter(ctx context.Context, endpoint string) zapcore.WriteSyncer {
	return &HTTPWriter{
		ctx: ctx,
		url: endpoint,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		wgLock: sync.Mutex{},
		wg:     &sync.WaitGroup{},
	}
}

// NewDynamicHTTPWriter builds a writer that resolves its destination(s) on each
// write via resolve. endpoint is the legacy fallback used by the resolver when
// LaunchDarkly config is missing/invalid. If resolve is nil it behaves exactly
// like NewHTTPWriter(ctx, endpoint).
func NewDynamicHTTPWriter(ctx context.Context, endpoint string, resolve LogRouteResolver) zapcore.WriteSyncer {
	return &HTTPWriter{
		ctx: ctx,
		url: endpoint,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		resolve: resolve,

		wgLock: sync.Mutex{},
		wg:     &sync.WaitGroup{},
	}
}

func (h *HTTPWriter) Write(source []byte) (n int, err error) {
	h.wgLock.Lock()
	// Capture the current WaitGroup to match Add/Done calls
	wg := h.wg
	wg.Add(1)
	h.wgLock.Unlock()

	// zap is reusing the buffer, so since we're processing it in a Go routine, we need to make a copy.
	p := make([]byte, len(source))
	copy(p, source)

	// Run in a goroutine to avoid blocking the main thread
	go func() {
		defer wg.Done()

		var dropped int
		var droppedBytes int
		var lastErr error
		route := func(line []byte) {
			if err := h.routeLogLine(line); err != nil {
				// Never log the line itself: it is customer log content.
				dropped++
				droppedBytes += len(line)
				lastErr = err
			}
		}

		start := 0
		for i, b := range p {
			if b == '\n' {
				if start < i { // Ignore empty lines
					route(p[start:i])
				}
				start = i + 1 // Move start to the next line
			}
		}

		// Handle the last line if there’s no trailing newline
		if start < len(p) {
			route(p[start:])
		}

		if dropped > 0 {
			if msg, ok := h.routeFailures.record(time.Now(), dropped, droppedBytes, lastErr); ok {
				log.Print(msg)
			}
		}
	}()

	return len(p), nil
}

func (h *HTTPWriter) Sync() error {
	h.wgLock.Lock()
	// Capture the current WaitGroup for Wait() and replace the WaitGroup with a new one
	// Should fix: WaitGroup is reused before previous Wait has returned
	wg := h.wg
	h.wg = &sync.WaitGroup{}
	h.wgLock.Unlock()

	wg.Wait()

	return nil
}

// routeLogLine resolves the current destination(s) and sends ONE log line.
// The primary write controls success; shadow writes are fire-and-forget and
// never affect the returned error. When no resolver is configured it sends to
// the legacy url (preserving today's behavior).
func (h *HTTPWriter) routeLogLine(line []byte) error {
	if h.resolve == nil {
		if err := h.sendLogLine(h.ctx, h.url, line); err != nil {
			recordLogWriterWrite(h.ctx, "primary", "failure", "send_error")

			return err
		}
		recordLogWriterWrite(h.ctx, "primary", "success", "")

		return nil
	}

	route := h.resolve(h.ctx)
	primary := route.PrimaryURL
	if primary == "" {
		// Resolver guarantees a non-empty primary on non-disabled routes, but
		// guard anyway to avoid a bad request.
		primary = h.url
	}

	ctx := h.ctx
	cancel := context.CancelFunc(func() {})
	if route.Timeout > 0 {
		ctx, cancel = context.WithTimeout(h.ctx, route.Timeout)
	}
	defer cancel()

	// Fire-and-forget shadow writes: line points into the private copy made in
	// Write, not the caller's/zap's reusable buffer, so it is safe to share with
	// shadow goroutines without another per-shadow allocation. Shadow outcomes
	// never influence the primary result. Concurrency is bounded by the resolved
	// route limit; excess writes are dropped to avoid unbounded goroutine/
	// connection growth (and to avoid a shadow log storm).
	maxInflight := route.MaxInflightShadowWrites
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflightShadowWrites
	}
	for _, shadowURL := range route.ShadowURLs {
		if !h.tryAcquireShadow(maxInflight) {
			// Semaphore full: drop this shadow write.
			recordLogWriterWrite(h.ctx, "shadow", "dropped", "saturated")

			continue
		}
		recordLogWriterShadowInflight(h.ctx, 1)

		go func(url string, payload []byte) {
			defer func() {
				h.shadowInflight.Add(-1)
				recordLogWriterShadowInflight(h.ctx, -1)
			}()

			shadowCtx := h.ctx
			shadowCancel := context.CancelFunc(func() {})
			if route.Timeout > 0 {
				shadowCtx, shadowCancel = context.WithTimeout(h.ctx, route.Timeout)
			}
			defer shadowCancel()

			if err := h.sendLogLine(shadowCtx, url, payload); err != nil {
				recordLogWriterWrite(shadowCtx, "shadow", "failure", "send_error")

				return
			}
			recordLogWriterWrite(shadowCtx, "shadow", "success", "")
		}(shadowURL, line)
	}

	if err := h.sendLogLine(ctx, primary, line); err != nil {
		recordLogWriterWrite(ctx, "primary", "failure", "send_error")

		return err
	}
	recordLogWriterWrite(ctx, "primary", "success", "")

	return nil
}

func (h *HTTPWriter) tryAcquireShadow(maxInflight int64) bool {
	for {
		current := h.shadowInflight.Load()
		if current >= maxInflight {
			return false
		}
		if h.shadowInflight.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func recordLogWriterWrite(ctx context.Context, route, result, reason string) {
	if logWriterWriteCount == nil {
		return
	}

	logWriterWriteCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("route", route),
		attribute.String("result", result),
		attribute.String("reason", reason),
	))
}

func recordLogWriterShadowInflight(ctx context.Context, delta int64) {
	if logWriterShadowInflight == nil {
		return
	}

	logWriterShadowInflight.Add(ctx, delta)
}

// sendLogLine handles sending ONE log line as an HTTP request to url.
func (h *HTTPWriter) sendLogLine(ctx context.Context, url string, line []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(line))
	if err != nil {
		return fmt.Errorf("error sending logs: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := h.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("error sending logs: %w", err)
	}
	defer response.Body.Close()

	// Always drain so the transport can reuse the connection; the body itself
	// is never surfaced in the returned error (it may echo request content).
	drainErr := drainResponseBody(response.Body)

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		statusErr := fmt.Errorf("error sending logs: unexpected HTTP status %d", response.StatusCode)

		return errors.Join(statusErr, drainErr)
	}

	return drainErr
}

func drainResponseBody(body io.Reader) error {
	if _, err := io.Copy(io.Discard, body); err != nil {
		return fmt.Errorf("error draining response body: %w", err)
	}

	return nil
}
