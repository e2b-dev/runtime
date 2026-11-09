package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPWriterSendLogLineStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		status  int
		wantErr bool
	}{
		{name: "success", status: http.StatusNoContent},
		{name: "client error", status: http.StatusBadRequest, wantErr: true},
		{name: "server error", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "rate limited", status: http.StatusTooManyRequests, wantErr: true},
		{name: "internal server error", status: http.StatusInternalServerError, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte("collector diagnostic"))
			}))
			t.Cleanup(server.Close)

			writer := &HTTPWriter{httpClient: server.Client()}
			err := writer.sendLogLine(t.Context(), server.URL, []byte(`{"secret":"not-in-error"}`))
			if tt.wantErr && err == nil {
				t.Fatal("expected an error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "not-in-error") {
				t.Fatalf("error contains request payload: %v", err)
			}
		})
	}
}

// TestHTTPWriterSendLogLineRespectsContextDeadline verifies a slow/hung
// destination (e.g. Vector under backpressure) doesn't block sendLogLine
// forever: once the caller-supplied context deadline passes, the request is
// aborted client-side and an error is returned promptly, regardless of
// whether the server ever responds. This is the timeout behavior routeLogLine
// relies on when a resolved route sets Timeout > 0.
func TestHTTPWriterSendLogLineRespectsContextDeadline(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	writer := &HTTPWriter{httpClient: server.Client()}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := writer.sendLogLine(ctx, server.URL, []byte(`{"msg":"slow"}`))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a slow/hung response once the context deadline passes")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("sendLogLine took %v to return; expected it to abort promptly once the 50ms deadline passed", elapsed)
	}
}

// TestHTTPWriterSendLogLineRespectsClientTimeout is the legacy-mode
// equivalent: when there is no per-request context deadline (route.Timeout ==
// 0, i.e. resolve is nil or the resolved route is unconfigured), the request
// must still be bounded by the http.Client's own Timeout, exactly as it was
// before per-request timeouts existed.
func TestHTTPWriterSendLogLineRespectsClientTimeout(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	writer := &HTTPWriter{httpClient: &http.Client{Timeout: 50 * time.Millisecond}}

	start := time.Now()
	err := writer.sendLogLine(t.Context(), server.URL, []byte(`{"msg":"slow"}`))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error from a slow/hung response once the client timeout passes")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("sendLogLine took %v to return; expected it to abort promptly once the 50ms client timeout passed", elapsed)
	}
}

// TestHTTPWriterWaitGroupReuse tests the race condition where WaitGroup is reused
// before previous Wait has returned, which should panic with:
// "sync: WaitGroup is reused before previous Wait has returned"
func TestHTTPWriterWaitGroupReuse(t *testing.T) {
	t.Parallel()
	// Create a mock HTTP server that responds slowly to increase chance of race
	requestCount := atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		// Small delay to simulate network latency and increase race window
		time.Sleep(1 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		server.Close()
	})

	ctx := t.Context()
	writer := NewHTTPWriter(ctx, server.URL)

	// Use a channel to detect panics
	panicChan := make(chan any, 1)
	done := make(chan bool)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicChan <- r
			}
			close(done)
		}()

		// Run multiple iterations to increase likelihood of hitting the race
		for range 20 {
			var wg sync.WaitGroup

			// Spawn multiple goroutines that write concurrently
			numWriters := 5
			for i := range numWriters {
				wg.Go(func() {
					// Write multiple log lines
					for j := range 3 {
						logLine := fmt.Sprintf(`{"level":"info","msg":"test log %d-%d"}`+"\n", i, j)
						_, err := writer.Write([]byte(logLine))
						if err != nil {
							t.Errorf("Write failed: %v", err)
						}
						// Yield to increase chance of interleaving
						runtime.Gosched()
					}
				})
			}

			// Spawn multiple goroutines that call Sync() concurrently
			numSyncers := 2
			for range numSyncers {
				wg.Go(func() {
					runtime.Gosched() // Let some Write() calls happen first
					err := writer.Sync()
					if err != nil {
						t.Errorf("Sync failed: %v", err)
					}
					// Immediately try to write again after sync
					// This creates the race: Sync() is waiting on WaitGroup,
					// but Write() immediately tries to reuse it
					logLine := `{"level":"info","msg":"post-sync log"}` + "\n"
					_, err = writer.Write([]byte(logLine))
					if err != nil {
						t.Errorf("Write after sync failed: %v", err)
					}
				})
			}

			// Wait for all goroutines to complete
			wg.Wait()

			// Final sync to wait for any pending writes
			err := writer.Sync()
			if err != nil {
				t.Errorf("Final sync failed: %v", err)
			}
		}
	}()

	// Wait for test to complete or timeout
	select {
	case panicMsg := <-panicChan:
		t.Fatalf("PANIC detected: %v", panicMsg)
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out")
	}
}

// TestHTTPWriterConcurrentWriteSync tests heavy concurrent usage
// This is a more aggressive test that tries to trigger the race condition
func TestHTTPWriterConcurrentWriteSync(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		server.Close()
	})

	ctx := t.Context()
	writer := NewHTTPWriter(ctx, server.URL)

	// Detect if panic occurs
	var panicDetected atomic.Bool

	// Run the test for a short duration with moderate concurrency
	stopChan := make(chan struct{})
	var testWg sync.WaitGroup

	// Writer goroutines
	for range 5 {
		testWg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					panicDetected.Store(true)
					t.Logf("PANIC in writer: %v", r)
				}
			}()

			for {
				select {
				case <-stopChan:
					return
				default:
					logLine := `{"level":"info","msg":"test"}` + "\n"
					writer.Write([]byte(logLine))
					// Small sleep to avoid overwhelming the server
					time.Sleep(1 * time.Millisecond)
					runtime.Gosched()
				}
			}
		})
	}

	// Sync goroutines - these call Sync() repeatedly
	for range 3 {
		testWg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					panicDetected.Store(true)
					t.Logf("PANIC in syncer: %v", r)
				}
			}()

			for {
				select {
				case <-stopChan:
					return
				default:
					writer.Sync()
					// Small sleep to avoid overwhelming the server
					time.Sleep(2 * time.Millisecond)
					runtime.Gosched()
				}
			}
		})
	}

	// Let the test run for a short duration
	time.Sleep(500 * time.Millisecond)
	close(stopChan)
	testWg.Wait()

	// Final sync
	err := writer.Sync()
	if err != nil {
		t.Errorf("Final sync failed: %v", err)
	}

	if panicDetected.Load() {
		t.Fatal("Race condition detected: WaitGroup reuse panic occurred")
	}
}

// TestHTTPWriterSequentialWrites tests basic sequential write and sync
func TestHTTPWriterSequentialWrites(t *testing.T) {
	t.Parallel()
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() {
		server.Close()
	})

	ctx := t.Context()
	writer := NewHTTPWriter(ctx, server.URL)

	// Write multiple log lines
	logLines := []string{
		`{"level":"info","msg":"line 1"}` + "\n",
		`{"level":"warn","msg":"line 2"}` + "\n",
		`{"level":"error","msg":"line 3"}` + "\n",
	}

	for _, line := range logLines {
		n, err := writer.Write([]byte(line))
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
		if n != len(line) {
			t.Errorf("Expected to write %d bytes, wrote %d", len(line), n)
		}
	}

	// Sync to ensure all writes complete
	err := writer.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Give a moment for async operations to complete
	time.Sleep(100 * time.Millisecond)

	if requestCount.Load() != int32(len(logLines)) {
		t.Errorf("Expected %d requests, got %d", len(logLines), requestCount.Load())
	}
}

// TestDynamicHTTPWriterPrimaryAndShadow verifies primary + shadow fan-out.
func TestDynamicHTTPWriterPrimaryAndShadow(t *testing.T) {
	t.Parallel()

	var primaryCount, shadowCount atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(primary.Close)
	shadow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		shadowCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(shadow.Close)

	resolve := func(context.Context) LogRoute {
		return LogRoute{
			PrimaryURL:              primary.URL,
			ShadowURLs:              []string{shadow.URL},
			Timeout:                 2 * time.Second,
			MaxInflightShadowWrites: 1,
		}
	}

	writer := NewDynamicHTTPWriter(t.Context(), primary.URL, resolve)
	_, err := writer.Write([]byte(`{"level":"info","msg":"line"}` + "\n"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if primaryCount.Load() != 1 {
		t.Errorf("expected 1 primary request, got %d", primaryCount.Load())
	}
	if shadowCount.Load() != 1 {
		t.Errorf("expected 1 shadow request, got %d", shadowCount.Load())
	}
}

// TestDynamicHTTPWriterShadowFailureIgnored verifies a broken shadow does not
// affect the primary write.
func TestDynamicHTTPWriterShadowFailureIgnored(t *testing.T) {
	t.Parallel()

	var primaryCount atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(primary.Close)
	shadowAttempted := make(chan struct{}, 1)
	shadow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		shadowAttempted <- struct{}{}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(shadow.Close)

	resolve := func(context.Context) LogRoute {
		return LogRoute{
			PrimaryURL:              primary.URL,
			ShadowURLs:              []string{shadow.URL},
			Timeout:                 500 * time.Millisecond,
			MaxInflightShadowWrites: 1,
		}
	}

	writer := NewDynamicHTTPWriter(t.Context(), primary.URL, resolve).(*HTTPWriter)
	if err := writer.routeLogLine([]byte(`{"msg":"x"}`)); err != nil {
		t.Fatalf("primary failed because shadow returned an error: %v", err)
	}

	select {
	case <-shadowAttempted:
	case <-time.After(time.Second):
		t.Fatal("shadow request was not attempted")
	}

	if primaryCount.Load() != 1 {
		t.Errorf("expected 1 primary request despite shadow failure, got %d", primaryCount.Load())
	}
}

// TestDynamicHTTPWriterShadowFanoutBounded verifies shadow writes are capped by
// the resolved limit: when it is saturated by in-flight (blocked) shadow
// requests, additional shadow writes are dropped while the primary always
// succeeds.
func TestDynamicHTTPWriterShadowFanoutBounded(t *testing.T) {
	t.Parallel()

	var primaryCount, shadowStarted atomic.Int32

	// release gates the shadow handler so we can hold the semaphore slots.
	release := make(chan struct{})
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(primary.Close)
	shadow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		shadowStarted.Add(1)
		<-release // block, holding a semaphore slot
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(shadow.Close)
	t.Cleanup(func() { close(release) })

	const shadowCap = 2
	resolve := func(context.Context) LogRoute {
		return LogRoute{
			PrimaryURL:              primary.URL,
			ShadowURLs:              []string{shadow.URL},
			Timeout:                 5 * time.Second,
			MaxInflightShadowWrites: shadowCap,
		}
	}

	writer := &HTTPWriter{
		ctx:        t.Context(),
		url:        primary.URL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		resolve:    resolve,
		wg:         &sync.WaitGroup{},
	}

	const writes = 50
	for range writes {
		if _, err := writer.Write([]byte(`{"msg":"x"}` + "\n")); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Wait until the shadow slots are saturated (bounded, no unbounded growth).
	deadline := time.Now().Add(5 * time.Second)
	for shadowStarted.Load() < int32(shadowCap) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	if got := shadowStarted.Load(); got > int32(shadowCap) {
		t.Errorf("expected at most %d concurrent shadow writes, got %d", shadowCap, got)
	}
	if got := primaryCount.Load(); got != writes {
		t.Errorf("expected %d primary writes, got %d", writes, got)
	}
}

// TestDynamicHTTPWriterNilResolverIsLegacy verifies a nil resolver behaves like
// the legacy writer (sends to the fixed endpoint).
func TestDynamicHTTPWriterNilResolverIsLegacy(t *testing.T) {
	t.Parallel()

	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	writer := NewDynamicHTTPWriter(t.Context(), server.URL, nil)
	if _, err := writer.Write([]byte(`{"msg":"x"}` + "\n")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if count.Load() != 1 {
		t.Errorf("expected 1 request with nil resolver, got %d", count.Load())
	}
}

// TestHTTPWriterWriteContinuesAfterFailedLine verifies a line the collector
// rejects does not stop the remaining lines of the same Write from being sent.
func TestHTTPWriterWriteContinuesAfterFailedLine(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var delivered []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) == "rejected" {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}
		mu.Lock()
		delivered = append(delivered, string(body))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	writer := NewHTTPWriter(t.Context(), server.URL)
	_, err := writer.Write([]byte("first\nrejected\nsecond\nthird"))
	require.NoError(t, err)
	require.NoError(t, writer.Sync())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"first", "second", "third"}, delivered)
}

// TestRouteFailureLogReportsAtMostOncePerInterval verifies failures from many
// Writes are folded into one stderr report per interval, with nothing lost
// from the counts.
func TestRouteFailureLogReportsAtMostOncePerInterval(t *testing.T) {
	t.Parallel()

	var failures routeFailureLog
	start := time.Now()
	sendErr := errors.New("collector down")

	msg, ok := failures.record(start, 1, 10, sendErr)
	require.True(t, ok, "first failure must be reported immediately")
	assert.Equal(t, "dropped 1 log line(s) (10 bytes) while routing since the last report; last error: collector down", msg)

	_, ok = failures.record(start.Add(time.Second), 2, 20, sendErr)
	assert.False(t, ok)
	_, ok = failures.record(start.Add(routeFailureLogInterval-time.Nanosecond), 1, 5, sendErr)
	assert.False(t, ok)

	msg, ok = failures.record(start.Add(routeFailureLogInterval), 1, 7, sendErr)
	require.True(t, ok, "a failure after the interval must be reported")
	assert.Contains(t, msg, "dropped 4 log line(s) (32 bytes)")
}
