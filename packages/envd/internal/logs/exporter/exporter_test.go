package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/host"
)

const writeLatencyBound = time.Second

type lockCheckingTransport struct {
	exporter *HTTPExporter
	result   chan<- bool
}

func (t *lockCheckingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	acquired := t.exporter.logLock.TryLock()
	if acquired {
		t.exporter.logLock.Unlock()
	}
	t.result <- acquired

	return &http.Response{
		StatusCode: http.StatusNoContent,
		Status:     http.StatusText(http.StatusNoContent),
		Body:       http.NoBody,
		Header:     make(http.Header),
		Request:    request,
	}, nil
}

func newTestExporter(collectorAddress string) *HTTPExporter {
	exporter := &HTTPExporter{
		client:       http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
		triggers:     make(chan struct{}, 1),
		jsonErrLog:   newRateLimitedLogger("test JSON error: %v"),
		sendErrLog:   newRateLimitedLogger("test send error: %v"),
		oversizedLog: newRateLimitedLogger("test oversized log: %d"),
	}
	if collectorAddress != "" {
		exporter.mmdsOpts.Store(&host.MMDSOpts{
			SandboxID:            "sandbox-id",
			TemplateID:           "template-id",
			LogsCollectorAddress: collectorAddress,
		})
	}

	return exporter
}

func requireEmptyBuffer(t *testing.T, exporter *HTTPExporter) {
	t.Helper()

	exporter.logLock.Lock()
	defer exporter.logLock.Unlock()

	assert.Nil(t, exporter.logs)
	assert.Zero(t, exporter.bufferedBytes)
}

func TestHTTPExporterFlushAndPurge(t *testing.T) {
	t.Parallel()

	var (
		requestLock sync.Mutex
		requests    [][]byte
	)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if !assert.NoError(t, err, "read request body") {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		requestLock.Lock()
		requests = append(requests, body)
		requestLock.Unlock()

		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)

	exporter := newTestExporter(collector.URL)
	exporter.addLogs([]byte(`{"message":"first"}`))
	exporter.addLogs([]byte(`{"message":"second"}`))

	require.NoError(t, exporter.FlushAndPurge(t.Context()))
	requireEmptyBuffer(t, exporter)

	requestLock.Lock()
	defer requestLock.Unlock()
	require.Len(t, requests, 2)
	assert.JSONEq(t, `{"message":"first","instanceID":"sandbox-id","envID":"template-id"}`, string(requests[0]))
	assert.JSONEq(t, `{"message":"second","instanceID":"sandbox-id","envID":"template-id"}`, string(requests[1]))
}

func TestHTTPExporterWriteFlushBarrier(t *testing.T) {
	t.Parallel()

	const lineCount = 256

	var (
		countsLock sync.Mutex
		counts     = make(map[int]int, lineCount)
	)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Sequence int `json:"sequence"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); !assert.NoError(t, err, "decode request") {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		countsLock.Lock()
		counts[payload.Sequence]++
		countsLock.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)

	exporter := newTestExporter(collector.URL)
	for sequence := range lineCount {
		line := fmt.Appendf(nil, `{"sequence":%d}`, sequence)
		written, err := exporter.Write(line)
		require.NoError(t, err)
		assert.Equal(t, len(line), written)
	}

	require.NoError(t, exporter.FlushAndPurge(t.Context()))
	requireEmptyBuffer(t, exporter)

	countsLock.Lock()
	defer countsLock.Unlock()
	require.Len(t, counts, lineCount)
	for sequence := range lineCount {
		assert.Equal(t, 1, counts[sequence], "sequence %d send count", sequence)
	}
}

func TestHTTPExporterFlushAndPurgePurgesConcurrentWriteWithoutBlocking(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)

	exporter := newTestExporter(collector.URL)
	_, err := exporter.Write([]byte(`{"message":"before"}`))
	require.NoError(t, err)

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- exporter.FlushAndPurge(t.Context())
	}()

	<-requestStarted
	writeDone := make(chan error, 1)
	go func() {
		_, err := exporter.Write([]byte(`{"message":"during"}`))
		writeDone <- err
	}()

	var writeErr error
	select {
	case writeErr = <-writeDone:
	case <-time.After(time.Second):
		close(releaseRequest)
		<-flushDone
		require.Fail(t, "Write blocked on the stalled collector")
	}

	close(releaseRequest)

	require.NoError(t, writeErr)
	require.NoError(t, <-flushDone)
	requireEmptyBuffer(t, exporter)
}

func TestHTTPExporterWriteDoesNotBlockOnStalledBackgroundSend(t *testing.T) {
	t.Parallel()

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var (
		requestOnce sync.Once
		releaseOnce sync.Once
	)
	release := func() {
		releaseOnce.Do(func() { close(releaseRequest) })
	}
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestOnce.Do(func() { close(requestStarted) })
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)
	t.Cleanup(release)

	exporter := newTestExporter(collector.URL)
	ctx, cancel := context.WithCancel(t.Context())
	backgroundDone := make(chan struct{})
	go func() {
		defer close(backgroundDone)
		exporter.start(ctx)
	}()

	_, err := exporter.Write([]byte(`{"message":"start stalled send"}`))
	require.NoError(t, err)
	select {
	case <-requestStarted:
	case <-time.After(writeLatencyBound):
		cancel()
		release()
		<-backgroundDone
		require.Fail(t, "background send did not reach the collector")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := exporter.Write([]byte(`{"message":"write during stalled send"}`))
		writeDone <- err
	}()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(writeLatencyBound):
		cancel()
		release()
		<-writeDone
		<-backgroundDone
		require.Fail(t, "Write blocked on the stalled background send")
	}

	cancel()
	release()
	select {
	case <-backgroundDone:
	case <-time.After(writeLatencyBound):
		require.Fail(t, "background exporter did not stop")
	}
}

func TestHTTPExporterConcurrentWritesStayBoundedDuringStalledBackgroundSend(t *testing.T) {
	t.Parallel()

	const (
		writerCount     = 32
		writesPerWriter = 100
	)

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var (
		requestOnce sync.Once
		releaseOnce sync.Once
	)
	release := func() {
		releaseOnce.Do(func() { close(releaseRequest) })
	}
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestOnce.Do(func() { close(requestStarted) })
		<-releaseRequest
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)
	t.Cleanup(release)

	exporter := newTestExporter(collector.URL)
	ctx, cancel := context.WithCancel(t.Context())
	backgroundDone := make(chan struct{})
	go func() {
		defer close(backgroundDone)
		exporter.start(ctx)
	}()

	_, err := exporter.Write([]byte(`{"message":"start stalled send"}`))
	require.NoError(t, err)
	select {
	case <-requestStarted:
	case <-time.After(writeLatencyBound):
		cancel()
		release()
		<-backgroundDone
		require.Fail(t, "background send did not reach the collector")
	}

	type writeResult struct {
		err           error
		latency       time.Duration
		bufferedBytes int
	}
	const totalWrites = writerCount * writesPerWriter
	results := make(chan writeResult, totalWrites)
	line := append([]byte(`{"message":"`), bytes.Repeat([]byte("x"), 4<<10)...)
	line = append(line, []byte(`"}`)...)

	var writers sync.WaitGroup
	for range writerCount {
		writers.Go(func() {
			for range writesPerWriter {
				start := time.Now()
				_, err := exporter.Write(line)
				latency := time.Since(start)

				exporter.logLock.Lock()
				bufferedBytes := exporter.bufferedBytes
				exporter.logLock.Unlock()
				results <- writeResult{err: err, latency: latency, bufferedBytes: bufferedBytes}
			}
		})
	}
	writersDone := make(chan struct{})
	go func() {
		writers.Wait()
		close(results)
		close(writersDone)
	}()

	writersTimedOut := false
	select {
	case <-writersDone:
	case <-time.After(5 * writeLatencyBound):
		writersTimedOut = true
	}

	var finalBufferedBytes, finalBufferedLogs int
	if !writersTimedOut {
		exporter.logLock.Lock()
		finalBufferedBytes = exporter.bufferedBytes
		finalBufferedLogs = len(exporter.logs)
		exporter.logLock.Unlock()
	}

	cancel()
	release()
	if writersTimedOut {
		<-writersDone
	}
	<-backgroundDone

	assert.False(t, writersTimedOut, "concurrent Writes blocked on the stalled background send")
	resultCount := 0
	for result := range results {
		resultCount++
		require.NoError(t, result.err)
		assert.Less(t, result.latency, writeLatencyBound)
		assert.LessOrEqual(t, result.bufferedBytes, maxBufferedBytes)
	}
	assert.Equal(t, totalWrites, resultCount)
	if !writersTimedOut {
		assert.Positive(t, finalBufferedBytes)
		assert.LessOrEqual(t, finalBufferedBytes, maxBufferedBytes)
		assert.Less(t, finalBufferedLogs, totalWrites, "the bounded buffer should evict old log lines")
	}
}

func TestHTTPExporterNeverHoldsLogLockAcrossIO(t *testing.T) {
	t.Parallel()

	t.Run("background send", func(t *testing.T) {
		t.Parallel()

		exporter := newTestExporter("http://collector.invalid")
		result := make(chan bool, 1)
		exporter.client.Transport = &lockCheckingTransport{exporter: exporter, result: result}
		ctx, cancel := context.WithCancel(t.Context())
		backgroundDone := make(chan struct{})
		go func() {
			defer close(backgroundDone)
			exporter.start(ctx)
		}()

		_, err := exporter.Write([]byte(`{"message":"background"}`))
		require.NoError(t, err)
		select {
		case acquired := <-result:
			assert.True(t, acquired, "logLock was held during the background RoundTrip")
		case <-time.After(writeLatencyBound):
			require.Fail(t, "background send did not call RoundTrip")
		}

		cancel()
		select {
		case <-backgroundDone:
		case <-time.After(writeLatencyBound):
			require.Fail(t, "background exporter did not stop")
		}
	})

	t.Run("flush and purge", func(t *testing.T) {
		t.Parallel()

		exporter := newTestExporter("http://collector.invalid")
		result := make(chan bool, 1)
		exporter.client.Transport = &lockCheckingTransport{exporter: exporter, result: result}
		exporter.addLogs([]byte(`{"message":"flush"}`))

		require.NoError(t, exporter.FlushAndPurge(t.Context()))
		select {
		case acquired := <-result:
			assert.True(t, acquired, "logLock was held during the FlushAndPurge RoundTrip")
		case <-time.After(writeLatencyBound):
			require.Fail(t, "FlushAndPurge did not call RoundTrip")
		}
	})
}

func TestHTTPExporterFlushAndPurgePurgesAfterCollectorError(t *testing.T) {
	t.Parallel()

	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(collector.Close)

	exporter := newTestExporter(collector.URL)
	exporter.addLogs([]byte(`{"message":"unsent"}`))

	require.Error(t, exporter.FlushAndPurge(t.Context()))
	requireEmptyBuffer(t, exporter)
}

func TestHTTPExporterFlushAndPurgePurgesAfterContextDeadline(t *testing.T) {
	t.Parallel()

	releaseRequest := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		<-releaseRequest
	}))
	t.Cleanup(collector.Close)

	exporter := newTestExporter(collector.URL)
	exporter.addLogs([]byte(`{"message":"timed-out"}`))

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	err := exporter.FlushAndPurge(ctx)
	close(releaseRequest)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	requireEmptyBuffer(t, exporter)
}

func TestHTTPExporterFlushAndPurgeExpiredContextPurgesWithoutSending(t *testing.T) {
	t.Parallel()

	var (
		requestLock sync.Mutex
		requests    int
	)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestLock.Lock()
		requests++
		requestLock.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)

	exporter := newTestExporter(collector.URL)
	exporter.addLogs([]byte(`{"message":"purge-only"}`))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := exporter.FlushAndPurge(ctx)
	require.ErrorIs(t, err, context.Canceled)
	requireEmptyBuffer(t, exporter)

	requestLock.Lock()
	defer requestLock.Unlock()
	assert.Zero(t, requests)
}

func TestHTTPExporterFlushAndPurgeWithoutMMDSOpts(t *testing.T) {
	t.Parallel()

	exporter := newTestExporter("")
	exporter.addLogs([]byte(`{"message":"not-configured"}`))

	require.NoError(t, exporter.FlushAndPurge(t.Context()))
	requireEmptyBuffer(t, exporter)
}

func TestHTTPExporterConcurrentWriteAndFlushAndPurge(t *testing.T) {
	t.Parallel()

	exporter := newTestExporter("")
	line := []byte(`{"message":"concurrent"}`)

	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 200 {
				_, err := exporter.Write(line)
				assert.NoError(t, err, "write log")
			}
		})
	}
	for range 4 {
		workers.Go(func() {
			for range 200 {
				assert.NoError(t, exporter.FlushAndPurge(t.Context()), "flush logs")
			}
		})
	}
	workers.Wait()

	_, err := exporter.Write(line)
	require.NoError(t, err)

	exporter.logLock.Lock()
	defer exporter.logLock.Unlock()
	require.NotEmpty(t, exporter.logs)
}

func TestHTTPExporterFlushAndPurgeDoesNotDuplicateBackgroundSends(t *testing.T) {
	t.Parallel()

	const lineCount = 100

	var (
		countsLock sync.Mutex
		counts     = make(map[int]int, lineCount)
	)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Sequence int `json:"sequence"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); !assert.NoError(t, err, "decode request") {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		countsLock.Lock()
		counts[payload.Sequence]++
		countsLock.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(collector.Close)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	mmdsChan := make(chan *host.MMDSOpts, 1)
	exporter := NewHTTPLogsExporter(ctx, mmdsChan)
	mmdsChan <- &host.MMDSOpts{LogsCollectorAddress: collector.URL}
	require.Eventually(t, func() bool {
		return exporter.mmdsOpts.Load() != nil
	}, time.Second, time.Millisecond)

	for sequence := range lineCount {
		exporter.addLogs(fmt.Appendf(nil, `{"sequence":%d}`, sequence))
	}
	require.NoError(t, exporter.FlushAndPurge(t.Context()))

	require.Eventually(t, func() bool {
		countsLock.Lock()
		defer countsLock.Unlock()

		return len(counts) == lineCount
	}, 5*time.Second, time.Millisecond)

	countsLock.Lock()
	defer countsLock.Unlock()
	for sequence := range lineCount {
		assert.Equal(t, 1, counts[sequence], "sequence %d send count", sequence)
	}
}
