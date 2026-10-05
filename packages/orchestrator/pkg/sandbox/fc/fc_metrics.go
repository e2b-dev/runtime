//go:build linux

package fc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	// metricsReaderBufSize is the scanner buffer for a single Firecracker metrics line.
	// 1 MB is well above the size of any single Firecracker metrics JSON line.
	metricsReaderBufSize = 1 * 1024 * 1024 // 1 MB

	// metricsFlushInterval controls how often we trigger a Firecracker metrics flush.
	metricsFlushInterval = 5 * time.Second
)

var (
	fcMeter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc")

	// direction attributes reused on every record call.
	directionKey = attribute.Key("direction")
	attrTX       = metric.WithAttributes(directionKey.String("tx"))
	attrRX       = metric.WithAttributes(directionKey.String("rx"))
	attrRead     = metric.WithAttributes(directionKey.String("read"))
	attrWrite    = metric.WithAttributes(directionKey.String("write"))

	// Counters — global totals, no sandbox_id to avoid high cardinality.
	fcNetFails         = utils.Must(telemetry.GetCounter(fcMeter, telemetry.SandboxFCNetFails))
	fcNetNoAvailBuffer = utils.Must(telemetry.GetCounter(fcMeter, telemetry.SandboxFCNetNoAvailBuffer))
	fcNetTapIOFails    = utils.Must(telemetry.GetCounter(fcMeter, telemetry.SandboxFCNetTapIOFails))

	// Histograms — per-sandbox distribution per flush, no sandbox_id.
	fcNetBytes                = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCNetBytes))
	fcNetPackets              = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCNetPackets))
	fcNetCount                = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCNetCount))
	fcNetRateLimiterThrottled = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCNetRateLimiterThrottled))
	// TX-only: no RX equivalent in Firecracker metrics.
	fcNetRateLimiterEventCount = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCNetRateLimiterEventCount))
	fcNetRemainingReqs         = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCNetRemainingReqs))

	// Block counters.
	fcBlockFails         = utils.Must(telemetry.GetCounter(fcMeter, telemetry.SandboxFCBlockFails))
	fcBlockNoAvailBuffer = utils.Must(telemetry.GetCounter(fcMeter, telemetry.SandboxFCBlockNoAvailBuffer))

	// Refused snapshot loads, by reason. Recorded from the API error rather
	// than from the metrics FIFO: a refused load stops the Firecracker process
	// before it flushes, so its own metrics never carry the failure.
	fcSnapshotLoadFailures = utils.Must(telemetry.GetCounter(fcMeter, telemetry.SandboxFCSnapshotLoadFailures))

	// Block histograms.
	fcBlockBytes                 = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockBytes))
	fcBlockCount                 = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockCount))
	fcBlockQueueEventCount       = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockQueueEventCount))
	fcBlockRateLimiterThrottled  = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockRateLimiterThrottled))
	fcBlockRateLimiterEventCount = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockRateLimiterEventCount))
	fcBlockIOEngineThrottled     = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockIOEngineThrottled))
	fcBlockRemainingReqs         = utils.Must(telemetry.GetHistogram(fcMeter, telemetry.SandboxFCBlockRemainingReqs))

	// Counter incremented by the stalled thread count at each 1 s poll.
	// rate() of this counter shows throttle intensity in real-time and
	// distinguishes dirty-page throttle (non-zero rate) from GCS cold cache
	// (zero rate) without requiring a Tempo trace lookup.
	balanceDirtyPageThreads = utils.Must(telemetry.GetCounter(fcMeter, telemetry.OrchestratorHostBalanceDirtyPagesThreads))
)

// dirtyPollInterval is how often monitorDirtyPageThrottle samples wchan entries.
// 1 s gives adequate resolution for stall episodes that last several seconds,
// and is consistent with other host-level pollers in pkg/metrics/host.go.
const dirtyPollInterval = 1 * time.Second

// balanceDirtyPagesWchan is the kernel wait-channel symbol written to
// /proc/self/task/*/wchan when a thread is parked in balance_dirty_pages.
// Fires for both per-BDI and global dirty-page throttle regardless of the
// global dirty/MemTotal ratio, making it the only reliable userspace signal
// for per-BDI throttle on large-RAM nodes.
const balanceDirtyPagesWchan = "balance_dirty_pages"

// countBalanceDirtyThreads returns the number of OS threads of the current
// process currently stalled in balance_dirty_pages by reading
// /proc/self/task/*/wchan. Returns 0 on any read error.
func countBalanceDirtyThreads() int {
	pid := os.Getpid()
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/wchan", pid, e.Name()))
		if err != nil {
			continue
		}
		if strings.TrimSpace(string(data)) == balanceDirtyPagesWchan {
			n++
		}
	}

	return n
}

func init() {
	go monitorDirtyPageThrottle()
}

// monitorDirtyPageThrottle runs for the lifetime of the process, sampling
// balance_dirty_pages thread counts every dirtyPollInterval and incrementing
// balanceDirtyPageThreads. rate() of the counter gives real-time throttle
// intensity; 0 means no dirty-page stalls are occurring.
func monitorDirtyPageThrottle() {
	ticker := time.NewTicker(dirtyPollInterval)
	defer ticker.Stop()
	for range ticker.C {
		// Add even when 0 so the counter exports from process start —
		// otherwise a node that never stalls has no series at all and
		// dashboards can't tell "no stalls" from "no metric".
		balanceDirtyPageThreads.Add(context.Background(), int64(countBalanceDirtyThreads()))
	}
}

// firecrackerNetMetrics is a subset of Firecracker's NetDeviceMetrics we export via OTEL.
// Full metric list: https://github.com/firecracker-microvm/firecracker/blob/main/docs/metrics.md
// Values are per-flush deltas; flush defaults to 60 s, additional flushes via FlushMetrics API.
type firecrackerNetMetrics struct {
	// TX
	TxBytesCount            uint64 `json:"tx_bytes_count"`
	TxPacketsCount          uint64 `json:"tx_packets_count"`
	TxCount                 uint64 `json:"tx_count"`
	TxFails                 uint64 `json:"tx_fails"`
	TxRateLimiterThrottled  uint64 `json:"tx_rate_limiter_throttled"`
	TxRateLimiterEventCount uint64 `json:"tx_rate_limiter_event_count"`
	TxRemainingReqsCount    uint64 `json:"tx_remaining_reqs_count"`
	NoTxAvailBuffer         uint64 `json:"no_tx_avail_buffer"`
	TapWriteFails           uint64 `json:"tap_write_fails"`
	// RX
	RxBytesCount           uint64 `json:"rx_bytes_count"`
	RxPacketsCount         uint64 `json:"rx_packets_count"`
	RxCount                uint64 `json:"rx_count"`
	RxFails                uint64 `json:"rx_fails"`
	RxRateLimiterThrottled uint64 `json:"rx_rate_limiter_throttled"`
	NoRxAvailBuffer        uint64 `json:"no_rx_avail_buffer"`
	TapReadFails           uint64 `json:"tap_read_fails"`
}

// firecrackerBlockMetrics is a subset of Firecracker's BlockDeviceMetrics we export via OTEL.
// Full metric list: https://github.com/firecracker-microvm/firecracker/blob/main/docs/metrics.md
// Values are per-flush deltas. The aggregate "block" key sums over all drives; we only have one (rootfs).
type firecrackerBlockMetrics struct {
	ReadBytes                  uint64 `json:"read_bytes"`
	WriteBytes                 uint64 `json:"write_bytes"`
	ReadCount                  uint64 `json:"read_count"`
	WriteCount                 uint64 `json:"write_count"`
	QueueEventCount            uint64 `json:"queue_event_count"`
	RateLimiterThrottledEvents uint64 `json:"rate_limiter_throttled_events"`
	RateLimiterEventCount      uint64 `json:"rate_limiter_event_count"`
	IOEngineThrottledEvents    uint64 `json:"io_engine_throttled_events"`
	NoAvailBuffer              uint64 `json:"no_avail_buffer"`
	ExecuteFails               uint64 `json:"execute_fails"`
	EventFails                 uint64 `json:"event_fails"`
	RemainingReqsCount         uint64 `json:"remaining_reqs_count"`
}

// firecrackerBalloonMetrics is a subset of Firecracker's BalloonDeviceMetrics.
// Counters are SharedIncMetric — each flush emits the delta since the previous
// serialize, so we accumulate them in the reader.
type firecrackerBalloonMetrics struct {
	FreePageHintCount   uint64 `json:"free_page_hint_count"`
	FreePageHintFreed   uint64 `json:"free_page_hint_freed"`
	FreePageHintFails   uint64 `json:"free_page_hint_fails"`
	FreePageReportCount uint64 `json:"free_page_report_count"`
	FreePageReportFreed uint64 `json:"free_page_report_freed"`
	FreePageReportFails uint64 `json:"free_page_report_fails"`
}

// firecrackerMetrics is the top-level structure of one Firecracker metrics JSON line.
type firecrackerMetrics struct {
	// UtcTimestampMs is when FC serialized the line; a flush requested at time
	// T produces a line stamped at or after T.
	UtcTimestampMs int64                     `json:"utc_timestamp_ms"`
	Net            firecrackerNetMetrics     `json:"net"`
	Block          firecrackerBlockMetrics   `json:"block"`
	Balloon        firecrackerBalloonMetrics `json:"balloon"`
}

// BalloonMetricsSnapshot is the cumulative-since-FC-start view of
// virtio-balloon counters, exposed via Process.BalloonMetrics.
type BalloonMetricsSnapshot struct {
	HintCount   uint64
	HintFreed   uint64
	HintFails   uint64
	ReportCount uint64
	ReportFreed uint64
	ReportFails uint64
}

// fphFlushReadTimeout caps how long FlushAndReadBalloonMetrics waits for the
// metrics-reader goroutine to consume FC's response line.
const fphFlushReadTimeout = 2 * time.Second

func accumulateBalloon(prev *BalloonMetricsSnapshot, b firecrackerBalloonMetrics) BalloonMetricsSnapshot {
	next := BalloonMetricsSnapshot{
		HintCount:   b.FreePageHintCount,
		HintFreed:   b.FreePageHintFreed,
		HintFails:   b.FreePageHintFails,
		ReportCount: b.FreePageReportCount,
		ReportFreed: b.FreePageReportFreed,
		ReportFails: b.FreePageReportFails,
	}
	if prev != nil {
		next.HintCount += prev.HintCount
		next.HintFreed += prev.HintFreed
		next.HintFails += prev.HintFails
		next.ReportCount += prev.ReportCount
		next.ReportFreed += prev.ReportFreed
		next.ReportFails += prev.ReportFails
	}

	return next
}

// BalloonMetrics returns the cumulative virtio-balloon counters observed so far.
func (p *Process) BalloonMetrics() BalloonMetricsSnapshot {
	if cur := p.balloonAccum.Load(); cur != nil {
		return *cur
	}

	return BalloonMetricsSnapshot{}
}

// FlushMetrics triggers an FC metrics flush. Non-blocking on the reader.
func (p *Process) FlushMetrics(ctx context.Context) error {
	return p.client.flushMetrics(ctx)
}

// FlushAndReadBalloonMetrics flushes and waits for the reader to ingest a
// line FC serialized at or after the request, returning the updated cumulative
// snapshot. The periodic flusher runs concurrently, so "the accumulator moved"
// is not enough: the line that moved it may predate this request and leave the
// caller's own discards for the next reader. On flush error (e.g. FC already
// torn down) returns the last observed snapshot.
func (p *Process) FlushAndReadBalloonMetrics(ctx context.Context) (BalloonMetricsSnapshot, error) {
	sinceMs := time.Now().UnixMilli()
	if err := p.client.flushMetrics(ctx); err != nil {
		return p.BalloonMetrics(), fmt.Errorf("flush metrics: %w", err)
	}
	if err := p.waitMetricsLineSince(ctx, sinceMs); err != nil {
		return p.BalloonMetrics(), err
	}

	return p.BalloonMetrics(), nil
}

// waitMetricsLineSince blocks until the reader has ingested a metrics line
// stamped at or after sinceMs, fphFlushReadTimeout at most.
func (p *Process) waitMetricsLineSince(ctx context.Context, sinceMs int64) error {
	deadline := time.Now().Add(fphFlushReadTimeout)
	for {
		if p.metricsLineMs.Load() >= sinceMs {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for fresh balloon metrics line")
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// runMetricsFlushLoop asks Firecracker to flush its metrics on every tick until
// the VM exits. It reports flush failures through onFlushErr.
func runMetricsFlushLoop(
	ctx context.Context,
	exit <-chan struct{},
	ticks <-chan time.Time,
	flush func(context.Context) error,
	onFlushErr func(error),
) {
	for {
		select {
		case <-exit:
			return
		case <-ticks:
			// select picks ready cases at random, so a tick can win against an
			// already-closed exit; re-check or the flush hits a dead Firecracker
			// and logs a spurious API failure.
			select {
			case <-exit:
				return
			default:
			}

			if err := flush(ctx); err != nil {
				// Firecracker can exit while the flush request is in flight,
				// after the pre-flush re-check above: the failure is then the
				// expected shutdown reset, not a real error worth warning about.
				select {
				case <-exit:
					return
				default:
				}

				onFlushErr(err)
			}
		}
	}
}

// startMetricsReader opens the metrics FIFO and starts a goroutine that reads
// Firecracker metrics lines and exports metrics via OTEL.
// It must be called before setMetrics so that the FIFO is open for reading
// before Firecracker opens the write end in response to PUT /metrics.
func (p *Process) startMetricsReader(ctx context.Context) {
	// Detach from the request context so the goroutine runs for the VM's lifetime
	// but still inherits trace values for logging.
	ctx = context.WithoutCancel(ctx)
	sandboxID := p.files.SandboxID
	metricsPath := p.metricsPath

	// Flusher: periodically triggers a Firecracker metrics flush so the reader receives
	// fresh data at metricsFlushInterval instead of the default 60 s.
	go func() {
		ticker := time.NewTicker(metricsFlushInterval)
		defer ticker.Stop()

		runMetricsFlushLoop(ctx, p.Exit.Done(), ticker.C, p.client.flushMetrics, func(err error) {
			logger.L().Warn(ctx, "failed to flush fc metrics",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)
		})
	}()

	go func() {
		// O_RDWR opens without blocking (no need to wait for a writer).
		// We keep this FD solely to unblock the open; the scanner reads from
		// a separate O_RDONLY FD below. On process exit we close the O_RDWR FD
		// to drop our write reference — once Firecracker also exits, the
		// O_RDONLY read receives EOF and the goroutine exits cleanly.
		rwFd, err := os.OpenFile(metricsPath, os.O_RDWR, os.ModeNamedPipe)
		if err != nil {
			logger.L().Warn(ctx, "failed to open fc metrics FIFO",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)

			return
		}

		// O_RDONLY succeeds immediately because O_RDWR already established both ends.
		rFd, err := os.OpenFile(metricsPath, os.O_RDONLY, os.ModeNamedPipe)
		if err != nil {
			rwFd.Close()
			logger.L().Warn(ctx, "failed to open fc metrics FIFO for reading",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)

			return
		}
		defer rFd.Close()

		// Drop our write reference on exit so the scanner can receive EOF.
		go func() {
			<-p.Exit.Done()
			rwFd.Close()
		}()

		scanner := bufio.NewScanner(rFd)
		scanner.Buffer(make([]byte, metricsReaderBufSize), metricsReaderBufSize)

		for scanner.Scan() {
			var m firecrackerMetrics
			if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
				logger.L().Warn(ctx, "failed to parse fc metrics line",
					zap.Error(err),
					logger.WithSandboxID(sandboxID),
				)

				continue
			}

			n := &m.Net

			// TX histograms — values are already per-flush deltas from Firecracker.
			fcNetBytes.Record(ctx, int64(n.TxBytesCount), attrTX)
			fcNetPackets.Record(ctx, int64(n.TxPacketsCount), attrTX)
			fcNetCount.Record(ctx, int64(n.TxCount), attrTX)
			fcNetRateLimiterEventCount.Record(ctx, int64(n.TxRateLimiterEventCount), attrTX)
			fcNetRemainingReqs.Record(ctx, int64(n.TxRemainingReqsCount), attrTX)

			// Only record throttled when non-zero to avoid polluting the histogram with idle intervals.
			if n.TxRateLimiterThrottled > 0 {
				fcNetRateLimiterThrottled.Record(ctx, int64(n.TxRateLimiterThrottled), attrTX)
			}

			// RX histograms.
			fcNetBytes.Record(ctx, int64(n.RxBytesCount), attrRX)
			fcNetPackets.Record(ctx, int64(n.RxPacketsCount), attrRX)
			fcNetCount.Record(ctx, int64(n.RxCount), attrRX)

			if n.RxRateLimiterThrottled > 0 {
				fcNetRateLimiterThrottled.Record(ctx, int64(n.RxRateLimiterThrottled), attrRX)
			}

			// Global error/event counters (only increment on non-zero values).
			if n.TxFails > 0 {
				fcNetFails.Add(ctx, int64(n.TxFails), attrTX)
			}
			if n.RxFails > 0 {
				fcNetFails.Add(ctx, int64(n.RxFails), attrRX)
			}
			if n.NoTxAvailBuffer > 0 {
				fcNetNoAvailBuffer.Add(ctx, int64(n.NoTxAvailBuffer), attrTX)
			}
			if n.NoRxAvailBuffer > 0 {
				fcNetNoAvailBuffer.Add(ctx, int64(n.NoRxAvailBuffer), attrRX)
			}
			if n.TapWriteFails > 0 {
				fcNetTapIOFails.Add(ctx, int64(n.TapWriteFails), attrTX)
			}
			if n.TapReadFails > 0 {
				fcNetTapIOFails.Add(ctx, int64(n.TapReadFails), attrRX)
			}

			// Block histograms — values are already per-flush deltas from Firecracker.
			b := &m.Block

			fcBlockBytes.Record(ctx, int64(b.ReadBytes), attrRead)
			fcBlockBytes.Record(ctx, int64(b.WriteBytes), attrWrite)
			fcBlockCount.Record(ctx, int64(b.ReadCount), attrRead)
			fcBlockCount.Record(ctx, int64(b.WriteCount), attrWrite)
			// Recorded unconditionally, unlike the throttle counters below: a stalled
			// virtio-blk queue shows up as no notifications reaching Firecracker while
			// requests are outstanding, and recording only non-zero values would render
			// that as absent rather than zero.
			fcBlockQueueEventCount.Record(ctx, int64(b.QueueEventCount))
			fcBlockRateLimiterEventCount.Record(ctx, int64(b.RateLimiterEventCount))
			fcBlockRemainingReqs.Record(ctx, int64(b.RemainingReqsCount))

			if b.RateLimiterThrottledEvents > 0 {
				fcBlockRateLimiterThrottled.Record(ctx, int64(b.RateLimiterThrottledEvents))
			}
			if b.IOEngineThrottledEvents > 0 {
				fcBlockIOEngineThrottled.Record(ctx, int64(b.IOEngineThrottledEvents))
			}

			// Block global error/event counters.
			if b.ExecuteFails > 0 || b.EventFails > 0 {
				fcBlockFails.Add(ctx, int64(b.ExecuteFails)+int64(b.EventFails))
			}
			if b.NoAvailBuffer > 0 {
				fcBlockNoAvailBuffer.Add(ctx, int64(b.NoAvailBuffer))
			}

			// Balloon: SharedIncMetric resets on flush, so accumulate. The
			// stamp goes last so a waiter that sees it also sees the balloon
			// counters of that line.
			next := accumulateBalloon(p.balloonAccum.Load(), m.Balloon)
			p.balloonAccum.Store(&next)
			p.metricsLineMs.Store(m.UtcTimestampMs)
		}

		if err := scanner.Err(); err != nil {
			if errors.Is(err, bufio.ErrTooLong) {
				logger.L().Error(ctx, "fc metrics line exceeded buffer size, metrics reader stopped",
					zap.Int("bufferSizeBytes", metricsReaderBufSize),
					logger.WithSandboxID(sandboxID),
				)
			} else {
				logger.L().Warn(ctx, "fc metrics FIFO scanner error",
					zap.Error(err),
					logger.WithSandboxID(sandboxID),
				)
			}
		}
	}()
}
