//go:build linux

package metrics

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	memoryPressure   = "some avg10=1.53 avg60=0.87 avg300=0.24 total=118263004\nfull avg10=0.71 avg60=0.40 avg300=0.11 total=53194377\n"
	cpuPressure      = "some avg10=0.00 avg60=0.12 avg300=0.30 total=7700123\n"
	ioPressure       = "some avg10=0 avg60=0 avg300=0 total=10\nfull avg10=0 avg60=0 avg300=0 total=4\n"
	interruptsSample = "           CPU0       CPU1       CPU2\n  0:         21          0          0   IO-APIC   2-edge      timer\nNMI:          0          1          0   Non-maskable interrupts\nCAL:        150        250        100   Function call interrupts\nTLB:         40         60          0   TLB shootdowns\nERR:          0\n"
	vmStatSample     = "nr_free_pages 123\nallocstall_dma 0\nallocstall_dma32 2\nallocstall_normal 40\nallocstall_movable 1\npgscan_kswapd 999\npgscan_direct 5000\npgsteal_direct 4100\npswpin 70\npswpout 300\n"
)

// fakeProc is a /proc whose files can change between collections.
type fakeProc struct {
	mu    sync.Mutex
	files map[string]string
	fail  map[string]error
	reads map[string]int
}

func newFakeProc(files map[string]string) *fakeProc {
	return &fakeProc{mu: sync.Mutex{}, files: files, fail: make(map[string]error), reads: make(map[string]int)}
}

func (f *fakeProc) set(path, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = content
}

func (f *fakeProc) failVMStat(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		delete(f.fail, "/proc/vmstat")

		return
	}
	f.fail["/proc/vmstat"] = err
}

func (f *fakeProc) readCount(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.reads[path]
}

func (f *fakeProc) readFile(path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads[path]++
	if err := f.fail[path]; err != nil {
		return nil, err
	}
	v, ok := f.files[path]
	if !ok {
		return nil, os.ErrNotExist
	}

	return []byte(v), nil
}

func allProc() *fakeProc {
	return newFakeProc(map[string]string{
		"/proc/pressure/memory": memoryPressure,
		"/proc/pressure/cpu":    cpuPressure,
		"/proc/pressure/io":     ioPressure,
		"/proc/vmstat":          vmStatSample,
		"/proc/interrupts":      interruptsSample,
	})
}

func startKernelCounters(t *testing.T, proc *fakeProc) (*sdkmetric.ManualReader, *observer.ObservedLogs) {
	t.Helper()

	reader, logs, k := newTestKernelCounters(t, proc)
	t.Cleanup(func() { require.NoError(t, k.Close(context.WithoutCancel(t.Context()))) })

	return reader, logs
}

func newTestKernelCounters(t *testing.T, proc *fakeProc) (*sdkmetric.ManualReader, *observer.ObservedLogs, *KernelCounters) {
	t.Helper()

	core, logs := observer.New(zap.InfoLevel)
	reader := sdkmetric.NewManualReader()
	k, err := newKernelCounters(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), proc.readFile, logger.NewTracedLoggerFromCore(core))
	require.NoError(t, err)

	return reader, logs, k
}

func collectSums(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	out := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "metric %s is not an int64 sum", m.Name)
			assert.Truef(t, sum.IsMonotonic, "metric %s should be monotonic", m.Name)
			for _, dp := range sum.DataPoints {
				out[m.Name+"|"+dp.Attributes.Encoded(attribute.DefaultEncoder())] = dp.Value
			}
		}
	}

	return out
}

var (
	stall  = string(telemetry.OrchestratorHostPressureStall)
	stalls = string(telemetry.OrchestratorHostReclaimStalls)
	pages  = string(telemetry.OrchestratorHostReclaimPages)
)

func TestPressureParserRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	for name, input := range map[string]string{
		"empty":               "",
		"only unknown scopes": "most avg10=0 total=1\n",
		"no total":            "some avg10=0.00 avg60=0.00\n",
		"bad total":           "some avg10=0.00 total=lots\n",
	} {
		_, err := pressureParser("io")([]byte(input))
		assert.Errorf(t, err, "input %q", name)
	}
}

// The first reading is the baseline: totals start at zero and each later
// reading adds every CPU's difference.
func TestInterruptsParserSumsEveryCPU(t *testing.T) {
	t.Parallel()

	parse := interruptsParser("amd64")
	samples, err := parse([]byte(interruptsSample))
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"function_call": 0, "tlb_shootdown": 0}, interruptsByKind(t, samples))

	samples, err = parse([]byte("           CPU0       CPU1       CPU2\nCAL:        160        255        100   Function call interrupts\nTLB:         41         60          2   TLB shootdowns\n"))
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"function_call": 15, "tlb_shootdown": 3}, interruptsByKind(t, samples))
}

func TestInterruptsParserCountsAcrossAWrap(t *testing.T) {
	t.Parallel()

	parse := interruptsParser("amd64")
	_, err := parse([]byte("  CPU0  CPU1\nTLB:  4294967290  10  TLB shootdowns\n"))
	require.NoError(t, err)
	samples, err := parse([]byte("  CPU0  CPU1\nTLB:  5  12  TLB shootdowns\n"))
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{"tlb_shootdown": 11 + 2}, interruptsByKind(t, samples))
}

// A CPU that comes online adds nothing in its first interval, so a column
// shift is not read as a wrap.
func TestInterruptsParserMatchesCPUsByName(t *testing.T) {
	t.Parallel()

	parse := interruptsParser("amd64")
	_, err := parse([]byte("  CPU0  CPU1\nTLB:  100  4000000000  TLB shootdowns\n"))
	require.NoError(t, err)
	samples, err := parse([]byte("  CPU0  CPU2\nTLB:  103  7  TLB shootdowns\n"))
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{"tlb_shootdown": 3}, interruptsByKind(t, samples))
}

// Hosts with many IRQ numbers pad the labels (" TLB:"). A read that fails
// after one good row changes nothing: the third read's TLB 140 is below the
// failed read's 150, so it only passes if that row was not kept.
func TestInterruptsParserKeepsTotalsAcrossABadRead(t *testing.T) {
	t.Parallel()

	parse := interruptsParser("amd64")
	_, err := parse([]byte("       CPU0       CPU1\n 120:     0     0   PCI-MSI   nvme0q1\n TLB:   100   200   TLB shootdowns\n CAL:  1000  2000   Function call interrupts\n"))
	require.NoError(t, err)
	_, err = parse([]byte("       CPU0       CPU1\n TLB:   150   250   TLB shootdowns\n CAL:  lots  2050   Function call interrupts\n"))
	require.Error(t, err)
	samples, err := parse([]byte("       CPU0       CPU1\n TLB:   140   260   TLB shootdowns\n CAL:  1100  2100   Function call interrupts\n"))
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{"function_call": 200, "tlb_shootdown": 100}, interruptsByKind(t, samples))
}

// A count that goes back is not a 32-bit wrap, so it is rejected rather than
// booked as about 2^32 interrupts.
func TestInterruptsParserRejectsACountThatGoesBack(t *testing.T) {
	t.Parallel()

	parse := interruptsParser("amd64")
	_, err := parse([]byte("  CPU0  CPU1\nTLB:  100  100  TLB shootdowns\n"))
	require.NoError(t, err)
	_, err = parse([]byte("  CPU0  CPU1\nTLB:  90  100  TLB shootdowns\n"))
	require.Error(t, err)
	samples, err := parse([]byte("  CPU0  CPU1\nTLB:  110  105  TLB shootdowns\n"))
	require.NoError(t, err)

	assert.Equal(t, map[string]int64{"tlb_shootdown": 15}, interruptsByKind(t, samples))
}

func TestInterruptsParserRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	for name, input := range map[string]string{
		"too few counts":           "  CPU0  CPU1\nTLB:  5\n",
		"too few with description": "  CPU0  CPU1\nTLB:  5  TLB shootdowns\n",
		"more counts than CPUs":    "  CPU0  CPU1\nTLB:  5  6  7  TLB shootdowns\n",
		"bad count":                "  CPU0  CPU1\nCAL:  5  lots  Function call interrupts\n",
	} {
		_, err := interruptsParser("amd64")([]byte(input))
		assert.Errorf(t, err, "input %q", name)
	}
}

// arm64 names its IPIs differently and has no TLB or CAL row, so the source
// is dropped there; on amd64 the rows always exist, so it is a bad read.
func TestInterruptsParserTreatsMissingRowsAsAMissingFileOffAMD64(t *testing.T) {
	t.Parallel()

	input := []byte("  CPU0\nIPI0:  3  Rescheduling interrupts\n")
	_, err := interruptsParser("arm64")(input)
	require.ErrorIs(t, err, fs.ErrNotExist)

	_, err = interruptsParser("amd64")(input)
	require.Error(t, err)
	assert.NotErrorIs(t, err, fs.ErrNotExist)
}

func interruptsByKind(t *testing.T, samples []kernelSample) map[string]int64 {
	t.Helper()

	got := make(map[string]int64)
	for _, s := range samples {
		kind, ok := s.attrs.Value("kind")
		require.True(t, ok)
		got[kind.AsString()] = s.value
	}

	return got
}

func TestVMStatParserRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	for name, input := range map[string]string{
		"no wanted field": "nr_free_pages 1\n",
		"bad number":      "pgscan_direct lots\n",
	} {
		_, err := parseVMStat([]byte(input))
		assert.Errorf(t, err, "input %q", name)
	}
}

func TestKernelCountersReportGrowthSinceTheFirstCollection(t *testing.T) {
	t.Parallel()

	proc := allProc()
	reader, _ := startKernelCounters(t, proc)

	first := collectSums(t, reader)
	assert.Len(t, first, 10)
	for series, value := range first {
		assert.Zerof(t, value, "series %s", series)
	}

	proc.set("/proc/pressure/memory", "some avg10=0 avg60=0 avg300=0 total=118263504\nfull avg10=0 avg60=0 avg300=0 total=53194577\n")
	proc.set("/proc/vmstat", "allocstall_normal 47\nallocstall_dma32 2\nallocstall_movable 1\npgscan_direct 6000\npgsteal_direct 4100\n")

	second := collectSums(t, reader)
	require.Len(t, second, 10)
	assert.Equal(t, int64(500), second[stall+"|resource=memory,scope=some"])
	assert.Equal(t, int64(200), second[stall+"|resource=memory,scope=full"])
	assert.Equal(t, int64(0), second[stall+"|resource=io,scope=full"])
	assert.Equal(t, int64(7), second[stalls+"|"])
	assert.Equal(t, int64(1000), second[pages+"|kind=scan"])
	assert.Equal(t, int64(0), second[pages+"|kind=steal"])
}

func TestKernelCountersNeverReportBelowTheBaseline(t *testing.T) {
	t.Parallel()

	proc := allProc()
	reader, _ := startKernelCounters(t, proc)

	collectSums(t, reader)
	proc.set("/proc/pressure/io", "some avg10=0 avg60=0 avg300=0 total=3\nfull avg10=0 avg60=0 avg300=0 total=1\n")

	after := collectSums(t, reader)
	require.Contains(t, after, stall+"|resource=io,scope=some")
	assert.Equal(t, int64(0), after[stall+"|resource=io,scope=some"])
}

func TestKernelCountersDisableAMissingSource(t *testing.T) {
	t.Parallel()

	proc := newFakeProc(map[string]string{"/proc/vmstat": vmStatSample})
	reader, _ := startKernelCounters(t, proc)

	first := collectSums(t, reader)
	second := collectSums(t, reader)

	assert.Len(t, first, 3)
	assert.Len(t, second, 3)
	assert.Equal(t, 1, proc.readCount("/proc/pressure/memory"))
}

func TestKernelCountersSkipAFailingSourceForOneCollection(t *testing.T) {
	t.Parallel()

	proc := allProc()
	proc.failVMStat(errors.New("permission denied"))
	proc.set("/proc/pressure/io", "garbage\n")
	reader, _ := startKernelCounters(t, proc)

	failing := collectSums(t, reader)
	assert.Len(t, failing, 5)
	assert.NotContains(t, failing, stalls+"|")
	assert.NotContains(t, failing, stall+"|resource=io,scope=some")

	proc.failVMStat(nil)
	proc.set("/proc/pressure/io", ioPressure)

	assert.Len(t, collectSums(t, reader), 10)
}

func TestKernelCountersLogWhenASourceStartsFailingAndWhenItRecovers(t *testing.T) {
	t.Parallel()

	proc := allProc()
	proc.failVMStat(errors.New("permission denied"))
	reader, logs := startKernelCounters(t, proc)

	for range 5 {
		collectSums(t, reader)
	}
	proc.failVMStat(nil)
	collectSums(t, reader)
	collectSums(t, reader)

	assert.Equal(t, 1, logs.FilterMessage("kernel counter source failing").Len())
	assert.Equal(t, 1, logs.FilterMessage("kernel counter source recovered").Len())
}

func TestKernelCountersReadVMStatOnceAndExportTheFieldsItHas(t *testing.T) {
	t.Parallel()

	proc := allProc()
	proc.set("/proc/vmstat", "pgscan_direct 5000\npgsteal_direct 4100\n")
	reader, logs := startKernelCounters(t, proc)

	var last map[string]int64
	for range 5 {
		last = collectSums(t, reader)
	}

	assert.Equal(t, 5, proc.readCount("/proc/vmstat"))
	assert.Contains(t, last, pages+"|kind=scan")
	assert.Contains(t, last, pages+"|kind=steal")
	assert.NotContains(t, last, stalls+"|")
	assert.Zero(t, logs.FilterMessage("kernel counter source failing").Len())
	assert.Zero(t, logs.FilterMessage("kernel counter source recovered").Len())
}

func TestKernelCountersStopReadingAfterClose(t *testing.T) {
	t.Parallel()

	proc := allProc()
	reader, _, k := newTestKernelCounters(t, proc)
	require.Len(t, collectSums(t, reader), 10)

	require.NoError(t, k.Close(t.Context()))
	require.NoError(t, k.Close(t.Context()))

	assert.Empty(t, collectSums(t, reader))
	assert.Equal(t, 1, proc.readCount("/proc/vmstat"))
}
