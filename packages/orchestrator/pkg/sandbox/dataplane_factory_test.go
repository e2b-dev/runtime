//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// errStopBeforeFirecracker ends a construction once both devices exist and
// before Firecracker is spawned, which these tests cannot do.
var errStopBeforeFirecracker = errors.New("stop before firecracker")

// planeTemplate is a template whose devices are in-memory buffers. Rootfs
// serves the first call (the overlay's) and fails the second (the one that
// precedes the Firecracker spawn on a resume), so a resume that gets past
// both device constructors stops right there.
type planeTemplate struct {
	capacityOnlyTemplate

	mu          sync.Mutex
	rootfsCalls int
	memfileErr  error
	// memfileAfter, when set, delays the memfile's answer until it is
	// closed, so a failure can be ordered after a provider's start.
	memfileAfter <-chan struct{}
	// memfileWaitsThroughCancel makes that wait ignore the context, like a
	// constructor that does not honour cancellation.
	memfileWaitsThroughCancel bool
	// log, when set, records "memfile.cancelled" when the context wins.
	log *eventLog
	// rootfsAfter, when set, delays the first rootfs answer the same way.
	rootfsAfter <-chan struct{}
}

func newPlaneTemplate(memfileErr error) *planeTemplate {
	return &planeTemplate{capacityOnlyTemplate: capacityOnlyTemplate{meta: metadata.Template{VcpuCount: 1}}, memfileErr: memfileErr}
}

func (*planeTemplate) Files() storage.CachePaths { return storage.CachePaths{} }

func (t *planeTemplate) Memfile(ctx context.Context) (block.ReadonlyDevice, error) {
	if t.memfileAfter != nil {
		if t.memfileWaitsThroughCancel {
			<-t.memfileAfter
		} else {
			select {
			case <-t.memfileAfter:
			case <-ctx.Done():
				if t.log != nil {
					t.log.add("memfile.cancelled")
				}

				return nil, ctx.Err()
			}
		}
	}
	if t.memfileErr != nil {
		return nil, t.memfileErr
	}

	return &inPlaceRODevice{data: make([]byte, 2*header.PageSize)}, nil
}

func (t *planeTemplate) Rootfs(context.Context) (block.ReadonlyDevice, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rootfsCalls++
	if t.rootfsCalls > 1 {
		return nil, errStopBeforeFirecracker
	}
	if t.rootfsAfter != nil {
		<-t.rootfsAfter
	}

	return &inPlaceRODevice{data: make([]byte, 2*header.PageSize)}, nil
}

// failingSlotPool refuses every slot and records that it did.
type failingSlotPool struct {
	slotPool

	log *eventLog
	// after, when set, holds the failure until every channel in it is
	// closed, so a test can fail the resume at a chosen point of the
	// device construction instead of racing it.
	after []<-chan struct{}
}

func (p failingSlotPool) Get(context.Context, *orchestrator.SandboxNetworkConfig, sandboxtypes.EgressClass) (*network.Slot, error) {
	for _, ch := range p.after {
		<-ch
	}
	p.log.add("slot.fail")

	return nil, errors.New("no slot")
}

// slotPool hands out a bare network slot and takes it back without a network.
type slotPool struct{}

func (slotPool) Get(context.Context, *orchestrator.SandboxNetworkConfig, sandboxtypes.EgressClass) (*network.Slot, error) {
	return &network.Slot{HostIP: net.IPv4(127, 0, 0, 1)}, nil
}

func (slotPool) ReturnAsync(context.Context, *network.Slot, network.ReleaseNotify, time.Duration) error {
	return nil
}

func (slotPool) Close(context.Context) error { return nil }

// eventLog records, in order, what the plane, its handler and its provider
// saw: the proof of which teardown ran and in what order.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.events...)
}

// planeHandler is a memory handler that starts, stops and exits at once.
type planeHandler struct {
	uffd.Handler

	log  *eventLog
	exit *utils.ErrorOnce
}

func newPlaneHandler(log *eventLog) *planeHandler {
	exit := utils.NewErrorOnce()
	_ = exit.SetSuccess()

	return &planeHandler{log: log, exit: exit}
}

func (h *planeHandler) Start(context.Context) error {
	h.log.add("handler.start")

	return nil
}

func (h *planeHandler) Stop() error {
	h.log.add("handler.stop")

	return nil
}

func (h *planeHandler) Ready() chan struct{} {
	ready := make(chan struct{})
	close(ready)

	return ready
}

func (h *planeHandler) Exit() *utils.ErrorOnce { return h.exit }
func (*planeHandler) SetMemfdUnusedTail(int64) {}
func (*planeHandler) SetSyncWP(bool)           {}
func (*planeHandler) SetInstallDedup(bool)     {}

// planeProvider is a root disk provider that records its lifecycle and the
// context it was started under.
type planeProvider struct {
	rootfs.Provider

	log *eventLog
	// started is closed by Start; cancelable records whether the context
	// Start received can be cancelled (the execution context cannot).
	started    chan struct{}
	startOnce  sync.Once
	cancelable atomic.Bool
}

func newPlaneProvider(log *eventLog) *planeProvider {
	return &planeProvider{log: log, started: make(chan struct{})}
}

func (p *planeProvider) Start(ctx context.Context) error {
	p.cancelable.Store(ctx.Done() != nil)
	p.log.add("provider.start")
	p.startOnce.Do(func() { close(p.started) })

	return nil
}

func (p *planeProvider) Close(context.Context) error {
	p.log.add("provider.close")

	return nil
}

func (*planeProvider) Path() (string, error) { return "", nil }

// testPlane is a data plane whose answers the test chooses.
type testPlane struct {
	log        *eventLog
	forErr     error
	memory     func() (uffd.Handler, error)
	rootfs     func() (rootfs.Provider, bool, error)
	req        SandboxDataPlaneRequest
	forSandbox int
	// rootfsCancelled records whether Rootfs saw a cancelled context.
	rootfsCancelled atomic.Bool
	// memoryAfter and rootfsAfter, when set, hold the constructor inside the
	// plane until they close, so a test can fail the resume while a device
	// is being built; memoryEntered and rootfsEntered close as each
	// constructor enters the plane, before that hold.
	memoryAfter   <-chan struct{}
	rootfsAfter   <-chan struct{}
	memoryEntered chan struct{}
	rootfsEntered chan struct{}
	memoryOnce    sync.Once
	rootfsOnce    sync.Once
}

func (d *testPlane) entered(once *sync.Once, ch chan struct{}) {
	if ch != nil {
		once.Do(func() { close(ch) })
	}
}

type testSandboxPlane struct{ dp *testPlane }

func (d *testPlane) ForSandbox(_ context.Context, req SandboxDataPlaneRequest) (SandboxDataPlane, error) {
	d.forSandbox++
	d.req = req
	if d.forErr != nil {
		return nil, d.forErr
	}

	return testSandboxPlane{dp: d}, nil
}

func (s testSandboxPlane) Memory(context.Context, block.ReadonlyDevice) (uffd.Handler, error) {
	s.dp.entered(&s.dp.memoryOnce, s.dp.memoryEntered)
	if s.dp.memoryAfter != nil {
		<-s.dp.memoryAfter
	}
	s.dp.log.add("memory")

	return s.dp.memory()
}

func (s testSandboxPlane) Rootfs(ctx context.Context, _ block.ReadonlyDevice) (rootfs.Provider, bool, error) {
	s.dp.entered(&s.dp.rootfsOnce, s.dp.rootfsEntered)
	if s.dp.rootfsAfter != nil {
		<-s.dp.rootfsAfter
	}
	s.dp.rootfsCancelled.Store(ctx.Err() != nil)
	s.dp.log.add("rootfs")

	return s.dp.rootfs()
}

func (s testSandboxPlane) Close(context.Context) error {
	s.dp.log.add("plane.close")

	return nil
}

// bindingPlane is a testPlane that also binds the handler to the disk.
type bindingPlane struct {
	*testPlane

	bindErr error
	bound   uffd.MemoryBackend
	primary uffd.Handler
	disk    rootfs.Provider
}

type bindingSandboxPlane struct {
	testSandboxPlane

	bp *bindingPlane
}

func (b *bindingPlane) ForSandbox(ctx context.Context, req SandboxDataPlaneRequest) (SandboxDataPlane, error) {
	sp, err := b.testPlane.ForSandbox(ctx, req)
	if err != nil {
		return nil, err
	}

	return bindingSandboxPlane{testSandboxPlane: sp.(testSandboxPlane), bp: b}, nil
}

func (s bindingSandboxPlane) BindMemory(_ context.Context, primary uffd.Handler, disk rootfs.Provider) (uffd.MemoryBackend, error) {
	s.bp.log.add("bind")
	s.bp.primary, s.bp.disk = primary, disk

	return s.bp.bound, s.bp.bindErr
}

func planeFactory(t *testing.T, plane DataPlane) *Factory {
	t.Helper()

	return planeFactoryOn(t, plane, slotPool{})
}

func planeFactoryOn(t *testing.T, plane DataPlane, slots network.PoolInterface) *Factory {
	t.Helper()
	flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = flags.Close(t.Context()) })

	return NewFactory(t.Context(), cfg.BuilderConfig{
		StorageConfig: storage.Config{SandboxCacheDir: t.TempDir()},
	}, slots, nil, flags, nil, cgroup.NewNoopManager(), nil, nil, NewSandboxesMap()).WithDataPlane(plane)
}

func resumeThrough(t *testing.T, plane DataPlane, template *planeTemplate) error {
	t.Helper()
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}
	sbx, err := planeFactory(t, plane).ResumeSandbox(t.Context(), template, NewConfig(Config{Vcpu: 1}), runtime, time.Time{}, time.Time{}, nil)
	require.Nil(t, sbx)
	require.Error(t, err)

	return err
}

func servingPlane(log *eventLog) (*testPlane, *planeProvider) {
	provider := newPlaneProvider(log)

	return &testPlane{
		log:    log,
		memory: func() (uffd.Handler, error) { return newPlaneHandler(log), nil },
		rootfs: func() (rootfs.Provider, bool, error) { return provider, true, nil },
	}, provider
}

// A plane that refuses the sandbox fails the resume before any device is
// built; there is nothing to close.
func TestResumeSandboxFailsWhenTheDataPlaneRefusesTheSandbox(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	refused := errors.New("no capacity for this sandbox")
	plane := &testPlane{log: log, forErr: refused}

	err := resumeThrough(t, plane, newPlaneTemplate(nil))
	require.ErrorIs(t, err, refused)
	assert.Empty(t, log.list())
}

// Once ForSandbox has returned, its Close runs on a failed construction even
// when neither device was asked for: here the memfile fails first.
func TestResumeSandboxClosesTheDataPlaneWhenConstructionFailsAfterForSandbox(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	noMemfile := errors.New("memfile unavailable")
	plane, _ := servingPlane(log)

	err := resumeThrough(t, plane, newPlaneTemplate(noMemfile))
	require.ErrorIs(t, err, noMemfile)
	assert.Equal(t, 1, plane.forSandbox)
	assert.Contains(t, log.list(), "plane.close")
	assert.NotContains(t, log.list(), "memory")
}

// A memory handler the plane cannot build, or returns as nil, fails the
// resume with a clear error and still closes the plane.
func TestResumeSandboxRejectsABadDataPlaneMemoryHandler(t *testing.T) {
	t.Parallel()
	broken := errors.New("memory worker did not start")
	for name, tc := range map[string]struct {
		memory func() (uffd.Handler, error)
		want   string
		is     error
	}{
		"error": {memory: func() (uffd.Handler, error) { return nil, broken }, want: "data plane memory handler", is: broken},
		"nil":   {memory: func() (uffd.Handler, error) { return nil, nil }, want: "data plane returned no memory handler"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			log := &eventLog{}
			plane, _ := servingPlane(log)
			plane.memory = tc.memory

			err := resumeThrough(t, plane, newPlaneTemplate(nil))
			require.ErrorContains(t, err, tc.want)
			if tc.is != nil {
				require.ErrorIs(t, err, tc.is)
			}
			assert.Contains(t, log.list(), "plane.close")
		})
	}
}

// A root disk the plane claims but cannot build, or returns as nil, fails the
// resume with a clear error and still closes the plane.
func TestResumeSandboxRejectsABadDataPlaneRootfsProvider(t *testing.T) {
	t.Parallel()
	broken := errors.New("disk worker did not start")
	for name, tc := range map[string]struct {
		rootfs func() (rootfs.Provider, bool, error)
		want   string
		is     error
	}{
		"error": {rootfs: func() (rootfs.Provider, bool, error) { return nil, false, broken }, want: "data plane rootfs provider", is: broken},
		"nil":   {rootfs: func() (rootfs.Provider, bool, error) { return nil, true, nil }, want: "data plane returned no rootfs provider"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			log := &eventLog{}
			plane, _ := servingPlane(log)
			plane.rootfs = tc.rootfs

			err := resumeThrough(t, plane, newPlaneTemplate(nil))
			require.ErrorContains(t, err, tc.want)
			if tc.is != nil {
				require.ErrorIs(t, err, tc.is)
			}
			assert.Contains(t, log.list(), "plane.close")
		})
	}
}

// When the plane serves the disk, its provider is started under the
// sandbox's execution context, not the resume's, and on teardown its Close
// and the handler's Stop run before the plane's Close.
func TestResumeSandboxServesTheDiskThroughTheDataPlane(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	plane, provider := servingPlane(log)

	err := resumeThrough(t, plane, newPlaneTemplate(nil))
	require.ErrorIs(t, err, errStopBeforeFirecracker, "both devices were built; the construction stopped at the Firecracker spawn")

	select {
	case <-provider.started:
	case <-time.After(5 * time.Second):
		require.Fail(t, "rootfs provider did not start")
	}
	assert.False(t, provider.cancelable.Load(), "the provider runs under the non-cancelable execution context")

	events := log.list()
	require.Contains(t, events, "plane.close")
	beforePlaneClose := events[:slices.Index(events, "plane.close")]
	assert.Contains(t, beforePlaneClose, "provider.close")
	assert.Contains(t, beforePlaneClose, "handler.stop")
}

// A MemoryBinder plane is bound once both devices exist, with the handler
// Memory returned and the provider that serves the disk; a failed or empty
// binding fails the resume.
func TestResumeSandboxBindsTheMemoryHandlerToTheDataPlaneDisk(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		bound   bool
		bindErr error
		want    string
	}{
		"bound":  {bound: true, want: errStopBeforeFirecracker.Error()},
		"error":  {bindErr: errors.New("bind refused"), want: "data plane memory binding"},
		"nilled": {want: "data plane returned no memory backend"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			log := &eventLog{}
			inner, provider := servingPlane(log)
			plane := &bindingPlane{testPlane: inner, bindErr: tc.bindErr}
			if tc.bound {
				plane.bound = newPlaneHandler(log)
			}

			err := resumeThrough(t, plane, newPlaneTemplate(nil))
			require.ErrorContains(t, err, tc.want)
			assert.Contains(t, log.list(), "bind")
			_, isHandler := plane.primary.(*planeHandler)
			assert.True(t, isHandler, "bound with the handler Memory returned")
			assert.Same(t, provider, plane.disk, "bound with the provider serving the disk")
			assert.Contains(t, log.list(), "plane.close")
		})
	}
}

// A fresh start asks the plane for the disk only, names the prepared rootfs
// file in the request, and closes provider and plane when the start fails.
func TestCreateSandboxServesTheDiskThroughTheDataPlane(t *testing.T) {
	t.Parallel()
	for name, prepared := range map[string]string{"template": "", "prepared": "/prepared/rootfs.ext4"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			log := &eventLog{}
			noMemfile := errors.New("memfile unavailable")
			plane, provider := servingPlane(log)
			template := newPlaneTemplate(noMemfile)
			// The provider starts on its own goroutine; fail the start only
			// once it has, so the teardown order below is the one under test.
			template.memfileAfter = provider.started
			runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}

			sbx, err := planeFactory(t, plane).CreateSandbox(t.Context(), NewConfig(Config{Vcpu: 1}), runtime, template, time.Minute, prepared, fc.ProcessOptions{}, nil)
			require.Nil(t, sbx)
			require.ErrorIs(t, err, noMemfile)

			assert.Equal(t, prepared, plane.req.PreparedRootfsPath)
			assert.NotEmpty(t, plane.req.RootfsCachePath)
			assert.NotEqual(t, plane.req.RootfsCachePath, plane.req.PreparedRootfsPath)
			events := log.list()
			assert.NotContains(t, events, "memory", "a fresh start has no memory handler")
			assert.Contains(t, events, "provider.start")
			assert.False(t, provider.cancelable.Load())
			assert.Less(t, slices.Index(events, "provider.close"), slices.Index(events, "plane.close"))
			assert.Equal(t, "plane.close", events[len(events)-1])
		})
	}
}

// A resume that fails on its network slot returns while the device
// constructors are still running. With constructors that ignore their
// cancelled context, the plane's Close waits for them: both devices are
// still asked for and torn down, and the plane closes once, last.
func TestResumeSandboxWaitsForTheDevicesBeforeClosingThePlane(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	plane, provider := servingPlane(log)
	template := newPlaneTemplate(nil)
	// Both constructors are inside the plane when the slot fails: the
	// orchestrator's own pre-checks have passed, and what the plane returns
	// afterwards must still be torn down before the plane closes.
	gate := make(chan struct{})
	plane.memoryAfter, plane.rootfsAfter = gate, gate
	plane.memoryEntered, plane.rootfsEntered = make(chan struct{}), make(chan struct{})
	// The slot fails only once both constructors are inside the plane, so
	// the failure cannot land ahead of them and skip the devices instead.
	factory := planeFactoryOn(t, plane, failingSlotPool{log: log, after: []<-chan struct{}{plane.memoryEntered, plane.rootfsEntered}})
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}

	done := make(chan error, 1)
	go func() {
		sbx, err := factory.ResumeSandbox(t.Context(), template, NewConfig(Config{Vcpu: 1}), runtime, time.Time{}, time.Time{}, nil)
		assert.Nil(t, sbx)
		done <- err
	}()

	require.Eventually(t, func() bool { return slices.Contains(log.list(), "slot.fail") }, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-done:
		require.Failf(t, "the resume returned before its devices finished", "%v", err)
	case <-time.After(100 * time.Millisecond):
	}
	assert.NotContains(t, log.list(), "plane.close", "the plane stays open while a constructor runs")
	assert.NotContains(t, log.list(), "memory")
	assert.NotContains(t, log.list(), "rootfs")

	close(gate)
	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the resume did not return once its devices finished")
	}
	require.ErrorContains(t, err, "no slot")

	// Start runs on its own goroutine under the execution context and is
	// not ordered against the plane; the teardown entries are.
	<-provider.started
	events := log.list()
	assert.Contains(t, events, "provider.start")
	assert.True(t, plane.rootfsCancelled.Load(), "the constructor saw the cancelled construction context")
	for _, e := range []string{"rootfs", "provider.close", "memory", "handler.stop"} {
		assert.Contains(t, events, e)
		assert.Less(t, slices.Index(events, e), slices.Index(events, "plane.close"), "%s runs before the plane closes", e)
	}
	assert.False(t, provider.cancelable.Load())
	assert.Equal(t, 1, countOf(events, "plane.close"), "the plane closes once")
	assert.Equal(t, 1, countOf(events, "provider.close"))
}

func countOf(events []string, e string) int {
	n := 0
	for _, x := range events {
		if x == e {
			n++
		}
	}

	return n
}

// A failed resume cancels the construction context before its cleanup runs:
// a constructor that honours it unwinds, so the memory handler is never
// asked for, and a constructor still running afterwards sees the cancelled
// context. The plane still closes once, last.
func TestResumeSandboxCancelsTheDeviceConstructionWhenItFails(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	plane, provider := servingPlane(log)
	template := newPlaneTemplate(nil)
	template.log = log
	memfileGate, rootfsGate := make(chan struct{}), make(chan struct{})
	template.memfileAfter, template.rootfsAfter = memfileGate, rootfsGate
	factory := planeFactoryOn(t, plane, failingSlotPool{log: log})
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}

	done := make(chan error, 1)
	go func() {
		sbx, err := factory.ResumeSandbox(t.Context(), template, NewConfig(Config{Vcpu: 1}), runtime, time.Time{}, time.Time{}, nil)
		assert.Nil(t, sbx)
		done <- err
	}()

	// The memfile wait honours the context, so it reports the cancellation
	// as soon as the failed resume cancels construction; the rootfs wait
	// does not, so the disk goroutine reaches the plane step after that
	// point and must find construction cancelled there.
	require.Eventually(t, func() bool { return slices.Contains(log.list(), "memfile.cancelled") }, 5*time.Second, 10*time.Millisecond)
	assert.NotContains(t, log.list(), "plane.close", "the plane stays open while the disk constructor runs")
	close(rootfsGate)

	var err error
	select {
	case err = <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the resume did not return once its devices unwound")
	}
	require.ErrorContains(t, err, "no slot")

	events := log.list()
	assert.NotContains(t, events, "memory", "a cancelled memfile fetch never reaches the plane")
	assert.NotContains(t, events, "rootfs", "a cancelled construction does not ask the plane for a disk")
	assert.NotContains(t, events, "provider.start", "no disk provider is built or started after the cancel")
	assert.False(t, plane.rootfsCancelled.Load(), "the plane's Rootfs is not called at all")
	assert.Equal(t, 1, countOf(events, "plane.close"), "the plane closes once")
	select {
	case <-provider.started:
		require.Fail(t, "the disk provider started after construction was cancelled")
	default:
	}
	close(memfileGate)
}

// The in-process plane has nothing to close, so a resume that fails on its
// network slot returns at once, while a constructor that ignores its
// cancelled context is still running.
func TestResumeSandboxOnTheInProcessPlaneReturnsWithoutWaitingForItsDevices(t *testing.T) {
	t.Parallel()
	log := &eventLog{}
	template := newPlaneTemplate(errStopBeforeFirecracker)
	gate := make(chan struct{})
	template.memfileAfter = gate
	template.memfileWaitsThroughCancel = true
	// The overlay fails before NBD is asked for.
	template.rootfsCalls = 1
	factory := planeFactoryOn(t, NewInProcessDataPlane(), failingSlotPool{log: log})
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: uuid.NewString(), ExecutionID: uuid.NewString()}

	done := make(chan error, 1)
	go func() {
		sbx, err := factory.ResumeSandbox(t.Context(), template, NewConfig(Config{Vcpu: 1}), runtime, time.Time{}, time.Time{}, nil)
		assert.Nil(t, sbx)
		done <- err
	}()
	t.Cleanup(func() { close(gate) })

	require.Eventually(t, func() bool { return slices.Contains(log.list(), "slot.fail") }, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-done:
		require.ErrorContains(t, err, "no slot")
	case <-time.After(5 * time.Second):
		require.Fail(t, "the resume on the in-process plane waited for a constructor that was still running")
	}
}
