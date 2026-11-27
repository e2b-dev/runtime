package sandbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// DataPlane builds the backends that serve a sandbox's devices: the handler
// that serves its guest memory over userfaultfd and the provider that serves
// its root disk. This orchestrator serves both from its own process, with
// the in-process userfaultfd handler and an NBD device; an edition may serve
// either from a process of its own by supplying a DataPlane through
// factories.Options.
//
// Every sandbox goes through the plane: a resume asks it for both devices, a
// fresh start (CreateSandbox, including one from a prepared rootfs file) asks
// it for the disk only, because a fresh start serves memory from the memfile
// without a handler.
type DataPlane interface {
	// ForSandbox prepares the data plane of one sandbox before its VM starts
	// or resumes. The returned object's Memory and Rootfs methods are each
	// called at most once, on separate goroutines and in either order, and a
	// fresh start never calls Memory, so an implementation that serves both
	// devices from one process ties them together here and releases
	// whatever it set up in Close, not in either method.
	//
	// ctx bounds the construction only: it is the request's context and is
	// cancelled when the request returns. Memory and Rootfs receive a
	// construction context derived from it that a failed resume cancels
	// before its cleanup runs, so a constructor that honours its context
	// unwinds instead of holding the failed request. Anything that must
	// outlive the call, a worker process above all, must not derive from
	// either and is released through SandboxDataPlane.Close.
	ForSandbox(ctx context.Context, req SandboxDataPlaneRequest) (SandboxDataPlane, error)
}

// SandboxDataPlaneRequest is what a DataPlane learns about a sandbox before
// choosing its backends.
type SandboxDataPlaneRequest struct {
	SandboxID string
	// UffdSocketPath is the Unix socket Firecracker's UFFD backend connects
	// to; an implementation may derive its other socket paths from it.
	UffdSocketPath string
	// RootfsCachePath is where the NBD provider keeps the sandbox's writable
	// rootfs cache; an implementation serving the disk itself may keep its
	// own state next to it.
	RootfsCachePath string
	// PreparedRootfsPath is set on a fresh start from a prepared rootfs
	// file, the one the Direct provider would serve in place of NBD; it is
	// empty on every other start and on every resume. It is not the cache
	// path above.
	PreparedRootfsPath string
	// CacheDir is the orchestrator's diff cache directory.
	CacheDir string
	Logger   logger.Logger
	Files    *storage.SandboxFiles
}

// SandboxDataPlane is the data plane of one sandbox.
//
// The orchestrator discovers a memory backend's optional capabilities by
// type assertion on the backend it keeps for the sandbox: uffd.CoWExporter
// (the copy-on-write export window; the synchronous copy is the fallback),
// uffd.MemoryUsageReporter (page-state and held-memory metrics) and
// uffd.BalloonModeLabeler (balloon labels on the serve metrics). A handler or
// a MemoryBinder result that does not implement them loses them silently, so
// a wrapper around the in-process handler must forward the ones it wants kept.
type SandboxDataPlane interface {
	// Memory returns the handler that will serve memfile on the sandbox's
	// UFFD socket. It must not return a nil handler without an error.
	Memory(ctx context.Context, memfile block.ReadonlyDevice) (uffd.Handler, error)
	// Rootfs returns the provider that serves the root disk over template,
	// the read-only root disk of the template the sandbox starts from, or
	// ok=false to let the orchestrator serve it over NBD (or, on a fresh start
	// from a prepared file, the Direct provider). The orchestrator resolves
	// template on the disk's own goroutine, alongside the memory and network
	// work, and under the construction context on a resume. ok=true with a
	// nil provider is an error.
	Rootfs(ctx context.Context, template block.ReadonlyDevice) (p rootfs.Provider, ok bool, err error)
	// Close releases what ForSandbox set up for the sandbox. The orchestrator
	// registers it on the sandbox's cleanup as soon as ForSandbox returns,
	// so it runs on every path after that point, including a failed
	// construction. It runs after the handler's Stop and the provider's
	// Close: on a resume that fails while a constructor is still running,
	// the failed resume first cancels the construction context, then the
	// entry waits for Memory, Rootfs and the handler's Start to return, so
	// neither device is asked for on a closed plane and their own late
	// teardown comes first. That wait has no other bound: the cleanup runs
	// under a context that is never cancelled, so a constructor that ignores
	// its context, or a handler whose Start blocks on an external party
	// (see uffd.Handler), holds the failed resume for as long as it runs.
	// The in-process plane, whose Close releases nothing, is registered
	// without the wait. On a reboot in place the entry moves to the next
	// lifecycle together with the provider it serves, and runs there, after
	// that provider's Close. It is the only place an implementation may rely
	// on for releasing per-sandbox state: Memory and Rootfs may each be
	// called once or not at all.
	Close(ctx context.Context) error
}

// closeDataPlaneAfter is the plane's release on a resume: it waits for the
// device constructors and the memory-serve step to finish, then closes the
// plane. A constructor that registers its device's teardown after the chain
// has run executes it inline, inside its own goroutine, which is why waiting
// here keeps that teardown ahead of Close. The wait is not bounded by the
// teardown's context, which the cleanup never cancels; a failed resume
// cancels the construction context before the cleanup runs, and that is
// what makes the constructors return.
func closeDataPlaneAfter(plane SandboxDataPlane, constructors ...<-chan struct{}) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, done := range constructors {
			<-done
		}

		return plane.Close(ctx)
	}
}

// inProcessPlane marks the plane whose Close releases nothing, so a resume
// registers that Close without waiting for the device constructors: a
// failed resume on the default path returns at once, as it did before the
// seam, while the constructors unwind on their cancelled context.
type inProcessPlane interface {
	inProcess()
}

// MemoryBinder is implemented by a SandboxDataPlane whose disk provider
// writes guest memory outside the primary handler and needs to be bound to
// it. On a resume, once both constructors have returned and before the VM
// resumes, the orchestrator calls BindMemory with the handler Memory returned
// and the provider serving the disk (the plane's own, or the NBD one), and
// keeps the returned backend as the sandbox's memory backend for pause,
// export and metrics. A fresh start has no memory handler and does not call
// it. The in-process data plane does not implement it.
type MemoryBinder interface {
	BindMemory(ctx context.Context, primary uffd.Handler, disk rootfs.Provider) (uffd.MemoryBackend, error)
}

// bindDataPlaneMemory returns the memory backend the sandbox keeps: the
// handler itself, or, for a MemoryBinder plane, what it binds the handler and
// the disk into. Both constructors have returned when it is called.
func bindDataPlaneMemory(ctx context.Context, plane SandboxDataPlane, handler *utils.Promise[uffd.Handler], disk rootfs.Provider) (uffd.MemoryBackend, error) {
	fcUffd, err := handler.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get uffd: %w", err)
	}

	binder, ok := plane.(MemoryBinder)
	if !ok {
		return fcUffd, nil
	}

	bound, err := binder.BindMemory(ctx, fcUffd, disk)
	if err != nil {
		return nil, fmt.Errorf("data plane memory binding: %w", err)
	}
	if bound == nil {
		return nil, errors.New("data plane returned no memory backend")
	}

	return bound, nil
}

// InProcessDataPlane is the data plane this orchestrator ships with: the
// userfaultfd handler in this process and the NBD root disk.
type InProcessDataPlane struct{}

// NewInProcessDataPlane returns the in-process data plane; NewFactory wires
// it in when it is given no other.
func NewInProcessDataPlane() *InProcessDataPlane { return &InProcessDataPlane{} }

func (*InProcessDataPlane) ForSandbox(_ context.Context, req SandboxDataPlaneRequest) (SandboxDataPlane, error) {
	return inProcessSandbox{req: req}, nil
}

type inProcessSandbox struct{ req SandboxDataPlaneRequest }

func (s inProcessSandbox) Memory(_ context.Context, memfile block.ReadonlyDevice) (uffd.Handler, error) {
	return uffd.New(memfile, s.req.UffdSocketPath, s.req.Logger), nil
}

func (inProcessSandbox) Rootfs(context.Context, block.ReadonlyDevice) (rootfs.Provider, bool, error) {
	return nil, false, nil
}

func (inProcessSandbox) inProcess() {}

// Close has nothing to release: the in-process plane sets up nothing in
// ForSandbox, and the handler and the NBD provider have their own teardown.
func (inProcessSandbox) Close(context.Context) error { return nil }
