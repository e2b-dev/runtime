//go:build linux

package chrooted

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted/testutils"
)

const (
	// benchPackages is the number of package directories the small-file
	// workload spreads its files over, like node_modules after an install.
	benchPackages = 64
	// benchFileSize is the size of each written file; most files an npm
	// install or a git checkout produces are a few KiB.
	benchFileSize = 2048
	// benchResidentMounts is how many volumes the resident benchmark keeps
	// open at once, standing in for that many sandboxes with a volume mounted.
	benchResidentMounts = 256
)

// smallFileWriter issues the same sequence of filesystem calls the NFS proxy
// issues when a sandbox creates and writes a small file, so the benchmark
// measures the confinement layer under a build-like workload rather than a
// single syscall.
//
// Per file (mirroring go-nfs CREATE, WRITE, COMMIT and the trailing GETATTR):
//
//	CREATE:  Stat(new) Stat(dir) Create Close Lstat(new) Chmod Lstat(new) Lstat(dir)
//	WRITE:   Stat(new) OpenFile Seek Write Close Lstat(new)
//	COMMIT:  Lstat(new)
//	GETATTR: Lstat(new)
type smallFileWriter struct {
	fs   *Chrooted
	root string
	data []byte

	knownDirs map[string]struct{}
}

func newSmallFileWriter(fs *Chrooted, root string) *smallFileWriter {
	return &smallFileWriter{
		fs:        fs,
		root:      root,
		data:      make([]byte, benchFileSize),
		knownDirs: make(map[string]struct{}, benchPackages),
	}
}

func (w *smallFileWriter) ensureDir(dir string) error {
	if _, ok := w.knownDirs[dir]; ok {
		return nil
	}

	// MKDIR: Stat(new) Stat(parent) MkdirAll Lstat(new) Lstat(parent)
	if _, err := w.fs.Stat(dir); err == nil {
		w.knownDirs[dir] = struct{}{}

		return nil
	}
	if _, err := w.fs.Stat(path.Dir(dir)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := w.fs.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(dir); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(path.Dir(dir)); err != nil {
		return err
	}

	w.knownDirs[dir] = struct{}{}

	return nil
}

func (w *smallFileWriter) writeFile(i int) error {
	dir := fmt.Sprintf("%s/node_modules/pkg-%d/lib", w.root, i%benchPackages)
	if err := w.ensureDir(dir); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}

	name := fmt.Sprintf("%s/index-%d.js", dir, i)

	// CREATE
	if _, err := w.fs.Stat(name); err == nil {
		return fmt.Errorf("%q already exists", name)
	}
	if _, err := w.fs.Stat(dir); err != nil {
		return err
	}
	f, err := w.fs.Create(name)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(name); err != nil {
		return err
	}
	if err := w.fs.Chmod(name, 0o644); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(name); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(dir); err != nil {
		return err
	}

	// WRITE
	info, err := w.fs.Stat(name)
	if err != nil {
		return err
	}
	f, err = w.fs.OpenFile(name, os.O_RDWR, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write(w.data); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(name); err != nil {
		return err
	}

	// COMMIT + GETATTR
	if _, err := w.fs.Lstat(name); err != nil {
		return err
	}
	if _, err := w.fs.Lstat(name); err != nil {
		return err
	}

	return nil
}

func benchChroot(b *testing.B) *Chrooted {
	b.Helper()

	fs, err := Chroot(b.TempDir())
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, fs.Close())
	})

	return fs
}

func reportFilesPerSecond(b *testing.B, files int) {
	b.Helper()

	b.ReportMetric(float64(files)/b.Elapsed().Seconds(), "files/s")
}

// BenchmarkChrooted_SmallFiles writes a node_modules-shaped tree of small
// files through one confined filesystem.
func BenchmarkChrooted_SmallFiles(b *testing.B) {
	// One writer, as seen from a single NFS connection: go-nfs handles the
	// requests of a connection one at a time, so this is the latency a
	// single-threaded build (tsc, git checkout) experiences per file.
	b.Run("sequential", func(b *testing.B) {
		fs := benchChroot(b)
		w := newSmallFileWriter(fs, "/w0")

		b.ResetTimer()
		for i := range b.N {
			require.NoError(b, w.writeFile(i))
		}
		reportFilesPerSecond(b, b.N)
	})

	// Many writers sharing one confined filesystem, as a sandbox running a
	// parallel installer (npm, pnpm, esbuild) does once the server handles
	// requests concurrently.
	b.Run("parallel", func(b *testing.B) {
		fs := benchChroot(b)

		var writers atomic.Int64
		var files atomic.Int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			w := newSmallFileWriter(fs, fmt.Sprintf("/w%d", writers.Add(1)))
			for i := 0; pb.Next(); i++ {
				require.NoError(b, w.writeFile(i))
				files.Add(1)
			}
		})
		reportFilesPerSecond(b, int(files.Load()))
	})

	// Many writers each on their own confined filesystem, as several
	// sandboxes with different volumes mounted do.
	b.Run("parallel-mounts", func(b *testing.B) {
		var mu sync.Mutex
		var filesystems []*Chrooted
		b.Cleanup(func() {
			mu.Lock()
			defer mu.Unlock()
			for _, fs := range filesystems {
				require.NoError(b, fs.Close())
			}
		})

		var files atomic.Int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			fs, err := Chroot(b.TempDir())
			require.NoError(b, err)
			mu.Lock()
			filesystems = append(filesystems, fs)
			mu.Unlock()

			w := newSmallFileWriter(fs, "/w")
			for i := 0; pb.Next(); i++ {
				require.NoError(b, w.writeFile(i))
				files.Add(1)
			}
		})
		reportFilesPerSecond(b, int(files.Load()))
	})
}

// BenchmarkChroot_Lifecycle measures opening and closing a confined
// filesystem, the cost the volume gRPC service pays on every request.
func BenchmarkChroot_Lifecycle(b *testing.B) {
	dir := b.TempDir()

	b.ResetTimer()
	for range b.N {
		fs, err := Chroot(dir)
		require.NoError(b, err)

		if _, err := fs.Stat("/"); err != nil {
			require.NoError(b, err)
		}

		require.NoError(b, fs.Close())
	}
}

// BenchmarkChroot_Resident keeps many confined filesystems open at once and
// reports what each one costs the process in OS threads, mount namespaces,
// file descriptors and resident memory. Those are the kernel limits
// (threads-max, user.max_mnt_namespaces, RLIMIT_NOFILE) a node full of
// sandboxes with mounted volumes runs into.
func BenchmarkChroot_Resident(b *testing.B) {
	dir := b.TempDir()
	filesystems := make([]*Chrooted, 0, benchResidentMounts)

	// Threads and namespaces released by Close disappear asynchronously, so
	// the baseline is taken once, before any mount, and the growth reported
	// is that of the last iteration's resident set.
	before := testutils.SnapshotProcStats(b)
	var after testutils.ProcStats

	b.ResetTimer()
	for range b.N {
		for range benchResidentMounts {
			fs, err := Chroot(dir)
			require.NoError(b, err)
			filesystems = append(filesystems, fs)
		}

		after = testutils.SnapshotProcStats(b)

		for _, fs := range filesystems {
			require.NoError(b, fs.Close())
		}
		filesystems = filesystems[:0]
	}
	b.StopTimer()

	testutils.ReportProcStatsDelta(b, before, after, benchResidentMounts)
}
