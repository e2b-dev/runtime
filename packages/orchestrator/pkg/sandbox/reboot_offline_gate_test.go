//go:build linux

package sandbox

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envdbin"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// The offline call site is the one worth driving end to end rather than through
// its helpers. decideOfflineSwap and envdbin.GatedReason are each proven as pure
// functions elsewhere; what is unproven is that they are WIRED correctly — the flag
// the right way round, the resolver's outcome fed to the relabel, and a deferral
// reaching the decision as binary_not_cached rather than as a probe failure. Any of
// those could be inverted and every other test would still pass.
//
// It is also the path that most deserves it: there is no version arbiter
// downstream, so a swap decided wrongly here is written into the rootfs.
func offlineGateFactory(t *testing.T, hostEnvdPath, cacheDir string, cacheOn bool) *Factory {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.EnvdBinaryCacheFlag.Key()).VariationForAll(cacheOn))
	td.Update(td.Flag(featureflags.EnvdOfflineUpgradeTargetFlag.Key()).ValueForAll(ldvalue.String("promoted")))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	f := &Factory{
		featureFlags: ff,
		config:       cfg.BuilderConfig{HostEnvdPath: hostEnvdPath},
	}
	if cacheDir != "" {
		f.envdBinCache = envdbin.NewCache(cacheDir, envdImageVersion)
	}

	return f
}

// drainWarm joins the background warm a miss starts, before t.TempDir()'s cleanup
// removes the directory it is still writing into.
//
// WarmAsync strips cancellation by design, so nothing on the calling path joins the
// goroutine: it outlives the resume that triggered it, which is the whole point of
// the cache. In a test that means it also outlives the test body, and cleanups run
// last-registered-first — t.TempDir() registers before anything here, so its
// RemoveAll runs last, concurrently with the warm. The directory empties, the warm
// re-creates it, and the cleanup fails with "directory not empty" naming a test
// that has already passed.
//
// envdbin has its own guard for this (newCacheForTest, which drains warming and
// inFlight), but those fields are unexported and unreachable from this package, so
// the observable is the published one. It also strengthens the assertion it follows:
// a deferral is only cheap if the miss really does warm for the next resume.
func drainWarm(t *testing.T, f *Factory, src string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, ok := f.EnvdBinCache().Lookup(src)

		return ok
	}, 10*time.Second, 20*time.Millisecond,
		"the miss must warm the cache in the background, and must finish doing so before "+
			"t.TempDir() removes the directory underneath it")
}

func TestOfflineGateDefersWhenTheBinaryIsNotCached(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeEnvdImage(t, src, "0.9.0")

	f := offlineGateFactory(t, src, filepath.Join(dir, "cache"), true)
	config := &Config{Envd: EnvdMetadata{Version: "0.6.12"}}
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}

	// Cache empty: the resolver misses, refuses to probe the source, and the swap
	// must be deferred rather than staged off the mount during a boot.
	require.Nil(t,
		f.envdOfflineUpgradePreBoot(t.Context(), config, runtime, true),
		"an uncached binary must defer the swap, not read the mount")

	drainWarm(t, f, src)
}

func TestOfflineGateSwapsOnceTheBinaryIsCached(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeEnvdImage(t, src, "0.9.0")

	cacheDir := filepath.Join(dir, "cache")
	f := offlineGateFactory(t, src, cacheDir, true)
	require.NoError(t, f.envdBinCache.Warm(t.Context(), src))

	config := &Config{Envd: EnvdMetadata{Version: "0.6.12"}}
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}

	// The positive twin: with the copy present the same call resolves 0.9.0 and
	// returns a pre-boot to run. Without this, the test above would pass just as
	// well against a gate that refuses everything.
	require.NotNil(t,
		f.envdOfflineUpgradePreBoot(t.Context(), config, runtime, true),
		"a cached binary newer than built-with must still swap")
}

// writeEnvdImage writes a stand-in envd that is a well-formed ELF image the
// structural check accepts, and not a runnable program -- it declares no
// segments. The cache validates the structure of every copy before probing it, so
// a text or shell-script fixture is rejected before any of these gates are
// reached; these tests inject the version probe, so the fixture never has to run.
// Deliberately a local copy of the same shape envdbin's own tests use -- a
// cross-package test helper would have to be exported from production code.
func writeEnvdImage(t *testing.T, path, version string) {
	t.Helper()

	h := make([]byte, elfHeaderLen)
	copy(h, []byte{0x7f, 'E', 'L', 'F', 2, 1, 1, 0})    // magic, 64-bit, little-endian, v1
	binary.LittleEndian.PutUint16(h[16:], 2)            // e_type    = ET_EXEC
	binary.LittleEndian.PutUint16(h[18:], 0x3e)         // e_machine = x86-64
	binary.LittleEndian.PutUint32(h[20:], 1)            // e_version
	binary.LittleEndian.PutUint16(h[52:], elfHeaderLen) // e_ehsize
	binary.LittleEndian.PutUint16(h[54:], 56)           // e_phentsize
	binary.LittleEndian.PutUint16(h[58:], 64)           // e_shentsize

	// The version rides after the header, which is what the injected probe reads.
	img := make([]byte, 0, len(h)+len(version))
	img = append(img, h...)
	img = append(img, version...)

	require.NoError(t, os.WriteFile(path, img, 0o755))
}

// envdImageVersion is the probe these tests inject: the version a stand-in image
// carries after its ELF header.
func envdImageVersion(_ context.Context, path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if len(b) > elfHeaderLen {
		b = b[elfHeaderLen:]
	}

	return strings.TrimSpace(string(b)), nil
}

// elfHeaderLen is the length of the header writeEnvdImage emits.
const elfHeaderLen = 64

// writeRunnableEnvd writes a stand-in binary that actually executes, because the
// flag-off path probes it with a real fork+exec rather than an injected function.
func writeRunnableEnvd(t *testing.T, path, version string) {
	t.Helper()

	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho "+version+"\n"), 0o755))
}

func TestOfflineGateFlagOffStillReadsTheSource(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeRunnableEnvd(t, src, "0.9.0")

	// Flag off: no cache is consulted at all, so an empty cache directory must not
	// stop the swap. This is the direction that proves the gate is not inverted —
	// with the flag off the branch must behave exactly as it did before the cache
	// existed, probing the source directly.
	f := offlineGateFactory(t, src, filepath.Join(dir, "cache"), false)
	config := &Config{Envd: EnvdMetadata{Version: "0.6.12"}}
	runtime := sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}

	require.NotNil(t,
		f.envdOfflineUpgradePreBoot(t.Context(), config, runtime, true),
		"flag off must keep swapping from the source, cache or no cache")
}

// TestOfflineResolveRelabelsADeferral is the assertion the nil/non-nil tests above
// cannot make: that a cache miss reaches the decision as binary_not_cached and not
// as getversion_failed. Both produce "no swap", so without looking at the reason a
// dropped relabel is invisible — and the two mean opposite things to an operator,
// one being "this node just booted" and the other "this target cannot be read".
func TestOfflineResolveRelabelsADeferral(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Two fixtures on purpose. The cached cases resolve through the cache's injected
	// probe, which reports the file's contents, so the version must BE the contents.
	// The flag-off case execs the file for real, so it must be runnable.
	src := filepath.Join(dir, "envd")
	writeEnvdImage(t, src, "0.9.0")
	runnable := filepath.Join(dir, "envd-runnable")
	writeRunnableEnvd(t, runnable, "0.9.0")

	sbCtx := featureflags.SandboxContext("sbx")
	tmplCtx := featureflags.TemplateContext("tpl")

	// Cache on, nothing warmed: the resolver defers.
	f := offlineGateFactory(t, src, filepath.Join(dir, "cache"), true)
	path, toVersion, reason, resolver := f.resolveOfflineTarget(t.Context(), "0.6.12", sbCtx, tmplCtx)
	require.Empty(t, path, "a deferral resolves no path to swap from")
	require.Empty(t, toVersion)
	require.NotNil(t, resolver, "the flag is on, so a resolver was constructed")
	require.Equal(t, envdbin.ReasonNotCached, reason,
		"a deferral must not be reported as a failed probe: one is expected on a fresh node, "+
			"the other is an operator error worth chasing")
	// Drained here rather than at the end: the blocks below happen to give this warm
	// time to land, which is exactly why the race is latent instead of absent.
	drainWarm(t, f, src)

	// Cache on and warmed: a real target resolves, with no reason at all.
	f = offlineGateFactory(t, src, filepath.Join(dir, "cache2"), true)
	require.NoError(t, f.envdBinCache.Warm(t.Context(), src))
	hitPath, hitVersion, reason, _ := f.resolveOfflineTarget(t.Context(), "0.6.12", sbCtx, tmplCtx)
	require.Equal(t, src, hitPath)
	require.Equal(t, "0.9.0", hitVersion)
	require.Empty(t, reason, "a resolved target carries no gated reason")

	// Flag off: the relabel must not fire, because no cache was consulted.
	f = offlineGateFactory(t, runnable, filepath.Join(dir, "cache3"), false)
	offPath, offVersion, reason, binCache := f.resolveOfflineTarget(t.Context(), "0.6.12", sbCtx, tmplCtx)
	require.Nil(t, binCache, "flag off constructs no resolver")
	require.Empty(t, reason)
	require.Equal(t, runnable, offPath, "flag off still resolves the source directly")
	require.Equal(t, "0.9.0", offVersion)
}

// The gate tests above stop at the pre-boot's construction: they assert whether
// one is returned, never what it does when the boot runs it. That leaves the half
// of the path that touches the rootfs unexercised -- and it is the half with no
// version arbiter downstream, where a wrong decision is written into a filesystem
// the guest is about to boot.
//
// The swap itself needs debugfs, NBD and privileges, so it is injected here. What
// is under test is the decision flow around it, and every one of its outcomes
// hands the same nil back to the resume: without observing whether the swap ran at
// all, and with which source, nothing distinguishes a deferral from a swap that
// tried and failed.
type recordedSwap struct {
	calls  int
	rootfs string
	src    string
	result rootfs.SwapResult
	err    error
}

func (r *recordedSwap) fn(_ context.Context, rootfsPath, srcPath string) (rootfs.SwapResult, error) {
	r.calls++
	r.rootfs, r.src = rootfsPath, srcPath

	return r.result, r.err
}

// TestOfflinePreBootDefersWhenTheCachedCopyIsRetired covers the window the gate
// cannot: the version is resolved when the pre-boot is built, and the bytes are
// read arbitrarily later, when the boot runs it. A promotion in between retires
// the copy, and the swap must then not run at all -- the source is not a
// substitute, since reading it would be the mount cost the cache exists to avoid
// and bytes whose version was never verified against them.
func TestOfflinePreBootDefersWhenTheCachedCopyIsRetired(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeEnvdImage(t, src, "0.9.0")

	f := offlineGateFactory(t, src, filepath.Join(dir, "cache"), true)
	swap := &recordedSwap{}
	f.swapEnvdBinary = swap.fn
	require.NoError(t, f.envdBinCache.Warm(t.Context(), src))

	entry, ok := f.envdBinCache.Lookup(src)
	require.True(t, ok)

	preBoot := f.envdOfflineUpgradePreBoot(t.Context(),
		&Config{Envd: EnvdMetadata{Version: "0.6.12"}},
		sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}, true)
	require.NotNil(t, preBoot, "a cached target newer than built-with resolves a swap")

	// Exactly what a promotion does between the resolve and the boot.
	require.NoError(t, os.Remove(entry.LocalPath))

	require.NoError(t, preBoot(t.Context(), filepath.Join(dir, "rootfs.ext4")),
		"a retired copy is a deferral, not a boot failure: the guest boots its own envd")
	require.Zero(t, swap.calls,
		"nothing on local disk holds the resolved bytes, so the rootfs must not be touched")
}

// TestOfflinePreBootSwapsFromTheCachedCopy is the positive twin, and it pins the
// claim the whole cache is for: the bytes written into the rootfs come from local
// disk, not from a second read of the mount whose version was never verified.
func TestOfflinePreBootSwapsFromTheCachedCopy(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := filepath.Join(dir, "envd")
	writeEnvdImage(t, src, "0.9.0")

	f := offlineGateFactory(t, src, filepath.Join(dir, "cache"), true)
	swap := &recordedSwap{}
	f.swapEnvdBinary = swap.fn
	require.NoError(t, f.envdBinCache.Warm(t.Context(), src))

	entry, ok := f.envdBinCache.Lookup(src)
	require.True(t, ok)

	preBoot := f.envdOfflineUpgradePreBoot(t.Context(),
		&Config{Envd: EnvdMetadata{Version: "0.6.12"}},
		sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}, true)
	require.NotNil(t, preBoot)

	rootfsPath := filepath.Join(dir, "rootfs.ext4")
	require.NoError(t, preBoot(t.Context(), rootfsPath))
	require.Equal(t, 1, swap.calls)
	require.Equal(t, rootfsPath, swap.rootfs)
	require.Equal(t, entry.LocalPath, swap.src,
		"the swap must stage the cached copy, not re-read the source off the mount")
	require.NotEqual(t, src, swap.src)
}

// TestOfflinePreBootFailsTheBootOnlyWhenTheRootfsIsUnbootable pins the one
// exception to the path's best-effort rule. Booting a rootfs whose envd was
// destroyed hands back a running sandbox nothing can reach; every other failure
// left the original binary in place, so it must boot rather than discard a
// customer's overlay.
func TestOfflinePreBootFailsTheBootOnlyWhenTheRootfsIsUnbootable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		wantErr bool
	}{
		{name: "swapped", err: nil},
		{name: "rootfs envd too large", err: rootfs.ErrEnvdTooLarge},
		{name: "rootfs has no envd", err: rootfs.ErrEnvdMissing},
		{name: "stat unreadable", err: rootfs.ErrStatUnparseable},
		{name: "source retired mid-swap", err: rootfs.ErrSwapSourceMissing},
		{name: "host fault", err: errors.New("debugfs exited 1")},
		{
			// The original was not restored, so the rootfs may have no usable envd.
			name: "unrecoverable", err: rootfs.ErrOfflineSwapUnrecoverable, wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			src := filepath.Join(dir, "envd")
			writeEnvdImage(t, src, "0.9.0")

			f := offlineGateFactory(t, src, filepath.Join(dir, "cache"), true)
			swap := &recordedSwap{err: tc.err}
			f.swapEnvdBinary = swap.fn
			require.NoError(t, f.envdBinCache.Warm(t.Context(), src))

			preBoot := f.envdOfflineUpgradePreBoot(t.Context(),
				&Config{Envd: EnvdMetadata{Version: "0.6.12"}},
				sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}, true)
			require.NotNil(t, preBoot)

			err := preBoot(t.Context(), filepath.Join(dir, "rootfs.ext4"))
			require.Equal(t, 1, swap.calls)
			if tc.wantErr {
				require.ErrorIs(t, err, rootfs.ErrOfflineSwapUnrecoverable,
					"an unbootable rootfs must fail the boot so the overlay is discarded")

				return
			}
			require.NoError(t, err, "every recoverable failure boots the original envd")
		})
	}
}
