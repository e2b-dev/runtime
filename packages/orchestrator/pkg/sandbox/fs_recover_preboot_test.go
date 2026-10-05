//go:build linux

package sandbox

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

func recoverTestFactory(t *testing.T, flagOn bool) *Factory {
	t.Helper()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.PrebootFsRecoveryFlag.Key()).VariationForAll(flagOn))
	client, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)

	return &Factory{featureFlags: client}
}

// recorder captures the outcome fsRecoverPreBoot reports, so the tests can
// assert what the create metric would be paired with.
func recorder() (func(rootfs.RecoverOutcome), *[]rootfs.RecoverOutcome) {
	var got []rootfs.RecoverOutcome

	return func(o rootfs.RecoverOutcome) { got = append(got, o) }, &got
}

func TestFsRecoverPreBoot_Gating(t *testing.T) {
	t.Parallel()

	runtime := sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}

	rec, got := recorder()
	assert.Nil(t,
		recoverTestFactory(t, true).fsRecoverPreBoot(t.Context(), runtime, true, false, rec),
		"quiesced snapshot has nothing to repair")
	assert.Equal(t, []rootfs.RecoverOutcome{rootfs.RecoverOutcomeSkippedQuiesced}, *got,
		"quiesced records skipped_quiesced so the create metric pairs it")

	rec, got = recorder()
	assert.Nil(t,
		recoverTestFactory(t, false).fsRecoverPreBoot(t.Context(), runtime, false, false, rec),
		"flag off keeps today's cold-boot behavior")
	assert.Empty(t, *got, "flag off records nothing; the create metric stays none")

	rec, _ = recorder()
	assert.NotNil(t,
		recoverTestFactory(t, true).fsRecoverPreBoot(t.Context(), runtime, false, true, rec),
		"non-quiesced with the flag on runs recovery (rescue trigger)")
	rec, _ = recorder()
	assert.NotNil(t,
		recoverTestFactory(t, true).fsRecoverPreBoot(t.Context(), runtime, false, false, rec),
		"non-quiesced with the flag on runs recovery (legacy trigger)")
}

// Executing the returned PreBootFn must propagate a recovery failure to the
// caller (which fails the start) — the closure must not swallow it — AND report
// the outcome so a replayed-then-hung start stays distinguishable on the create
// metric. A non-NBD rootfs path makes RecoverFilesystem refuse without spawning
// anything, so the closure returns the operational error rather than nil.
func TestFsRecoverPreBoot_ClosurePropagatesFailure(t *testing.T) {
	t.Parallel()

	runtime := sandboxtypes.RuntimeMetadata{SandboxID: "sbx", TemplateID: "tpl"}
	rec, got := recorder()
	fn := recoverTestFactory(t, true).fsRecoverPreBoot(t.Context(), runtime, false, true, rec)
	require.NotNil(t, fn)

	err := fn(t.Context(), "/dev/sda")
	require.Error(t, err, "recovery failure must propagate so the boot fails")
	require.ErrorIs(t, err, rootfs.ErrRecoveryFailed)
	assert.Equal(t, []rootfs.RecoverOutcome{rootfs.RecoverOutcomeFailed}, *got,
		"the failed outcome is reported so create pairs fs_recovery=failed_operational")
}

func TestChainPreBoot(t *testing.T) {
	t.Parallel()

	assert.Nil(t, chainPreBoot(nil, nil), "all-nil chain preserves the no-callback fast path")

	var order []string
	mk := func(name string, err error) PreBootFn {
		return func(_ context.Context, _ string) error {
			order = append(order, name)

			return err
		}
	}
	boom := assert.AnError
	fn := chainPreBoot(nil, mk("recover", nil), mk("swap", nil))
	require.NotNil(t, fn)
	require.NoError(t, fn(t.Context(), "/dev/nbd0"))
	assert.Equal(t, []string{"recover", "swap"}, order)

	order = nil
	fn = chainPreBoot(mk("recover", boom), mk("swap", nil))
	require.ErrorIs(t, fn(t.Context(), "/dev/nbd0"), boom)
	assert.Equal(t, []string{"recover"}, order, "first failure stops the chain")
}
