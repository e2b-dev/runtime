package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// pauseSucceedingSandboxClient accepts every Pause.
type pauseSucceedingSandboxClient struct {
	orchestrator.SandboxServiceClient
}

func (*pauseSucceedingSandboxClient) Pause(context.Context, *orchestrator.SandboxPauseRequest, ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	return &orchestrator.SandboxPauseResponse{}, nil
}

type noopSnapshotCache struct{}

func (noopSnapshotCache) Invalidate(context.Context, string) {}

// seedFilesystemOnlyBuild leaves the sandbox with one ready filesystem-only build, as
// a completed memory:false pause or snapshot would.
func seedFilesystemOnlyBuild(t *testing.T, o *Orchestrator, node *nodemanager.Node, sbx sandbox.Sandbox) {
	t.Helper()

	row, err := o.throttledUpsertSnapshot(t.Context(), buildUpsertSnapshotParams(sbx, node, true, true))
	require.NoError(t, err)
	require.NoError(t, o.finishSnapshotBuild(t.Context(), row.BuildID))
}

// lastSnapshotBuildKind is the kind recorded on the build a resume of the
// sandbox starts from.
func lastSnapshotBuildKind(t *testing.T, o *Orchestrator, sandboxID string) bool {
	t.Helper()

	row, err := o.sqlcDB.GetLastSnapshot(t.Context(), sandboxID)
	require.NoError(t, err)

	return row.EnvBuild.FilesystemOnly
}

// A pause the node refuses leaves the sandbox running on its previous build,
// so the row must keep describing that build's kind: here a filesystem-only
// build must not be relabelled as a memory one by a refused memory pause.
func TestPauseSandbox_RefusedPauseKeepsSnapshotKind(t *testing.T) {
	t.Parallel()

	o, _, node, sbx := newPauseFixture(t, errors.New("node exploded"))
	seedFilesystemOnlyBuild(t, o, node, sbx)

	require.Error(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))

	kind, err := o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, kind, "refused pause must not change the kind of the ready build")
}

// A pause that succeeds records the requested kind together with the ready
// build, in both directions.
func TestPauseSandbox_SuccessRecordsRequestedKind(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseSucceedingSandboxClient{})
	o.snapshotCache = noopSnapshotCache{}
	seedFilesystemOnlyBuild(t, o, node, sbx)

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	kind, err := o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.False(t, kind, "a memory pause records a memory build")
	assert.False(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the memory pause's build is labelled memory")
	buildStatus, _ := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupReady, buildStatus)

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause, FilesystemOnly: true}))
	kind, err = o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, kind, "a filesystem-only pause records a filesystem-only build")
	assert.True(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the filesystem-only pause's build is labelled filesystem-only")
}

// pauseCapturingDiskOnlyClient answers every Pause as a disk-only capture, as
// a node does for a powered-off guest.
type pauseCapturingDiskOnlyClient struct {
	orchestrator.SandboxServiceClient
}

func (*pauseCapturingDiskOnlyClient) Pause(context.Context, *orchestrator.SandboxPauseRequest, ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	return &orchestrator.SandboxPauseResponse{FilesystemOnly: true}, nil
}

// A full pause the node captured as disk-only records the captured kind on
// both the snapshot row and the build, not the requested one.
func TestPauseSandbox_SuccessRecordsCapturedKind(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseCapturingDiskOnlyClient{})
	o.snapshotCache = noopSnapshotCache{}

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	kind, err := o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, kind, "the snapshot row carries the captured kind")
	assert.True(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the build carries the captured kind")
	buildStatus, _ := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupReady, buildStatus)
}

// pauseCapturingLateClient answers a pause once its caller's deadline has
// passed, captured disk-only, as a node does that took the whole budget.
type pauseCapturingLateClient struct {
	orchestrator.SandboxServiceClient
}

func (*pauseCapturingLateClient) Pause(ctx context.Context, _ *orchestrator.SandboxPauseRequest, _ ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	<-ctx.Done()

	return &orchestrator.SandboxPauseResponse{FilesystemOnly: true}, nil
}

// The captured kind is recorded in the detached finalization, so a capture
// that used up the pause budget is not discarded on the kind write.
func TestPauseSandbox_CapturedKindSurvivesAnExhaustedBudget(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseCapturingLateClient{})
	o.snapshotCache = noopSnapshotCache{}

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	require.NoError(t, o.pauseSandbox(ctx, node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	assert.True(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the build carries the captured kind")
	buildStatus, _ := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupReady, buildStatus)
}

// The captured kind's write goes through the client's retry: one transient
// failure, injected once by a trigger, does not lose the finished snapshot.
func TestPauseSandbox_CapturedKindRetriesATransientWrite(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseCapturingDiskOnlyClient{})
	o.snapshotCache = noopSnapshotCache{}
	seedFilesystemOnlyBuild(t, o, node, sbx)
	_, err := db.SqlcClient.Pool().Exec(t.Context(), `
		CREATE SEQUENCE kind_write_attempt;
		CREATE FUNCTION fail_kind_write_once() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF nextval('kind_write_attempt') = 1 THEN
				RAISE EXCEPTION 'transient connection failure' USING ERRCODE = '08006';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER fail_kind_write BEFORE UPDATE OF filesystem_only ON env_builds
			FOR EACH ROW EXECUTE FUNCTION fail_kind_write_once();
	`)
	require.NoError(t, err)

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	var attempts int64
	require.NoError(t, db.SqlcClient.Pool().QueryRow(t.Context(), "SELECT last_value FROM kind_write_attempt").Scan(&attempts))
	assert.GreaterOrEqual(t, attempts, int64(2), "the refused write was retried")
	assert.True(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the new build is the one a resume starts from, labelled disk-only")
	buildStatus, _ := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupReady, buildStatus)
}

// The captured-kind write and the status write share one detached budget: a
// finalization slower than it fails while the sandbox's transition is still
// held, instead of each write getting a fresh budget.
func TestPauseSandbox_CapturedKindSharesTheFinalizationBudget(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseCapturingDiskOnlyClient{})
	o.snapshotCache = noopSnapshotCache{}
	half := buildStatusWriteTimeout * 6 / 10
	_, err := db.SqlcClient.Pool().Exec(t.Context(), fmt.Sprintf(`
		CREATE FUNCTION slow_build_write() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_sleep(%f);
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER slow_kind_write BEFORE UPDATE OF filesystem_only ON env_builds
			FOR EACH ROW EXECUTE FUNCTION slow_build_write();
		CREATE TRIGGER slow_status_write BEFORE UPDATE OF status ON env_builds
			FOR EACH ROW EXECUTE FUNCTION slow_build_write();
	`, half.Seconds()))
	require.NoError(t, err)

	err = o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause})
	require.Error(t, err, "two writes of 0.6 of the budget each exceed one shared budget")
}
