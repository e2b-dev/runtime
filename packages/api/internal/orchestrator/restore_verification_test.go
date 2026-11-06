package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// restoreScriptHook runs afterRestore, with the record key the restore script
// wrote, once the script has written the record back and, when failNextRead
// is set, fails the read that follows: the ownership check after the restore.
type restoreScriptHook struct {
	afterRestore func(recordKey string)
	failNextRead bool

	armed atomic.Bool
	fired atomic.Bool
}

func (h *restoreScriptHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *restoreScriptHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *restoreScriptHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "get" && isRecordKey(cmd.Args()[1].(string)) && h.armed.CompareAndSwap(true, false) {
			h.fired.Store(true)

			return errors.New("verification read unavailable")
		}

		err := next(ctx, cmd)
		if err == nil && isRestoreRunningScript(cmd) {
			h.afterRestore(cmd.Args()[3].(string))
			h.armed.Store(h.failNextRead)
		}

		return err
	}
}

// restoreSandboxScript runs with one key, the record, and two payloads: the
// expected and the restored record. The lock release also runs a one-key
// script on the record's lock key, with a single token.
func isRestoreRunningScript(cmd redis.Cmder) bool {
	if cmd.Name() != "evalsha" && cmd.Name() != "eval" {
		return false
	}
	args := cmd.Args()
	if len(args) != 6 {
		return false
	}
	numKeys, _ := args[2].(int)
	key, _ := args[3].(string)

	return numKeys == 1 && isRecordKey(key) && !strings.Contains(key, "lock:sandbox:")
}

// isRecordKey reports whether key is a sandbox record key of either form:
// team-tagged (":sandboxes:") or sandbox-tagged (":record"). Contains, not
// HasPrefix: a namespaced test client puts its prefix before the key.
func isRecordKey(key string) bool {
	return strings.Contains(key, ":sandboxes:") || strings.Contains(key, ":record")
}

func successorOf(sbx sandbox.Sandbox) sandbox.Sandbox {
	resumed := sbx
	resumed.ExecutionID = uuid.NewString()
	resumed.NodeID = "node-2"

	return resumed
}

// writePastLock replaces the record under key with sbx, bypassing the storage
// lock as a replica whose lock expired would. Add waits for the lock, which
// the restore holds while a hook runs. The key is the one the hook observed,
// which already carries the test client's namespace; the client's prefix
// hook skips a key that carries it, so the write lands on the same key.
func (f refusalFixture) writePastLock(ctx context.Context, key string, sbx sandbox.Sandbox) error {
	data, err := json.Marshal(sbx)
	if err != nil {
		return err
	}

	return f.client.Set(ctx, key, data, 0).Err()
}

// A write that bypasses the storage lock, as a replica whose lock expired
// makes, can replace the record right after the restore wrote it back. The
// ownership check reads the record again and yields to the successor: it is
// not rewritten, not removed, and its VM is not killed.
func TestRemoveSandbox_SuccessorAfterRestoreIsSuperseded(t *testing.T) {
	t.Parallel()

	var f refusalFixture
	var resumed sandbox.Sandbox
	hook := &restoreScriptHook{afterRestore: func(recordKey string) {
		stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		require.Equal(t, sandbox.StateRunning, stored.State)
		require.Equal(t, f.sbx.ExecutionID, stored.ExecutionID)

		resumed = successorOf(f.sbx)
		require.NoError(t, f.writePastLock(t.Context(), recordKey, resumed))
	}}
	f = newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr(), hook)
	node, ok := f.o.nodes.Get(f.o.scopedNodeID(f.sbx.ClusterID, f.sbx.NodeID))
	require.True(t, ok)
	stub := &pauseStubClient{err: refusedPauseErr()}
	node.SetSandboxClient(stub)

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxNotFound)
	require.ErrorIs(t, err, sandbox.ErrExecutionMismatch)
	assert.Zero(t, stub.deleteCount())

	stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, resumed.ExecutionID, stored.ExecutionID)
	assert.Equal(t, resumed.NodeID, stored.NodeID)
	assert.Equal(t, sandbox.StateRunning, stored.State)
	assert.True(t, stored.RefusedUntil.IsZero())
	assert.Never(t, func() bool { return f.recorder.stoppedCount() != 0 }, 200*time.Millisecond, 10*time.Millisecond)
	assert.Equal(t, map[string]int64{"superseded/request": 1}, f.restoreOutcomes(t))
}

// A restore that cannot verify its ownership keeps the sandbox: the record
// that is there, restored or a successor's, survives untouched and the VM is
// not killed.
func TestRemoveSandbox_VerificationReadFailurePreservesSandbox(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"restored execution", "successor execution"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var f refusalFixture
			var expected sandbox.Sandbox
			hook := &restoreScriptHook{failNextRead: true, afterRestore: func(recordKey string) {
				stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
				require.NoError(t, err)
				require.Equal(t, sandbox.StateRunning, stored.State)

				if name == "successor execution" {
					require.NoError(t, f.writePastLock(t.Context(), recordKey, successorOf(f.sbx)))
				}
				expected, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
				require.NoError(t, err)
			}}
			f = newRefusalFixture(t, true, consts.LocalClusterID, refusedPauseErr(), hook)
			node, ok := f.o.nodes.Get(f.o.scopedNodeID(f.sbx.ClusterID, f.sbx.NodeID))
			require.True(t, ok)
			stub := &pauseStubClient{err: refusedPauseErr()}
			node.SetSandboxClient(stub)

			err := f.removePause(t)
			require.True(t, hook.fired.Load())
			assert.Zero(t, stub.deleteCount())
			require.ErrorIs(t, err, PauseQueueExhaustedError{})
			stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, expected, stored)
			assert.Never(t, func() bool { return f.recorder.stoppedCount() != 0 }, 200*time.Millisecond, 10*time.Millisecond)
			assert.Equal(t, map[string]int64{"restored/request": 1}, f.restoreOutcomes(t))
		})
	}
}
