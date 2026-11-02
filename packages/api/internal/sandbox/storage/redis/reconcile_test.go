package redis

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// TestReconcile_CapsConcurrentPipelines pins that concurrent Reconcile calls
// keep at most maxConcurrentReconciles pipelines in flight. The node sync
// reconciles every node at once, and each uncapped call holds a connection on
// every shard it reads.
func TestReconcile_CapsConcurrentPipelines(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	// Every GET pipeline is held for a moment so that concurrent calls
	// overlap; peak is the most in flight at once.
	var inFlight, peak atomic.Int64
	client.AddHook(cmdHook{pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
		if len(cmds) == 0 || cmds[0].Name() != "get" {
			return next(ctx, cmds)
		}

		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)

		return next(ctx, cmds)
	}})

	started := time.Now().Add(-2 * orphanGracePeriod)

	var wg sync.WaitGroup
	for i := range 3 * maxConcurrentReconciles {
		wg.Go(func() {
			sbx := sandboxtypes.NodeSandbox{
				SandboxID: fmt.Sprintf("sbx-%d", i),
				TeamID:    uuid.New(),
				StartTime: started,
			}
			storage.Reconcile(t.Context(), []sandboxtypes.NodeSandbox{sbx}, fmt.Sprintf("node-%d", i))
		})
	}
	wg.Wait()

	require.LessOrEqual(t, peak.Load(), int64(maxConcurrentReconciles))
	require.Greater(t, peak.Load(), int64(1), "the calls never overlapped, so the cap was not exercised")
}

// TestReconcile_SpansSeveralBatches pins that Reconcile classifies every node
// sandbox correctly when one team's candidates are split across several
// pipelines: each result has to line up with the candidate it was fetched for,
// or a live sandbox in one chunk would be reported as an orphan from another.
//
// It also asserts the chunking itself through a client hook: the GETs of every
// pipeline sent and the keys of every MGET, none above sandboxScanBatchSize,
// one of each per candidate. The orphan-set check alone passes with a single
// unbounded pipeline, so without this a revert of the chunking would go
// unnoticed.
func TestReconcile_SpansSeveralBatches(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	teamID := uuid.New()
	started := time.Now().Add(-2 * orphanGracePeriod)

	hook, snapshot := recordGets()
	client.AddHook(hook)

	// More than two full chunks plus a partial one, all in the store.
	stored := sandboxScanBatchSize*2 + 37
	seedTeamSandboxes(t, client, teamID, stored, sandboxtypes.StateRunning)

	var nodeSandboxes []sandboxtypes.NodeSandbox
	for i := range stored {
		nodeSandboxes = append(nodeSandboxes, sandboxtypes.NodeSandbox{
			SandboxID: fmt.Sprintf("sbx-%s-%04d", sandboxtypes.StateRunning, i),
			TeamID:    teamID,
			StartTime: started,
		})
	}

	// Orphans the node reports but the store never saw, placed so that they
	// land in the first, a middle, and the final chunk.
	orphanIDs := []string{"orphan-first", "orphan-middle", "orphan-last"}
	orphans := []sandboxtypes.NodeSandbox{
		{SandboxID: orphanIDs[0], TeamID: teamID, StartTime: started},
		{SandboxID: orphanIDs[1], TeamID: teamID, StartTime: started},
		{SandboxID: orphanIDs[2], TeamID: teamID, StartTime: started},
	}
	nodeSandboxes = slices.Insert(nodeSandboxes, 0, orphans[0])
	nodeSandboxes = slices.Insert(nodeSandboxes, sandboxScanBatchSize+10, orphans[1])
	nodeSandboxes = append(nodeSandboxes, orphans[2])

	// Too young to be judged, even though it is not in the store.
	nodeSandboxes = append(nodeSandboxes, sandboxtypes.NodeSandbox{
		SandboxID: "fresh-not-yet-stored",
		TeamID:    teamID,
		StartTime: time.Now(),
	})

	got := storage.Reconcile(t.Context(), nodeSandboxes, "test-node")

	gotIDs := make([]string, 0, len(got))
	for _, sbx := range got {
		gotIDs = append(gotIDs, sbx.SandboxID)
	}
	require.ElementsMatch(t, orphanIDs, gotIDs, "exactly the stored-nowhere, old-enough sandboxes are orphans")
	for _, sbx := range got {
		assert.Equal(t, teamID, sbx.TeamID)
	}

	// Candidates = stored + 3 orphans; the fresh one is filtered before any
	// read. 552 refs / 256 per chunk = 3 chunks. The records are in the team
	// form, so every chunk's sandbox-form pipeline misses them all and one
	// team-form MGET of the same size follows: 256, 256, 40 of each.
	candidates := stored + len(orphans)
	var want []int
	for start := 0; start < candidates; start += sandboxScanBatchSize {
		want = append(want, min(sandboxScanBatchSize, candidates-start))
	}
	gets, mgets := snapshot()
	require.Equal(t, want, gets, "GETs per pipeline sent")
	require.Equal(t, want, mgets, "keys per MGET sent")
}
