package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func memoryTeam() *teamtypes.Team {
	team := testTeam()
	team.Limits.MaxRamMb = 8192

	return team
}

// growableBuild boots with 1024 MiB and can grow to maxMB; 0 is a fixed size.
// startMB is the size the build records for its starts; 0 records none.
// A build with a maximum carries an envd that lets plugged memory be used, as a
// template built for growth does.
func growableBuild(maxMB, startMB int64) queries.EnvBuild {
	build := testBuild()
	build.RamMb = 1024
	if maxMB != 0 {
		build.MaxRamMb = &maxMB
		envd := "0.10.202611180937-6f95478318f"
		build.EnvdVersion = &envd
	}
	if startMB != 0 {
		build.StartMemoryMb = &startMB
	}

	return build
}

// Every start from a build with a memory maximum sends the size the API
// accounts, so the node enforces it and the placement gate covers it: a create
// from a template sends boot RAM, one from a snapshot template of a grown
// sandbox and a resume send the size the build recorded. A build that records
// no size sends none and leaves the record's size unknown.
func TestCreateSandbox_MemoryFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		startMB    int64
		isResume   bool
		wantSent   int64
		wantTotal  int64
		wantRecord int64
		// wantNode is what node accounting releases on a kill; wantTotal when 0.
		wantNode int64
	}{
		{name: "create from a template sends boot RAM", startMB: 1024, wantSent: 1024, wantTotal: 1024, wantRecord: 1024},
		{name: "create from a snapshot template of a grown sandbox", startMB: 3072, wantSent: 3072, wantTotal: 3072, wantRecord: 3072},
		{name: "resume sends the recorded size", isResume: true, startMB: 2048, wantSent: 2048, wantTotal: 2048, wantRecord: 2048},
		{name: "a build without a recorded size sends nothing", isResume: true, wantTotal: 1024, wantNode: 4096},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &cpuRecordingSandboxClient{}
			o := newCreateSandboxTestOrchestrator(t,
				nodemanager.WithOrchestratorVersion(clusters.SandboxMemoryResizeMinVersion.String()),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) },
			)
			o.featureFlagsClient = sandboxCPUFlags(t, true)
			build := growableBuild(4096, tt.startMB)
			fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			}

			now := time.Now()
			team := memoryTeam()
			sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-mem-"+uuid.New().String()[:8], uuid.New().String(), team,
				fetcher, now, now.Add(time.Hour), time.Hour, tt.isResume, false, sandbox.CreationMetadata{})
			require.Nil(t, apiErr)

			req := rec.request.Load()
			require.NotNil(t, req)
			assert.Equal(t, int64(1024), req.GetSandbox().GetRamMb(), "ram_mb stays boot RAM")
			assert.Equal(t, tt.wantSent, req.GetSandbox().GetMemoryMb())
			assert.Equal(t, int64(1024), sbx.RamMB)
			assert.Equal(t, tt.wantTotal, sbx.TotalMemoryMB())
			assert.Equal(t, tt.wantRecord, sbx.MemoryMB)
			stored, err := o.GetSandbox(t.Context(), team.ID, sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantTotal, stored.TotalMemoryMB())
			wantNode := tt.wantNode
			if wantNode == 0 {
				wantNode = tt.wantTotal
			}
			assert.Equal(t, wantNode, stored.ToNodeSandbox().RamMB, "node accounting releases what placement charged")
			if tt.wantSent > 0 {
				assert.Contains(t, stored.RequiredFeatures, "memory-resize", "a move needs the memory gate the start needed")
			} else {
				assert.NotContains(t, stored.RequiredFeatures, "memory-resize")
			}
		})
	}
}

// A node below the memory-resize release takes no start from a build with a
// memory maximum: the API sends every such start its size, which the gate
// sees.
func TestCreateSandbox_MemoryRefusals(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		version string
	}{
		{name: "an orchestrator before the template maximum, a default create", version: clusters.TemplateMemoryMaximumMinVersion.String()},
		{name: "an orchestrator that plugs but reserves the whole maximum, a default create", version: "0.30.202611161022"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &cpuRecordingSandboxClient{}
			o := newCreateSandboxTestOrchestrator(t,
				nodemanager.WithOrchestratorVersion(tt.version),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) },
			)
			o.featureFlagsClient = sandboxCPUFlags(t, true)
			build := growableBuild(4096, 1024)
			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), "sbx-mem-old-"+uuid.NewString()[:8], uuid.NewString(), memoryTeam(),
				func(context.Context) (SandboxMetadata, *api.APIError) {
					return SandboxMetadata{Build: build}, nil
				}, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{})
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
			assert.Zero(t, rec.calls.Load())
		})
	}
}

// A sandbox whose record lost its size (an older writer dropped the field)
// pauses with no start size on its snapshot build; the next start then sends
// no memory_mb, so the orchestrator keeps its snapshot's size, and placement
// accounts the template's maximum as the bound on it. Billing and the record
// keep boot RAM, the size the API can show.
func TestUnknownMemorySizeThroughPauseAndResume(t *testing.T) {
	t.Parallel()

	record := sandbox.Sandbox{RamMB: 1024}
	assert.Nil(t, snapshotStartMemoryMB(record), "a record without the size pauses with none")

	build := growableBuild(4096, 0)
	memoryMB, known := BuildStartMemoryMB(build.RamMb, build.MaxRamMb, build.StartMemoryMb)
	assert.False(t, known)
	assert.Equal(t, int64(1024), memoryMB)
	assert.Equal(t, int64(4096), placementMemoryMiB(build, memoryMB, known), "placement bounds an unknown size by the maximum")

	knownBuild := growableBuild(4096, 2048)
	memoryMB, known = BuildStartMemoryMB(knownBuild.RamMb, knownBuild.MaxRamMb, knownBuild.StartMemoryMb)
	assert.True(t, known)
	assert.Equal(t, int64(2048), placementMemoryMiB(knownBuild, memoryMB, known), "a known size is accounted as is")

	fixed := growableBuild(0, 0)
	memoryMB, known = BuildStartMemoryMB(fixed.RamMb, fixed.MaxRamMb, fixed.StartMemoryMb)
	assert.Equal(t, int64(1024), placementMemoryMiB(fixed, memoryMB, known), "a build without a maximum is boot RAM")

	rec := &cpuRecordingSandboxClient{}
	o := newCreateSandboxTestOrchestrator(t,
		nodemanager.WithOrchestratorVersion(clusters.SandboxMemoryResizeMinVersion.String()),
		func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) },
	)
	o.featureFlagsClient = sandboxCPUFlags(t, true)
	fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
		return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
	}
	now := time.Now()
	sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-mem-"+uuid.New().String()[:8], uuid.New().String(), memoryTeam(),
		fetcher, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{})
	require.Nil(t, apiErr)
	req := rec.request.Load()
	require.NotNil(t, req)
	assert.Zero(t, req.GetSandbox().GetMemoryMb(), "no size is sent for an unknown size")
	assert.Zero(t, sbx.MemoryMB, "the record keeps the size unknown")
	assert.Equal(t, int64(1024), sbx.TotalMemoryMB(), "billing and get/list keep boot RAM")
	assert.Equal(t, int64(4096), sbx.PlacedMemoryMB, "the record keeps what placement charged")
	assert.Equal(t, int64(4096), nodeSandboxResources(sbx.ToNodeSandbox()).MiBMemory, "a kill releases what placement charged")
	assert.Nil(t, snapshotStartMemoryMB(sbx), "and its next pause still records none")
}
