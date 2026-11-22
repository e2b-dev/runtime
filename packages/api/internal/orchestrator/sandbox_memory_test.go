package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	teamtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
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

// withEnvd is build with envd version v; "" records none.
func withEnvd(build queries.EnvBuild, v string) queries.EnvBuild {
	if v == "" {
		build.EnvdVersion = nil

		return build
	}
	build.EnvdVersion = &v

	return build
}

func TestPickSandboxMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		enabled   bool
		build     queries.EnvBuild
		requested *int32
		teamMax   int64
		create    bool
		want      int64
		unknown   bool
		code      string
	}{
		{name: "fixed build without request", build: growableBuild(0, 0), want: 1024},
		{name: "fixed build ignores a recorded size", build: growableBuild(0, 2048), want: 1024},
		{name: "a template build starts at boot RAM", build: growableBuild(4096, 1024), want: 1024},
		{name: "a build without a recorded size is unknown", build: growableBuild(4096, 0), want: 1024, unknown: true},
		{name: "a snapshot build keeps its recorded size", build: growableBuild(4096, 3072), want: 3072},
		{name: "flag off refuses a request", build: growableBuild(4096, 1024), requested: new(int32(2048)), code: errCodeSandboxMemoryDisabled},
		{name: "fixed build accepts its own size", enabled: true, build: growableBuild(0, 0), requested: new(int32(1024)), want: 1024},
		{name: "fixed build refuses another size", enabled: true, build: growableBuild(0, 0), requested: new(int32(2048)), code: errCodeSandboxMemoryNotResizable},
		{name: "grow at create", enabled: true, build: growableBuild(4096, 1024), requested: new(int32(2048)), want: 2048},
		{name: "a request on an unknown build is known", enabled: true, build: growableBuild(4096, 0), requested: new(int32(2048)), want: 2048},
		{name: "the whole maximum", enabled: true, build: growableBuild(4096, 1024), requested: new(int32(4096)), want: 4096},
		{name: "below boot RAM", enabled: true, build: growableBuild(4096, 1024), requested: new(int32(512)), code: errCodeSandboxMemoryOutOfRange},
		{name: "above the maximum", enabled: true, build: growableBuild(4096, 1024), requested: new(int32(4224)), code: errCodeSandboxMemoryOutOfRange},
		{name: "not whole slots", enabled: true, build: growableBuild(4096, 1024), requested: new(int32(1100)), code: errCodeSandboxMemoryOutOfRange},
		{name: "resume grows", enabled: true, build: growableBuild(4096, 2048), requested: new(int32(3072)), want: 3072},
		{name: "resume keeps its size", enabled: true, build: growableBuild(4096, 2048), requested: new(int32(2048)), want: 2048},
		{name: "resume refuses a shrink", enabled: true, build: growableBuild(4096, 2048), requested: new(int32(1024)), code: errCodeSandboxMemoryShrinkUnsupported},
		{name: "growth above the team limit", enabled: true, build: growableBuild(4096, 1024), teamMax: 2048, requested: new(int32(3072)), code: errCodeSandboxMemoryTeamLimit},
		{name: "growth on an envd release before the limit follows memory", enabled: true, build: withEnvd(growableBuild(4096, 1024), "0.10.2"), requested: new(int32(2048)), code: errCodeSandboxMemoryEnvdOutdated},
		{name: "growth on a build without a recorded envd", enabled: true, build: withEnvd(growableBuild(4096, 1024), ""), requested: new(int32(2048)), code: errCodeSandboxMemoryEnvdOutdated},
		{name: "growth on an envd build before the floor", enabled: true, build: withEnvd(growableBuild(4096, 1024), "0.10.202611131051-689661d9296"), requested: new(int32(2048)), code: errCodeSandboxMemoryEnvdOutdated},
		{name: "growth on the first envd build at the floor", enabled: true, build: withEnvd(growableBuild(4096, 1024), minEnvdBuildForMemoryGrowth), requested: new(int32(2048)), want: 2048},
		{name: "growth on a later envd release line", enabled: true, build: withEnvd(growableBuild(4096, 1024), "0.11.0"), requested: new(int32(2048)), want: 2048},
		{name: "an old envd still starts at boot RAM", enabled: true, build: withEnvd(growableBuild(4096, 1024), "0.10.2"), requested: new(int32(1024)), want: 1024},
		{name: "an old envd resume keeps its size", enabled: true, build: withEnvd(growableBuild(4096, 2048), "0.10.2"), requested: new(int32(2048)), want: 2048},
		{name: "an old envd resume without a request keeps its size", build: withEnvd(growableBuild(4096, 2048), "0.10.2"), want: 2048},
		{name: "a kept size above a lowered team limit", enabled: true, build: growableBuild(4096, 3072), teamMax: 2048, requested: new(int32(3072)), want: 3072},
		{name: "a resume keeps an inherited size above a lowered team limit", build: growableBuild(4096, 3072), teamMax: 2048, want: 3072},
		{name: "a create inherits a snapshot template's size within the team limit", build: growableBuild(4096, 3072), create: true, want: 3072},
		{name: "a create refuses an inherited size above the team limit", build: growableBuild(4096, 3072), teamMax: 2048, create: true, code: errCodeSandboxMemoryTeamLimit},
		{name: "a create naming the inherited size above the team limit", enabled: true, build: growableBuild(4096, 3072), teamMax: 2048, requested: new(int32(3072)), create: true, code: errCodeSandboxMemoryTeamLimit},
		{name: "a create at boot RAM under a team limit below it", build: growableBuild(4096, 1024), teamMax: 512, create: true, want: 1024},
		{name: "a create with an unknown size is not refused", build: growableBuild(4096, 0), teamMax: 512, create: true, want: 1024, unknown: true},
		{name: "a create inheriting a grown size on an old envd", build: withEnvd(growableBuild(4096, 3072), "0.10.2"), create: true, code: errCodeSandboxMemoryEnvdOutdated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			team := memoryTeam()
			if tt.teamMax != 0 {
				team.Limits.MaxRamMb = tt.teamMax
			}
			got, known, apiErr := pickSandboxMemory(t.Context(), sandboxCPUFlags(t, tt.enabled), team, tt.build, tt.requested, tt.create)
			if tt.code != "" {
				require.NotNil(t, apiErr)
				assert.Equal(t, http.StatusBadRequest, apiErr.Code)
				assert.Equal(t, tt.code, apiErr.ErrorCode)

				return
			}
			require.Nil(t, apiErr)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, !tt.unknown, known)
		})
	}
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
		requested  *int32
		startMB    int64
		isResume   bool
		wantSent   int64
		wantTotal  int64
		wantRecord int64
		// wantNode is what node accounting releases on a kill; wantTotal when 0.
		wantNode int64
	}{
		{name: "create from a template sends boot RAM", startMB: 1024, wantSent: 1024, wantTotal: 1024, wantRecord: 1024},
		{name: "create grows", startMB: 1024, requested: new(int32(3072)), wantSent: 3072, wantTotal: 3072, wantRecord: 3072},
		{name: "create from a snapshot template of a grown sandbox", startMB: 3072, wantSent: 3072, wantTotal: 3072, wantRecord: 3072},
		{name: "resume sends the recorded size", isResume: true, startMB: 2048, wantSent: 2048, wantTotal: 2048, wantRecord: 2048},
		{name: "resume grows", isResume: true, startMB: 2048, requested: new(int32(4096)), wantSent: 4096, wantTotal: 4096, wantRecord: 4096},
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
				fetcher, now, now.Add(time.Hour), time.Hour, tt.isResume, false, sandbox.CreationMetadata{}, WithMemoryMB(tt.requested))
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

// A requested size is refused before any worker is asked, and a node below
// the memory-resize release takes no start from a build with a memory maximum,
// named size or not.
func TestCreateSandbox_MemoryRefusals(t *testing.T) {
	t.Parallel()

	t.Run("invalid size reaches no worker", func(t *testing.T) {
		t.Parallel()

		rec := &cpuRecordingSandboxClient{}
		o := newCreateSandboxTestOrchestrator(t, func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) })
		o.featureFlagsClient = sandboxCPUFlags(t, true)
		build := growableBuild(4096, 2048)
		now := time.Now()
		_, apiErr := o.CreateSandbox(t.Context(), "sbx-mem-bad-"+uuid.NewString()[:8], uuid.NewString(), memoryTeam(),
			func(context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{Build: build}, nil
			}, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{}, WithMemoryMB(new(int32(1024))))
		require.NotNil(t, apiErr)
		assert.Equal(t, errCodeSandboxMemoryShrinkUnsupported, apiErr.ErrorCode)
		assert.Zero(t, rec.calls.Load())
	})

	// Another team's public snapshot template of a grown sandbox: the create
	// inherits its size, which the caller's team limit does not allow.
	for _, isResume := range []bool{false, true} {
		t.Run(fmt.Sprintf("inherited size above the caller's team limit, resume=%v", isResume), func(t *testing.T) {
			t.Parallel()

			rec := &cpuRecordingSandboxClient{}
			o := newCreateSandboxTestOrchestrator(t,
				nodemanager.WithOrchestratorVersion(clusters.SandboxMemoryResizeMinVersion.String()),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) },
			)
			o.featureFlagsClient = sandboxCPUFlags(t, true)
			build := growableBuild(4096, 3072)
			team := memoryTeam()
			team.Limits.MaxRamMb = 2048
			now := time.Now()
			sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-mem-limit-"+uuid.NewString()[:8], uuid.NewString(), team,
				func(context.Context) (SandboxMetadata, *api.APIError) {
					return SandboxMetadata{TemplateID: "public-snapshot", Build: build}, nil
				}, now, now.Add(time.Hour), time.Hour, isResume, false, sandbox.CreationMetadata{})
			if isResume {
				require.Nil(t, apiErr, "a sandbox keeps a size it already has above a lowered limit")
				assert.Equal(t, int64(3072), sbx.TotalMemoryMB())

				return
			}
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Equal(t, errCodeSandboxMemoryTeamLimit, apiErr.ErrorCode)
			assert.Zero(t, rec.calls.Load(), "refused before any worker is asked")
		})
	}

	for _, tt := range []struct {
		name      string
		version   string
		requested *int32
	}{
		{name: "an orchestrator before the template maximum, with a size", version: clusters.TemplateMemoryMaximumMinVersion.String(), requested: new(int32(2048))},
		{name: "an orchestrator that plugs but reserves the whole maximum, with a size", version: "0.30.202611161022", requested: new(int32(2048))},
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
				}, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{}, WithMemoryMB(tt.requested))
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.Code)
			assert.Zero(t, rec.calls.Load())
		})
	}
}

func TestCreateSandboxMemoryOverrideRefusesJoin(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			t.Parallel()

			o := newCreateSandboxTestOrchestrator(t)
			o.featureFlagsClient = sandboxCPUFlags(t, enabled)
			team := memoryTeam()
			sandboxID := "sbx-mem-join-" + uuid.NewString()
			finish, wait, err := o.sandboxStore.Reserve(t.Context(), team.ID, sandboxID, 10)
			require.NoError(t, err)
			require.Nil(t, wait)
			defer finish(sandbox.Sandbox{}, errors.New("test reservation released"))

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			now := time.Now()
			_, apiErr := o.CreateSandbox(ctx, sandboxID, uuid.NewString(), team,
				func(context.Context) (SandboxMetadata, *api.APIError) {
					assert.Fail(t, "a joined or refused request must not fetch the snapshot")

					return SandboxMetadata{}, nil
				}, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{}, WithMemoryMB(new(int32(2048))))
			require.NotNil(t, apiErr)
			if enabled {
				assert.Equal(t, http.StatusConflict, apiErr.Code)
				assert.Equal(t, ErrCodeStartInFlight, apiErr.ErrorCode)

				return
			}
			// With the feature off the 400 comes first, not the 409.
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Equal(t, errCodeSandboxMemoryDisabled, apiErr.ErrorCode)
		})
	}
}

// A build that cannot grow takes a request for its own boot RAM without
// sending the field, so the start needs no memory-resize release.
func TestCreateSandbox_FixedBuildOwnSizeSendsNothing(t *testing.T) {
	t.Parallel()

	rec := &cpuRecordingSandboxClient{}
	o := newCreateSandboxTestOrchestrator(t, func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) })
	o.featureFlagsClient = sandboxCPUFlags(t, true)
	build := growableBuild(0, 0)
	now := time.Now()
	sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-mem-fixed-"+uuid.NewString()[:8], uuid.NewString(), memoryTeam(),
		func(context.Context) (SandboxMetadata, *api.APIError) {
			return SandboxMetadata{Build: build}, nil
		}, now, now.Add(time.Hour), time.Hour, false, false, sandbox.CreationMetadata{}, WithMemoryMB(new(int32(1024))))
	require.Nil(t, apiErr)
	req := rec.request.Load()
	require.NotNil(t, req)
	assert.Zero(t, req.GetSandbox().GetMemoryMb())
	assert.Equal(t, int64(1024), sbx.TotalMemoryMB())
}

// A start that named memoryMB is counted once placement answered: accepted when
// a node took it, rejected with the memory code when the node refused the size,
// failed with the API's code for anything else.
func TestPlacementMemoryOutcome(t *testing.T) {
	t.Parallel()

	shrink := placement.SandboxCreateError{Attempts: 1, LastErr: createRefusedStatusWith(t, codes.InvalidArgument, "memory hotplug refused (shrink): the requested memory does not fit the sandbox", http.StatusBadRequest)}
	capacity := placement.NoNodesAvailableError{}

	outcome, code := placementMemoryOutcome(nil, nil)
	assert.Equal(t, memoryOverrideAccepted, outcome)
	assert.Empty(t, code)

	outcome, code = placementMemoryOutcome(shrink, placementAPIError(shrink, true))
	assert.Equal(t, memoryOverrideRejected, outcome)
	assert.Equal(t, errCodeSandboxMemoryShrinkUnsupported, code)

	capacityErr := placementAPIError(capacity, true)
	outcome, code = placementMemoryOutcome(capacity, capacityErr)
	assert.Equal(t, memoryOverrideFailed, outcome)
	assert.Equal(t, capacityErr.ErrorCode, code)
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
