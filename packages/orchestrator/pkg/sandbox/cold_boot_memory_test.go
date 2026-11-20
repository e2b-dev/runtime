//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// The guest memory file holds boot RAM and then the hotplug region, so a cold boot's memfile
// must cover both: sized at boot RAM alone, the region's pages would fall outside it.
func TestNewColdBootMemfileCoversTheRegion(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		cfg      Config
		size     int64
		pageSize int64
	}{
		"hugepages with a region":    {Config{RamMB: 1024, MemoryHotplugMB: 3072, HugePages: true}, units.MBToBytes(4096), header.HugepageSize},
		"hugepages without a region": {Config{RamMB: 1024, HugePages: true}, units.MBToBytes(1024), header.HugepageSize},
		"4 KiB pages with a region":  {Config{RamMB: 512, MemoryHotplugMB: 128}, units.MBToBytes(640), header.PageSize},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memfile, err := NewColdBootMemfile(&tc.cfg, uuid.New())
			require.NoError(t, err)
			size, err := memfile.Size(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tc.size, size)
			assert.Equal(t, tc.pageSize, memfile.BlockSize())
		})
	}
}

// A cold boot carries its config's region into the Firecracker process; one its boot cannot
// carry is refused, and no region needs no capability.
func TestColdBootMemoryHotplug(t *testing.T) {
	t.Parallel()

	in := fc.ProcessOptions{InitScriptPath: "/sbin/init"}

	got, err := coldBootMemoryHotplug(NewConfig(Config{MemoryHotplugMB: 3072, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.17-0.4.1"}}), in)
	require.NoError(t, err)
	assert.Equal(t, int64(3072), got.MemoryHotplugMB)
	assert.Equal(t, "/sbin/init", got.InitScriptPath)

	got, err = coldBootMemoryHotplug(NewConfig(Config{FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.3.1"}}), in)
	require.NoError(t, err)
	assert.Zero(t, got.MemoryHotplugMB)

	_, err = coldBootMemoryHotplug(NewConfig(Config{MemoryHotplugMB: 3072, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.3.1"}}), in)
	require.ErrorIs(t, err, fc.ErrMemoryHotplugUnsupported)
	_, err = coldBootMemoryHotplug(NewConfig(Config{MemoryHotplugMB: 3072, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.17-0.4.1", Guest: firmwareProfile{}}}), in)
	require.ErrorIs(t, err, fc.ErrMemoryHotplugUnsupported)
}

// CreateSandbox runs the region check before any resource: the template's files are never touched.
func TestCreateSandboxRefusesAnUnsupportedRegion(t *testing.T) {
	t.Parallel()

	_, err := (&Factory{}).CreateSandbox(
		t.Context(),
		NewConfig(Config{Vcpu: 2, RamMB: 1024, MemoryHotplugMB: 3072, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.3.1"}}),
		sandboxtypes.RuntimeMetadata{},
		capacityOnlyTemplate{},
		0,
		"",
		fc.ProcessOptions{},
		nil,
	)
	require.ErrorIs(t, err, fc.ErrMemoryHotplugUnsupported)
}

func TestRebootMemoryHotplugMB(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(3072), rebootMemoryHotplugMB(0, 3072), "a request without a region keeps the lineage's")
	assert.Equal(t, int64(1024), rebootMemoryHotplugMB(1024, 3072), "an explicit region may resize a cold-booted VM")
	assert.Zero(t, rebootMemoryHotplugMB(0, 0), "a lineage without a region boots without one")
}

// A reboot or filesystem-only resume comes back with its lineage's region unless the request names
// one: the config carries it to the process and the memfile covers it, as the vCPU capacity is carried.
func TestPrepareColdBootCarriesTheRegion(t *testing.T) {
	t.Parallel()

	ff, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	for name, tc := range map[string]struct{ requested, recorded, want int64 }{
		"inherits the recorded region":             {requested: 0, recorded: 3072, want: 3072},
		"keeps an explicit region":                 {requested: 1024, recorded: 3072, want: 1024},
		"explicit region on a lineage without one": {requested: 1024, recorded: 0, want: 1024},
		"no region anywhere":                       {requested: 0, recorded: 0, want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tpl := templatemocks.NewMockTemplate(t)
			tpl.EXPECT().Files().Return(storage.CachePaths{Paths: storage.Paths{BuildID: uuid.NewString()}})
			config := NewConfig(Config{
				Vcpu: 2, RamMB: 1024, HugePages: true, MemoryHotplugMB: tc.requested,
				Envd:              EnvdMetadata{Version: "0.5.0"},
				FirecrackerConfig: fc.Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.17-0.4.1"},
			})
			meta := metadata.Template{Version: metadata.CurrentVersion, VcpuCount: 2, MemoryHotplugMB: tc.recorded}

			boot, err := (&Factory{featureFlags: ff}).prepareColdBoot(t.Context(), tpl, meta, config, sandboxtypes.RuntimeMetadata{SandboxID: "sbx-" + uuid.NewString()})
			require.NoError(t, err)
			assert.Equal(t, tc.want, config.MemoryHotplugMB)
			assert.Zero(t, config.MemoryMB, "a lineage that plugged nothing asks for nothing")

			memfile, err := boot.template.Memfile(t.Context())
			require.NoError(t, err)
			size, err := memfile.Size(t.Context())
			require.NoError(t, err)
			assert.Equal(t, units.MBToBytes(1024+tc.want), size)

			opts, err := coldBootMemoryHotplug(config, boot.processOptions)
			require.NoError(t, err)
			assert.Equal(t, tc.want, opts.MemoryHotplugMB)
		})
	}
}

// A memory resume of a snapshot with a region is refused on a Firecracker that cannot export it,
// before any resource is taken; the same snapshot without a region resumes as before.
func TestResumeRefusesARegionBeforeResourceInitialization(t *testing.T) {
	t.Parallel()

	_, err := (&Factory{}).ResumeSandbox(
		t.Context(),
		capacityOnlyTemplate{meta: metadata.Template{VcpuCount: 2, MemoryHotplugMB: 3072}},
		NewConfig(Config{Vcpu: 2, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14-0.3.1"}}),
		sandboxtypes.RuntimeMetadata{},
		time.Time{},
		time.Time{},
		nil,
	)
	require.ErrorIs(t, err, fc.ErrMemoryHotplugFirecracker)
}

// A memory resume is not refused by rules that belong to a boot: an old kernel name, a region a
// later slot size would reject, or a guest without a command line. It goes on to resource
// initialization, which the test template marks by panicking.
func TestResumeIgnoresBootOnlyRegionRules(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]fc.Config{
		"guest kernel below the floor": {KernelVersion: "vmlinux-5.10.186", FirecrackerVersion: "v1.17-0.4.1"},
		"no kernel command line":       {KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.17-0.4.1", Guest: firmwareProfile{}},
	} {
		for _, region := range []int64{3072, 100} {
			t.Run(fmt.Sprintf("%s, %d MiB", name, region), func(t *testing.T) {
				t.Parallel()

				assert.PanicsWithValue(t, "resource initialization", func() {
					_, _ = (&Factory{}).ResumeSandbox(
						t.Context(),
						capacityOnlyTemplate{meta: metadata.Template{VcpuCount: 2, MemoryHotplugMB: region}},
						NewConfig(Config{Vcpu: 2, FirecrackerConfig: cfg}),
						sandboxtypes.RuntimeMetadata{},
						time.Time{},
						time.Time{},
						nil,
					)
				})
			})
		}
	}
}

func TestRequestedMemoryHotplug(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		memoryMB, ramMB, regionMB, recordedMB int64
		memoryResume                          bool
		want                                  int64
		wantErr                               error
	}{
		"no size keeps the snapshot's request":     {memoryMB: 0, ramMB: 1024, regionMB: 3072, recordedMB: 2048, memoryResume: true, want: 2048},
		"boot RAM only":                            {memoryMB: 1024, ramMB: 1024, regionMB: 3072, want: 0},
		"grow within the region":                   {memoryMB: 3072, ramMB: 1024, regionMB: 3072, recordedMB: 1024, memoryResume: true, want: 2048},
		"the whole region":                         {memoryMB: 4096, ramMB: 1024, regionMB: 3072, want: 3072},
		"below boot RAM":                           {memoryMB: 512, ramMB: 1024, regionMB: 3072, wantErr: fc.ErrMemoryHotplugUnsupported},
		"beyond the region":                        {memoryMB: 4224, ramMB: 1024, regionMB: 3072, wantErr: fc.ErrMemoryHotplugUnsupported},
		"no region":                                {memoryMB: 2048, ramMB: 1024, regionMB: 0, wantErr: fc.ErrMemoryHotplugUnsupported},
		"not a whole block":                        {memoryMB: 2049, ramMB: 1024, regionMB: 3072, wantErr: fc.ErrMemoryHotplugUnsupported},
		"whole blocks but not a whole slot":        {memoryMB: 1024 + 1026, ramMB: 1024, regionMB: 3072, wantErr: fc.ErrMemoryRequestInvalid},
		"a memory resume cannot shrink":            {memoryMB: 2048, ramMB: 1024, regionMB: 3072, recordedMB: 2048, memoryResume: true, wantErr: fc.ErrMemoryHotplugShrink},
		"a cold boot may come back smaller":        {memoryMB: 2048, ramMB: 1024, regionMB: 3072, recordedMB: 2048, want: 1024},
		"a memory resume may keep its size":        {memoryMB: 3072, ramMB: 1024, regionMB: 3072, recordedMB: 2048, memoryResume: true, want: 2048},
		"a memory resume without a region at boot": {memoryMB: 1024, ramMB: 1024, regionMB: 0, memoryResume: true, want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := requestedMemoryHotplug(tc.memoryMB, tc.ramMB, tc.regionMB, tc.recordedMB, tc.memoryResume)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	_, err := requestedMemoryHotplug(2048, 1024, 3072, 2048, true)
	assert.Equal(t, "shrink", fc.MemoryHotplugRefusalReason(err))
	_, err = requestedMemoryHotplug(9999, 1024, 3072, 0, false)
	assert.Equal(t, "invalid_request", fc.MemoryHotplugRefusalReason(err))
	require.ErrorIs(t, err, fc.ErrMemoryRequestInvalid)
}

// A memory resume reads the restored device only when it has one and has a size to check: a named
// size over a region, or a recorded one. A template without a region may name boot RAM alone, which
// sets no target, so the restore makes no hotplug query.
func TestRestoreMemoryHotplugTarget(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		regionMB, memoryMB, requestMB int64
		want                          int64
		explicit, ok                  bool
	}{
		"no region, boot RAM named":           {regionMB: 0, memoryMB: 1024, requestMB: 0},
		"no region, nothing named":            {regionMB: 0},
		"a region, nothing named or recorded": {regionMB: 3072},
		"a region, boot RAM named":            {regionMB: 3072, memoryMB: 1024, requestMB: 0, want: 0, explicit: true, ok: true},
		"a region, a size named":              {regionMB: 3072, memoryMB: 3072, requestMB: 2048, want: 2048, explicit: true, ok: true},
		"a region, a recorded size kept":      {regionMB: 3072, requestMB: 2048, want: 2048, ok: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			target, explicit, ok := restoreMemoryHotplugTarget(tc.regionMB, tc.memoryMB, tc.requestMB)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, target)
			assert.Equal(t, tc.explicit, explicit)
		})
	}
}

// A memory resume that asks for less memory than its snapshot had, or for more than the region
// holds, is refused before any resource is taken.
func TestResumeRefusesAnUnfitMemoryRequestBeforeResourceInitialization(t *testing.T) {
	t.Parallel()

	meta := metadata.Template{VcpuCount: 2, MemoryHotplugMB: 3072, MemoryHotplugRequestedMB: 2048}
	for name, tc := range map[string]struct {
		memoryMB int64
		want     error
	}{
		"shrink":            {memoryMB: 2048, want: fc.ErrMemoryHotplugShrink},
		"beyond the region": {memoryMB: 8192, want: fc.ErrMemoryHotplugUnsupported},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := (&Factory{}).ResumeSandbox(
				t.Context(),
				capacityOnlyTemplate{meta: meta},
				NewConfig(Config{Vcpu: 2, RamMB: 1024, MemoryMB: tc.memoryMB, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.17-0.4.1"}}),
				sandboxtypes.RuntimeMetadata{},
				time.Time{},
				time.Time{},
				nil,
			)
			require.ErrorIs(t, err, tc.want)
		})
	}

	_, err := (&Factory{}).ResumeSandbox(
		t.Context(),
		capacityOnlyTemplate{meta: metadata.Template{VcpuCount: 2}},
		NewConfig(Config{Vcpu: 2, RamMB: 1024, MemoryMB: 2048, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.17-0.4.1"}}),
		sandboxtypes.RuntimeMetadata{},
		time.Time{},
		time.Time{},
		nil,
	)
	require.ErrorIs(t, err, fc.ErrMemoryRequestInvalid, "a template without a region cannot grow")
}

// A cold boot refuses a memory request its region cannot hold, before any resource is taken.
func TestColdBootRefusesAnUnfitMemoryRequest(t *testing.T) {
	t.Parallel()

	_, err := coldBootMemoryHotplug(NewConfig(Config{RamMB: 1024, MemoryMB: 8192, MemoryHotplugMB: 3072, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.17-0.4.1"}}), fc.ProcessOptions{})
	require.ErrorIs(t, err, fc.ErrMemoryHotplugUnsupported)
	_, err = coldBootMemoryHotplug(NewConfig(Config{RamMB: 1024, MemoryMB: 2048, MemoryHotplugMB: 3072, FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.17-0.4.1"}}), fc.ProcessOptions{})
	require.NoError(t, err)
}

// A reboot plugs back what its lineage had asked for unless the request names a size.
func TestPrepareColdBootPlugsBackTheRecordedRequest(t *testing.T) {
	t.Parallel()

	ff, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	for name, tc := range map[string]struct{ memoryMB, recorded, want int64 }{
		"plugs back the recorded request": {memoryMB: 0, recorded: 2048, want: 3072},
		"an explicit size wins":           {memoryMB: 2048, recorded: 2048, want: 2048},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tpl := templatemocks.NewMockTemplate(t)
			tpl.EXPECT().Files().Return(storage.CachePaths{Paths: storage.Paths{BuildID: uuid.NewString()}})
			config := NewConfig(Config{
				Vcpu: 2, RamMB: 1024, HugePages: true, MemoryMB: tc.memoryMB,
				Envd:              EnvdMetadata{Version: "0.5.0"},
				FirecrackerConfig: fc.Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.17-0.4.1"},
			})
			meta := metadata.Template{Version: metadata.CurrentVersion, VcpuCount: 2, MemoryHotplugMB: 3072, MemoryHotplugRequestedMB: tc.recorded}

			_, err := (&Factory{featureFlags: ff}).prepareColdBoot(t.Context(), tpl, meta, config, sandboxtypes.RuntimeMetadata{SandboxID: "sbx-" + uuid.NewString()})
			require.NoError(t, err)
			assert.Equal(t, tc.want, config.MemoryMB)
		})
	}
}

func TestMemoryHotplugUnusedBytes(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name                          string
		regionMB, slotMB, requestedMB int64
		wantMB                        int64
	}{
		{"no region", 0, 0, 0, 0},
		{"nothing requested leaves the whole region unmapped", 3072, 0, 0, 3072},
		{"a whole-slot request maps exactly it", 3072, 0, 1024, 2048},
		{"a partial slot is mapped whole", 3072, 0, 1026, 1920},
		{"the whole region maps everything", 3072, 0, 3072, 0},
		{"the device's slot size wins over the default", 3072, 512, 1100, 1536},
		{"a larger slot rounds further", 3072, 1024, 1026, 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantMB<<20, memoryHotplugUnusedBytes(tt.regionMB, tt.slotMB, tt.requestedMB))
		})
	}
}
