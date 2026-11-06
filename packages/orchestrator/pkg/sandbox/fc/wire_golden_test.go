//go:build linux

package fc

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// wireRecorder is a Firecracker API stub that answers every request with 204
// and keeps the body it received, keyed by method and path.
type wireRecorder struct {
	mu      sync.Mutex
	bodies  map[string]string
	answers map[string]stubAnswer
}

// stubAnswer is a canned response for one method and path.
type stubAnswer struct {
	status int
	body   string
}

// refuseMmds makes the stub answer the metadata put with a 400 carrying body instead of 204.
func (r *wireRecorder) refuseMmds(body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers["PUT /mmds"] = stubAnswer{status: http.StatusBadRequest, body: body}
}

func newWireRecorder(t *testing.T) (*apiClient, *wireRecorder) {
	t.Helper()

	dir, err := os.MkdirTemp("", "fc") //nolint:usetesting // t.TempDir embeds the test name and overflows the unix sun_path limit
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := dir + "/fc.sock"
	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	rec := &wireRecorder{bodies: map[string]string{}, answers: map[string]stubAnswer{}}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { //nolint:gosec // no timeouts needed for a stub on a private socket
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.bodies[r.Method+" "+r.URL.Path] = string(body)
		canned, ok := rec.answers[r.Method+" "+r.URL.Path]
		rec.mu.Unlock()
		if ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(canned.status)
			_, _ = w.Write([]byte(canned.body))

			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	return newApiClient(socket), rec
}

func (r *wireRecorder) body(t *testing.T, key string) string {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()
	body, ok := r.bodies[key]
	require.True(t, ok, "no request %q; got %v", key, r.keysLocked())

	return body
}

func (r *wireRecorder) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.keysLocked()
}

func (r *wireRecorder) keysLocked() []string {
	keys := make([]string, 0, len(r.bodies))
	for k := range r.bodies {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}

// stubRootfs serves one path; nothing else of the provider is reached.
type stubRootfs struct {
	rootfs.Provider

	path string
}

func (s stubRootfs) Path() (string, error) { return s.path, nil }

// goldenProcess is a Process on the stub socket with the slot, paths and
// provider Create reads, and no Firecracker behind it.
func goldenProcess(t *testing.T, versions Config, slotIdx int) (*Process, *wireRecorder, *network.Slot) {
	t.Helper()

	client, rec := newWireRecorder(t)
	slot, err := network.NewSlot("golden", slotIdx, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	cacheDir := t.TempDir()
	files := (storage.CachePaths{}).NewSandboxFiles("sbx-golden")
	config := cfg.BuilderConfig{StorageConfig: storage.Config{SandboxCacheDir: cacheDir}}
	require.NoError(t, os.MkdirAll(filepath.Dir(files.SandboxCacheRootfsLinkPath(config.StorageConfig)), 0o755))

	return &Process{
		Versions:       versions,
		client:         client,
		slot:           slot,
		files:          files,
		config:         config,
		rootfsProvider: stubRootfs{path: filepath.Join(cacheDir, "rootfs.ext4")},
		kernelPath:     "/fc-kernels/" + versions.KernelVersion + "/vmlinux.bin",
		rootfsPath:     "/fc-vm/sbx-golden/" + versions.RootfsFileName(),
	}, rec, slot
}

// noRateLimit disables both buckets, so no rate_limiter is sent.
var noRateLimit = RateLimiterConfig{Ops: TokenBucketConfig{BucketSize: -1}, Bandwidth: TokenBucketConfig{BucketSize: -1}}

func machineBody(vcpus, memMB int64, extra string) string {
	// Firecracker refuses SMT on arm64, so machineConfigModel sets it false there.
	return fmt.Sprintf(`{"vcpu_count":%d,"mem_size_mib":%d,"smt":%t,"track_dirty_pages":false%s}`, vcpus, memMB, runtime.GOARCH != archARM64, extra)
}

// The exact requests a Linux sandbox's Create sends to Firecracker, produced
// by the code that decides them, pinned so the fields a guest profile can add
// (machine-config extensions, blk_size, an empty boot_args) stay out of the
// Linux bodies and the devices a profile can skip (entropy, balloon, CPU
// template) stay in: the fleet's Firecracker rejects a body with a field it
// does not know.
func TestLinuxCreateWireBodies(t *testing.T) {
	t.Parallel()

	p, rec, slot := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.14.1_431f1fc"}, 31)
	options := ProcessOptions{InitScriptPath: "/sbin/init", CPUTemplate: &cputemplate.Template{KvmCapabilities: []string{"!121"}}}

	require.NoError(t, p.configureGuestDevices(t.Context(), 2, 512, false, true, false, options, noRateLimit, noRateLimit))

	assert.Equal(t, []string{
		"PUT /balloon", "PUT /boot-source", "PUT /cpu-config", "PUT /drives/rootfs", "PUT /entropy", "PUT /machine-config", "PUT /mmds/config",
		"PUT /network-interfaces/" + slot.VpeerName(),
	}, rec.keys())

	ipv4 := fmt.Sprintf("%s::%s:%s:instance:%s:off:%s", slot.NamespaceIP(), slot.TapIPString(), slot.TapMaskString(), slot.VpeerName(), slot.TapName())
	kernelArgs := buildKernelArgs(ipv4, options).String()
	require.NotEmpty(t, kernelArgs)
	assert.JSONEq(t, fmt.Sprintf(`{"boot_args":%q,"kernel_image_path":"/fc-kernels/vmlinux-6.1/vmlinux.bin"}`, kernelArgs), rec.body(t, "PUT /boot-source"))
	assert.JSONEq(t, `{"drive_id":"rootfs","path_on_host":"/fc-vm/sbx-golden/rootfs.ext4","is_root_device":true}`, rec.body(t, "PUT /drives/rootfs"))
	assert.JSONEq(t, fmt.Sprintf(`{"iface_id":%q,"host_dev_name":%q,"guest_mac":%q}`, slot.VpeerName(), slot.TapName(), slot.TapMAC()), rec.body(t, "PUT /network-interfaces/"+slot.VpeerName()))
	assert.JSONEq(t, fmt.Sprintf(`{"version":"V2","network_interfaces":[%q]}`, slot.VpeerName()), rec.body(t, "PUT /mmds/config"))
	assert.JSONEq(t, machineBody(2, 512, ""), rec.body(t, "PUT /machine-config"))
	assert.JSONEq(t, `{"kvm_capabilities":["!121"]}`, rec.body(t, "PUT /cpu-config"))
	assert.JSONEq(t, `{"amount_mib":0,"deflate_on_oom":false,"free_page_reporting":true}`, rec.body(t, "PUT /balloon"))
	assert.JSONEq(t, `{"rate_limiter":{"bandwidth":{"one_time_burst":0,"refill_time":100,"size":1024}}}`, rec.body(t, "PUT /entropy"))
}

// What a firmware-booted profile's Create sends instead: its extra field on
// the machine, an empty command line for the firmware, a 512-byte-sector disk
// that is not the root device, and no entropy device, balloon or CPU
// template, whatever the options asked for.
func TestFirmwareProfileCreateWireBodies(t *testing.T) {
	t.Parallel()

	p, rec, slot := goldenProcess(t, Config{KernelVersion: "firmware-fd", FirecrackerVersion: "v1.18.0-dev_4bb5b4de0", Guest: firmwareProfile{}}, 32)
	options := ProcessOptions{InitScriptPath: "/sbin/init", CPUTemplate: &cputemplate.Template{KvmCapabilities: []string{"!121"}}}

	require.NoError(t, p.configureGuestDevices(t.Context(), 2, 4096, true, true, true, options, noRateLimit, noRateLimit))

	assert.Equal(t, []string{
		"PUT /boot-source", "PUT /drives/rootfs", "PUT /machine-config", "PUT /mmds/config", "PUT /network-interfaces/" + slot.VpeerName(),
	}, rec.keys())
	assert.JSONEq(t, `{"boot_args":"","kernel_image_path":"/fc-kernels/firmware-fd/vmlinux.bin"}`, rec.body(t, "PUT /boot-source"))
	assert.JSONEq(t, `{"drive_id":"rootfs","path_on_host":"/fc-vm/sbx-golden/rootfs.ext4","is_root_device":false,"blk_size":512}`, rec.body(t, "PUT /drives/rootfs"))
	assert.JSONEq(t, machineBody(2, 4096, `,"huge_pages":"2M","x_extension":true`), rec.body(t, "PUT /machine-config"))
}

// extraProfile is a guest profile whose boot adds the given machine fields.
type extraProfile struct {
	guest.Profile

	extra map[string]json.RawMessage
}

func (p extraProfile) Boot() guest.BootConfig { return guest.BootConfig{MachineConfigExtra: p.extra} }

// A profile's extra machine-configuration field may not override a field the
// orchestrator sets, nor be invalid JSON: the boot fails before the first
// Firecracker request, so the VM is never half-configured.
func TestMachineConfigExtraIsRefusedBeforeTheFirstRequest(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		extra map[string]json.RawMessage
		want  string
		slot  int
	}{
		"colliding key":   {map[string]json.RawMessage{"vcpu_count": json.RawMessage(`8`)}, `"vcpu_count" is already set`, 35},
		"colliding pages": {map[string]json.RawMessage{"huge_pages": json.RawMessage(`"None"`)}, `"huge_pages" is already set`, 36},
		"invalid JSON":    {map[string]json.RawMessage{"x_extension": json.RawMessage(`{`)}, "not valid JSON", 37},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, rec, _ := goldenProcess(t, Config{KernelVersion: "firmware-fd", FirecrackerVersion: "v1.18.0-dev_4bb5b4de0", Guest: extraProfile{Profile: guest.Default(), extra: tc.extra}}, tc.slot)
			p.cmd = &exec.Cmd{}
			err := p.configureGuestDevices(t.Context(), 2, 512, true, false, false, ProcessOptions{InitScriptPath: "/sbin/init"}, noRateLimit, noRateLimit)
			require.ErrorContains(t, err, tc.want)
			assert.Empty(t, rec.keys(), "nothing reached Firecracker")
		})
	}
}

// The restore: a snapshot this runtime wrote loads with no field beyond the
// memory backend.
func TestLinuxResumeWireBody(t *testing.T) {
	t.Parallel()

	ready := make(chan struct{})
	close(ready)

	linux, rec, slot := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.14.1_431f1fc"}, 33)
	ifaceID, override := linux.resumeNetworkOverride()
	assert.Equal(t, slot.VpeerName(), ifaceID)
	assert.Nil(t, override)
	require.NoError(t, linux.client.loadSnapshot(t.Context(), "/tmp/uffd.sock", ready, stubSnapfile("/cache/snapfile"), false, false, linux.Versions.GuestProfile().Restore().ClockRealtime, override, "", "team-1"))
	assert.JSONEq(t, `{"snapshot_path":"/cache/snapfile","mem_backend":{"backend_path":"/tmp/uffd.sock","backend_type":"Uffd"}}`, rec.body(t, "PUT /snapshot/load"))
}

// A firmware-booted profile's restore advances the clock, and a snapshot baked
// with a foreign interface name is reopened on the slot's tap.
func TestFirmwareProfileResumeWireBody(t *testing.T) {
	t.Parallel()

	ready := make(chan struct{})
	close(ready)

	firmware, rec, slot := goldenProcess(t, Config{KernelVersion: "firmware-fd", FirecrackerVersion: "v1.18.0-dev_4bb5b4de0", Guest: firmwareProfile{}, NetIfaceID: "net0"}, 34)
	ifaceID, override := firmware.resumeNetworkOverride()
	assert.Equal(t, "net0", ifaceID)
	require.NoError(t, firmware.client.loadSnapshot(t.Context(), "/tmp/uffd.sock", ready, stubSnapfile("/cache/snapfile"), false, false, firmware.Versions.GuestProfile().Restore().ClockRealtime, override, "", "team-1"))
	assert.JSONEq(t, fmt.Sprintf(`{"snapshot_path":"/cache/snapfile","mem_backend":{"backend_path":"/tmp/uffd.sock","backend_type":"Uffd"},"clock_realtime":true,"network_overrides":[{"iface_id":"net0","host_dev_name":%q}]}`, slot.TapName()), rec.body(t, "PUT /snapshot/load"))

	_, override = (&Process{Versions: Config{}, slot: slot}).resumeNetworkOverride()
	assert.Nil(t, override, "a snapshot this code base wrote needs no override")
}
