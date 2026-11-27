package sandboxes

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

// TestSandboxCreate_FilesystemOnlyAutoPauseRejectsAutoResume verifies the
// create-time guard: a filesystem-only auto-pause (autoPauseMemory:false)
// cannot be combined with auto-resume, because such a snapshot can only be
// resumed explicitly (traffic auto-resume refuses it).
func TestSandboxCreate_FilesystemOnlyAutoPauseRejectsAutoResume(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	autoPause := true
	autoPauseMemory := false
	timeout := int32(30)
	resp, err := c.PostSandboxesWithResponse(t.Context(), api.NewSandbox{
		TemplateID:      setup.SandboxTemplateID,
		Timeout:         &timeout,
		AutoPause:       &autoPause,
		AutoPauseMemory: &autoPauseMemory,
		AutoResume:      &api.SandboxAutoResumeConfig{Enabled: true},
	}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode(),
		"filesystem-only auto-pause combined with auto-resume should be rejected")
}

// TestSandboxCreate_FilesystemOnlyAutoPauseRequiresAutoPause verifies the
// create-time guard: autoPauseMemory:false only controls a timeout auto-pause,
// so it is rejected without autoPause (it would otherwise be a no-op).
func TestSandboxCreate_FilesystemOnlyAutoPauseRequiresAutoPause(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	autoPause := false
	autoPauseMemory := false
	timeout := int32(30)
	resp, err := c.PostSandboxesWithResponse(t.Context(), api.NewSandbox{
		TemplateID:      setup.SandboxTemplateID,
		Timeout:         &timeout,
		AutoPause:       &autoPause,
		AutoPauseMemory: &autoPauseMemory,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode(),
		"autoPauseMemory:false without autoPause should be rejected")
}

// pauseFilesystemOnly pauses the sandbox as a filesystem-only snapshot
// (memory:false): only the rootfs is persisted, so resuming it cold-boots
// (reboots) the guest instead of restoring memory.
func pauseFilesystemOnly(t *testing.T, c *api.ClientWithResponses, sandboxID string) {
	t.Helper()

	memory := false
	resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sandboxID,
		api.PostSandboxesSandboxIDPauseJSONRequestBody{Memory: &memory}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode(), "filesystem-only pause should succeed")
}

// TestSandboxConnect_FilesystemOnlyResumes verifies that POST /connect resumes a
// paused filesystem-only snapshot by cold-booting (reboot) it — connect is the
// intended way to bring such a sandbox back (in-memory state was already
// discarded at pause time via memory:false). Auto-resume, by contrast, still
// refuses it (see TestSandboxNoAutoResumeFilesystemOnly).
func TestSandboxConnect_FilesystemOnlyResumes(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

	pauseFilesystemOnly(t, c, sbx.SandboxID)

	// Generous timeout so a slow placement under load can't expire the end time
	// before the reboot runs (see TestSandboxResume_FilesystemOnlyReboots).
	connectResp, err := c.PostSandboxesSandboxIDConnectWithResponse(t.Context(), sbx.SandboxID,
		api.PostSandboxesSandboxIDConnectJSONRequestBody{Timeout: 120}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, connectResp.StatusCode(),
		"connecting to a paused filesystem-only snapshot should resume it (cold boot)")
	require.NotNil(t, connectResp.JSON201)
	assert.Equal(t, sbx.SandboxID, connectResp.JSON201.SandboxID)

	// It must be running after the reboot.
	res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200)
	assert.Equal(t, api.Running, res.JSON200.State)
}

// TestSandboxResume_FilesystemOnlyReboots verifies the filesystem-only happy
// path: an explicit resume of a disk-only snapshot is allowed and cold-boots
// (reboots) the guest. The rootfs must survive the reboot (a marker written
// before the pause is still there), while a fresh kernel boot id proves the
// guest was rebooted rather than restored from a memory snapshot.
func TestSandboxResume_FilesystemOnlyReboots(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Boot id before pause, and a marker written to the persisted rootfs.
	bootBefore, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	bootBefore = strings.TrimSpace(bootBefore)
	require.NotEmpty(t, bootBefore)

	const marker = "fs-only-survives-reboot"
	err = utils.ExecCommandAsRoot(t, ctx, sbx, envdClient,
		"/bin/sh", "-c", "echo "+marker+" > /home/user/fs-only-marker.txt")
	require.NoError(t, err)

	// Pause filesystem-only, then resume explicitly (allowed; cold-boots).
	pauseFilesystemOnly(t, c, sbx.SandboxID)

	// Use a generous timeout: a cold boot goes through placement, and under
	// parallel load that can take long enough that a short default timeout would
	// expire the requested end time before RebootSandbox runs (it rejects an
	// already-past end time), making the resume flaky.
	resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: new(int32(120))}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resumeResp.StatusCode(),
		"explicit resume of a filesystem-only snapshot should succeed (cold boot)")

	// The rootfs must survive the reboot.
	got, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/home/user/fs-only-marker.txt")
	require.NoError(t, err)
	assert.Equal(t, marker, strings.TrimSpace(got), "rootfs marker must survive the reboot")

	// A fresh boot id proves a cold boot rather than a memory restore.
	bootAfter, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	assert.NotEqual(t, bootBefore, strings.TrimSpace(bootAfter),
		"boot id should change — a filesystem-only resume cold-boots the guest")

	// The cold boot must restore the template's default user/workdir. A fresh
	// envd starts as root, and only the /init call re-establishes the default;
	// a memory resume hides this by restoring envd's user from RAM, so the
	// reboot path must re-send it explicitly. Run without a user header so envd
	// falls back to its default (would be root//root if /init didn't set it).
	whoami, err := utils.ExecCommandAsDefaultUserWithOutput(t, ctx, sbx, envdClient, "whoami")
	require.NoError(t, err)
	assert.Equal(t, "user", strings.TrimSpace(whoami),
		"default user after a filesystem-only reboot must be the template user, not root")

	pwd, err := utils.ExecCommandAsDefaultUserWithOutput(t, ctx, sbx, envdClient, "pwd")
	require.NoError(t, err)
	assert.Equal(t, "/home/user", strings.TrimSpace(pwd),
		"default workdir after a filesystem-only reboot must be the template user's home")
}

// TestSandboxAutoPause_FilesystemOnly verifies the auto-pause filesystem-only
// path: a sandbox created with autoPauseMemory=false is auto-paused on timeout
// as a filesystem-only snapshot (no memory), so resuming it cold-boots from the
// rootfs. The rootfs marker survives and a fresh kernel boot id proves the
// reboot, just like an explicit filesystem-only pause — but here the snapshot
// kind is driven by the sandbox's auto-pause policy, not a per-request flag.
func TestSandboxAutoPause_FilesystemOnly(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	ctx := t.Context()

	// autoPause + autoPauseMemory:false => a timeout auto-pause is filesystem-only.
	sbx := utils.SetupSandboxWithCleanup(t, c,
		utils.WithAutoPause(true), utils.WithAutoPauseMemory(false))
	envdClient := setup.GetEnvdClient(t, ctx)

	bootBefore, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	bootBefore = strings.TrimSpace(bootBefore)
	require.NotEmpty(t, bootBefore)

	const marker = "auto-pause-fs-only-survives-reboot"
	err = utils.ExecCommandAsRoot(t, ctx, sbx, envdClient,
		"/bin/sh", "-c", "echo "+marker+" > /home/user/auto-pause-marker.txt")
	require.NoError(t, err)

	// Force the end time into the past so the evictor auto-pauses it now. With
	// autoPauseMemory:false the evictor takes a filesystem-only snapshot.
	timeoutResp, err := c.PostSandboxesSandboxIDTimeout(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDTimeoutJSONRequestBody{Timeout: 0}, setup.WithAPIKey())
	require.NoError(t, err)
	require.NoError(t, timeoutResp.Body.Close())

	require.Eventually(t, func() bool {
		res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
		require.NoError(t, err)

		return res.StatusCode() == http.StatusOK && res.JSON200 != nil && res.JSON200.State == api.Paused
	}, 15*time.Second, 50*time.Millisecond, "sandbox was not auto-paused")

	// Explicit resume — a filesystem-only snapshot cold-boots (it cannot be
	// auto-resumed by traffic). Generous timeout: a cold boot goes through
	// placement, which can be slow under parallel load.
	resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: new(int32(120))}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resumeResp.StatusCode(),
		"resuming an auto-paused filesystem-only snapshot should cold-boot it")

	// The rootfs must survive the reboot.
	got, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/home/user/auto-pause-marker.txt")
	require.NoError(t, err)
	assert.Equal(t, marker, strings.TrimSpace(got), "rootfs marker must survive the auto-pause reboot")

	// A fresh boot id proves a cold boot rather than a memory restore.
	bootAfter, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	bootAfter = strings.TrimSpace(bootAfter)
	assert.NotEqual(t, bootBefore, bootAfter,
		"boot id should change — a filesystem-only auto-pause cold-boots on resume")

	// The auto-pause policy must survive the pause/resume cycle: force a second
	// timeout and confirm the sandbox auto-pauses filesystem-only *again* (another
	// cold boot), proving the policy was persisted on the snapshot and restored on
	// resume rather than reverting to a memory snapshot.
	timeoutResp, err = c.PostSandboxesSandboxIDTimeout(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDTimeoutJSONRequestBody{Timeout: 0}, setup.WithAPIKey())
	require.NoError(t, err)
	require.NoError(t, timeoutResp.Body.Close())

	require.Eventually(t, func() bool {
		res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
		require.NoError(t, err)

		return res.StatusCode() == http.StatusOK && res.JSON200 != nil && res.JSON200.State == api.Paused
	}, 15*time.Second, 50*time.Millisecond, "sandbox was not auto-paused on the second cycle")

	resumeResp, err = c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: new(int32(120))}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resumeResp.StatusCode(),
		"second resume of the auto-paused filesystem-only snapshot should cold-boot it")

	bootAfterSecond, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	assert.NotEqual(t, bootAfter, strings.TrimSpace(bootAfterSecond),
		"boot id should change again — the filesystem-only auto-pause policy must persist across resume")
}
