package orchestrator

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxMemoryIntegrity(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	// Resuming without an explicit timeout gives the sandbox only the 15s
	// default; hashing the tmpfs after resume can take longer than that under
	// load, so the sandbox would be evicted mid-check.
	resumeTimeout := int32(300)

	// Build a template with stress-ng and time pre-installed so subtests
	// skip the page-fault-heavy apt-get phase that saturates decompression
	// under parallel load.
	tmpl := utils.BuildTemplate(t, utils.TemplateBuildOptions{
		Name: "memory-integrity-deps",
		BuildData: api.TemplateBuildStartV2{
			FromTemplate: &setup.SandboxTemplateID,
			Steps: &[]api.TemplateStep{{
				Type: "RUN",
				Args: &[]string{"apt-get update && apt-get install -y stress-ng time", "root"},
			}},
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	t.Run("tmpfs hash", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(300), utils.WithTemplateID(tmpl.TemplateID))
		sbxId := sbx.SandboxID

		envdClient := setup.GetEnvdClient(t, t.Context())

		tmpfsFile := "/mnt/testfile"

		percentageOfFreeMemoryToUse := 60
		// Create a tmpfs with up to 60% of the remaining free RAM and fill it with random data
		// Disable swap to ensure we're testing pure RAM-based storage
		// Always use at least 64MB, but not more than the configured share of free memory.
		memCmd := fmt.Sprintf(`
TOTAL_MEM_MB=$(free -m | awk '/^Mem:/ {print $2}')
USED_MEM_MB=$(free -m | awk '/^Mem:/ {print $3}')
FREE_MEM_MB=$(free -m | awk '/^Mem:/ {print $7}')
# Calculate %d%% of free memory, rounding down
MEM_MB=$(( (FREE_MEM_MB * %d) / 100 ))
if [ "$MEM_MB" -lt 64 ]; then MEM_MB=64; fi
echo "Total memory: ${TOTAL_MEM_MB} MB"
echo "Used memory before tmpfs mount: ${USED_MEM_MB} MB"
echo "Free memory before tmpfs mount: ${FREE_MEM_MB} MB"
echo "Memory to use in integrity test (%d%% of free, min 64MB): ${MEM_MB} MB"
mount -t tmpfs -o size=${MEM_MB}M tmpfs /mnt
/usr/bin/time -v dd if=/dev/urandom of="%s" bs=1M count=${MEM_MB}
USED_MEM_MB_AFTER=$(free -m | awk '/^Mem:/ {print $3}')
echo "Used memory after tmpfs mount and file fill: ${USED_MEM_MB_AFTER} MB"
`, percentageOfFreeMemoryToUse, percentageOfFreeMemoryToUse, percentageOfFreeMemoryToUse, tmpfsFile)

		_, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", memCmd)
		require.NoError(t, err)

		hashCmd := fmt.Sprintf(`sha256sum "%s" | awk '{print $1}'`, tmpfsFile)
		readHash := func() string {
			t.Helper()

			var output string
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				var err error
				output, err = utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", hashCmd)
				require.NoError(c, err)
			}, 3*time.Minute, 2*time.Second)

			return strings.TrimSpace(output)
		}

		hashCmdOutput := readHash()
		require.NotEmpty(t, hashCmdOutput, "Failed to extract hash from command output")

		pauseIterations := 2

		for i := range pauseIterations {
			resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, resp.StatusCode())

			res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode())
			require.NotNil(t, res.JSON200)
			assert.Equal(t, api.Paused, res.JSON200.State)

			sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: &resumeTimeout}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, sbxResume.StatusCode())
			require.NotNil(t, sbxResume.JSON201)
			assert.Equal(t, sbxResume.JSON201.SandboxID, sbxId)

			// Check the tmpfs hash and compare it with the original hash
			hashCmdOutputAfterResume := readHash()

			assert.Equal(t, hashCmdOutput, hashCmdOutputAfterResume, "Hash mismatch on iteration %d: memory integrity check failed", i)
		}
	})

	t.Run("write after read survives pause", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(300))
		sbxId := sbx.SandboxID
		envdClient := setup.GetEnvdClient(t, t.Context())

		exec := func(cmd string) string {
			t.Helper()
			out, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", cmd)
			require.NoError(t, err)

			return strings.TrimSpace(out)
		}
		pause := func() {
			t.Helper()
			resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, resp.StatusCode())
		}
		resume := func() {
			t.Helper()
			r, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: &resumeTimeout}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, r.StatusCode())
		}

		exec(`echo -n A > /tmp/v`)
		pause()
		resume()
		exec(`cat /tmp/v > /dev/null && echo -n B > /tmp/v`)
		pause()
		resume()
		assert.Equal(t, "B", exec(`cat /tmp/v`))
	})

	t.Run("stress-ng verify", func(t *testing.T) {
		t.Parallel()

		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(300), utils.WithTemplateID(tmpl.TemplateID))

		envdClient := setup.GetEnvdClient(t, t.Context())

		// get a bounded share of free memory and use it as the vm-bytes
		percentageOfFreeMemoryToUse := 60

		getFreeMemoryCmd := `free -m | awk '/^Mem:/ {print $7}'`
		freeMemoryStr, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", getFreeMemoryCmd)
		require.NoError(t, err)

		freeMemoryStr = strings.TrimSpace(freeMemoryStr)
		freeMemoryMB, err := strconv.ParseInt(freeMemoryStr, 10, 64)
		require.NoError(t, err, "Failed to parse free memory value: %s", freeMemoryStr)

		vmBytes := fmt.Sprintf("%dM", freeMemoryMB*int64(percentageOfFreeMemoryToUse)/100)

		// Run stress-ng verify
		verifyCmd := fmt.Sprintf(`/usr/bin/time -v stress-ng --vm 1 --vm-bytes %s --verify -v -t 5s`, vmBytes)
		verifyOutput, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", verifyCmd)
		require.NoError(t, err)

		// Verify stress-ng completed successfully
		assert.Contains(t, verifyOutput, "successful run completed", "stress-ng did not complete successfully")
		assert.Contains(t, verifyOutput, "metrics-check: all stressor metrics validated and sane", "stress-ng metrics check failed")
	})
}
