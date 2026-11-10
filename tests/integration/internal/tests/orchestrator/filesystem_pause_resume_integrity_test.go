package orchestrator

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

type fsWritePattern string

const (
	fsWriteContiguous fsWritePattern = "contiguous"
	fsWriteScattered  fsWritePattern = "scattered"
)

// The default lifetimes (30s on create, 15s on resume) are shorter than the write and hash take on a loaded runner.
const fsIntegritySandboxTimeout int32 = 120

func TestSandboxFilesystemPauseResumeIntegrity(t *testing.T) {
	t.Parallel()

	t.Run("contiguous write hash survives pause", func(t *testing.T) {
		t.Parallel()
		runFilesystemPauseResumeIntegrityCase(t, filesystemPauseResumeCase{
			name:       "contiguous",
			filePath:   "/home/user/fs-integrity-contiguous.bin",
			writeGiB:   0,
			writeBytes: 64 * 1024 * 1024,
			cycles:     1,
			pattern:    fsWriteContiguous,
		})
	})

	t.Run("scattered write hash survives pause", func(t *testing.T) {
		t.Parallel()
		runFilesystemPauseResumeIntegrityCase(t, filesystemPauseResumeCase{
			name:       "scattered",
			filePath:   "/home/user/fs-integrity-scattered.bin",
			writeGiB:   0,
			writeBytes: 32 * 1024 * 1024,
			cycles:     1,
			pattern:    fsWriteScattered,
		})
	})

	t.Run("zeroed ranges and truncate survive pause", func(t *testing.T) {
		t.Parallel()
		runFilesystemPauseResumeTruncateCase(t)
	})
}

func TestSandboxFilesystemPauseResumeIntegrityStress(t *testing.T) {
	t.Parallel()

	if os.Getenv("TESTS_FS_INTEGRITY_STRESS") != "1" {
		t.Skip("set TESTS_FS_INTEGRITY_STRESS=1 to run the large pause/resume filesystem stress test")
	}

	writeGiB := getenvInt(t, "TESTS_FS_INTEGRITY_GIB", 2)
	cycles := getenvInt(t, "TESTS_FS_INTEGRITY_CYCLES", 10)

	t.Run("contiguous", func(t *testing.T) {
		t.Parallel()
		runFilesystemPauseResumeIntegrityCase(t, filesystemPauseResumeCase{
			name:       fmt.Sprintf("stress-contiguous-%dGiB", writeGiB),
			filePath:   fmt.Sprintf("/home/user/fs-integrity-stress-contiguous-%dGiB.bin", writeGiB),
			writeGiB:   writeGiB,
			writeBytes: int64(writeGiB) * 1024 * 1024 * 1024,
			cycles:     cycles,
			pattern:    fsWriteContiguous,
		})
	})

	t.Run("scattered", func(t *testing.T) {
		t.Parallel()
		runFilesystemPauseResumeIntegrityCase(t, filesystemPauseResumeCase{
			name:       fmt.Sprintf("stress-scattered-%dGiB", writeGiB),
			filePath:   fmt.Sprintf("/home/user/fs-integrity-stress-scattered-%dGiB.bin", writeGiB),
			writeGiB:   writeGiB,
			writeBytes: int64(writeGiB) * 1024 * 1024 * 1024,
			cycles:     cycles,
			pattern:    fsWriteScattered,
		})
	})
}

type filesystemPauseResumeCase struct {
	name       string
	filePath   string
	writeGiB   int
	writeBytes int64
	cycles     int
	pattern    fsWritePattern
}

func runFilesystemPauseResumeIntegrityCase(t *testing.T, tc filesystemPauseResumeCase) {
	t.Helper()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(fsIntegritySandboxTimeout))
	sbxID := sbx.SandboxID
	envdClient := setup.GetEnvdClient(t, t.Context())

	exec := func(script string) string {
		t.Helper()
		out, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-lc", script)
		require.NoError(t, err)

		return strings.TrimSpace(out)
	}
	pause := func() {
		t.Helper()
		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())
	}
	resume := func() {
		t.Helper()
		resp, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: new(fsIntegritySandboxTimeout)}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
	}

	exec(buildWriteScript(tc.filePath, tc.writeBytes, tc.pattern))
	expectedHash := exec(`sha256sum "` + tc.filePath + `" | awk '{print $1}'`)
	expectedSize := exec(`stat -c %s "` + tc.filePath + `"`)

	for i := range tc.cycles {
		pause()
		resume()

		assert.Equal(t, expectedHash, exec(`sha256sum "`+tc.filePath+`" | awk '{print $1}'`), "hash mismatch after cycle %d", i+1)
		assert.Equal(t, expectedSize, exec(`stat -c %s "`+tc.filePath+`"`), "size mismatch after cycle %d", i+1)
	}
}

func runFilesystemPauseResumeTruncateCase(t *testing.T) {
	t.Helper()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(fsIntegritySandboxTimeout))
	sbxID := sbx.SandboxID
	envdClient := setup.GetEnvdClient(t, t.Context())

	exec := func(script string) string {
		t.Helper()
		out, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-lc", script)
		require.NoError(t, err)

		return strings.TrimSpace(out)
	}
	pause := func() {
		t.Helper()
		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())
	}
	resume := func() {
		t.Helper()
		resp, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{Timeout: new(fsIntegritySandboxTimeout)}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resp.StatusCode())
	}

	const filePath = "/home/user/fs-integrity-zero-truncate.bin"

	exec(fmt.Sprintf(`
python3 - <<'PY'
import os
path = %q
chunk = 1024 * 1024
with open(path, "wb", buffering=0) as f:
    f.write(b"\x07" * (16 * chunk))
    f.flush()
    os.fsync(f.fileno())
with open(path, "r+b", buffering=0) as f:
    zero = b"\x00" * chunk
    for off in (2 * chunk, 6 * chunk, 10 * chunk):
        f.seek(off)
        f.write(zero)
    f.truncate(12 * chunk)
    f.flush()
    os.fsync(f.fileno())
PY`, filePath))
	expectedHash := exec(`sha256sum "` + filePath + `" | awk '{print $1}'`)
	expectedSize := exec(`stat -c %s "` + filePath + `"`)

	cycles := getenvInt(t, "TESTS_FS_INTEGRITY_TRUNCATE_CYCLES", 1)

	for i := range cycles {
		pause()
		resume()

		assert.Equal(t, expectedHash, exec(`sha256sum "`+filePath+`" | awk '{print $1}'`), "hash mismatch after cycle %d", i+1)
		assert.Equal(t, expectedSize, exec(`stat -c %s "`+filePath+`"`), "size mismatch after cycle %d", i+1)
	}
}

func buildWriteScript(filePath string, writeBytes int64, pattern fsWritePattern) string {
	strideBytes := 8 * 1024 * 1024

	switch pattern {
	case fsWriteContiguous:
		return fmt.Sprintf(`
python3 - <<'PY'
import os
path = %q
remaining = %d
chunk = 1024 * 1024
buf = b"\x01" * chunk
with open(path, "wb", buffering=0) as f:
    while remaining > 0:
        n = min(chunk, remaining)
        f.write(buf[:n])
        remaining -= n
    f.flush()
    os.fsync(f.fileno())
PY`, filePath, writeBytes)
	case fsWriteScattered:
		return fmt.Sprintf(`
python3 - <<'PY'
import os
path = %q
remaining = %d
chunk = 1024 * 1024
stride = %d
buf = b"\x01" * chunk
offset = 0
with open(path, "wb", buffering=0) as f:
    while remaining > 0:
        n = min(chunk, remaining)
        f.seek(offset)
        f.write(buf[:n])
        remaining -= n
        offset += stride
    f.flush()
    os.fsync(f.fileno())
PY`, filePath, writeBytes, strideBytes)
	default:
		panic(fmt.Sprintf("unsupported pattern: %s", pattern))
	}
}

func getenvInt(t *testing.T, key string, fallback int) int {
	t.Helper()

	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	require.NoErrorf(t, err, "invalid integer value for %s", key)

	return parsed
}
