package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Not parallel: overrides the package-level sandboxHttpClient, as the sibling init tests do.
func TestInitEnvdReportsTheFailedVolume(t *testing.T) { //nolint:paralleltest
	orig := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: 5 * time.Second}
	t.Cleanup(func() { sandboxHttpClient = orig })

	initWith := func(t *testing.T, header string) error {
		t.Helper()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if header != "" {
				w.Header().Set(envdFailedVolumeHeader, header)
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("failed to setup NFS volumes"))
		}))
		t.Cleanup(srv.Close)

		s, _ := newMemoryTestSandbox(t, srv.URL)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		return s.initEnvd(ctx, StartTypeResume, false, envdInitOptions{})
	}

	t.Run("names the volume", func(t *testing.T) { //nolint:paralleltest
		err := initWith(t, "/mnt/v7")

		var volumeErr VolumeMountError
		require.ErrorAs(t, err, &volumeErr)
		assert.Equal(t, "/mnt/v7", volumeErr.Path)
		assert.Equal(t, `failed to mount volume at "/mnt/v7"`, volumeErr.Error())
	})

	t.Run("other failures are not volume errors", func(t *testing.T) { //nolint:paralleltest
		err := initWith(t, "")

		require.ErrorContains(t, err, "unexpected status code: 400")
		var volumeErr VolumeMountError
		assert.NotErrorAs(t, err, &volumeErr)
	})
}

func TestEnvdWaitForVolumes(t *testing.T) {
	t.Parallel()

	noDeadline := t.Context()
	assert.Equal(t, 10*time.Second, envdWaitForVolumes(noDeadline, 10*time.Second, 0), "no volumes keep the plain wait")
	assert.Equal(t, 40*time.Second+200*time.Millisecond, envdWaitForVolumes(noDeadline, 10*time.Second, 2))
	assert.Equal(t, 70*time.Second, envdWaitForVolumes(noDeadline, 10*time.Second, 300))

	// The volume allowance ends before the request's deadline...
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	assert.InDelta(t, float64(58*time.Second), float64(envdWaitForVolumes(ctx, 10*time.Second, 300)), float64(time.Second))
	assert.Equal(t, 40*time.Second+200*time.Millisecond, envdWaitForVolumes(ctx, 10*time.Second, 2), "a wait that fits is unchanged")

	// ...but never cuts the plain envd wait.
	short, cancelShort := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelShort()
	assert.Equal(t, 10*time.Second, envdWaitForVolumes(short, 10*time.Second, 300))
}
