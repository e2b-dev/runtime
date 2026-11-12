//go:build linux

package server

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func TestCreateVolumeMountModelsResolvesNconnect(t *testing.T) {
	t.Parallel()

	volumeID := uuid.NewString()
	volumes := func(nconnects ...uint32) []*orchestrator.SandboxVolumeMount {
		out := make([]*orchestrator.SandboxVolumeMount, 0, len(nconnects))
		for _, n := range nconnects {
			out = append(out, &orchestrator.SandboxVolumeMount{Id: volumeID, Name: "data", Path: "/mnt/data", Type: "test", Nconnect: n})
		}

		return out
	}

	// A mount that requests none takes the node's default; one that asks
	// overrides it, and a default of 0 mounts without the option.
	mounts, err := createVolumeMountModelsFromAPI(volumes(0, 4), 1)
	require.NoError(t, err)
	require.Len(t, mounts, 2)
	assert.Equal(t, 1, mounts[0].Nconnect, "an unspecified nconnect takes the node's default")
	assert.Equal(t, 4, mounts[1].Nconnect, "an explicit nconnect overrides the default")

	mounts, err = createVolumeMountModelsFromAPI(volumes(0), 0)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	assert.Equal(t, 0, mounts[0].Nconnect, "a zero default mounts without the option")

	// An out-of-range value is refused however it was resolved: the API
	// validates what a client sends, this guards the gRPC entry.
	for _, tc := range []struct {
		requested, fallback uint32
	}{
		{requested: 17, fallback: 1},
		{requested: 0, fallback: 17},
	} {
		_, err := createVolumeMountModelsFromAPI(volumes(tc.requested), int(tc.fallback))
		require.ErrorContains(t, err, "invalid nconnect")
	}
}
