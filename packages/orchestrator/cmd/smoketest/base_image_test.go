//go:build linux

package smoketest

import (
	"context"
	_ "embed"
	"io"
	"log"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases/base/distro"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

//go:embed testdata/ubuntu.Dockerfile
var smokeDockerfile string

func prepareSmokeImage(t *testing.T, ctx context.Context) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, smokeBuildTimeout)
	defer cancel()
	i := slices.IndexFunc(distro.Profiles, func(p distro.Profile) bool { return slices.Contains(p.IDs, "ubuntu") })
	require.NotEqual(t, -1, i, "Ubuntu provisioning profile is required")

	server := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0)),
		registry.WithBlobHandler(registry.NewDiskBlobHandler(t.TempDir()))))
	t.Cleanup(server.Close)
	ref, err := name.NewTag(server.Listener.Addr().String() + "/smoke/ubuntu:22.04")
	require.NoError(t, err)

	t.Log("Preparing Ubuntu provisioning packages for all Firecracker versions")
	cmd := exec.CommandContext(ctx, "docker", "build", "--load", "--tag", ref.Name(),
		"--platform", "linux/"+utils.TargetArch(), "--build-arg", "PACKAGES="+strings.Join(distro.Profiles[i].Packages, " "), "-")
	cmd.Stdin = strings.NewReader(smokeDockerfile)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "preparing smoke image:\n%s", out)

	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		out, err := exec.CommandContext(cleanupCtx, "docker", "image", "rm", ref.Name()).CombinedOutput()
		assert.NoError(t, err, "removing smoke image tag:\n%s", out)
	}()
	// The registry is on the test's loopback, which a remote Docker daemon cannot reach.
	archive := filepath.Join(t.TempDir(), "ubuntu.tar")
	out, err = exec.CommandContext(ctx, "docker", "image", "save", "--output", archive, ref.Name()).CombinedOutput()
	require.NoError(t, err, "saving smoke image:\n%s", out)
	image, err := tarball.ImageFromPath(archive, &ref)
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, image, remote.WithContext(ctx)), "pushing smoke image to test registry")

	return ref.Name()
}
