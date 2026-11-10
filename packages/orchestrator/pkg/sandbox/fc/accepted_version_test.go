//go:build linux

package fc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

var errNotTheVersionLine = errors.New("not the version line")

// versionLineProfile accepts only one Firecracker version.
type versionLineProfile struct{ guest.Profile }

func (versionLineProfile) Name() string  { return "version-line" }
func (versionLineProfile) Label() string { return "version-line" }
func (versionLineProfile) Firecracker() guest.FirecrackerSelection {
	return guest.FirecrackerSelection{AcceptVersion: func(version string) error {
		if version != "v1.17-100.0.1" {
			return errNotTheVersionLine
		}

		return nil
	}}
}

func TestRequireAcceptedVersion(t *testing.T) {
	t.Parallel()

	versions := Config{FirecrackerVersion: "v1.17-0.2.0", Guest: versionLineProfile{guest.Default()}}
	require.ErrorIs(t, requireAcceptedVersion(versions), errNotTheVersionLine)

	versions.FirecrackerVersion = "v1.17-100.0.1"
	require.NoError(t, requireAcceptedVersion(versions))

	require.NoError(t, requireAcceptedVersion(Config{FirecrackerVersion: "v0"}), "the default profile accepts every version")
}
