//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// TestFirecrackerSupports pins what the wrapper adds over the fcversion
// predicates (whose floors sandbox_features_test.go owns): it reads the
// RUNNING version off the sandbox config, and an unparsable version fails
// CLOSED — a version-gated feature must never engage on a build outside the
// release contract.
func TestFirecrackerSupports(t *testing.T) {
	t.Parallel()

	mkSbx := func(version string) *sandbox.Sandbox {
		return &sandbox.Sandbox{Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				FirecrackerConfig: fc.Config{FirecrackerVersion: version},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sbx"},
		}}
	}

	cases := []struct {
		version string
		inPlace bool
	}{
		{"v1.14-0.2.0", true},
		{"v1.14.1_6ecb627", false}, // legacy build carrying the endpoints: still refused
		{"garbage", false},         // unparsable: fail closed
	}

	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()

			sbx := mkSbx(tc.version)
			assert.Equal(t, tc.inPlace,
				firecrackerSupports(t.Context(), sbx, "in-place checkpoint", (*fcversion.Info).HasInPlaceCheckpoint))
		})
	}
}
