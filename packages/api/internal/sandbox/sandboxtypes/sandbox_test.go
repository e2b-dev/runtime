package sandboxtypes

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// TestToNodeSandbox_CopiesEveryField guards a conversion that fails silently:
// a field left behind does not break the build, it just makes the kill path
// act on a zero value — an empty execution ID stops evicting the routing
// entry, a zero VCpu stops correcting the node's resource accounting.
//
// The zero-value sweep is the part that keeps working as NodeSandbox grows: a
// new field fails here until it is both set below and copied in ToNodeSandbox.
func TestToNodeSandbox_CopiesEveryField(t *testing.T) {
	t.Parallel()

	sbx := Sandbox{
		SandboxID:   "sbx-1",
		ExecutionID: "exec-1",
		TeamID:      uuid.New(),
		NodeID:      "node-1",
		ClusterID:   uuid.New(),
		StartTime:   time.Now(),
		VCpu:        2,
		RamMB:       512,

		FirecrackerVersion: "v1.12.1_210cbac",
	}

	got := sbx.ToNodeSandbox()

	assert.Equal(t, sbx.SandboxID, got.SandboxID)
	assert.Equal(t, sbx.ExecutionID, got.ExecutionID)
	assert.Equal(t, sbx.TeamID, got.TeamID)
	assert.Equal(t, sbx.NodeID, got.NodeID)
	assert.Equal(t, sbx.ClusterID, got.ClusterID)
	assert.Equal(t, sbx.StartTime, got.StartTime)
	assert.Equal(t, sbx.VCpu, got.VCpu)
	assert.Equal(t, sbx.RamMB, got.RamMB)
	assert.True(t, got.HugePages)

	// Reported by the node only; a stored sandbox has no counterpart.
	nodeOnly := map[string]bool{"Ended": true, "GuestEvent": true, "DiskKept": true}

	value := reflect.ValueOf(got)
	for i := range value.NumField() {
		if nodeOnly[value.Type().Field(i).Name] {
			continue
		}

		assert.Falsef(t, value.Field(i).IsZero(),
			"NodeSandbox.%s was not copied from the stored sandbox", value.Type().Field(i).Name)
	}
}

func TestToNodeSandbox_HugePagesFollowsTheFirecrackerVersion(t *testing.T) {
	t.Parallel()

	assert.True(t, Sandbox{FirecrackerVersion: "v1.7.0_abcdef0"}.ToNodeSandbox().HugePages)
	assert.False(t, Sandbox{FirecrackerVersion: "v1.5.0_abcdef0"}.ToNodeSandbox().HugePages)
	assert.False(t, Sandbox{FirecrackerVersion: "not-a-version"}.ToNodeSandbox().HugePages)
}
