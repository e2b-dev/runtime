package fc

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidMachineVcpus(t *testing.T) {
	t.Parallel()

	for _, n := range []int64{1, 2, 4, MaxVcpus} {
		assert.True(t, ValidMachineVcpus(n), "VM size %d", n)
	}
	for _, n := range []int64{0, -1, MaxVcpus + 1} {
		assert.False(t, ValidMachineVcpus(n), "VM size %d", n)
	}
	assert.Equal(t, runtime.GOARCH == "arm64", ValidMachineVcpus(3))
}
