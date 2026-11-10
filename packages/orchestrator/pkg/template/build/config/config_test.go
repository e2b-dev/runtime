package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVcpuSizing(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "absent", VcpuSizing(2, 0))
	assert.Equal(t, "equal", VcpuSizing(2, 2))
	assert.Equal(t, "resizable", VcpuSizing(2, 8))
}
