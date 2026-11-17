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

func TestMemorySizing(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "absent", MemorySizing(1024, 0))
	assert.Equal(t, "equal", MemorySizing(1024, 1024))
	assert.Equal(t, "resizable", MemorySizing(1024, 4096))
}

func TestMemoryHotplugMB(t *testing.T) {
	t.Parallel()

	assert.Zero(t, TemplateConfig{MemoryMB: 1024}.MemoryHotplugMB(), "absent maximum")
	assert.Zero(t, TemplateConfig{MemoryMB: 1024, MaxMemoryMB: 1024}.MemoryHotplugMB(), "maximum equal to boot RAM")
	assert.Equal(t, int64(3072), TemplateConfig{MemoryMB: 1024, MaxMemoryMB: 4096}.MemoryHotplugMB())
}
