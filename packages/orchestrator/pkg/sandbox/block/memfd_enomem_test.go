//go:build linux

package block

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A memfd larger than the address space cannot be mapped, so the mmap fails
// with ENOMEM the way the hugepage pool refuses it; NewFromFd marks that with
// GuestMemoryError and keeps the errno's text.
func TestNewFromFdMarksENOMEM(t *testing.T) {
	t.Parallel()

	fd, err := unix.MemfdCreate("enomem", 0)
	require.NoError(t, err)
	require.NoError(t, unix.Ftruncate(fd, 1<<50))

	_, err = NewFromFd(fd)
	require.ErrorIs(t, err, unix.ENOMEM)
	_, ok := errors.AsType[GuestMemoryError](err)
	assert.True(t, ok, "the mmap ENOMEM is the pool's answer")
	assert.Equal(t, "mmap memfd: cannot allocate memory", err.Error())
}
