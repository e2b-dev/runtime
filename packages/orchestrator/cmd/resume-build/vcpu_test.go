package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRunVcpu(t *testing.T) {
	t.Parallel()

	_, err := defaultRunVcpu(0, false)
	require.ErrorContains(t, err, "snapshot size unrecorded; pass -vcpu")

	for name, tc := range map[string]struct {
		recorded int64
		coldBoot bool
		want     int64
	}{
		"recorded memory resume": {recorded: 4, want: 4},
		"recorded cold boot":     {recorded: 4, coldBoot: true, want: 4},
		"unrecorded cold boot":   {coldBoot: true, want: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := defaultRunVcpu(tc.recorded, tc.coldBoot)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
