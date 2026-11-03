package featureflags

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

//nolint:paralleltest,tparallel // t.Setenv
func TestEnvBoolOr(t *testing.T) {
	const key = "ENVD_BINARY_CACHE_TEST_ONLY"

	for _, tc := range []struct {
		name     string
		set      bool
		value    string
		fallback bool
		want     bool
	}{
		{name: "unset keeps the fallback", fallback: true, want: true},
		{name: "unset keeps a false fallback", fallback: false, want: false},
		{name: "empty keeps the fallback", set: true, value: "", fallback: true, want: true},
		{name: "true overrides a false fallback", set: true, value: "true", fallback: false, want: true},
		{name: "false overrides a true fallback", set: true, value: "false", fallback: true, want: false},
		{name: "1 is true", set: true, value: "1", fallback: false, want: true},
		{name: "0 is false", set: true, value: "0", fallback: true, want: false},
		// An unparseable value must not silently read as false: that would turn a
		// typo into a silent disable of whatever it gates.
		{name: "garbage keeps the fallback", set: true, value: "yes-please", fallback: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.value)
			}

			require.Equal(t, tc.want, envBoolOr(key, tc.fallback))
		})
	}
}

func TestGuestResetFlagsFallbackOffWithoutTheirVariable(t *testing.T) {
	t.Parallel()

	for key, flag := range map[string]BoolFlag{
		"GUEST_REBOOT_IN_PLACE": GuestRebootInPlaceFlag,
		"GUEST_POWEROFF_END":    GuestPowerOffEndFlag,
		"SANDBOX_ENDED_ON_NODE": SandboxEndedOnNodeFlag,
	} {
		if _, set := os.LookupEnv(key); set {
			continue
		}
		require.False(t, flag.Fallback(), key)
	}
}
