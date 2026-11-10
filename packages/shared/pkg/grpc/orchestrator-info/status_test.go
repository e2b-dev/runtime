package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceInfoStatusCanAcceptNewRequests(t *testing.T) {
	t.Parallel()

	cases := map[ServiceInfoStatus]bool{
		ServiceInfoStatus_Healthy:      true,
		ServiceInfoStatus_Draining:     false,
		ServiceInfoStatus_Standby:      false,
		ServiceInfoStatus_Unhealthy:    false,
		ServiceInfoStatus_ShuttingDown: false,
		ServiceInfoStatus_NotReady:     false,
		ServiceInfoStatus(9999):        false,
	}

	for status, expected := range cases {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, expected, status.CanAcceptNewRequests())
		})
	}

	// Guard the enum itself: every status the proto declares must be covered
	// above, so adding one to info.proto fails here instead of silently
	// inheriting the unroutable default.
	t.Run("covers every declared status", func(t *testing.T) {
		t.Parallel()

		for value, name := range ServiceInfoStatus_name {
			_, ok := cases[ServiceInfoStatus(value)]
			assert.Truef(t, ok, "ServiceInfoStatus %s is not covered by this test", name)
		}
	})
}
