package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetentionDaysDefaultsAnUnsetTTLAndCapsALongOne(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		teamTTLDays int64
		want        int64
	}{
		"an unset TTL keeps events for the default":   {teamTTLDays: 0, want: DefaultEventsTTLDays},
		"a negative TTL keeps events for the default": {teamTTLDays: -1, want: DefaultEventsTTLDays},
		"a TTL within the cap is kept":                {teamTTLDays: 30, want: 30},
		"a TTL above the cap is capped":               {teamTTLDays: 1000, want: MaxEventsTTLDays},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, RetentionDays(tc.teamTTLDays))
		})
	}
}
