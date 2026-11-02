package process

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDetermineTimeoutFromHeader covers the Connect-Timeout-Ms header parsing:
// absent or zero means no timeout, and values that do not fit in an int64 of
// nanoseconds must be rejected instead of wrapping to a wrong timeout.
func TestDetermineTimeoutFromHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		header  string
		want    time.Duration
		wantErr bool
	}{
		{name: "absent header means no timeout", header: "", want: 0},
		{name: "zero means no timeout", header: "0", want: 0},
		{name: "parses milliseconds", header: "1500", want: 1500 * time.Millisecond},
		{name: "largest non-overflowing value is accepted", header: "9223372036854", want: 9223372036854 * time.Millisecond},
		{name: "non-numeric value is rejected", header: "1s", wantErr: true},
		{name: "negative value is rejected", header: "-1", wantErr: true},
		{name: "value overflowing int64 nanoseconds wraps negative and must be rejected", header: "9223372036854775807", wantErr: true},
		{name: "value wrapping to a short timeout must be rejected", header: "288230376151711745", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			header := http.Header{}
			if tc.header != "" {
				header.Set("Connect-Timeout-Ms", tc.header)
			}

			got, err := determineTimeoutFromHeader(header)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
