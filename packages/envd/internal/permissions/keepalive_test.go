package permissions

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestParseKeepAliveInterval(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		value string
		want  time.Duration
	}{
		"missing":  {value: "", want: 90 * time.Second},
		"invalid":  {value: "not-a-number", want: 90 * time.Second},
		"zero":     {value: "0", want: 90 * time.Second},
		"negative": {value: "-1", want: 90 * time.Second},
		"overflow": {value: "9223372037", want: 90 * time.Second},
		"one":      {value: "1", want: time.Second},
		"default":  {value: "90", want: 90 * time.Second},
		"maximum":  {value: "9223372036", want: 9_223_372_036 * time.Second},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, parseKeepAliveInterval(tt.value))
		})
	}
}

func TestGetKeepAliveTicker_NonPositiveFallsBack(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"0", "-1"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			req := connect.NewRequest(&emptypb.Empty{})
			req.Header().Set("Keepalive-Ping-Interval", value)

			require.NotPanics(t, func() {
				ticker, reset := GetKeepAliveTicker(req)
				reset()
				ticker.Stop()
			})
		})
	}
}
