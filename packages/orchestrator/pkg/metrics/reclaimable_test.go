//go:build linux

package metrics

import "testing"

// reclaimableBytes is the estimate the density-observability gauge exports; it
// must never over-report (it feeds a later reclaim decision), so these lock its
// floor-at-zero and free-page-reporting behavior.
func TestReclaimableBytes(t *testing.T) {
	t.Parallel()

	const mib = int64(1) << 20

	tests := []struct {
		name              string
		faulted           int64
		guestInUse        int64
		freePageReporting bool
		want              int64
	}{
		{
			name:       "faulted above in-use yields the gap",
			faulted:    2048 * mib,
			guestInUse: 512 * mib,
			want:       1536 * mib,
		},
		{
			name:       "in-use above faulted floors at zero (never negative)",
			faulted:    256 * mib,
			guestInUse: 512 * mib,
			want:       0,
		},
		{
			name:       "equal footprint and use yields zero",
			faulted:    512 * mib,
			guestInUse: 512 * mib,
			want:       0,
		},
		{
			name:       "no guest report yields the whole footprint as upper bound",
			faulted:    1024 * mib,
			guestInUse: 0,
			want:       1024 * mib,
		},
		{
			name:              "free-page-reporting active reports zero (pool self-reclaims)",
			faulted:           2048 * mib,
			guestInUse:        128 * mib,
			freePageReporting: true,
			want:              0,
		},
		{
			name:    "zero footprint yields zero",
			faulted: 0, guestInUse: 0, want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := reclaimableBytes(tt.faulted, tt.guestInUse, tt.freePageReporting)
			if got != tt.want {
				t.Errorf("reclaimableBytes(%d, %d, %v) = %d, want %d",
					tt.faulted, tt.guestInUse, tt.freePageReporting, got, tt.want)
			}
		})
	}
}
