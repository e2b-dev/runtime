// Package timestamp converts Go timestamps for use in ClickHouse queries.
package timestamp

import "time"

const (
	minUnixNano int64 = -1 << 63
	maxUnixNano int64 = 1<<63 - 1
)

var (
	minUnixNanoTime = time.Unix(0, minUnixNano).UTC()
	maxUnixNanoTime = time.Unix(0, maxUnixNano).UTC()
)

// UnixNano returns t as nanoseconds since the Unix epoch, clamped to the int64
// range accepted by ClickHouse's fromUnixTimestamp64Nano.
func UnixNano(t time.Time) int64 {
	t = t.UTC()
	switch {
	case t.Before(minUnixNanoTime):
		return minUnixNano
	case t.After(maxUnixNanoTime):
		return maxUnixNano
	default:
		return t.UnixNano()
	}
}
