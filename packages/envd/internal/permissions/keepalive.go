package permissions

import (
	"math"
	"strconv"
	"time"

	"connectrpc.com/connect"
)

const defaultKeepAliveInterval = 90 * time.Second

func GetKeepAliveTicker[T any](req *connect.Request[T]) (*time.Ticker, func()) {
	keepAliveIntervalHeader := req.Header().Get("Keepalive-Ping-Interval")

	interval := defaultKeepAliveInterval
	keepAliveIntervalInt, err := strconv.ParseInt(keepAliveIntervalHeader, 10, 64)
	// Validate seconds before multiplication, which could overflow time.Duration.
	if err == nil && keepAliveIntervalInt > 0 && keepAliveIntervalInt <= math.MaxInt64/int64(time.Second) {
		interval = time.Duration(keepAliveIntervalInt) * time.Second
	}

	ticker := time.NewTicker(interval)

	return ticker, func() {
		ticker.Reset(interval)
	}
}
