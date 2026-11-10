package permissions

import (
	"strconv"
	"time"

	"connectrpc.com/connect"
)

const defaultKeepAliveInterval = 90 * time.Second

const maxKeepAliveIntervalSeconds = (1<<63 - 1) / int64(time.Second)

func parseKeepAliveInterval(value string) time.Duration {
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 || seconds > maxKeepAliveIntervalSeconds {
		return defaultKeepAliveInterval
	}

	return time.Duration(seconds) * time.Second
}

func GetKeepAliveTicker[T any](req *connect.Request[T]) (*time.Ticker, func()) {
	interval := parseKeepAliveInterval(req.Header().Get("Keepalive-Ping-Interval"))

	ticker := time.NewTicker(interval)

	return ticker, func() {
		ticker.Reset(interval)
	}
}
