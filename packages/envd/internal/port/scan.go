package port

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/shirou/gopsutil/v4/net"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type Scanner struct {
	logger   *zerolog.Logger
	scanExit chan struct{}
	subs     *smap.Map[*ScannerSubscriber]
	period   time.Duration
}

func (s *Scanner) Destroy() {
	close(s.scanExit)
}

func NewScanner(logger *zerolog.Logger, period time.Duration) *Scanner {
	return &Scanner{
		logger:   logger,
		period:   period,
		subs:     smap.New[*ScannerSubscriber](),
		scanExit: make(chan struct{}),
	}
}

func (s *Scanner) AddSubscriber(id string, filter *ScannerFilter) *ScannerSubscriber {
	subscriber := NewScannerSubscriber(id, filter)
	s.subs.Insert(id, subscriber)

	return subscriber
}

// ScanAndBroadcast starts scanning open TCP ports and broadcasts every open port to all subscribers.
func (s *Scanner) ScanAndBroadcast() {
	ticker := time.NewTicker(s.period)
	defer ticker.Stop()

	for {
		// tcp monitors both ipv4 and ipv6 connections.
		processes, err := net.Connections("tcp")
		if err != nil {
			if s.logger != nil {
				s.logger.Error().Err(err).Msg("Failed to scan open TCP connections")
			}
		} else {
			for _, sub := range s.subs.Items() {
				// Pass scanExit so a subscriber that stopped consuming can't park this
				// loop on a send and keep it from reaching the select below.
				sub.Signal(processes, s.scanExit)
			}
		}
		select {
		case <-s.scanExit:
			return
		case <-ticker.C:
		}
	}
}
