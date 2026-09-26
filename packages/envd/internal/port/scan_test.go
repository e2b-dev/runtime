package port

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewScanner(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := zerolog.New(buf)

	s := NewScanner(&logger, 50*time.Millisecond)
	require.NotNil(t, s)
	assert.NotNil(t, s.subs)
	assert.Equal(t, 50*time.Millisecond, s.period)

	sub := s.AddSubscriber("test-sub", &ScannerFilter{
		IPs:   []string{"127.0.0.1"},
		State: "LISTEN",
	})
	require.NotNil(t, sub)
	assert.Equal(t, "test-sub", sub.id)
}

func TestScanAndBroadcastLifecycle(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := zerolog.New(buf)

	s := NewScanner(&logger, 10*time.Millisecond)
	sub := s.AddSubscriber("sub-1", nil)

	received := make(chan struct{}, 1)
	stopDrain := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopDrain:
				return
			case <-sub.Messages:
				select {
				case received <- struct{}{}:
				default:
				}
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		s.ScanAndBroadcast()
		close(done)
	}()

	select {
	case <-received:
		// Received at least one scan message
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for scan message")
	}

	s.Destroy()

	select {
	case <-done:
		// Scanner goroutine exited promptly
	case <-time.After(1 * time.Second):
		t.Fatal("ScanAndBroadcast did not exit after Destroy()")
	}

	close(stopDrain)
}

func TestSubscriberFilter(t *testing.T) {
	sub := NewScannerSubscriber("sub-filter", &ScannerFilter{
		IPs:   []string{"127.0.0.1", "localhost"},
		State: "LISTEN",
	})

	conns := []net.ConnectionStat{
		{
			Status: "LISTEN",
			Laddr:  net.Addr{IP: "127.0.0.1", Port: 8080},
		},
		{
			Status: "ESTABLISHED",
			Laddr:  net.Addr{IP: "127.0.0.1", Port: 8080},
		},
		{
			Status: "LISTEN",
			Laddr:  net.Addr{IP: "192.168.1.1", Port: 8080},
		},
	}

	exit := make(chan struct{})
	defer close(exit)

	go sub.Signal(conns, exit)

	select {
	case filtered := <-sub.Messages:
		require.Len(t, filtered, 1)
		assert.Equal(t, uint32(8080), filtered[0].Laddr.Port)
		assert.Equal(t, "127.0.0.1", filtered[0].Laddr.IP)
		assert.Equal(t, "LISTEN", filtered[0].Status)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for filtered messages")
	}
}
