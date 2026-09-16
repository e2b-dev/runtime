package port

import (
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/net"
)

// TestScannerSubscriber_Signal_ReturnsOnExit verifies that Signal unparks when
// the scanner's exit channel closes, instead of blocking forever on the
// unbuffered Messages channel because nothing is receiving.
func TestScannerSubscriber_Signal_ReturnsOnExit(t *testing.T) {
	t.Parallel()

	sub := NewScannerSubscriber("test", nil)
	exit := make(chan struct{})

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		sub.Signal(nil, exit)
	}()

	// Nothing is receiving and exit is still open, so Signal must stay parked.
	select {
	case <-returned:
		t.Fatal("Signal() returned before the message was taken or exit was closed")
	case <-time.After(50 * time.Millisecond):
	}

	close(exit)

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Signal() stayed blocked after exit was closed — goroutine leaked")
	}
}

// TestScannerSubscriber_Signal_DeliversToSlowReceiver pins the backpressure
// contract: the exit escape must not make delivery lossy for a receiver that is
// merely slow rather than gone. StartForwarding holds a lock across each
// refresh, and ExportForwardsHold holds it across a live upgrade, so parking the
// send until the receiver catches up is the intended behaviour.
func TestScannerSubscriber_Signal_DeliversToSlowReceiver(t *testing.T) {
	t.Parallel()

	sub := NewScannerSubscriber("test", nil)
	exit := make(chan struct{})
	defer close(exit)

	go sub.Signal([]net.ConnectionStat{{Pid: 42}}, exit)

	// Receive late, so Signal is already parked on the send.
	time.Sleep(50 * time.Millisecond)

	select {
	case got := <-sub.Messages:
		if len(got) != 1 || got[0].Pid != 42 {
			t.Fatalf("got %+v, want the signalled connection", got)
		}
	case <-time.After(time.Second):
		t.Fatal("slow receiver never got the message")
	}
}

// TestScanner_Destroy_ExitsWithStalledSubscriber covers the shutdown path:
// StartForwarding returns on context cancellation and stops draining Messages,
// leaving the broadcast loop parked mid-send when Destroy is called. Without the
// exit escape the send never unparks, the scanExit select is unreachable, and
// ScanAndBroadcast leaks.
func TestScanner_Destroy_ExitsWithStalledSubscriber(t *testing.T) {
	t.Parallel()

	scanner := NewScanner(nil, 10*time.Millisecond)
	scanner.AddSubscriber("stalled-sub", nil)

	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner.ScanAndBroadcast()
	}()

	// Let the loop reach its first Signal and park there.
	time.Sleep(50 * time.Millisecond)

	scanner.Destroy()

	select {
	case <-scanDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ScanAndBroadcast did not exit after Destroy() — goroutine leaked")
	}
}
