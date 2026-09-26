package port

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestStartForwarding_StopsOnClosedMessages pins the defensive guard on the
// scan-result receive. Nothing closes Messages today, but a one-value receive
// would degrade badly if that ever changed: a closed channel is permanently
// ready, so the loop would spin on nil procs and stop every socat on each pass.
// The two-value receive turns that into a clean stop.
func TestStartForwarding_StopsOnClosedMessages(t *testing.T) {
	t.Parallel()

	l := zerolog.Nop()
	scanner := NewScanner(&l, time.Hour)
	f := &Forwarder{
		logger:            &l,
		ports:             make(map[string]*PortToForward),
		sourceIP:          defaultGatewayIP,
		scannerSubscriber: scanner.AddSubscriber("test", nil),
	}

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		f.StartForwarding(t.Context())
	}()

	// Nothing is scanning (period is an hour and ScanAndBroadcast is never
	// started), so this close cannot race a Signal.
	close(f.scannerSubscriber.Messages)

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("StartForwarding did not stop after Messages was closed")
	}
}
