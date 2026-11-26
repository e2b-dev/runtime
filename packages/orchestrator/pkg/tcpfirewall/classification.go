//go:build linux

package tcpfirewall

import (
	"net"
	"sync"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
)

const (
	// This budget covers only the time spent reading HTTP Host or TLS SNI.
	maxPendingClassificationsPerSandbox = 64
	classificationTimeout               = 5 * time.Second
)

type classificationSlot struct {
	key      string
	deadline time.Time
}

// classificationAdmission bounds connections before tcpproxy calls a route
// handler. The existing connection limiter starts at that handler and covers
// the rest of the connection's lifetime.
type classificationAdmission struct {
	limiter *connlimit.ConnectionLimiter
	pending sync.Map // net.Conn -> classificationSlot
	timeout time.Duration
}

func newClassificationAdmission() *classificationAdmission {
	return &classificationAdmission{
		limiter: connlimit.NewConnectionLimiter(),
		timeout: classificationTimeout,
	}
}

func (a *classificationAdmission) admit(conn net.Conn, key string) bool {
	if _, ok := a.limiter.TryAcquire(key, maxPendingClassificationsPerSandbox); !ok {
		return false
	}

	deadline := time.Now().Add(a.timeout)
	if err := conn.SetReadDeadline(deadline); err != nil {
		a.limiter.Release(key)
		return false
	}

	a.pending.Store(conn, classificationSlot{key: key, deadline: deadline})
	return true
}

// complete releases the classification budget at the first route handler.
// A timed-out classifier must not be forwarded as a CIDR-only connection.
func (a *classificationAdmission) complete(conn net.Conn) bool {
	value, ok := a.pending.LoadAndDelete(conn)
	if !ok {
		return false
	}

	slot := value.(classificationSlot)
	a.limiter.Release(slot.key)
	if !time.Now().Before(slot.deadline) {
		return false
	}

	return conn.SetReadDeadline(time.Time{}) == nil
}
