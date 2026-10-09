package port

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// socatCgroupManager reports the socat cgroup as frozen or not, the way a guest that froze
// it itself would. It hands out no cgroup fd, so nothing is ever cloned into a cgroup here.
type socatCgroupManager struct {
	root   string
	frozen atomic.Bool
}

func (m *socatCgroupManager) GetFileDescriptor(cgroups.ProcessType) (int, bool) { return 0, false }
func (m *socatCgroupManager) Freeze(cgroups.ProcessType) error                  { return nil }
func (m *socatCgroupManager) Unfreeze(cgroups.ProcessType) error                { return nil }
func (m *socatCgroupManager) Frozen(cgroups.ProcessType) (bool, error)          { return false, nil }
func (m *socatCgroupManager) Close() error                                      { return nil }
func (m *socatCgroupManager) Root() string                                      { return m.root }
func (m *socatCgroupManager) ChildrenOf(string) ([]string, error)               { return nil, nil }
func (m *socatCgroupManager) FreezeAt(string) error                             { return nil }
func (m *socatCgroupManager) UnfreezeAt(string) error                           { return nil }
func (m *socatCgroupManager) FreezeRequestedAt(string) (bool, error)            { return false, nil }

func (m *socatCgroupManager) PathOf(pt cgroups.ProcessType) (string, bool) {
	if pt != cgroups.ProcessTypeSocat {
		return "", false
	}

	return filepath.Join(m.root, "socats"), true
}

func (m *socatCgroupManager) FrozenAt(string) (bool, error) { return m.frozen.Load(), nil }

// TestStartForwarding_RetriesAPortSkippedWhileFrozen pins that a refused forward is not
// remembered as a forwarded one. The scan loop records a port before starting its socat,
// and the map is keyed by listener pid+port; left in place after a refusal, the entry reads
// as forwarded on every later scan and the port stays dark until the listener reopens.
func TestStartForwarding_RetriesAPortSkippedWhileFrozen(t *testing.T) {
	t.Parallel()

	mgr := &socatCgroupManager{root: t.TempDir()}
	mgr.frozen.Store(true)

	l := zerolog.Nop()
	scanner := NewScanner(time.Hour)
	f := &Forwarder{
		logger:            &l,
		ports:             make(map[string]*PortToForward),
		sourceIP:          defaultGatewayIP,
		scannerSubscriber: scanner.AddSubscriber("test", nil),
		freezer:           cgroups.NewWorkloadFreezer(mgr),
	}

	go f.StartForwarding(t.Context())

	listener := net.ConnectionStat{
		Family: syscall.AF_INET,
		Laddr:  net.Addr{IP: "127.0.0.1", Port: 65000},
		Status: "LISTEN",
		Pid:    4242,
	}
	key := fmt.Sprintf("%d-%d", listener.Pid, listener.Laddr.Port)
	scan := func() { f.scannerSubscriber.Messages <- []net.ConnectionStat{listener} }
	tracked := func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		_, ok := f.ports[key]

		return ok
	}

	// Messages is unbuffered and the loop only receives again once it has finished the
	// previous scan, so the second send returning means the first one was processed.
	scan()
	scan()
	assert.False(t, tracked(), "a forward refused while the cgroup is frozen must not be recorded")

	mgr.frozen.Store(false)

	scan()
	scan()
	assert.True(t, tracked(), "once the cgroup is thawed the next scan must attempt the forward again")
}
