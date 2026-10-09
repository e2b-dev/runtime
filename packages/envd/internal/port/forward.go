// portf (port forward) periodaically scans opened TCP ports on the 127.0.0.1 (or localhost)
// and launches `socat` process for every such port in the background.
// socat forward traffic from `sourceIP`:port to the 127.0.0.1:port.

// WARNING: portf isn't thread safe!

package port

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"sync"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
)

type PortState string

const (
	PortStateForward PortState = "FORWARD"
	PortStateDelete  PortState = "DELETE"
)

var defaultGatewayIP = net.IPv4(169, 254, 0, 21)

type PortToForward struct {
	socat *exec.Cmd
	// socatPid is the pid of a socat re-adopted across a live-upgrade, for which
	// there is no *exec.Cmd (the old envd's runtime — and its Wait goroutine —
	// were replaced by execve). 0 for socats this envd spawned itself.
	socatPid int
	// Process ID of the process that's listening on port.
	pid int32
	// family version of the ip.
	family uint32
	state  PortState
	port   uint32
}

// socatPID returns the pid of the forwarding socat regardless of whether it was
// spawned by this envd (*exec.Cmd) or re-adopted across a live-upgrade (pid
// only). Returns 0 when there is no socat.
func (p *PortToForward) socatPID() int {
	if p.socat != nil && p.socat.Process != nil {
		return p.socat.Process.Pid
	}

	return p.socatPid
}

type Forwarder struct {
	logger *zerolog.Logger
	// freezer both places the socat (through its manager) and admits the spawn. The socat
	// cgroup is on the freeze allowlist, so it is never ours to freeze -- but the guest can
	// freeze it, and a socat cloned into a frozen cgroup wedges envd exactly as a user
	// process would.
	freezer *cgroups.WorkloadFreezer
	// mu guards the ports map. The scan loop is single-goroutine, but the
	// live-upgrade export (ExportForwards) reads the map from the upgrade
	// goroutine concurrently, so map access is serialized.
	mu sync.Mutex
	// Map of ports that are being currently forwarded.
	ports             map[string]*PortToForward
	scannerSubscriber *ScannerSubscriber
	sourceIP          net.IP
}

func NewForwarder(
	logger *zerolog.Logger,
	scanner *Scanner,
	freezer *cgroups.WorkloadFreezer,
) *Forwarder {
	scannerSub := scanner.AddSubscriber(
		"port-forwarder",
		// We only want to forward ports that are actively listening on localhost.
		&ScannerFilter{
			IPs:   []string{"127.0.0.1", "localhost", "::1"},
			State: "LISTEN",
		},
	)

	return &Forwarder{
		logger:            logger,
		sourceIP:          defaultGatewayIP,
		ports:             make(map[string]*PortToForward),
		scannerSubscriber: scannerSub,
		freezer:           freezer,
	}
}

func (f *Forwarder) StartForwarding(ctx context.Context) {
	if f.scannerSubscriber == nil {
		f.logger.Error().Msg("Cannot start forwarding because scanner subscriber is nil")

		return
	}

	for {
		// Wait for the next scan result or context cancellation.
		select {
		case <-ctx.Done():
			return
		case procs, ok := <-f.scannerSubscriber.Messages:
			if !ok {
				return
			}

			// Serialize the whole refresh against a concurrent ExportForwards
			// (live-upgrade). stop/startPortForwarding below are called with the
			// lock held and must not take it themselves.
			f.mu.Lock()

			// Now we are going to refresh all ports that are being forwarded in the `ports` map. Maybe add new ones
			// and maybe remove some.

			// Go through the ports that are currently being forwarded and set all of them
			// to the `DELETE` state. We don't know yet if they will be there after refresh.
			for _, v := range f.ports {
				v.state = PortStateDelete
			}

			// Let's refresh our map of currently forwarded ports and mark the currently opened ones with the "FORWARD" state.
			// This will make sure we won't delete them later.
			for _, p := range procs {
				key := fmt.Sprintf("%d-%d", p.Pid, p.Laddr.Port)

				// We check if the opened port is in our map of forwarded ports.
				val, portOk := f.ports[key]
				if portOk {
					// Just mark the port as being forwarded so we don't delete it.
					// The actual socat process that handles forwarding should be running from the last iteration.
					val.state = PortStateForward
				} else {
					f.logger.Debug().
						Str("ip", p.Laddr.IP).
						Uint32("port", p.Laddr.Port).
						Uint32("family", familyToIPVersion(p.Family)).
						Str("state", p.Status).
						Msg("Detected new opened port on localhost that is not forwarded")

					// The opened port wasn't in the map so we create a new PortToForward and start forwarding.
					ptf := &PortToForward{
						pid:    p.Pid,
						port:   p.Laddr.Port,
						state:  PortStateForward,
						family: familyToIPVersion(p.Family),
					}
					f.ports[key] = ptf
					if retry := f.startPortForwarding(ctx, ptf); retry {
						// Not tracked, so the next scan sees an unforwarded port and tries
						// again. Left in the map it would read as forwarded from then on,
						// and the port would stay dark until its listener reopened.
						delete(f.ports, key)
					}
				}
			}

			// We go through the ports map one more time and stop forwarding all ports
			// that stayed marked as "DELETE".
			for _, v := range f.ports {
				if v.state == PortStateDelete {
					f.stopPortForwarding(v)
				}
			}

			f.mu.Unlock()
		}
	}
}

// startPortForwarding starts the socat for p. retry reports a start that was refused
// rather than failed -- the socat cgroup is frozen -- which the caller should attempt
// again on the next scan. A socat that fails to start is not retried: nothing about the
// next scan would make it succeed, and every attempt would log an error.
func (f *Forwarder) startPortForwarding(ctx context.Context, p *PortToForward) (retry bool) {
	// https://unix.stackexchange.com/questions/311492/redirect-application-listening-on-localhost-to-listening-on-external-interface
	// socat -d -d TCP4-LISTEN:4000,bind=169.254.0.21,fork TCP4:localhost:4000
	// reuseaddr is used to fix the "Address already in use" error when restarting socat quickly.
	cmd := exec.CommandContext(ctx,
		"socat", "-d", "-d", "-d",
		fmt.Sprintf("TCP4-LISTEN:%v,bind=%s,reuseaddr,fork", p.port, f.sourceIP.To4()),
		fmt.Sprintf("TCP%d:localhost:%v", p.family, p.port),
	)

	cgroupFD, ok := f.freezer.Manager().GetFileDescriptor(cgroups.ProcessTypeSocat)

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	applyCgroupFD(cmd.SysProcAttr, cgroupFD, ok)

	f.logger.Debug().
		Str("socatCmd", cmd.String()).
		Int32("pid", p.pid).
		Uint32("family", p.family).
		IPAddr("sourceIP", f.sourceIP.To4()).
		Uint32("port", p.port).
		Msg("About to start port forwarding")

	// A socat cloned into a frozen cgroup never reaches execve, and Go's fork/exec waits
	// for that execve in a syscall the runtime cannot preempt -- so the next
	// garbage-collection stop-the-world stops all of envd. Skip the forward instead and let the next scan retry it; a port that is not
	// forwarded for a while is a degradation rather than a dead sandbox. Debug rather than
	// Warn because it repeats every scan for as long as the guest keeps the cgroup frozen.
	releaseSpawn, err := f.freezer.BeginSpawn(ctx, cgroups.ProcessTypeSocat)
	if err != nil {
		f.logger.Debug().
			Uint32("port", p.port).
			Err(err).
			Msg("Deferring port forwarding: the socat cgroup is frozen")

		return true
	}
	err = cmd.Start()
	releaseSpawn()

	if err != nil {
		f.logger.
			Error().
			Str("socatCmd", cmd.String()).
			Err(err).
			Msg("Failed to start port forwarding - failed to start socat")

		return false
	}

	go func() {
		if err := cmd.Wait(); err != nil {
			f.logger.
				Debug().
				Str("socatCmd", cmd.String()).
				Err(err).
				Msg("Port forwarding socat process exited")
		}
	}()

	p.socat = cmd

	return false
}

func (f *Forwarder) stopPortForwarding(p *PortToForward) {
	pid := p.socatPID()
	if pid <= 0 {
		return
	}

	defer func() { p.socat = nil; p.socatPid = 0 }()

	logger := f.logger.With().
		Int("socatPid", pid).
		Int32("pid", p.pid).
		Uint32("family", p.family).
		IPAddr("sourceIP", f.sourceIP.To4()).
		Uint32("port", p.port).
		Logger()

	logger.Debug().Msg("Stopping port forwarding")

	// Kill the socat's process group. A re-adopted socat has no *exec.Cmd Wait
	// goroutine to reap it, so ImportForwards started reaper.Reap for it; a
	// self-spawned socat is reaped by its startPortForwarding Wait goroutine.
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		logger.Error().Err(err).Msg("Failed to kill process group")

		return
	}

	logger.Debug().Msg("Stopped port forwarding")
}

// ExportForwards snapshots the active port-forwards as typed ForwardedPort
// messages, carried across an envd live-upgrade. The socat children survive the
// same-PID execve; carrying their pids lets the new forwarder re-adopt them
// (ImportForwards) instead of spawning duplicate socats that would contend on
// the same bind address and leak the originals as un-reaped zombies.
func (f *Forwarder) ExportForwards() []*upgrade.ForwardedPort {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.exportLocked()
}

// ExportForwardsHold is ExportForwards but keeps the forwarder mutex held,
// returning a release func the caller invokes to resume scanning. The
// live-upgrade path holds it from the snapshot through the execve so the scan
// loop cannot spawn a new socat in that window — a socat started after the
// snapshot would be orphaned by the swap (never carried, never re-adopted) or
// duplicate a port the new envd re-adopts, contending on the same bind address.
// A successful execve replaces this process and the held lock vanishes with it;
// on failure the caller's release restores scanning under the old envd.
func (f *Forwarder) ExportForwardsHold() ([]*upgrade.ForwardedPort, func()) {
	f.mu.Lock()

	return f.exportLocked(), f.mu.Unlock
}

// exportLocked snapshots the active port-forwards. The caller must hold f.mu.
func (f *Forwarder) exportLocked() []*upgrade.ForwardedPort {
	out := make([]*upgrade.ForwardedPort, 0, len(f.ports))
	for key, p := range f.ports {
		pid := p.socatPID()
		if pid <= 0 {
			continue
		}
		out = append(out, &upgrade.ForwardedPort{
			Key:         key,
			Port:        p.port,
			ListenerPid: p.pid,
			Family:      p.family,
			SocatPid:    int32(pid),
		})
	}

	return out
}

// ImportForwards re-adopts the socats carried across a live-upgrade: it seeds the
// ports map so the next scan recognizes each already-forwarded port (and does
// not spawn a duplicate socat), and starts a reaper for each re-adopted socat
// (there is no surviving *exec.Cmd Wait goroutine for it). It MUST run before
// StartForwarding so the seeding is race-free. A socat that did not survive the
// handover is skipped, so the next scan respawns a fresh one for the still-open
// port.
func (f *Forwarder) ImportForwards(forwards []*upgrade.ForwardedPort) (readopted int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, fp := range forwards {
		pid := int(fp.GetSocatPid())
		if pid <= 0 {
			continue
		}
		// Only re-adopt a socat that is still alive (kill -0). One that exited
		// during the handover window is left out so the next scan respawns rather
		// than recording a dead socat as "forwarded".
		if syscall.Kill(pid, 0) != nil {
			continue
		}
		f.ports[fp.GetKey()] = &PortToForward{
			pid:      fp.GetListenerPid(),
			port:     fp.GetPort(),
			family:   fp.GetFamily(),
			socatPid: pid,
			state:    PortStateForward,
		}
		go reaper.Reap(pid)
		readopted++
	}

	return readopted
}

func familyToIPVersion(family uint32) uint32 {
	switch family {
	case syscall.AF_INET:
		return 4
	case syscall.AF_INET6:
		return 6
	default:
		return 0 // Unknown or unsupported family
	}
}
