//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/gaissmai/extnetip"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/ngrok/firewall_toolkit/pkg/expressions"
	"github.com/ngrok/firewall_toolkit/pkg/set"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

const (
	tableName = "slot-firewall"
	dnsPort   = 53

	// The non-DNS UDP rule references one of two named counters, which
	// outlive every chain flush; a chain replacement moves to the other one
	// in the flush that installs it, so the retired chain's packets never mix
	// with the new chain's.
	nonDNSUDPCounterNameA = "non_dns_udp_a"
	nonDNSUDPCounterNameB = "non_dns_udp_b"

	postureDefault  = "default"
	postureRules    = "rules"
	postureBYOP     = "byop"
	postureIsolated = "isolated"
	postureDeny     = "deny"
)

// NonDNSUDPMode selects what the filter chain does with guest UDP datagrams
// to ports other than 53: leave them alone, count them, or count and drop them.
type NonDNSUDPMode string

const (
	NonDNSUDPModeOff   NonDNSUDPMode = "off"
	NonDNSUDPModeCount NonDNSUDPMode = "count"
	NonDNSUDPModeDrop  NonDNSUDPMode = "drop"
)

// ParseNonDNSUDPMode maps a configured value to a mode; anything else is off.
func ParseNonDNSUDPMode(value string) NonDNSUDPMode {
	switch NonDNSUDPMode(value) {
	case NonDNSUDPModeCount, NonDNSUDPModeDrop:
		return NonDNSUDPMode(value)
	default:
		return NonDNSUDPModeOff
	}
}

// ChainDescriptor is the non-DNS UDP mode and the egress posture a filter
// chain was built with: posture is "default", "rules" (tenant CIDRs), "byop"
// or "isolated" (DenyEgress).
type ChainDescriptor struct {
	Mode    NonDNSUDPMode
	Posture string
}

// NonDNSUDPTally is what the named counter held when it was harvested.
type NonDNSUDPTally struct {
	Packets uint64
	Bytes   uint64
}

// NonDNSUDPSnapshot is one harvest of a named counter together with the
// chain it was read from.
type NonDNSUDPSnapshot struct {
	ChainDescriptor
	NonDNSUDPTally
}

// namedCounter is one of the table's counter objects with the chain whose
// packets it holds: the installed chain while referenced, the retired chain
// until its final read (the one before it changes hands, or a release), and
// an off chain when it holds nothing.
type namedCounter struct {
	obj   *nftables.CounterObj
	chain ChainDescriptor
}

type Firewall struct {
	// mu serializes the shared conn buffer, which is committed on Flush(), and
	// keeps installed, the counters and the tally one consistent view.
	mu sync.Mutex

	// netNS is the slot's network namespace, held open for the firewall's
	// lifetime and closed last: conn dials through its descriptor, so every
	// operation and the close reach the slot's table from any thread.
	netNS ns.NetNS

	// conn is a lasting connection into netNS. A connection that lost its
	// socket dials per request through the same descriptor.
	conn  *nftables.Conn
	table *nftables.Table

	// Filter chain in PREROUTING
	filterChain *nftables.Chain

	predefinedDenySet  set.Set
	predefinedAllowSet set.Set

	userDenySet  set.Set
	userAllowSet set.Set

	tapInterface string

	allowedRanges []string

	// headFilter holds the egress proxy's rules installRules puts ahead of
	// its own.
	headFilter [][]expr.Any

	// counters alternate as the non-DNS UDP rule's target; live indexes the
	// one the installed chain references, -1 when it references none.
	counters [2]namedCounter
	live     int

	// installed describes the chain the kernel holds; set only after a
	// successful flush, as one value.
	installed ChainDescriptor

	// tally accumulates every harvest since the last TakeNonDNSUDPTally,
	// keyed by the chain the packets were counted under.
	tally map[ChainDescriptor]NonDNSUDPTally

	// closed is set by Close. Once it is, conn is gone and netNS's descriptor
	// number may belong to another file, so nothing touches either again.
	closed bool
}

// errFirewallClosed is what every operation on a closed firewall returns.
var errFirewallClosed = errors.New("slot firewall is closed")

// beforeFlush, when set, runs after a replacement chain is buffered and
// before it is committed; tests place traffic in that window. Nothing in
// production sets it.
var beforeFlush func()

// NewFirewall builds the slot's table and default chain in the network
// namespace at netNSPath, which it keeps open until Close. headFilter lists
// the expressions of the rules the egress proxy puts ahead of the firewall's
// own, EgressProxy.HeadFilterRules.
func NewFirewall(netNSPath string, tapIf string, orchestratorInternalIP string, extraAllowedCIDRs []string, headFilter [][]expr.Any, nonDNSUDPMode NonDNSUDPMode) (_ *Firewall, err error) {
	netNS, err := ns.GetNS(netNSPath)
	if err != nil {
		return nil, fmt.Errorf("open slot network namespace: %w", err)
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, netNS.Close())
		}
	}()

	conn, err := dialNetNS(netNS)
	if err != nil {
		return nil, fmt.Errorf("new nftables conn: %w", err)
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, conn.CloseLasting())
		}
	}()

	table := conn.AddTable(&nftables.Table{
		Name:   tableName,
		Family: nftables.TableFamilyINet,
	})

	// Filter chain in PREROUTING
	// This handles: allow/deny decisions for traffic from the tap interface
	policy := nftables.ChainPolicyAccept
	filterChain := conn.AddChain(&nftables.Chain{
		Name:     "PREROUTE_FILTER",
		Table:    table,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRef(-150),
		Policy:   &policy,
	})

	// Create deny-set and allow-set
	alwaysDenySet, err := set.New(conn, table, "filtered_always_denylist", nftables.TypeIPAddr)
	if err != nil {
		return nil, fmt.Errorf("new deny set: %w", err)
	}
	alwaysAllowSet, err := set.New(conn, table, "filtered_always_allowlist", nftables.TypeIPAddr)
	if err != nil {
		return nil, fmt.Errorf("new allow set: %w", err)
	}

	denySet, err := set.New(conn, table, "filtered_denylist", nftables.TypeIPAddr)
	if err != nil {
		return nil, fmt.Errorf("new deny set: %w", err)
	}
	allowSet, err := set.New(conn, table, "filtered_allowlist", nftables.TypeIPAddr)
	if err != nil {
		return nil, fmt.Errorf("new allow set: %w", err)
	}

	controlIP := fmt.Sprintf("%s/32", orchestratorInternalIP)

	counters := [2]namedCounter{
		{obj: &nftables.CounterObj{Table: table, Name: nonDNSUDPCounterNameA}, chain: ChainDescriptor{Mode: NonDNSUDPModeOff}},
		{obj: &nftables.CounterObj{Table: table, Name: nonDNSUDPCounterNameB}, chain: ChainDescriptor{Mode: NonDNSUDPModeOff}},
	}
	for _, counter := range counters {
		conn.AddObj(counter.obj)
	}

	installed := ChainDescriptor{Mode: nonDNSUDPMode, Posture: egressPosture(false, nil, nil, false)}
	live := -1
	if nonDNSUDPMode != NonDNSUDPModeOff {
		live = 0
		counters[0].chain = installed
	}

	fw := &Firewall{
		netNS:              netNS,
		conn:               conn,
		table:              table,
		predefinedDenySet:  alwaysDenySet,
		predefinedAllowSet: alwaysAllowSet,
		userDenySet:        denySet,
		userAllowSet:       allowSet,
		tapInterface:       tapIf,
		allowedRanges: append(
			[]string{controlIP},
			extraAllowedCIDRs...,
		),
		filterChain: filterChain,
		headFilter:  headFilter,
		counters:    counters,
		live:        live,
		installed:   installed,
	}

	// Install default rules and initial set data in a single flush.
	fw.installRules(false, nonDNSUDPMode, counters[0].obj, false)
	if err := fw.bufferUserRules(nil, nil); err != nil {
		return nil, fmt.Errorf("error while configuring initial data: %w", err)
	}
	if err := fw.conn.Flush(); err != nil {
		return nil, fmt.Errorf("flush initial firewall rules: %w", err)
	}

	return fw, nil
}

// NonDNSUDPMode returns the mode the installed chain carries.
func (fw *Firewall) NonDNSUDPMode() NonDNSUDPMode {
	return fw.Installed().Mode
}

// Installed returns the descriptor of the chain the kernel holds.
func (fw *Firewall) Installed() ChainDescriptor {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	return fw.installed
}

func egressPosture(byop bool, allowedCIDRs, deniedCIDRs []string, defaultDeny bool) string {
	switch {
	case defaultDeny:
		return postureDeny
	case byop:
		return postureBYOP
	case len(allowedCIDRs)+len(deniedCIDRs) > 0:
		return postureRules
	default:
		return postureDefault
	}
}

// Harvest reads and zeroes both named counters, each in one request, folds
// the packets into the tally under the chain that counted them and returns
// what the installed chain's counter held. Every counter is attempted; the
// error joins every failed read, and a counter whose read failed keeps what
// it holds for the next attempt.
func (fw *Firewall) Harvest() (NonDNSUDPSnapshot, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return NonDNSUDPSnapshot{}, errFirewallClosed
	}

	return fw.harvestLocked()
}

// harvestLocked is Harvest for callers that hold fw.mu. It is a retired
// counter's final read.
func (fw *Firewall) harvestLocked() (NonDNSUDPSnapshot, error) {
	snapshot := NonDNSUDPSnapshot{ChainDescriptor: fw.installed}
	var errs []error
	for i := range fw.counters {
		read, err := fw.harvestCounterLocked(i, true)
		if err != nil {
			errs = append(errs, err)

			continue
		}
		if i == fw.live {
			snapshot = read
		}
	}

	return snapshot, errors.Join(errs...)
}

// harvestCounterLocked reads and zeroes counter i in one request and folds
// the packets into the tally under the chain that counted them; a counter
// marked off holds nothing and is not read. A retired chain's rule is
// flushed before every CPU has stopped running it, so its counter can grow
// a little after the flush is acknowledged: the counter keeps the retired
// chain across a read unless final is set, so the next read still credits
// those packets to that chain. Caller holds fw.mu.
func (fw *Firewall) harvestCounterLocked(i int, final bool) (NonDNSUDPSnapshot, error) {
	counter := &fw.counters[i]
	snapshot := NonDNSUDPSnapshot{ChainDescriptor: counter.chain}
	if counter.chain.Mode == NonDNSUDPModeOff {
		return snapshot, nil
	}

	obj, err := fw.conn.ResetObject(counter.obj)
	if err != nil {
		return snapshot, fmt.Errorf("reset non-DNS UDP counter %s: %w", counter.obj.Name, err)
	}
	read, ok := obj.(*nftables.CounterObj)
	if !ok {
		return snapshot, fmt.Errorf("reset non-DNS UDP counter %s: unexpected object %T", counter.obj.Name, obj)
	}
	snapshot.NonDNSUDPTally = NonDNSUDPTally{Packets: read.Packets, Bytes: read.Bytes}

	if fw.tally == nil {
		fw.tally = make(map[ChainDescriptor]NonDNSUDPTally)
	}
	tally := fw.tally[counter.chain]
	tally.Packets += snapshot.Packets
	tally.Bytes += snapshot.Bytes
	fw.tally[counter.chain] = tally

	if final && i != fw.live {
		counter.chain = ChainDescriptor{Mode: NonDNSUDPModeOff}
	}

	return snapshot, nil
}

// spareCounterIndex is the counter the installed chain does not reference.
func (fw *Firewall) spareCounterIndex() int {
	if fw.live == 0 {
		return 1
	}

	return 0
}

// replaceChainLocked rebuilds the filter chain as next in one flush: build
// buffers the rules against the spare counter, so the retired chain's packets
// stay in its own counter, which is harvested under the retired descriptor
// once the new chain is in place and again, as its final read, right before
// it changes hands. A failed flush leaves the kernel's chain, the live counter
// and anything still unread as they were. Caller holds fw.mu.
func (fw *Firewall) replaceChainLocked(ctx context.Context, next ChainDescriptor, build func(counter *nftables.CounterObj) error) error {
	spare := fw.spareCounterIndex()
	// Whatever the spare counter still holds, late packets of the chain it
	// was retired from or a read that failed, belongs to that chain, not to
	// next. The descriptor moves only once the flush has succeeded.
	if err := fw.harvestWithRetry(ctx, "of the counter to be reused", func() error {
		_, err := fw.harvestCounterLocked(spare, false)

		return err
	}); err != nil {
		return err
	}

	fw.conn.FlushChain(fw.filterChain)
	if err := build(fw.counters[spare].obj); err != nil {
		return err
	}
	if beforeFlush != nil {
		beforeFlush()
	}
	if err := fw.conn.Flush(); err != nil {
		return fmt.Errorf("flush replacement chain: %w", err)
	}

	retired := fw.live
	fw.installed = next
	fw.live = -1
	fw.counters[spare].chain = ChainDescriptor{Mode: NonDNSUDPModeOff}
	if next.Mode != NonDNSUDPModeOff {
		fw.live = spare
		fw.counters[spare].chain = next
	}
	if retired >= 0 {
		// Not the final read: the retired counter keeps its chain for the
		// packets a CPU still running the old rule adds after this.
		if _, err := fw.harvestCounterLocked(retired, false); err != nil {
			fw.logHarvestError(ctx, err)
		}
	}

	return nil
}

// TakeNonDNSUDPTally returns every tally harvested since the last call and
// forgets it, so the owner exports each packet once.
func (fw *Firewall) TakeNonDNSUDPTally() map[ChainDescriptor]NonDNSUDPTally {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	tally := fw.tally
	fw.tally = nil

	return tally
}

func (fw *Firewall) logHarvestError(ctx context.Context, err error) {
	logger.L().Error(ctx, "failed to harvest the slot firewall non-DNS UDP counter",
		zap.String("tap_interface", fw.tapInterface), zap.Error(err))
}

// harvestWithRetry runs a counter read and, after a failure, runs it once
// more, so one transient failure fails no rebuild, release or close; a read
// that fails twice is the error. what names the read in the warning and the
// error.
func (fw *Firewall) harvestWithRetry(ctx context.Context, what string, read func() error) error {
	err := read()
	if err == nil {
		return nil
	}

	logger.L().Warn(ctx, "retrying the read of the slot firewall non-DNS UDP counters",
		zap.String("read", what), zap.String("tap_interface", fw.tapInterface), zap.Error(err))
	if err := read(); err != nil {
		return fmt.Errorf("harvest the slot firewall non-DNS UDP counters %s: %w", what, err)
	}

	return nil
}

// Close harvests the counters a last time, deletes the table, closes the
// connection and, last, the namespace descriptor; the tally stays readable
// through TakeNonDNSUDPTally. The descriptor keeps the namespace alive even
// once its name is unlinked, so the counters are read and the table deleted
// whatever the calling thread's namespace. A table that is already gone is
// not an error. A second Close does nothing and, like every other operation
// after the first, returns errFirewallClosed.
//
// The read is tried once more after a failure: a slot whose return is cut
// short by the pool's shutdown skips the release, so this read is that
// tenant's only one. A read that fails twice is joined into the error, after
// the table is deleted and the connection and the descriptor are closed as
// on every other path, so the caller reports the packets as lost rather
// than exported; the tally keeps what was read.
func (fw *Firewall) Close(ctx context.Context) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return errFirewallClosed
	}
	fw.closed = true

	harvestErr := fw.harvestWithRetry(ctx, "at close", func() error {
		_, err := fw.harvestLocked()

		return err
	})

	fw.conn.DelTable(&nftables.Table{
		Name:   tableName,
		Family: nftables.TableFamilyINet,
	})
	deleteErr := fw.conn.Flush()
	if errors.Is(deleteErr, unix.ENOENT) {
		deleteErr = nil
	}

	return errors.Join(harvestErr, deleteErr, fw.conn.CloseLasting(), fw.netNS.Close())
}

// dialNetNS opens a lasting netlink connection into the namespace.
func dialNetNS(netNS ns.NetNS) (*nftables.Conn, error) {
	return nftables.New(nftables.AsLasting(), nftables.WithNetNSFd(int(netNS.Fd())))
}

// resetConn replaces fw.conn with a fresh lasting netlink connection into
// the slot namespace, discarding buffered messages and any sticky
// serialization error — nftables.Conn has no API for that (see
// https://github.com/google/nftables/pull/324). When no new connection can be
// opened the old one is kept: without its socket it dials per request, through
// the same namespace descriptor, so it still reaches the slot's table.
func (fw *Firewall) resetConn(ctx context.Context) error {
	closeErr := fw.conn.CloseLasting()

	conn, err := dialNetNS(fw.netNS)
	if err != nil {
		resetErr := errors.Join(closeErr, fmt.Errorf("open new lasting nftables conn: %w", err))
		logger.L().Error(ctx, "firewall nftables conn reset failed; reusing the old conn",
			zap.String("tap_interface", fw.tapInterface), zap.Error(resetErr))

		return resetErr
	}

	fw.conn = conn
	if closeErr != nil {
		logger.L().Error(ctx, "firewall nftables conn reset after apply failure encountered errors",
			zap.String("tap_interface", fw.tapInterface), zap.Error(closeErr))
	} else {
		logger.L().Warn(ctx, "firewall nftables conn reset after apply failure",
			zap.String("tap_interface", fw.tapInterface))
	}

	return closeErr
}

// tapIfaceMatch returns expressions that match packets from the tap interface.
func (fw *Firewall) tapIfaceMatch() []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyIIFNAME, Register: 1},
		&expr.Cmp{
			Register: 1,
			Op:       expr.CmpOpEq,
			Data:     append([]byte(fw.tapInterface), 0), // null-terminated interface name
		},
	}
}

// accept returns an expression that accepts the packet.
func accept() []expr.Any {
	return []expr.Any{
		&expr.Verdict{Kind: expr.VerdictAccept},
	}
}

// addSetFilterRule adds a filter rule that matches destination IPs in a set.
// If drop is true, packets are dropped. Otherwise, they are accepted.
// This applies to ALL protocols.
func (fw *Firewall) addSetFilterRule(ipSet *nftables.Set, drop bool) {
	var verdict []expr.Any
	if drop {
		verdict = []expr.Any{&expr.Verdict{Kind: expr.VerdictDrop}}
	} else {
		verdict = accept()
	}

	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: append(append(fw.tapIfaceMatch(),
			expressions.IPv4DestinationAddress(1),
			expressions.IPSetLookUp(ipSet, 1)),
			verdict...,
		),
	})
}

// addNonTCPSetFilterRule adds a filter rule that matches ONLY non-TCP traffic to destinations in a set.
// If drop is true, packets are dropped. Otherwise, they are accepted.
// TCP traffic is NOT affected by this rule (iptables REDIRECT handles TCP traffic).
func (fw *Firewall) addNonTCPSetFilterRule(ipSet *nftables.Set, drop bool) {
	var verdict []expr.Any
	if drop {
		verdict = []expr.Any{&expr.Verdict{Kind: expr.VerdictDrop}}
	} else {
		verdict = accept()
	}

	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: append(append(fw.tapIfaceMatch(),
			// Match non-TCP protocol (protocol != TCP)
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{
				Op:       expr.CmpOpNeq,
				Register: 1,
				Data:     []byte{unix.IPPROTO_TCP},
			},
			// Check dest in set
			expressions.IPv4DestinationAddress(1),
			expressions.IPSetLookUp(ipSet, 1)),
			verdict...,
		),
	})
}

// ctDirReply is conntrack's reply direction (IP_CT_DIR_REPLY): the packet
// travels opposite to the one that opened the flow.
const ctDirReply = 1

// establishedMatch matches tap packets on an ESTABLISHED or RELATED flow.
func (fw *Firewall) establishedMatch() []expr.Any {
	return append(fw.tapIfaceMatch(),
		&expr.Ct{Key: expr.CtKeySTATE, Register: 1},
		&expr.Bitwise{
			SourceRegister: 1,
			DestRegister:   1,
			Len:            4,
			Mask:           binaryutil.NativeEndian.PutUint32(expr.CtStateBitESTABLISHED | expr.CtStateBitRELATED),
			Xor:            binaryutil.NativeEndian.PutUint32(0),
		},
		&expr.Cmp{
			Op:       expr.CmpOpNeq,
			Register: 1,
			Data:     binaryutil.NativeEndian.PutUint32(0),
		},
	)
}

// addEstablishedAcceptRules buffers the two rules that accept tap traffic on
// ESTABLISHED/RELATED flows ahead of the policy sets, so the guest's replies
// are allowed even when their destination sits in a deny set:
//   - every protocol but UDP, in both directions;
//   - every protocol in the reply direction, i.e. the guest answering a flow
//     opened from outside.
//
// UDP the guest sends on a flow it opened matches neither, so the policy
// judges every such datagram and a stricter policy applies to it at once.
// Buffer-only; the caller flushes.
func (fw *Firewall) addEstablishedAcceptRules() {
	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: append(append(fw.establishedMatch(),
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{
				Op:       expr.CmpOpNeq,
				Register: 1,
				Data:     []byte{unix.IPPROTO_UDP},
			}),
			accept()...,
		),
	})

	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: append(append(fw.establishedMatch(),
			&expr.Ct{Key: expr.CtKeyDIRECTION, Register: 1},
			&expr.Cmp{
				Op:       expr.CmpOpEq,
				Register: 1,
				Data:     []byte{ctDirReply},
			}),
			accept()...,
		),
	})
}

// addHeadFilterRules buffers the egress proxy's head rules. Buffer-only;
// the caller flushes.
func (fw *Firewall) addHeadFilterRules() {
	for _, exprs := range fw.headFilter {
		fw.conn.AddRule(&nftables.Rule{Table: fw.table, Chain: fw.filterChain, Exprs: exprs})
	}
}

// addTapDropRule buffers a rule that drops every packet from the tap interface,
// regardless of protocol or destination. Buffer-only; the caller flushes.
func (fw *Firewall) addTapDropRule() {
	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: append(fw.tapIfaceMatch(),
			&expr.Verdict{Kind: expr.VerdictDrop},
		),
	})
}

// addNonDNSUDPRule buffers a rule that counts every IPv4 UDP datagram from the
// tap interface whose destination port is not 53 and whose destination is in
// neither allow set, on the given named counter, and, in drop mode, drops
// it. Nothing is buffered in off mode. Buffer-only; the caller flushes.
func (fw *Firewall) addNonDNSUDPRule(mode NonDNSUDPMode, counter *nftables.CounterObj) {
	if mode == NonDNSUDPModeOff {
		return
	}

	exprs := append(fw.tapIfaceMatch(),
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.IPPROTO_UDP},
		},
		// UDP destination port: transport header offset 2, length 2.
		&expr.Payload{
			DestRegister: 1,
			Base:         expr.PayloadBaseTransportHeader,
			Offset:       2,
			Len:          2,
		},
		&expr.Cmp{
			Op:       expr.CmpOpNeq,
			Register: 1,
			Data:     binaryutil.BigEndian.PutUint16(dnsPort),
		},
		// The inet table also sees IPv6; the IPv4 destination load below
		// would read source-address bytes of an IPv6 header.
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.NFPROTO_IPV4},
		},
		expressions.IPv4DestinationAddress(1),
		notInSet(fw.predefinedAllowSet.Set()),
		notInSet(fw.userAllowSet.Set()),
		&expr.Objref{Type: int(nftables.ObjTypeCounter), Name: counter.Name},
	)
	if mode == NonDNSUDPModeDrop {
		exprs = append(exprs, &expr.Verdict{Kind: expr.VerdictDrop})
	}

	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: exprs,
	})
}

// notInSet returns a lookup that matches when register 1 is not in the set.
func notInSet(ipSet *nftables.Set) *expr.Lookup {
	lookup := expressions.IPSetLookUp(ipSet, 1)
	lookup.Invert = true

	return lookup
}

// installRules buffers the filter chain rules. When byop is true, Rule 3 drops
// non-TCP only (TCP shifts to the userspace SOCKS5 proxy); otherwise it drops
// all protocols. The non-DNS UDP rule counts on counter. Buffer-only; the
// caller must Flush.
func (fw *Firewall) installRules(byop bool, nonDNSUDPMode NonDNSUDPMode, counter *nftables.CounterObj, defaultDeny bool) {
	// FILTER CHAIN (PREROUTING, priority -150)
	//   0. EgressProxy.HeadFilterRules, if any
	//   0. IPv4 UDP to a port other than 53, destination in neither allow set → named counter, DROP in drop mode (absent in off mode)
	//   1. ESTABLISHED/RELATED, non-UDP → accept
	//   1. ESTABLISHED/RELATED, reply direction → accept (UDP the guest sends on its own flows falls through)
	//   2. predefinedAllowSet → accept (all protocols)
	//   3. predefinedDenySet → DROP (all protocols, or non-TCP only when byop)
	//   4. Non-TCP: userAllowSet → accept
	//   5. Non-TCP: userDenySet → DROP
	//   6. Default: ACCEPT (TCP handled by iptables REDIRECT in host netns)

	// Rule 0: the egress proxy's head rules, ahead of every rule of the
	// firewall's own.
	fw.addHeadFilterRules()

	// Rule 0: ahead of the established accept, so every datagram of a flow is
	// counted and, in drop mode, dropped, whether or not a reply came back.
	fw.addNonDNSUDPRule(nonDNSUDPMode, counter)

	// Rule 1: ESTABLISHED/RELATED → accept, except UDP the guest sends on a
	// flow it opened, which the rules below judge on every datagram.
	fw.addEstablishedAcceptRules()

	// Rule 2: predefinedAllowSet → accept (all protocols)
	fw.addSetFilterRule(fw.predefinedAllowSet.Set(), false)

	// Rule 3: predefinedDenySet → DROP (all protocols, or non-TCP only in BYOP).
	if byop {
		fw.addNonTCPSetFilterRule(fw.predefinedDenySet.Set(), true)
	} else {
		fw.addSetFilterRule(fw.predefinedDenySet.Set(), true)
	}

	// Rule 4: Non-TCP + userAllowSet → accept
	// Only non-TCP traffic is affected; TCP goes to proxy
	fw.addNonTCPSetFilterRule(fw.userAllowSet.Set(), false)

	// Rule 5: Non-TCP + userDenySet → DROP
	// Only non-TCP traffic is affected; TCP goes to proxy
	fw.addNonTCPSetFilterRule(fw.userDenySet.Set(), true)

	// Rule 6 (default-deny only): terminal non-TCP drop. Established flows,
	// the predefined allow set (orchestrator control IP + extra allowed CIDRs)
	// and the user allow set were already accepted above, so what reaches here
	// is non-TCP the operator did not allow — dropped. TCP is unaffected (it is
	// REDIRECTed to the userspace proxy, which enforces default-deny there).
	if defaultDeny {
		fw.addNonTCPDefaultDenyRule()
	}

	// Default policy: ACCEPT
	// - Non-TCP not in user sets: allowed by default, OR dropped by Rule 6 under default-deny
	// - TCP: iptables REDIRECT handles TCP traffic to proxy
}

// addNonTCPDefaultDenyRule buffers a terminal rule that drops every non-TCP
// packet from the tap interface. Installed last under the default-deny posture,
// after the established/allow-set accepts, so only traffic the operator did not
// explicitly allow reaches it. TCP is never matched (it leaves via the REDIRECT
// to the userspace proxy). Buffer-only; the caller flushes.
func (fw *Firewall) addNonTCPDefaultDenyRule() {
	fw.conn.AddRule(&nftables.Rule{
		Table: fw.table,
		Chain: fw.filterChain,
		Exprs: append(fw.tapIfaceMatch(),
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{
				Op:       expr.CmpOpNeq,
				Register: 1,
				Data:     []byte{unix.IPPROTO_TCP},
			},
			&expr.Verdict{Kind: expr.VerdictDrop},
		),
	})
}

// bufferUserRules buffers a full replacement of every firewall set.
// Buffer-only; the caller flushes.
func (fw *Firewall) bufferUserRules(allowedCIDRs, deniedCIDRs []string) error {
	// 1. Reset predefined deny set to default blocked ranges (buffered, no flush).
	if err := fw.predefinedDenySet.ClearAndAddElements(fw.conn, sandbox_network.DeniedSandboxSetData); err != nil {
		return fmt.Errorf("reset predefined deny set: %w", err)
	}

	// 2. Reset predefined allow set to allowedRanges (buffered, no flush).
	allowedSetData, err := set.AddressStringsToSetData(fw.allowedRanges)
	if err != nil {
		return fmt.Errorf("parse initial allowed CIDRs: %w", err)
	}
	if err := fw.predefinedAllowSet.ClearAndAddElements(fw.conn, allowedSetData); err != nil {
		return fmt.Errorf("reset predefined allow set: %w", err)
	}

	// 3. Replace user deny set with new denied CIDRs (buffered, no flush).
	if err := clearAndReplaceCIDRs(fw.conn, fw.userDenySet, deniedCIDRs); err != nil {
		return fmt.Errorf("replace user deny set: %w", err)
	}

	// 4. Replace user allow set with new allowed CIDRs (buffered, no flush).
	if err := clearAndReplaceCIDRs(fw.conn, fw.userAllowSet, allowedCIDRs); err != nil {
		return fmt.Errorf("replace user allow set: %w", err)
	}

	return nil
}

// ApplyRules reinstalls the filter chain in the given BYOP and non-DNS UDP
// modes and replaces all firewall sets, committed in a single atomic flush so the kernel never
// holds the new Rule 3 mode with stale user sets. The chain is always rebuilt
// from scratch; on flush failure the kernel keeps the previous ruleset and no
// in-memory state can desync from it. The outgoing chain's packets are
// harvested from its own counter after the switch, so a rebuild loses none
// and credits none to the new chain.
//
// On any failure the conn is replaced via resetConn, so a poisoned batch can
// never leak into a later flush.
func (fw *Firewall) ApplyRules(ctx context.Context, byop bool, nonDNSUDPMode NonDNSUDPMode, allowedCIDRs, deniedCIDRs []string, defaultDeny bool) (err error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return errFirewallClosed
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, fw.resetConn(ctx))
		}
	}()

	// Default-deny forces the non-DNS UDP rule into drop mode: the terminal
	// non-TCP drop below would drop those datagrams anyway, but keeping the
	// counter rule in drop mode preserves the per-slot UDP tally.
	if defaultDeny {
		nonDNSUDPMode = NonDNSUDPModeDrop
	}

	next := ChainDescriptor{Mode: nonDNSUDPMode, Posture: egressPosture(byop, allowedCIDRs, deniedCIDRs, defaultDeny)}

	return fw.replaceChainLocked(ctx, next, func(counter *nftables.CounterObj) error {
		fw.installRules(byop, nonDNSUDPMode, counter, defaultDeny)

		return fw.bufferUserRules(allowedCIDRs, deniedCIDRs)
	})
}

// DenyEgress rebuilds the filter chain so that every packet originating from
// the guest (the tap interface) is dropped, except replies on flows opened into
// the guest and non-UDP ESTABLISHED/RELATED traffic. Unlike the user deny set —
// which drops only non-TCP traffic and leaves TCP to the egress proxy — this
// drops ALL protocols and allows NO guest-initiated destination, so a sandbox
// isolated this way cannot reach the network at all, including the
// orchestrator's own in-sandbox IP (which also fronts the NFS proxy, portmapper
// and hyperloop). The orchestrator still drives the resume because it connects
// INTO the guest (envd /init, health probes) and those replies match
// ESTABLISHED/RELATED; nothing about the resume needs the guest to open a
// connection outward. It backs the throwaway pause-resume prefetch harvest
// sandbox, whose envd init must not egress — the throwaway is also resumed with
// its volume mounts suppressed so /init does not attempt the (now-blocked) NFS
// mount. The egress proxy's head rules are left out: they extend the ordinary
// policy, and there is none here.
//
// Like ApplyRules the chain is rebuilt from scratch and committed in a single
// flush; on failure the conn is reset so a poisoned batch cannot leak into a
// later flush.
func (fw *Firewall) DenyEgress(ctx context.Context) (err error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return errFirewallClosed
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, fw.resetConn(ctx))
		}
	}()

	next := ChainDescriptor{Mode: NonDNSUDPModeOff, Posture: postureIsolated}

	return fw.replaceChainLocked(ctx, next, func(*nftables.CounterObj) error {
		// Rule 1: ESTABLISHED/RELATED → accept, as in ApplyRules (lets the
		// orchestrator-driven control path, which connects into the guest, get
		// its replies back).
		fw.addEstablishedAcceptRules()
		// Rule 2: everything else from the tap → drop, all protocols. No
		// guest-initiated egress is allowed, not even to the orchestrator IP.
		fw.addTapDropRule()

		return nil
	})
}

var maxIPv4 = netip.MustParseAddr("255.255.255.255")

// clearAndReplaceCIDRs clears a set and repopulates it with the given CIDRs.
// Buffered — nothing commits until conn.Flush() in ReplaceUserRules.
func clearAndReplaceCIDRs(conn *nftables.Conn, s set.Set, cidrs []string) error {
	toolkitCIDRs, boundaryStart := splitEgressCIDRs(cidrs)

	if len(toolkitCIDRs) == 0 {
		conn.FlushSet(s.Set())
	} else {
		data, err := set.AddressStringsToSetData(toolkitCIDRs)
		if err != nil {
			return err
		}

		if err := s.ClearAndAddElements(conn, data); err != nil {
			return err
		}
	}

	if !boundaryStart.IsValid() {
		return nil
	}

	// A lone start element (no interval end) runs to the top — nft's own encoding;
	// an explicit 255.255.255.255 end is exclusive and would drop the broadcast.
	if err := conn.SetAddElements(s.Set(), []nftables.SetElement{
		{Key: boundaryStart.AsSlice()},
	}); err != nil {
		return fmt.Errorf("add max-boundary CIDR element: %w", err)
	}

	return nil
}

// splitEgressCIDRs separates toolkit-encodable CIDRs from max-ending ranges (the
// toolkit's end.Next() overflows there). Those collapse to one [boundaryStart,
// maxIPv4] interval; ordinary CIDRs it subsumes are dropped, since the non-merge
// set silently corrupts overlapping elements in a single flush.
func splitEgressCIDRs(cidrs []string) (toolkit []string, boundaryStart netip.Addr) {
	toolkit = make([]string, 0, len(cidrs))
	for _, cidr := range cidrs {
		start, end, ok := ipv4Range(cidr)
		if ok && end == maxIPv4 {
			if !boundaryStart.IsValid() || start.Less(boundaryStart) {
				boundaryStart = start
			}

			continue
		}

		toolkit = append(toolkit, cidr)
	}

	if !boundaryStart.IsValid() {
		return toolkit, boundaryStart
	}

	kept := toolkit[:0]
	for _, cidr := range toolkit {
		if start, _, ok := ipv4Range(cidr); ok && !start.Less(boundaryStart) {
			continue
		}
		kept = append(kept, cidr)
	}

	return kept, boundaryStart
}

// ipv4Range returns the [start, end] of an IPv4 CIDR or bare address; ok is
// false for non-IPv4 or unparseable input.
func ipv4Range(cidr string) (start, end netip.Addr, ok bool) {
	if p, err := netip.ParsePrefix(cidr); err == nil {
		if !p.Addr().Is4() {
			return netip.Addr{}, netip.Addr{}, false
		}
		start, end = extnetip.Range(p.Masked())

		return start, end, true
	}
	if a, err := netip.ParseAddr(cidr); err == nil && a.Is4() {
		return a, a, true
	}

	return netip.Addr{}, netip.Addr{}, false
}
