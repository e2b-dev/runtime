//go:build linux

package network

import (
	"github.com/coreos/go-iptables/iptables"
	"github.com/google/nftables/expr"
)

type EgressProxy interface {
	OnSlotCreate(s *Slot, tables *iptables.IPTables) error
	OnSlotDelete(s *Slot, tables *iptables.IPTables) error

	CABundle(envdVersion string) string

	// SupportsBYOP reports whether this build can tunnel TCP egress
	// through a user-supplied SOCKS5 proxy.
	SupportsBYOP() bool

	// HeadFilterRules returns rules the slot firewall installs ahead of its
	// default filter chain on every ApplyRules, never on DenyEgress.
	HeadFilterRules(s *Slot) [][]expr.Any
}

// NoopEgressProxy is a no-op implementation of EgressProxy.
type NoopEgressProxy struct{}

var _ EgressProxy = (*NoopEgressProxy)(nil)

func NewNoopEgressProxy() NoopEgressProxy {
	return NoopEgressProxy{}
}

func (NoopEgressProxy) OnSlotCreate(_ *Slot, _ *iptables.IPTables) error {
	return nil
}

func (NoopEgressProxy) OnSlotDelete(_ *Slot, _ *iptables.IPTables) error {
	return nil
}

func (NoopEgressProxy) CABundle(_ string) string {
	return ""
}

func (NoopEgressProxy) SupportsBYOP() bool {
	return false
}

func (NoopEgressProxy) HeadFilterRules(_ *Slot) [][]expr.Any {
	return nil
}
