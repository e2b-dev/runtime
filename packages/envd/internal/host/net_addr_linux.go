//go:build linux

package host

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// RoutableAddrCIDR is the RFC 7600 IPv4 dummy address EnsureRoutableAddr adds
// to the guest's eth0. RFC 7600 reserves 192.0.0.8 for hosts with no other
// usable IPv4 address and it is never routed, so unlike 100.64.0.0/10
// (Tailscale's own CGNAT range) or the RFC 1918 ranges it cannot clash with a
// user network or VPN.
const RoutableAddrCIDR = "192.0.0.8/32"

// routableAddrLink is the guest's network interface, configured by the
// orchestrator through the kernel's ip= argument (169.254.0.21/30).
const routableAddrLink = "eth0"

// EnsureRoutableAddr adds the RFC 7600 dummy address (192.0.0.8/32) to eth0
// with IFA_F_NOPREFIXROUTE.
//
// The guest's only IPv4 address is link-local, and software that requires a
// non-link-local address to consider the host online treats the sandbox as
// offline. The reported case is Tailscale: its network monitor does not count
// 169.254.0.0/16 as usable, so tailscaled pauses its control client before
// login and auth keys silently never reach the control plane — even though
// outbound connectivity is fine.
//
// The address is added noprefixroute, so no prefix route is created: egress
// keeps selecting the gateway-subnet address (169.254.0.21) as its source and
// nothing routes to 192.0.0.8, leaving inbound traffic unchanged.
//
// Idempotent across the sandbox lifecycle — a cold boot runs envd again and a
// repeated add is a no-op (EEXIST), while a memory-snapshot resume keeps the
// address because it lives in kernel state. It talks to NETLINK_ROUTE directly
// rather than shelling out to `ip addr`, because some base images ship no
// iproute2 and envd (unlike the orchestrator) does not depend on a netlink
// library; golang.org/x/sys/unix is already an envd dependency.
func EnsureRoutableAddr() error {
	return ensureRoutableAddr(routableAddrLink, RoutableAddrCIDR)
}

func ensureRoutableAddr(linkName, cidr string) error {
	ip, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return fmt.Errorf("parse dummy address %s: %w", cidr, err)
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return fmt.Errorf("dummy address %s is not IPv4", cidr)
	}
	link, err := net.InterfaceByName(linkName)
	if err != nil {
		return fmt.Errorf("find link %s: %w", linkName, err)
	}
	prefixLen, _ := ipnet.Mask.Size()

	msg := buildAddrMsg(unix.RTM_NEWADDR, unix.NLM_F_CREATE|unix.NLM_F_EXCL,
		link.Index, uint8(prefixLen), ip4)

	return sendAddrMsg(msg)
}

// buildAddrMsg encodes an RTM_NEWADDR request for a single IPv4 address,
// mirroring what `ip addr add <ip>/32 dev <link> noprefixroute` sends. The
// 8-bit IfAddrmsg flags field cannot hold IFA_F_NOPREFIXROUTE (0x200), so it
// travels in the IFA_FLAGS attribute; IFA_ADDRESS and IFA_LOCAL carry the same
// address (this is not point-to-point); and a /32 gets no IFA_BROADCAST (the
// kernel only wants one for prefixes shorter than /31).
func buildAddrMsg(msgType uint16, extraFlags uint16, ifIndex int, prefixLen uint8, ip4 net.IP) []byte {
	const (
		hdrLen  = unix.SizeofNlMsghdr  // 16
		ifaLen  = unix.SizeofIfAddrmsg // 8
		ipLen   = net.IPv4len          // payload of IFA_LOCAL / IFA_ADDRESS
		flagLen = 4                    // payload of IFA_FLAGS (uint32)
	)
	attrLen := func(payload int) int { return unix.SizeofRtAttr + payload + nlaPad(payload) }
	total := hdrLen + ifaLen +
		attrLen(ipLen) + // IFA_LOCAL
		attrLen(ipLen) + // IFA_ADDRESS
		attrLen(flagLen) // IFA_FLAGS

	buf := make([]byte, total)

	// nlmsghdr
	binary.NativeEndian.PutUint32(buf[0:4], uint32(total))
	binary.NativeEndian.PutUint16(buf[4:6], msgType)
	binary.NativeEndian.PutUint16(buf[6:8], unix.NLM_F_REQUEST|unix.NLM_F_ACK|extraFlags)
	binary.NativeEndian.PutUint32(buf[8:12], 1) // seq
	binary.NativeEndian.PutUint32(buf[12:16], 0)

	// ifaddrmsg
	off := hdrLen
	buf[off] = unix.AF_INET
	buf[off+1] = prefixLen
	buf[off+2] = 0 // legacy 8-bit flags; NOPREFIXROUTE goes into IFA_FLAGS
	buf[off+3] = unix.RT_SCOPE_UNIVERSE
	binary.NativeEndian.PutUint32(buf[off+4:off+ifaLen], uint32(ifIndex))
	off += ifaLen

	putAttr := func(typ uint16, payload []byte) {
		binary.NativeEndian.PutUint16(buf[off:off+2], uint16(unix.SizeofRtAttr+len(payload)))
		binary.NativeEndian.PutUint16(buf[off+2:off+4], typ)
		copy(buf[off+unix.SizeofRtAttr:], payload)
		off += unix.SizeofRtAttr + len(payload) + nlaPad(len(payload))
	}
	putAttr(unix.IFA_LOCAL, ip4)
	putAttr(unix.IFA_ADDRESS, ip4)
	flags := make([]byte, flagLen)
	binary.NativeEndian.PutUint32(flags, unix.IFA_F_NOPREFIXROUTE)
	putAttr(unix.IFA_FLAGS, flags)

	return buf
}

// nlaPad is the padding aligning the next netlink attribute to NLA_ALIGNTO.
func nlaPad(payloadLen int) int {
	return -payloadLen & (unix.NLA_ALIGNTO - 1)
}

// sendAddrMsg sends one address request to NETLINK_ROUTE and waits for the
// kernel ACK. EEXIST means the address is already configured (a re-exec'd envd
// after a live upgrade), which satisfies the caller just like success.
func sendAddrMsg(msg []byte) error {
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	if err != nil {
		return fmt.Errorf("open netlink route socket: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()

	if err := unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return fmt.Errorf("bind netlink route socket: %w", err)
	}
	if err := unix.Sendto(fd, msg, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}); err != nil {
		return fmt.Errorf("send netlink address request: %w", err)
	}

	buf := make([]byte, 4096)
	for {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			return fmt.Errorf("receive netlink ack: %w", err)
		}
		if n < unix.SizeofNlMsghdr+4 {
			return errors.New("short netlink ack")
		}
		switch msgType := binary.NativeEndian.Uint16(buf[4:6]); msgType {
		case unix.NLMSG_NOOP:
			continue
		case unix.NLMSG_ERROR:
			errno := int32(binary.NativeEndian.Uint32(buf[unix.SizeofNlMsghdr : unix.SizeofNlMsghdr+4]))
			if errno == 0 {
				return nil // ACK, no error
			}
			nlErr := unix.Errno(-errno)
			if errors.Is(nlErr, unix.EEXIST) {
				return nil
			}

			return fmt.Errorf("netlink address request: %w", nlErr)
		default:
			return fmt.Errorf("unexpected netlink reply type %d", msgType)
		}
	}
}
