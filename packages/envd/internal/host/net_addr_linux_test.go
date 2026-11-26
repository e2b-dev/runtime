//go:build linux

package host

import (
	"encoding/binary"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestNlaPad(t *testing.T) {
	assert.Equal(t, 0, nlaPad(0))
	assert.Equal(t, 3, nlaPad(1))
	assert.Equal(t, 2, nlaPad(2))
	assert.Equal(t, 1, nlaPad(3))
	assert.Equal(t, 0, nlaPad(4))
	assert.Equal(t, 3, nlaPad(5))
}

// TestBuildAddrMsgEncodesNoPrefixRoute checks the request against the wire
// format `ip addr add 192.0.0.8/32 dev <link> noprefixroute` produces: the
// flag must travel in the IFA_FLAGS attribute (it does not fit the 8-bit
// legacy field), and a /32 must carry no IFA_BROADCAST.
func TestBuildAddrMsgEncodesNoPrefixRoute(t *testing.T) {
	ip4 := net.IPv4(192, 0, 0, 8).To4()
	msg := buildAddrMsg(unix.RTM_NEWADDR, unix.NLM_F_CREATE|unix.NLM_F_EXCL, 7, 32, ip4)

	// nlmsghdr
	assert.Len(t, msg, int(binary.NativeEndian.Uint32(msg[0:4])))
	assert.EqualValues(t, unix.RTM_NEWADDR, binary.NativeEndian.Uint16(msg[4:6]))
	flags := binary.NativeEndian.Uint16(msg[6:8])
	for _, want := range []uint16{unix.NLM_F_REQUEST, unix.NLM_F_ACK, unix.NLM_F_CREATE, unix.NLM_F_EXCL} {
		assert.NotZero(t, flags&want)
	}

	// ifaddrmsg
	off := unix.SizeofNlMsghdr
	assert.EqualValues(t, unix.AF_INET, msg[off])
	assert.EqualValues(t, 32, msg[off+1], "prefixlen")
	assert.Zero(t, msg[off+2], "legacy flags field must stay zero; NOPREFIXROUTE goes into IFA_FLAGS")
	assert.EqualValues(t, unix.RT_SCOPE_UNIVERSE, msg[off+3])
	assert.EqualValues(t, 7, binary.NativeEndian.Uint32(msg[off+4:off+8]))
	off += unix.SizeofIfAddrmsg

	// rtattr walk
	attrs := map[uint16][]byte{}
	for off < len(msg) {
		attrLen := int(binary.NativeEndian.Uint16(msg[off : off+2]))
		attrType := binary.NativeEndian.Uint16(msg[off+2 : off+4])
		require.GreaterOrEqual(t, attrLen, unix.SizeofRtAttr)
		attrs[attrType] = msg[off+unix.SizeofRtAttr : off+attrLen]
		off += attrLen + nlaPad(attrLen-unix.SizeofRtAttr)
	}

	assert.Equal(t, ip4, net.IP(attrs[unix.IFA_LOCAL]))
	assert.Equal(t, ip4, net.IP(attrs[unix.IFA_ADDRESS]))
	require.Contains(t, attrs, uint16(unix.IFA_FLAGS))
	assert.EqualValues(t, unix.IFA_F_NOPREFIXROUTE, binary.NativeEndian.Uint32(attrs[unix.IFA_FLAGS]))
	assert.NotContains(t, attrs, uint16(unix.IFA_BROADCAST), "a /32 gets no broadcast address")
}

func TestEnsureRoutableAddrRejectsBadInput(t *testing.T) {
	// Neither case needs root: parsing and the link lookup fail first.
	assert.Error(t, ensureRoutableAddr("eth0", "not-a-cidr"))
	assert.Error(t, ensureRoutableAddr("e2b-nonexistent0", RoutableAddrCIDR))
	assert.Error(t, ensureRoutableAddr("lo", "2001:db8::1/128"), "the dummy address contract is IPv4-only")
}

// TestEnsureRoutableAddrRoundTrip exercises the real netlink path against the
// loopback interface: add, re-add (the EEXIST branch a re-exec'd envd hits),
// then delete. Needs root and mutates only this interface's address list.
func TestEnsureRoutableAddrRoundTrip(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("skipping netlink address round trip: needs root")
	}

	ip4 := net.IPv4(192, 0, 0, 8).To4()
	lo, err := net.InterfaceByName("lo")
	require.NoError(t, err)

	del := buildAddrMsg(unix.RTM_DELADDR, 0, lo.Index, 32, ip4)
	// Start and end clean regardless of how the test exits.
	_ = sendAddrMsg(del) // best-effort cleanup of a possibly absent address
	t.Cleanup(func() { _ = sendAddrMsg(del) })

	require.NoError(t, ensureRoutableAddr("lo", RoutableAddrCIDR))
	assert.True(t, hasAddr(t, lo.Index, "192.0.0.8"), "the dummy address must be on the link after the add")

	// A second add is the resume/re-exec case: EEXIST must read as success.
	assert.NoError(t, ensureRoutableAddr("lo", RoutableAddrCIDR))

	require.NoError(t, sendAddrMsg(del))
	assert.False(t, hasAddr(t, lo.Index, "192.0.0.8"), "the address must be gone after the delete")
}

// hasAddr dumps the link's addresses over netlink and looks for ip.
func hasAddr(t *testing.T, ifIndex int, ip string) bool {
	t.Helper()

	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC, unix.NETLINK_ROUTE)
	require.NoError(t, err)
	defer func() { _ = unix.Close(fd) }()
	require.NoError(t, unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}))

	req := make([]byte, unix.SizeofNlMsghdr+unix.SizeofIfAddrmsg)
	binary.NativeEndian.PutUint32(req[0:4], uint32(len(req)))
	binary.NativeEndian.PutUint16(req[4:6], unix.RTM_GETADDR)
	binary.NativeEndian.PutUint16(req[6:8], unix.NLM_F_REQUEST|unix.NLM_F_DUMP)
	binary.NativeEndian.PutUint32(req[8:12], 1)
	req[unix.SizeofNlMsghdr+0] = unix.AF_INET
	require.NoError(t, unix.Sendto(fd, req, 0, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}))

	want := net.ParseIP(ip).To4()
	buf := make([]byte, 32*1024)
	for {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		require.NoError(t, err)
		if n == 0 {
			return false
		}
		for off := 0; off+unix.SizeofNlMsghdr <= n; {
			msgLen := int(binary.NativeEndian.Uint32(buf[off : off+4]))
			msgType := binary.NativeEndian.Uint16(buf[off+4 : off+6])
			if msgType == unix.NLMSG_DONE {
				return false
			}
			require.EqualValues(t, unix.RTM_NEWADDR, msgType, "dump replies are RTM_NEWADDR messages")
			body := buf[off+unix.SizeofNlMsghdr : off+msgLen]
			if len(body) >= unix.SizeofIfAddrmsg &&
				int(binary.NativeEndian.Uint32(body[4:8])) == ifIndex {
				for a := unix.SizeofIfAddrmsg; a+unix.SizeofRtAttr <= len(body); {
					attrLen := int(binary.NativeEndian.Uint16(body[a : a+2]))
					attrType := binary.NativeEndian.Uint16(body[a+2 : a+4])
					if attrLen < unix.SizeofRtAttr {
						break
					}
					if attrType == unix.IFA_ADDRESS &&
						net.IP(body[a+unix.SizeofRtAttr:a+attrLen]).Equal(want) {
						return true
					}
					a += attrLen + nlaPad(attrLen-unix.SizeofRtAttr)
				}
			}
			off += msgLen + nlaPad(msgLen)
		}
	}
}
