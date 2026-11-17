//go:build !linux

package host

// RoutableAddrCIDR mirrors the linux implementation for tests on other hosts.
const RoutableAddrCIDR = "192.0.0.8/32"

func EnsureRoutableAddr() error { return nil }
