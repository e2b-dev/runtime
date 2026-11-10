package redis

import "testing"

// TestMain turns the slot assertions on for the whole package: a command
// whose keys span cluster hash slots panics on the standalone server, as a
// cluster refuses it. A test that must send one to the server bypasses
// RunScript, as TestRedisErrorsAreCounted does.
func TestMain(m *testing.M) {
	slotAssertions.Store(true)
	m.Run()
}
