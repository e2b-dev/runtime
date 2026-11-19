package orchestrator

// BuildStartMemoryMB is the memory a start from a build runs with when it names
// no size, and whether the API knows it. A build without a memory maximum runs
// at boot RAM. One with a maximum records the size in start_memory_mb: boot RAM
// for a template build, the size the sandbox ran with for a snapshot build. A
// build that does not record it was made without the size; the orchestrator
// then keeps the size its own snapshot recorded, which the API does not know.
func BuildStartMemoryMB(ramMB int64, maxRAMMB, startMemoryMB *int64) (int64, bool) {
	if maxRAMMB == nil {
		return ramMB, true
	}
	if startMemoryMB == nil {
		return ramMB, false
	}

	return max(ramMB, *startMemoryMB), true
}
