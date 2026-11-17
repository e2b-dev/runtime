package host

// MemoryLayout is how the guest has its memory online, for the orchestrator to confirm
// that hotplugged memory went where the online policy puts it. Every field is a report:
// what cannot be read is left empty or 0, and a guest without memory hotplug reports only
// the split.
type MemoryLayout struct {
	// OnlinePolicy is memory_hotplug.online_policy ("contig-zones", "auto-movable"),
	// empty when the kernel has no such parameter.
	OnlinePolicy string `json:"policy,omitempty"`
	// AutoOnline is /sys/devices/system/memory/auto_online_blocks ("offline", "online",
	// "online_kernel", "online_movable"), empty when unreadable.
	AutoOnline string `json:"auto_online,omitempty"`
	// Movable is the memory managed in ZONE_MOVABLE, in bytes: what a later unplug can
	// reliably take back.
	Movable uint64 `json:"movable"`
	// Kernel is the memory managed in every other zone, in bytes.
	Kernel uint64 `json:"kernel"`
}
