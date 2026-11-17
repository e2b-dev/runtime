//go:build !linux

package host

// ReadMemoryLayout reports nothing on guests without Linux memory hotplug.
func ReadMemoryLayout() MemoryLayout { return MemoryLayout{} }
