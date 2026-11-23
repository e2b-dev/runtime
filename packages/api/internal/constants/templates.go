package constants

const (
	MinTemplateCPU        = int64(1)
	MaxTemplateCPU        = int64(32)
	MinTemplateMemory     = int64(128)
	DefaultTemplateCPU    = int64(2)
	DefaultTemplateMemory = int64(1024)

	// MemoryHotplugSlotMiB is the step memory above boot RAM is added in: one
	// hotplug slot, the unit the orchestrator plugs and reserves in.
	MemoryHotplugSlotMiB = int64(128)
	// MaxMemoryHotplugMiB is the largest memory a template can add above boot RAM.
	MaxMemoryHotplugMiB = int64(128 * 1024)
)
