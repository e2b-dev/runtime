package host

// OOMKill is a process the guest kernel killed for running out of memory.
type OOMKill struct {
	Seq     int64  `json:"seq"`     // Kernel log sequence number
	Process string `json:"process"` // Name of the killed process
}
