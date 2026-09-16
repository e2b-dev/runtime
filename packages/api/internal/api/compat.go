package api

// Keep the long-standing public enum names stable when OpenAPI codegen finds
// duplicate values in newer, independently named schemas.
const (
	Kill    SandboxOnTimeout = SandboxOnTimeoutKill
	Pause   SandboxOnTimeout = SandboxOnTimeoutPause
	Paused  SandboxState     = SandboxStatePaused
	Running SandboxState     = SandboxStateRunning
)
