package sandboxtypes

// PauseRefusalDomain is the ErrorInfo domain of a pause the orchestrator
// refused before it touched the sandbox.
const PauseRefusalDomain = "orchestrator.e2b.dev"

// PauseRefusedUnsupportedReason marks a FailedPrecondition that refused a
// pause the guest cannot take, such as a filesystem-only pause of a guest
// without a filesystem quiesce: the sandbox keeps running, and the same request will never succeed.
const PauseRefusedUnsupportedReason = "PAUSE_REFUSED_UNSUPPORTED"
