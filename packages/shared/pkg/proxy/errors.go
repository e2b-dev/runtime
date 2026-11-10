package proxy

import (
	"errors"
)

var (
	ErrInvalidHost      = errors.New("invalid url host")
	ErrInvalidSandboxID = errors.New("invalid sandbox ID")
)

type InvalidSandboxPortError struct {
	Port    string
	wrapped error
}

func (e InvalidSandboxPortError) Error() string {
	return "invalid sandbox port"
}

func (e InvalidSandboxPortError) Unwrap() error {
	return e.wrapped
}

// Presence is what a proxy knows about whether it has a sandbox, and what it
// answers a probe with (see HeaderProbe).
type Presence uint8

const (
	// PresenceUnknown is an answer given before the sandbox was looked up,
	// or after a lookup that failed.
	PresenceUnknown Presence = iota
	// PresenceAbsent is a sandbox looked up and not found.
	PresenceAbsent
	// PresenceHeld is a sandbox found, whether or not the request reaches it.
	PresenceHeld
)

// SandboxError is a request for a sandbox that the proxy answers itself
// instead of forwarding. Each implementation is a discriminator: it names what
// happened, the sandbox, and its own Presence, and the handler picks the
// response from its type (writeSandboxError). The unexported sandboxError
// closes the set to this package, and gochecksumtype fails the build of a
// type switch over it that misses a type.
//
//sumtype:decl
type SandboxError interface {
	error
	SandboxID() string
	Presence() Presence
	sandboxError()
}

var (
	_ SandboxError = (*SandboxNotFoundError)(nil)
	_ SandboxError = (*SandboxResumePermissionDeniedError)(nil)
	_ SandboxError = (*SandboxStillTransitioningError)(nil)
	_ SandboxError = (*TrafficAccessTokenError)(nil)
	_ SandboxError = (*InternalRouteError)(nil)
	_ SandboxError = (*SandboxCapacityError)(nil)
	_ SandboxError = (*SandboxPlacementRefusedError)(nil)
	_ SandboxError = (*SandboxResourceExhaustedError)(nil)
)

// SandboxNotFoundError answers the not-found page. The page is the same
// whatever its presence; only a probe learns the difference, so a sandbox
// this proxy holds is never reported as elsewhere.
type SandboxNotFoundError struct {
	sandboxID string
	presence  Presence
}

// NewErrSandboxNotFound reports a sandbox the proxy cannot route. presence is
// what the proxy knows: PresenceAbsent for a sandbox it looked up and does
// not have, PresenceHeld for one it has without a usable route, and
// PresenceUnknown for a lookup that could not tell.
func NewErrSandboxNotFound(sandboxId string, presence Presence) *SandboxNotFoundError {
	return &SandboxNotFoundError{sandboxID: sandboxId, presence: presence}
}

func (e SandboxNotFoundError) Error() string {
	return "sandbox not found"
}

func (e SandboxNotFoundError) SandboxID() string { return e.sandboxID }

func (e SandboxNotFoundError) Presence() Presence { return e.presence }

func (SandboxNotFoundError) sandboxError() {}

// SandboxResumePermissionDeniedError means the control plane found the
// sandbox and refused to resume it. cause is the control plane's refusal,
// kept so the log says why; the page never shows it.
type SandboxResumePermissionDeniedError struct {
	sandboxID string
	cause     error
}

func NewErrSandboxResumePermissionDenied(sandboxId string, cause error) *SandboxResumePermissionDeniedError {
	return &SandboxResumePermissionDeniedError{sandboxID: sandboxId, cause: cause}
}

func (e SandboxResumePermissionDeniedError) Error() string {
	if e.cause == nil {
		return "sandbox resume permission denied"
	}

	return "sandbox resume permission denied: " + e.cause.Error()
}

func (e SandboxResumePermissionDeniedError) Unwrap() error { return e.cause }

func (e SandboxResumePermissionDeniedError) SandboxID() string { return e.sandboxID }

func (SandboxResumePermissionDeniedError) Presence() Presence { return PresenceHeld }

func (SandboxResumePermissionDeniedError) sandboxError() {}

// SandboxStillTransitioningError means the sandbox is pausing and a retry
// in a moment will find it. Unavailable renders it as 503 instead of 409:
// the SDK reads a 409 on the envd port as "already exists".
type SandboxStillTransitioningError struct {
	sandboxID   string
	Unavailable bool
}

func NewErrSandboxStillTransitioning(sandboxId string) *SandboxStillTransitioningError {
	return &SandboxStillTransitioningError{sandboxID: sandboxId}
}

func NewErrSandboxStillTransitioningUnavailable(sandboxId string) *SandboxStillTransitioningError {
	return &SandboxStillTransitioningError{sandboxID: sandboxId, Unavailable: true}
}

func (e SandboxStillTransitioningError) Error() string {
	return "sandbox is still transitioning"
}

func (e SandboxStillTransitioningError) SandboxID() string { return e.sandboxID }

func (SandboxStillTransitioningError) Presence() Presence { return PresenceHeld }

func (SandboxStillTransitioningError) sandboxError() {}

// TrafficAccessTokenError means the request lacks the sandbox's traffic
// access token, or carries a different one.
type TrafficAccessTokenError struct {
	sandboxID string
	Header    string
	Missing   bool
}

func NewErrMissingTrafficAccessToken(sandboxId, header string) *TrafficAccessTokenError {
	return &TrafficAccessTokenError{sandboxID: sandboxId, Header: header, Missing: true}
}

func NewErrInvalidTrafficAccessToken(sandboxId, header string) *TrafficAccessTokenError {
	return &TrafficAccessTokenError{sandboxID: sandboxId, Header: header}
}

func (e TrafficAccessTokenError) Error() string {
	if e.Missing {
		return "missing traffic access token"
	}

	return "invalid traffic access token"
}

func (e TrafficAccessTokenError) SandboxID() string { return e.sandboxID }

func (TrafficAccessTokenError) Presence() Presence { return PresenceHeld }

func (TrafficAccessTokenError) sandboxError() {}

// InternalRouteError means the request addressed a route reserved for the
// control plane, which nothing reaching the proxy is entitled to call. It is
// named for what happened so the logs say so; for why the response is a 404 that
// still explains itself, see template.NewInternalRouteError.
type InternalRouteError struct {
	sandboxID string
	Path      string
}

func NewErrInternalRoute(sandboxId, path string) *InternalRouteError {
	return &InternalRouteError{sandboxID: sandboxId, Path: path}
}

func (e InternalRouteError) Error() string {
	return "internal route is not reachable through the proxy"
}

func (e InternalRouteError) SandboxID() string { return e.sandboxID }

// Presence is unknown: the route is refused before the lookup.
func (InternalRouteError) Presence() Presence { return PresenceUnknown }

func (InternalRouteError) sandboxError() {}

// SandboxCapacityError means the resume could not be carried out right now
// (no node could take the sandbox, the control plane was unreachable, or the
// proxy had no room to wait); unlike SandboxResourceExhaustedError it is not
// the team's limit and clears on its own. Message is the control plane's own
// wording, kept for the log; the page never shows it.
type SandboxCapacityError struct {
	sandboxID string
	Message   string
	presence  Presence
	// Shed marks a request turned away because the proxy had no room to
	// wait; its answer carries no Retry-After, so clients that honour the
	// header do not come back in one wave while the lot is still full, and
	// an error_code, so it is told from the capacity answer without one.
	Shed bool
}

// NewErrSandboxCapacity reports a resume that could not be carried out right
// now. presence is what the proxy knows: PresenceHeld for a placement the API
// attempted for a sandbox it found and that a retry may complete (a placement
// timeout, a create that failed on the node), and PresenceUnknown for a
// failure before or without the API's answer (an unreachable API, a proxy
// overloaded or shutting down).
func NewErrSandboxCapacity(sandboxId string, presence Presence, message string) *SandboxCapacityError {
	return &SandboxCapacityError{sandboxID: sandboxId, Message: message, presence: presence}
}

func NewErrSandboxShed(sandboxId string, message string) *SandboxCapacityError {
	return &SandboxCapacityError{sandboxID: sandboxId, Message: message, Shed: true}
}

func (e SandboxCapacityError) Error() string {
	return "sandbox cannot be resumed right now"
}

func (e SandboxCapacityError) SandboxID() string { return e.sandboxID }

func (e SandboxCapacityError) Presence() Presence { return e.presence }

func (SandboxCapacityError) sandboxError() {}

// SandboxPlacementRefusedError means no node in the cluster can ever take
// the sandbox (it needs a feature none of them supports), so no retry helps
// and the answer carries no retry hint.
type SandboxPlacementRefusedError struct {
	sandboxID string
	// Message is the control plane's own wording, kept for the log.
	Message string
	// ErrorCode is the API's code for the refusal, shown to clients.
	ErrorCode string
	// StatusCode is the HTTP status the API answered the refusal with, so
	// one it answered as a bad request is not a 503 the client retries;
	// zero means 503.
	StatusCode int
}

func NewErrSandboxPlacementRefused(sandboxId string, message string, errorCode string, statusCode int) *SandboxPlacementRefusedError {
	return &SandboxPlacementRefusedError{
		sandboxID:  sandboxId,
		Message:    message,
		ErrorCode:  errorCode,
		StatusCode: statusCode,
	}
}

func (e SandboxPlacementRefusedError) Error() string {
	return "sandbox cannot be placed on this cluster"
}

func (e SandboxPlacementRefusedError) SandboxID() string { return e.sandboxID }

func (SandboxPlacementRefusedError) Presence() Presence { return PresenceHeld }

func (SandboxPlacementRefusedError) sandboxError() {}

type SandboxResourceExhaustedError struct {
	sandboxID string
	Message   string
}

func NewErrSandboxResourceExhausted(sandboxId string, message string) *SandboxResourceExhaustedError {
	return &SandboxResourceExhaustedError{sandboxID: sandboxId, Message: message}
}

func (e SandboxResourceExhaustedError) Error() string {
	return "sandbox resource exhausted"
}

func (e SandboxResourceExhaustedError) SandboxID() string { return e.sandboxID }

func (SandboxResourceExhaustedError) Presence() Presence { return PresenceHeld }

func (SandboxResourceExhaustedError) sandboxError() {}
