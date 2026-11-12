package placement

import (
	"fmt"
)

// CapacityShortageReason classifies why placement found no node with room for
// the request. It is for server-side logs and traces only; it never changes
// the client-facing message.
type CapacityShortageReason string

const (
	// ShortageUnknown is the zero value: the shortage was not classified.
	ShortageUnknown CapacityShortageReason = ""
	// ShortageHugepagePool: every sampled node that matched CPU, features and
	// labels failed the hugepage memory admission (MemoryAdmission) — the
	// memory pool, not CPU, is the binding constraint.
	ShortageHugepagePool CapacityShortageReason = "hugepage_pool_full"
	// ShortageNodeRefusals: nodes answered ResourceExhausted (running cap,
	// starting cap, a guest still stopping, or not accepting) until the
	// deadline, rather than failing admission up front.
	ShortageNodeRefusals CapacityShortageReason = "node_refusals"
	// ShortageNoCandidate: no node remained to try — every one excluded, or
	// the cluster drained mid-placement.
	ShortageNoCandidate CapacityShortageReason = "no_candidate_node"
)

// NoNodesAvailableError reports no capacity: no candidate node at all, or
// nothing but ResourceExhausted refusals until the deadline. The diagnostic
// fields below say which, for logs and traces; they do not affect the
// client-facing message, and the zero value keeps the original behaviour.
type NoNodesAvailableError struct {
	// Reason classifies the shortage (hugepage pool vs node refusals vs no
	// candidate). ShortageUnknown when placement did not classify it.
	Reason CapacityShortageReason
	// Attempts is the completed create attempts before giving up.
	Attempts int
	// Refusals is the ResourceExhausted refusals seen across nodes.
	Refusals int
	// PoolsFull reports that at least one sampled node failed memory admission.
	PoolsFull bool
	// LastRefusal is the classified label of the last ResourceExhausted a node
	// returned (see classifyRefusal); "" when no node refused.
	LastRefusal string
}

var _ error = NoNodesAvailableError{}

// Error keeps the original wording ("no nodes available") so callers matching
// on it still work; the diagnostic fields carry the detail.
func (NoNodesAvailableError) Error() string {
	return "no nodes available"
}

// UnsupportedFeatureError reports that no orchestrator in the cluster is new
// enough for a capability the request asked for; separate from
// NoNodesAvailableError because no retry can succeed.
type UnsupportedFeatureError struct {
	Features   []string
	MinVersion string
}

var _ error = UnsupportedFeatureError{}

func (e UnsupportedFeatureError) Error() string {
	return fmt.Sprintf("no available orchestrator at version %s or above for feature(s) %v", e.MinVersion, e.Features)
}

// PlacementTimeoutError is returned when the request context expires while
// placing; Attempts counts the completed create attempts.
type PlacementTimeoutError struct {
	Attempts int
}

var _ error = PlacementTimeoutError{}

func (e PlacementTimeoutError) Error() string {
	return fmt.Sprintf("request timed out after %d placement attempt(s)", e.Attempts)
}

// SandboxCreateError is returned when every placement attempt failed; LastErr
// carries the last non-ResourceExhausted create error.
type SandboxCreateError struct {
	Attempts int
	LastErr  error
}

var _ error = SandboxCreateError{}

func (e SandboxCreateError) Error() string {
	return fmt.Sprintf("failed to create a new sandbox after %d attempt(s): %v", e.Attempts, e.LastErr)
}

func (e SandboxCreateError) Unwrap() error {
	return e.LastErr
}
