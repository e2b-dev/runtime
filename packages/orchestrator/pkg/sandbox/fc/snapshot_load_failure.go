//go:build linux

package fc

import (
	"context"
	"errors"
	"strings"

	openapiruntime "github.com/go-openapi/runtime"

	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
)

// snapshotLoadFailureReason labels a refused snapshot load. It is deliberately
// a small closed set: the fault text Firecracker returns is influenced by the
// snapshot's own contents, so it is classified here and never emitted as a
// metric attribute.
type snapshotLoadFailureReason string

const (
	// snapshotLoadVcpuMSR is an MSR in the snapshot that the reading host's
	// kernel will not accept — a kernel difference, not a CPU one. Every such
	// host refuses the same snapshot, so its template is unusable, not slow.
	snapshotLoadVcpuMSR snapshotLoadFailureReason = "vcpu_msr"
	// snapshotLoadVcpuOther is any other vCPU restore failure.
	snapshotLoadVcpuOther snapshotLoadFailureReason = "vcpu_other"
	// snapshotLoadMissingFile is a file the load had to open not existing:
	// the snapshot file, or a memory backing file. The orchestrator hands
	// Firecracker the path and Firecracker opens it, so a snapshot file removed
	// under a starting sandbox surfaces here and nowhere else.
	snapshotLoadMissingFile snapshotLoadFailureReason = "missing_file"
	// snapshotLoadBadRequest is every other refusal, and the bucket a fault we
	// have not classified lands in: Firecracker answered 400 and did not name
	// the vCPUs. Memory backend, snapshot version and a snapshot file that
	// exists but cannot be read all arrive here, as does a reworded fault from
	// a future version.
	snapshotLoadBadRequest snapshotLoadFailureReason = "bad_request"
	// snapshotLoadUnavailable is a non-400 answer: the load never reached the
	// snapshot.
	snapshotLoadUnavailable snapshotLoadFailureReason = "unavailable"
	// snapshotLoadTransport is the API socket being unreachable or answering
	// unintelligibly, which says nothing about the snapshot.
	snapshotLoadTransport snapshotLoadFailureReason = "transport"
	// snapshotLoadTimeout is the load overrunning the request deadline.
	// Firecracker never answered, but the sandbox never resumed either.
	snapshotLoadTimeout snapshotLoadFailureReason = "timeout"
	// snapshotLoadCanceled is the caller going away mid-load: not a broken
	// snapshot, and the one reason an alert leaves out.
	snapshotLoadCanceled snapshotLoadFailureReason = "canceled"
)

// classifySnapshotLoadFailure maps an error from Firecracker's PUT
// /snapshot/load to the reason attribute. err must be non-nil.
//
// Firecracker's answer is read before the context: a refusal that lands as the
// caller goes away is still a refusal.
func classifySnapshotLoadFailure(err error) snapshotLoadFailureReason {
	if badRequest, ok := errors.AsType[*operations.LoadSnapshotBadRequest](err); ok {
		return classifySnapshotLoadFault(badRequest.GetPayload())
	}

	// Every other status arrives as the default response, including the 2xx
	// codes go-swagger reports as errors because they are undeclared.
	if _, ok := errors.AsType[*operations.LoadSnapshotDefault](err); ok {
		return snapshotLoadUnavailable
	}

	if _, ok := errors.AsType[*openapiruntime.APIError](err); ok {
		return snapshotLoadUnavailable
	}

	// No answer. The deadline is the orchestrator's own request timeout, so it
	// expiring is an overrun load rather than a caller losing interest.
	if errors.Is(err, context.DeadlineExceeded) {
		return snapshotLoadTimeout
	}

	if errors.Is(err, context.Canceled) {
		return snapshotLoadCanceled
	}

	return snapshotLoadTransport
}

// classifySnapshotLoadFault reads the fault text of a 400. An absent payload
// is still a refusal, so it counts as one.
func classifySnapshotLoadFault(payload *models.Error) snapshotLoadFailureReason {
	if payload == nil {
		return snapshotLoadBadRequest
	}

	// Rust renders ENOENT as "No such file or directory (os error 2)" inside
	// the open failure ("Failed to open snapshot file: ..."). The wrapper alone
	// is not enough: it also carries a permission or I/O error on a file that
	// exists.
	if strings.Contains(payload.FaultMessage, "No such file or directory (os error 2)") {
		return snapshotLoadMissingFile
	}

	// Firecracker nests the cause: "Failed to restore from snapshot: Failed to
	// build microVM from snapshot: Failed to restore vCPUs: ... Failed to set
	// all KVM MSRs for this vCPU." Match on the vCPU stage and then on the MSR
	// step within it, so a vCPU failure for another reason is still separated
	// from a memory-backend or version refusal.
	if !strings.Contains(payload.FaultMessage, "Failed to restore vCPUs") {
		return snapshotLoadBadRequest
	}

	if strings.Contains(payload.FaultMessage, "KVM MSRs") {
		return snapshotLoadVcpuMSR
	}

	return snapshotLoadVcpuOther
}
