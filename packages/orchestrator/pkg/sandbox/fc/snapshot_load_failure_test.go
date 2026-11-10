//go:build linux

package fc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	openapiruntime "github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
)

// Firecracker's fault for an MSR the reading host's kernel refuses. Kept whole,
// as it arrives: the classifier matches two nested stages inside it.
const msrFault = "Load snapshot error: Failed to restore from snapshot: Failed to build microVM from snapshot: Failed to restore vCPUs: Failed to run action on vcpu: Failed to set all KVM MSRs for this vCPU. Only a partial write was done."

func badRequest(fault string) error {
	return &operations.LoadSnapshotBadRequest{Payload: &models.Error{FaultMessage: fault}}
}

func TestClassifySnapshotLoadFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want snapshotLoadFailureReason
	}{
		{
			name: "msr fault names the vcpu and the msr step",
			err:  badRequest(msrFault),
			want: snapshotLoadVcpuMSR,
		},
		{
			// The reason exists so that an MSR alert does not absorb every
			// other way vCPU restore can fail.
			name: "vcpu restore failing elsewhere is not an msr failure",
			err:  badRequest("Load snapshot error: Failed to restore from snapshot: Failed to build microVM from snapshot: Failed to restore vCPUs: Failed to run action on vcpu: Failed to set the TSC frequency."),
			want: snapshotLoadVcpuOther,
		},
		{
			name: "refusal that does not reach the vcpus",
			err:  badRequest("Load snapshot error: Failed to restore from snapshot: Cannot deserialize the microVM state: Snapshot file was created by a different version"),
			want: snapshotLoadBadRequest,
		},
		{
			// A snapshot file removed under a starting sandbox: Firecracker
			// opens the path the orchestrator passes, so this is where it shows.
			name: "snapshot file that does not exist",
			err:  badRequest("Load snapshot error: Failed to restore from snapshot: Failed to open snapshot file: No such file or directory (os error 2)"),
			want: snapshotLoadMissingFile,
		},
		{
			name: "memory backing file that does not exist",
			err:  badRequest("Load snapshot error: Failed to restore from snapshot: Failed to open memory backing file: No such file or directory (os error 2)"),
			want: snapshotLoadMissingFile,
		},
		{
			// The open-failure wrapper also carries errors on a file that
			// exists; only ENOENT is a missing file.
			name: "snapshot file that exists but cannot be opened",
			err:  badRequest("Load snapshot error: Failed to restore from snapshot: Failed to open snapshot file: Permission denied (os error 13)"),
			want: snapshotLoadBadRequest,
		},
		{
			// A later Firecracker rewording the fault must still count as a
			// refusal, so the total stays complete and only the breakdown
			// loses precision.
			name: "unrecognized fault text still counts as a refusal",
			err:  badRequest("Load snapshot error: something we have never seen"),
			want: snapshotLoadBadRequest,
		},
		{
			name: "400 without a payload is still a refusal",
			err:  &operations.LoadSnapshotBadRequest{},
			want: snapshotLoadBadRequest,
		},
		{
			name: "wrapped fault is classified through the wrapping",
			err:  fmt.Errorf("error loading snapshot: %w", badRequest(msrFault)),
			want: snapshotLoadVcpuMSR,
		},
		{
			name: "non-400 status never reached the snapshot",
			err:  operations.NewLoadSnapshotDefault(500),
			want: snapshotLoadUnavailable,
		},
		{
			name: "api error never reached the snapshot",
			err:  openapiruntime.NewAPIError("load snapshot", nil, 503),
			want: snapshotLoadUnavailable,
		},
		{
			name: "unreachable socket says nothing about the snapshot",
			err:  fmt.Errorf("post unix socket: %w", &net.OpError{Op: "dial", Err: errors.New("connection refused")}),
			want: snapshotLoadTransport,
		},
		{
			// The one reason an alert leaves out, so it must not absorb a load
			// that never finished.
			name: "caller cancellation is not a broken snapshot",
			err:  fmt.Errorf("load snapshot: %w", context.Canceled),
			want: snapshotLoadCanceled,
		},
		{
			// The orchestrator's own request timeout: the load overran.
			name: "deadline is a load that never finished",
			err:  fmt.Errorf("load snapshot: %w", context.DeadlineExceeded),
			want: snapshotLoadTimeout,
		},
		{
			// Firecracker can answer while the caller is already gone. Reading
			// the cancel instead would drop a dead template from the total.
			name: "an answer outranks a cancel that arrived with it",
			err:  fmt.Errorf("load snapshot: %w", errors.Join(badRequest(msrFault), context.Canceled)),
			want: snapshotLoadVcpuMSR,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, classifySnapshotLoadFailure(tc.err))
		})
	}
}
