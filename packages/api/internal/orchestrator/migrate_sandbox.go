package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/sandboxmigration"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

const (
	// migrationCallTimeout bounds every call to the migration service but
	// Migrate, so a service that accepts connections and stops answering
	// cannot hold a request or the poller forever.
	migrationCallTimeout = 5 * time.Second
	// migrateCallTimeout bounds one Migrate call. The manager describes the
	// sandbox on the source before it accepts, so Migrate takes longer than
	// the other calls.
	migrateCallTimeout = 10 * time.Second
	// migrateAttempts bounds the Migrate calls of one migration while the
	// answer is undecided or Aborted. Every call repeats the migration ID, and the
	// manager answers a repeated ID with the migration it already accepted.
	migrateAttempts = 3
	// migrationSettleTimeout bounds the record writes of one migration's
	// outcome, apart from the call that listed it: a slow list leaves the
	// settlement its own time, and one slow record does not starve the rest
	// of the list.
	migrationSettleTimeout = 5 * time.Second
)

var (
	errMigrationNotConfigured = errors.New("sandbox migration is not configured")
	errNotMigratable          = errors.New("the sandbox cannot be migrated")
	errNodeUnavailable        = errors.New("the node is not available for a migration")
	errManagerUnavailable     = errors.New("the migration service is not available")
	// errMigrationUndecided says Migrate failed without a refusal: the
	// service may still accept it, so the record stays held.
	errMigrationUndecided = errors.New("the migration may have been accepted, the hold is kept")
)

// StartMigration accepts a migration of the sandbox's running execution to
// targetNodeID. It reads the record, requires State running, a Full-Router
// NetworkPlacement with a Router ID, a source node and a target node both
// known to this client with distinct service instances and room on the
// target, and checks the migration service answers. It then holds the record
// under a new migration ID until now + operation budget + termination budget,
// both read from the team's flags, and calls Migrate with those coordinates
// and the operation deadline. It returns the Migration.
//
// The manager runs a termination on its own budget. The hold outlives the
// operation deadline by the termination budget, so the poller reads every
// placement while the record is held. A termination that stopped the guest
// and is read after the hold lapsed is ended by the poller without the hold.
//
// Migrate is called up to migrateAttempts times while its answer is
// undecided or Aborted. A refusal releases the hold and returns the error.
// Aborted means the source did not describe the execution in time and the
// manager wrote nothing: an Aborted last answer releases the hold too, since
// the API is the only caller of the migration ID and its tries are
// sequential, so no request of that ID was accepted. Other undecided answers
// on every attempt keep the hold and return errMigrationUndecided.
//
// The target must accept new requests. The source must be ready or draining:
// a draining node still serves its sandboxes and is the usual one to migrate
// off.
func (o *Orchestrator) StartMigration(ctx context.Context, teamID uuid.UUID, sandboxID, targetNodeID string) (*sandboxmigration.Migration, error) {
	if o.migrations == nil {
		return nil, fmt.Errorf("start a migration of sandbox %q: %w", sandboxID, errMigrationNotConfigured)
	}

	sbx, err := o.sandboxStore.Get(ctx, teamID, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("read the sandbox: %w", err)
	}
	if sbx.State != sandbox.StateRunning {
		return nil, fmt.Errorf("%w: it is %s", errNotMigratable, sbx.State)
	}
	network := sbx.NetworkPlacement
	if network.GetExecutionId() != sbx.ExecutionID ||
		catalog.NetworkMode(network.GetNetworkMode()) != catalog.NetworkModeFullRouter ||
		network.GetRouterId() == "" {
		return nil, fmt.Errorf("%w: its execution is not served by a Router", errNotMigratable)
	}

	source, _, err := o.migrationParticipant(sbx.ClusterID, sbx.NodeID, func(n *nodemanager.Node) bool {
		status := n.Status()

		return status == api.NodeStatusReady || status == api.NodeStatusDraining
	})
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	target, targetNode, err := o.migrationParticipant(sbx.ClusterID, targetNodeID, (*nodemanager.Node).CanAcceptNewRequests)
	if err != nil {
		return nil, fmt.Errorf("target: %w", err)
	}
	if source.GetNodeId() == target.GetNodeId() || source.GetServiceInstanceId() == target.GetServiceInstanceId() {
		return nil, fmt.Errorf("%w: the target is the node it runs on", errNotMigratable)
	}
	// The same admission a create runs: the execution needs room in the
	// target's hugepage pool.
	node := sbx.ToNodeSandbox()
	resources := nodemanager.SandboxResources{CPUs: node.VCpu, MiBMemory: node.RamMB, HugePages: node.HugePages}
	memory := placement.MemoryRequirementFor(ctx, o.featureFlagsClient, teamID, sandboxID)
	if ok, _ := placement.MemoryAdmission(targetNode, resources, memory); !ok {
		return nil, fmt.Errorf("%w: the target has no memory for the execution", errNodeUnavailable)
	}

	migrationID := uuid.NewString()
	if err := o.migrationServiceReachable(ctx, migrationID); err != nil {
		return nil, err
	}

	team := featureflags.TeamContext(teamID.String())
	operationBudget := time.Duration(o.featureFlagsClient.IntFlag(ctx, featureflags.SandboxMigrationOperationBudgetSeconds, team)) * time.Second
	terminationBudget := time.Duration(o.featureFlagsClient.IntFlag(ctx, featureflags.SandboxMigrationTerminationBudgetSeconds, team)) * time.Second
	hold := sandbox.Hold{TeamID: teamID, SandboxID: sandboxID, ExecutionID: sbx.ExecutionID, ID: migrationID}
	deadline := time.Now().Add(operationBudget)
	holdDeadline := deadline.Add(terminationBudget)
	if err := o.sandboxStore.HoldExecution(ctx, hold, holdDeadline); err != nil {
		return nil, fmt.Errorf("hold the sandbox: %w", err)
	}

	req := sandboxmigration.MigrateRequest_builder{
		MigrationId:       migrationID,
		SandboxId:         sandboxID,
		ExecutionId:       sbx.ExecutionID,
		TeamId:            teamID.String(),
		Source:            source,
		Target:            target,
		RouterId:          network.GetRouterId(),
		OperationDeadline: timestamppb.New(deadline),
	}.Build()
	var resp *sandboxmigration.MigrateResponse
	for range migrateAttempts {
		migrateCtx, cancel := context.WithTimeout(ctx, migrateCallTimeout)
		resp, err = o.migrations.Migrate(migrateCtx, req)
		cancel()
		if err == nil || migrationRefused(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		err = fmt.Errorf("start the migration: %w", err)
		// Only a refusal or an Aborted last answer says the migration service
		// holds nothing. Any other failure, a lost reply or a timeout
		// included, may still turn into an accepted migration after this call
		// returns, so the hold stays: the poll settles or releases it like any
		// other, and the hold's own deadline bounds the wait when the
		// migration never appears.
		if status.Code(err) == codes.Aborted {
			err = fmt.Errorf("source node %q did not describe the sandbox for the migration: %w", source.GetNodeId(), err)
		} else if !migrationRefused(err) {
			return nil, errors.Join(err, errMigrationUndecided)
		}
		if releaseErr := o.sandboxStore.ReleaseHeldExecution(context.WithoutCancel(ctx), hold); releaseErr != nil {
			return nil, errors.Join(err, fmt.Errorf("release the hold: %w", releaseErr))
		}

		return nil, err
	}

	return resp.GetMigration(), nil
}

// migrationServiceReachable asks the migration service for the migration
// about to be started, which it cannot know yet. Only NotFound shows the
// service answers, so anything else stops StartMigration before the record is
// held.
func (o *Orchestrator) migrationServiceReachable(ctx context.Context, migrationID string) error {
	ctx, cancel := context.WithTimeout(ctx, migrationCallTimeout)
	defer cancel()

	_, err := o.migrations.GetMigration(ctx, sandboxmigration.GetMigrationRequest_builder{MigrationId: migrationID}.Build())
	if status.Code(err) == codes.NotFound {
		return nil
	}
	if err == nil {
		err = errors.New("it reports a migration under a new ID")
	}

	return fmt.Errorf("%w: %w", errManagerUnavailable, err)
}

// migrationRefused reports a Migrate the migration service answered and did
// not accept.
func migrationRefused(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.PermissionDenied, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

// migrationParticipant resolves nodeID to the orchestrator process this
// client knows for it, when usable says the node may take part, and returns
// the node it resolved: a node the sync replaces meanwhile is not read again.
func (o *Orchestrator) migrationParticipant(clusterID uuid.UUID, nodeID string, usable func(*nodemanager.Node) bool) (*sandboxmigration.Participant, *nodemanager.Node, error) {
	node := o.GetNode(clusterID, nodeID)
	if node == nil {
		return nil, nil, fmt.Errorf("%w: node %q is unknown", errNodeUnavailable, nodeID)
	}
	if !usable(node) {
		return nil, nil, fmt.Errorf("%w: node %q is %s", errNodeUnavailable, nodeID, node.Status())
	}
	instanceID := node.Metadata().ServiceInstanceID
	if instanceID == "" {
		return nil, nil, fmt.Errorf("%w: node %q reports no service instance", errNodeUnavailable, nodeID)
	}

	return sandboxmigration.Participant_builder{NodeId: nodeID, ServiceInstanceId: instanceID}.Build(), node, nil
}

func (o *Orchestrator) GetMigration(ctx context.Context, migrationID string) (*sandboxmigration.Migration, error) {
	if o.migrations == nil {
		return nil, fmt.Errorf("read migration %q: %w", migrationID, errMigrationNotConfigured)
	}

	ctx, cancel := context.WithTimeout(ctx, migrationCallTimeout)
	defer cancel()
	resp, err := o.migrations.GetMigration(ctx, sandboxmigration.GetMigrationRequest_builder{MigrationId: migrationID}.Build())
	if err != nil {
		return nil, err
	}

	return resp.GetMigration(), nil
}

func (o *Orchestrator) TerminateMigration(ctx context.Context, migrationID string) (*sandboxmigration.Migration, error) {
	if o.migrations == nil {
		return nil, fmt.Errorf("terminate migration %q: %w", migrationID, errMigrationNotConfigured)
	}

	ctx, cancel := context.WithTimeout(ctx, migrationCallTimeout)
	defer cancel()
	resp, err := o.migrations.TerminateMigration(ctx, sandboxmigration.TerminateMigrationRequest_builder{MigrationId: migrationID}.Build())
	if err != nil {
		return nil, err
	}

	return resp.GetMigration(), nil
}
