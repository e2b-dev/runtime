package orchestrator

import (
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// The flag is in seconds, and a flag that does not exist yet reads as no
// interval, so a build that carries the refresh changes nothing until it is set.
func TestNodeRefreshInterval_IsOffUntilTheFlagSetsSeconds(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	flags, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	o := &Orchestrator{featureFlagsClient: flags}

	assert.Zero(t, o.nodeRefreshInterval(t.Context()))

	source.Update(source.Flag(featureflags.NodeInfoRefreshIntervalSecondsFlag.Key()).ValueForAll(ldvalue.Int(5)))

	assert.Equal(t, 5*time.Second, o.nodeRefreshInterval(t.Context()))
}

// A remote cluster's nodes are left to their sync: a refresh reaches only the
// local cluster's.
func TestRefreshNodes_RefreshesTheLocalClusterOnly(t *testing.T) {
	t.Parallel()

	parked := &infogrpc.ServiceInfoResponse{ServiceStatus: infogrpc.ServiceInfoStatus_Standby}
	local := nodemanager.NewTestNode("local", api.NodeStatusReady, 0, 1, nodemanager.WithServiceInfoResponse(parked))
	local.ClusterID = consts.LocalClusterID
	remote := nodemanager.NewTestNode("remote", api.NodeStatusReady, 0, 1, nodemanager.WithServiceInfoResponse(parked))

	o := &Orchestrator{nodes: smap.New[*nodemanager.Node]()}
	o.registerNode(local)
	o.registerNode(remote)

	o.refreshNodes(t.Context())

	assert.Equal(t, api.NodeStatusStandby, local.Status())
	assert.Equal(t, api.NodeStatusReady, remote.Status())
}
