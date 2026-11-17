package plugin

import (
	"testing"

	"github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/require"
)

func TestCountReadyNodesIncludesIneligibleNodes(t *testing.T) {
	t.Parallel()

	nodes := []*api.NodeListStub{
		{Status: api.NodeStatusReady, SchedulingEligibility: api.NodeSchedulingEligible},
		{Status: api.NodeStatusReady, SchedulingEligibility: api.NodeSchedulingIneligible},
		{Status: api.NodeStatusDown, SchedulingEligibility: api.NodeSchedulingEligible},
	}

	require.Equal(t, 2, countReadyNodes(nodes))
}
