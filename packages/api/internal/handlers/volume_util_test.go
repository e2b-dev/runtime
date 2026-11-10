package handlers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
)

func TestVolumeTokenAudience(t *testing.T) {
	t.Parallel()

	store := &APIStore{config: cfg.Config{DomainName: "e2b.app"}}

	t.Run("falls back to the deployment domain", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "https://api.e2b.app", store.volumeTokenAudience(nil))
	})

	t.Run("uses the BYOC domain when set", func(t *testing.T) {
		t.Parallel()

		domain := "custom.example.com"
		assert.Equal(t, "https://api.custom.example.com", store.volumeTokenAudience(&domain))
	})
}

func TestVolumeContentDomain(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	domain := "custom.example.com"

	store := &APIStore{
		clusters: clusters.NewTestPool(clusters.NewTestCluster(clusterID, &domain)),
	}

	teamWith := func(id *uuid.UUID) *types.Team {
		return &types.Team{Team: &dbtypes.Team{ID: uuid.New(), ClusterID: id}}
	}

	t.Run("no cluster returns nil domain", func(t *testing.T) {
		t.Parallel()

		got, err := store.volumeContentDomain(teamWith(nil))
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("BYOC cluster returns its domain", func(t *testing.T) {
		t.Parallel()

		got, err := store.volumeContentDomain(teamWith(&clusterID))
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, domain, *got)
	})

	t.Run("unknown cluster returns error", func(t *testing.T) {
		t.Parallel()

		unknown := uuid.New()
		got, err := store.volumeContentDomain(teamWith(&unknown))
		require.ErrorIs(t, err, ErrClusterNotFound)
		assert.Nil(t, got)
	})
}

func TestFindNodesByVolumeLabel(t *testing.T) {
	t.Parallel()

	newNode := func(id string, labels ...string) *nodemanager.Node {
		return nodemanager.NewTestNode(id, api.NodeStatusReady, 0, 1, nodemanager.WithLabels(labels))
	}

	matchedIDs := func(nodes []*nodemanager.Node) []string {
		var ids []string
		for _, n := range nodes {
			ids = append(ids, n.ID)
		}

		return ids
	}

	cases := map[string]struct {
		nodes             []*nodemanager.Node
		expectedLabel     string
		expectedMatched   []string
		expectedUnmatched []string
	}{
		"splits matched and unmatched preserving order": {
			nodes: []*nodemanager.Node{
				newNode("a", "ssd"),
				newNode("b", "default"),
				newNode("c", "ssd", "default"),
				newNode("d", "hdd"),
			},
			expectedLabel:     "ssd",
			expectedMatched:   []string{"a", "c"},
			expectedUnmatched: []string{"b", "d"},
		},
		"all nodes match": {
			nodes: []*nodemanager.Node{
				newNode("a", "ssd"),
				newNode("b", "ssd"),
			},
			expectedLabel:     "ssd",
			expectedMatched:   []string{"a", "b"},
			expectedUnmatched: nil,
		},
		"no nodes match": {
			nodes: []*nodemanager.Node{
				newNode("a", "hdd"),
				newNode("b", "default"),
			},
			expectedLabel:     "ssd",
			expectedMatched:   nil,
			expectedUnmatched: []string{"a", "b"},
		},
		"empty input": {
			nodes:             nil,
			expectedLabel:     "ssd",
			expectedMatched:   nil,
			expectedUnmatched: nil,
		},
		"node with no labels never matches": {
			nodes:             []*nodemanager.Node{newNode("a")},
			expectedLabel:     "default",
			expectedMatched:   nil,
			expectedUnmatched: []string{"a"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			matched, unmatched := findNodesByVolumeLabel(tc.nodes, tc.expectedLabel)

			assert.Equal(t, tc.expectedMatched, matchedIDs(matched))
			assert.Equal(t, tc.expectedUnmatched, matchedIDs(unmatched))
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "closed connection", err: net.ErrClosed, want: true},
		{name: "context deadline", err: context.DeadlineExceeded, want: true},
		{name: "node unavailable", err: status.Error(codes.Unavailable, "connection refused"), want: true},
		{name: "wrapped node unavailable", err: fmt.Errorf("create volume: %w", status.Error(codes.Unavailable, "connection refused")), want: true},
		{name: "remote deadline may have applied the change", err: status.Error(codes.DeadlineExceeded, "deadline exceeded"), want: false},
		{name: "invalid argument", err: status.Error(codes.InvalidArgument, "bad volume"), want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, isRetryableError(tc.err))
		})
	}
}

func TestExecuteOnNodes(t *testing.T) {
	t.Parallel()

	unavailable := status.Error(codes.Unavailable, "connection refused")
	remoteDeadline := status.Error(codes.DeadlineExceeded, "deadline exceeded")

	tests := []struct {
		name      string
		statuses  map[string]api.NodeStatus // nodes default to ready
		results   map[string]error          // nodes default to success
		wantErr   error
		wantCalls []string
	}{
		{
			name:      "unavailable node moves on to the next",
			results:   map[string]error{"a": unavailable},
			wantCalls: []string{"a", "b"},
		},
		{
			name:      "remote deadline stops the loop",
			results:   map[string]error{"a": remoteDeadline},
			wantErr:   remoteDeadline,
			wantCalls: []string{"a"},
		},
		{
			name:      "every node unavailable",
			results:   map[string]error{"a": unavailable, "b": unavailable},
			wantErr:   ErrNoHealthyOrchestratorFound,
			wantCalls: []string{"a", "b"},
		},
		{
			name:      "node that cannot accept requests is skipped",
			statuses:  map[string]api.NodeStatus{"a": api.NodeStatusUnhealthy},
			wantCalls: []string{"b"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var nodes []*nodemanager.Node
			nodeByClient := map[*clusters.GRPCClient]string{}
			for _, id := range []string{"a", "b"} {
				nodeStatus, ok := tc.statuses[id]
				if !ok {
					nodeStatus = api.NodeStatusReady
				}

				node := nodemanager.NewTestNode(id, nodeStatus, 0, 1)
				client, _ := node.GetClient(t.Context())
				nodeByClient[client] = id
				nodes = append(nodes, node)
			}

			var calls []string
			err := executeOnNodes(t.Context(), nodes, func(_ context.Context, client *clusters.GRPCClient) error {
				id := nodeByClient[client]
				calls = append(calls, id)

				return tc.results[id]
			})

			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			assert.Equal(t, tc.wantCalls, calls)
		})
	}
}

func TestConvertFromDBMountsToAPIMounts(t *testing.T) {
	t.Parallel()

	t.Run("converts valid mounts", func(t *testing.T) {
		t.Parallel()

		mounts := []*dbtypes.SandboxVolumeMountConfig{
			{Name: "v1", Path: "/mnt/data1"},
			{Name: "v2", Path: "/mnt/data2"},
		}

		res := convertFromDBMountsToAPIMounts(mounts)
		require.NotNil(t, res)
		assert.Len(t, *res, 2)
		assert.Equal(t, "v1", (*res)[0].Name)
		assert.Equal(t, "/mnt/data1", (*res)[0].Path)
		assert.Equal(t, "v2", (*res)[1].Name)
		assert.Equal(t, "/mnt/data2", (*res)[1].Path)
	})

	t.Run("skips nil items safely without panic", func(t *testing.T) {
		t.Parallel()

		mounts := []*dbtypes.SandboxVolumeMountConfig{
			{Name: "v1", Path: "/mnt/data1"},
			nil,
			{Name: "v2", Path: "/mnt/data2"},
		}

		res := convertFromDBMountsToAPIMounts(mounts)
		require.NotNil(t, res)
		assert.Len(t, *res, 2)
		assert.Equal(t, "v1", (*res)[0].Name)
		assert.Equal(t, "v2", (*res)[1].Name)
	})

	t.Run("empty input returns non-nil pointer to empty slice", func(t *testing.T) {
		t.Parallel()

		res := convertFromDBMountsToAPIMounts(nil)
		require.NotNil(t, res)
		assert.Empty(t, *res)
	})
}
