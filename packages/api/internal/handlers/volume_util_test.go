package handlers

import (
	"context"
	"errors"
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
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
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
		return &types.Team{Team: &authqueries.Team{ID: uuid.New(), ClusterID: id}}
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
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error is not retryable",
			err:      nil,
			expected: false,
		},
		{
			name:     "net.ErrClosed is retryable",
			err:      net.ErrClosed,
			expected: true,
		},
		{
			name:     "context.DeadlineExceeded is retryable",
			err:      context.DeadlineExceeded,
			expected: true,
		},
		{
			name:     "context.Canceled is retryable",
			err:      context.Canceled,
			expected: true,
		},
		{
			name:     "gRPC codes.Unavailable is retryable",
			err:      status.Error(codes.Unavailable, "connection refused"),
			expected: true,
		},
		{
			name:     "gRPC codes.DeadlineExceeded is retryable",
			err:      status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			expected: true,
		},
		{
			name:     "gRPC codes.Canceled is retryable",
			err:      status.Error(codes.Canceled, "request canceled"),
			expected: true,
		},
		{
			name:     "gRPC codes.NotFound is not retryable",
			err:      status.Error(codes.NotFound, "volume not found"),
			expected: false,
		},
		{
			name:     "gRPC codes.InvalidArgument is not retryable",
			err:      status.Error(codes.InvalidArgument, "bad volume name"),
			expected: false,
		},
		{
			name:     "generic error is not retryable",
			err:      errors.New("something went wrong"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, isRetryableError(tc.err))
		})
	}
}

