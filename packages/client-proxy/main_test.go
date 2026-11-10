package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
)

// namedCloser records its name on the shared list when closed.
type namedCloser struct {
	name  string
	order *[]string
}

func (c namedCloser) Close(_ context.Context) error {
	*c.order = append(*c.order, c.name)

	return nil
}

// closableResumer is a resumer that also has to be closed.
type closableResumer struct {
	namedCloser
}

func (closableResumer) Init(_ context.Context) {}

func (closableResumer) Resume(_ context.Context, _ string, _ uint64, _ string, _ string) (*catalog.SandboxInfo, error) {
	return nil, nil
}

// The resume flights close before the feature flags, which a flight still
// running reads, and before the catalog; the resumer the flights called
// closes last.
func TestShutdownClosers_ResumeFlightsCloseBeforeWhatTheyRead(t *testing.T) {
	t.Parallel()

	var order []string
	resumer := closableResumer{namedCloser{name: "resumer", order: &order}}
	closers := shutdownClosers(
		namedCloser{name: "feature-flags", order: &order},
		namedCloser{name: "catalog", order: &order},
		namedCloser{name: "resume-flights", order: &order},
		resumer,
	)
	for _, c := range closers {
		require.NoError(t, c.Close(t.Context()))
	}

	require.Equal(t, []string{"resume-flights", "feature-flags", "catalog", "resumer"}, order)
}

// Without a resumer there are no resume flights to close, and the resumer
// is not closed either.
func TestShutdownClosers_WithoutAResumer(t *testing.T) {
	t.Parallel()

	var order []string
	closers := shutdownClosers(
		namedCloser{name: "feature-flags", order: &order},
		namedCloser{name: "catalog", order: &order},
		nil,
		nil,
	)
	for _, c := range closers {
		require.NoError(t, c.Close(t.Context()))
	}

	require.Equal(t, []string{"feature-flags", "catalog"}, order)
}
