package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func TestGetResourceReportsVersionAndCommitApart(t *testing.T) {
	t.Parallel()

	res, err := GetResource(t.Context(), "node-1", "svc", "abc123d", "0.30.202609120421-abc123def45", "instance-1")
	require.NoError(t, err)

	set := res.Set()
	version, ok := set.Value(semconv.ServiceVersionKey)
	require.True(t, ok)
	assert.Equal(t, "0.30.202609120421-abc123def45", version.AsString())

	commit, ok := set.Value(ServiceCommitKey)
	require.True(t, ok)
	assert.Equal(t, "abc123d", commit.AsString())
}

func TestGetResourceOmitsEmptyCommit(t *testing.T) {
	t.Parallel()

	res, err := GetResource(t.Context(), "node-1", "svc", "", "0.30.0", "instance-1")
	require.NoError(t, err)

	set := res.Set()
	version, ok := set.Value(semconv.ServiceVersionKey)
	require.True(t, ok)
	assert.Equal(t, "0.30.0", version.AsString())

	_, ok = set.Value(attribute.Key("service.commit"))
	assert.False(t, ok)
}
