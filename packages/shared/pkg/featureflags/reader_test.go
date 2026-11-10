package featureflags

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

// The mtls package takes its flag reader as an interface it defines, so the
// match is checked here, where the reader lives, without a production import.
var _ mtls.FlagReader = StringReader{}

const readerKey = "listener-mode"

func TestStringReader(t *testing.T) {
	t.Parallel()

	permissive := new(ldvalue.String("permissive"))
	tests := []struct {
		name     string
		serve    *ldvalue.Value        // nil leaves the flag out of the environment
		client   func(*Client) *Client // nil reads through the client serving the flag
		fallback string
		want     string
	}{
		{name: "served value", serve: permissive, fallback: "off", want: "permissive"},
		{name: "missing flag is the fallback passed in", serve: nil, fallback: "required", want: "required"},
		{name: "an empty fallback is returned as is", serve: nil, fallback: "", want: ""},
		{name: "wrong type is the fallback", serve: new(ldvalue.Int(3)), fallback: "off", want: "off"},
		{name: "nil client", serve: permissive, client: func(*Client) *Client { return nil }, fallback: "env", want: "env"},
		{name: "no SDK client", serve: permissive, client: func(*Client) *Client { return &Client{} }, fallback: "env", want: "env"},
		{name: "keyless client holds only the registered flags", serve: permissive, client: func(c *Client) *Client {
			return newClient(c.ld, true, "test-environment", "orchestrator")
		}, fallback: "env", want: "env"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			if tt.serve != nil {
				source.Update(source.Flag(readerKey).ValueForAll(*tt.serve))
			}
			client, err := NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
			reader := client.StringReader()
			if tt.client != nil {
				reader = tt.client(client).StringReader()
			}

			assert.Equal(t, tt.want, reader.String(t.Context(), readerKey, tt.fallback))
		})
	}
}

// A rule on the deployment environment is how one cluster is told apart from
// another, so the reader must carry the process contexts.
func TestStringReaderCarriesTheProcessContexts(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	source.Update(source.Flag(readerKey).
		Variations(ldvalue.String("off"), ldvalue.String("permissive")).
		FallthroughVariationIndex(0).
		VariationIndexForKey(DeploymentEnvironmentKind, "test-environment", 1))
	shared, err := NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, shared.Close(context.WithoutCancel(t.Context()))) })

	staging := newClient(shared.ld, false, "test-environment", "orchestrator")
	production := newClient(shared.ld, false, "other-environment", "orchestrator")

	assert.Equal(t, "permissive", staging.StringReader().String(t.Context(), readerKey, "env"))
	assert.Equal(t, "off", production.StringReader().String(t.Context(), readerKey, "env"))
}

// The reader's whole purpose: a flag-backed mode source over the real client
// starts on the environment fallback and moves to the flag once a value is
// served, and reports which — the source attribute the mode gauges carry.
func TestStringReaderDrivesAFlagModeSource(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	client, err := NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })

	allow, err := mtls.ParseAllowList([]string{"spiffe://example.internal/ns/e2b/sa/caller"})
	require.NoError(t, err)
	listener := mtls.NewFlagModeSource(client.StringReader(), "listener-mode", mtls.ModePermissive, allow)
	hop := mtls.NewFlagClientModeSource(client.StringReader(), "hop-mode", mtls.ClientOff)

	mode, from := listener.ModeWithSource(t.Context())
	assert.Equal(t, mtls.ModePermissive, mode, "no flag yet: the environment fallback")
	assert.Equal(t, mtls.SourceFallback, from)
	clientMode, from := hop.ClientModeWithSource(t.Context())
	assert.Equal(t, mtls.ClientOff, clientMode)
	assert.Equal(t, mtls.SourceFallback, from)

	source.Update(source.Flag("listener-mode").ValueForAll(ldvalue.String("required")))
	source.Update(source.Flag("hop-mode").ValueForAll(ldvalue.String("on")))

	mode, from = listener.ModeWithSource(t.Context())
	assert.Equal(t, mtls.ModeRequired, mode, "the flag value wins over the fallback")
	assert.Equal(t, mtls.SourceFlag, from)
	clientMode, from = hop.ClientModeWithSource(t.Context())
	assert.Equal(t, mtls.ClientOn, clientMode)
	assert.Equal(t, mtls.SourceFlag, from)
}
