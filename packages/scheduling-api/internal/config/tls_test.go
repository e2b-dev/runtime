package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestServerTLSInvalidConfiguration(t *testing.T) {
	t.Parallel()
	proc, _ := servicetest.NewProcess(t, nil)
	for _, settings := range []mtls.ListenerSettings{
		{},
		{Mode: "permissive"},
		{Mode: "invalid"},
		{Allow: []string{"*"}},
		{Allow: []string{"spiffe://test.example/api"}},
	} {
		_, err := ServerTLS(proc, settings)
		require.Error(t, err)
	}
	_, err := ServerTLS(proc, mtls.ListenerSettings{Mode: "off"})
	require.NoError(t, err)
}
