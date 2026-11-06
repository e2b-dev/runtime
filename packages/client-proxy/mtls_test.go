package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/proxy/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestAPIHopReportsUnderItsKeyAndReadsItsFlag(t *testing.T) {
	t.Parallel()

	// The flag turns on a hop that names its expected server; a hop that
	// names none refuses the flip and stays off, reported as refused.
	for _, tc := range []struct {
		name         string
		expect       []string
		mode, source string
	}{
		{"names its server", []string{"spiffe://td.example.internal/ns/control/sa/api"}, "on", mtls.SourceFlag},
		{"names no server", nil, "off", mtls.SourceRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			flags, source := servicetest.Flags(t)
			proc, metrics := servicetest.NewProcess(t, flags.StringReader())
			settings := cfg.TLSConfig{API: mtls.HopSettings{Expect: tc.expect}}
			_, err := newAPIHop(proc, settings)
			require.NoError(t, err)
			assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, "api", "off", mtls.SourceFallback))

			servicetest.SetFlag(source, "mtls-client-proxy-api-client-mode", "on")
			_, err = newAPIHop(proc, settings)
			require.NoError(t, err)
			assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, "api", tc.mode, tc.source))
		})
	}
}
