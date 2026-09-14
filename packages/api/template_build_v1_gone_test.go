package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// The validator answers 404 for a path absent from the spec, so an old CLI
// only learns to migrate if these routes are registered ahead of it.
func TestTemplateBuildV1RoutesAreGone(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSpec()
	require.NoError(t, err)

	ff, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	server := NewGinServer(t.Context(), cfg.Config{}, telemetry.NewNoopClient(), logger.NewNopLogger(), nil, nil, nil, ff, swagger, 0)

	for _, path := range []string{
		"/templates",
		"/templates/my-template",
		"/templates/my-template/builds/00000000-0000-0000-0000-000000000000",
		"/v2/templates",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
			rr := httptest.NewRecorder()
			server.Handler.ServeHTTP(rr, req)

			require.Equal(t, http.StatusGone, rr.Code, rr.Body.String())
			require.Contains(t, rr.Body.String(), "https://e2b.dev/docs/template/migration-v2")
		})
	}
}
