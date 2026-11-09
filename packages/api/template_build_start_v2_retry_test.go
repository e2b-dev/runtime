package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestTemplateBuildStartV2IdempotencyKeyBeforeHandler(t *testing.T) {
	t.Parallel()

	const path = "/v2/templates/{templateID}/builds/{buildID}"
	for _, tc := range []struct {
		name     string
		key      string
		skipAuth bool
		wantCode int
		wantText string
	}{
		{name: "authentication", key: "previous", wantCode: http.StatusUnauthorized},
		{name: "uppercase key", key: "Previous", skipAuth: true, wantCode: http.StatusBadRequest, wantText: "Idempotency-Key"},
		{name: "invalid character", key: "previous_key", skipAuth: true, wantCode: http.StatusBadRequest, wantText: "Idempotency-Key"},
		{name: "too long", key: strings.Repeat("a", 256), skipAuth: true, wantCode: http.StatusBadRequest, wantText: "Idempotency-Key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec, err := api.GetSpec()
			require.NoError(t, err)
			if tc.skipAuth {
				spec.Paths.Value(path).Post.Security = nil
			}
			source := ldtestdata.DataSource()
			source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST /v2/templates/:templateID/builds/:buildID": true})))
			flags, err := featureflags.NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
			server := NewGinServer(t.Context(), cfg.Config{}, telemetry.NewNoopClient(), logger.NewNopLogger(), nil, nil, nil, nil, flags, spec)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/templates/tpl/builds/00000000-0000-0000-0000-000000000000", strings.NewReader(`{"fromImage":"alpine"}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", tc.key)
			recorder := httptest.NewRecorder()
			server.Handler.ServeHTTP(recorder, req)
			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), tc.wantText)
			require.NotContains(t, recorder.Result().Header, "Should-Retry")
		})
	}
}
