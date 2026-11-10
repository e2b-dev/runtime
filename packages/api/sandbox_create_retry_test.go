package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestSandboxCreateRetryBeforeHandler(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		body     string
		key      string
		skipAuth bool
		wantCode int
	}{
		{name: "authentication", body: `{"templateID":"base"}`, wantCode: http.StatusUnauthorized},
		{name: "validation", body: `{}`, skipAuth: true, wantCode: http.StatusBadRequest},
		{name: "keyed validation", body: `{}`, key: "previous", skipAuth: true, wantCode: http.StatusBadRequest},
		{name: "invalid field type", body: `{"templateID":"base","timeout":"invalid"}`, skipAuth: true, wantCode: http.StatusBadRequest},
		{name: "body limit", body: strings.Repeat(" ", maxUploadLimit+1), skipAuth: true, wantCode: http.StatusRequestEntityTooLarge},
		{name: "recovered panic", body: `{"templateID":"base"}`, skipAuth: true, wantCode: http.StatusInternalServerError},
	} {
		for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
			t.Run(tc.name+path, func(t *testing.T) {
				t.Parallel()
				spec, err := api.GetSpec()
				require.NoError(t, err)
				if tc.skipAuth {
					spec.Paths.Value(path).Post.Security = nil
				}
				source := ldtestdata.DataSource()
				source.Update(source.Flag(featureflags.APIIdempotencyRoutes.Key()).ValueForAll(ldvalue.CopyArbitraryValue(map[string]bool{"POST " + path: true})))
				flags, err := featureflags.NewClientWithDatasource(source)
				require.NoError(t, err)
				t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
				server := NewGinServer(t.Context(), cfg.Config{}, telemetry.NewNoopClient(), logger.NewNopLogger(), nil, nil, nil, flags, spec)
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
				if tc.key != "" {
					req.Header.Set("Idempotency-Key", tc.key)
				}
				recorder := httptest.NewRecorder()
				server.Handler.ServeHTTP(recorder, req)
				require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
				assert.NotContains(t, recorder.Result().Header, "Should-Retry")
			})
		}
	}
}
