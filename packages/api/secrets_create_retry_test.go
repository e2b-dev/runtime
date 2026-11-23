package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestSecretsCreateAdmissionRetryHint(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		keys       []string
		skipAuth   bool
		chunked    bool
		wantCode   int
	}{
		{name: "authentication", body: `{}`, wantCode: http.StatusUnauthorized},
		{name: "schema", body: `{}`, skipAuth: true, wantCode: http.StatusBadRequest},
		{name: "key format", body: `{"name":"token","value":"private-value"}`, keys: []string{"Invalid"}, skipAuth: true, wantCode: http.StatusBadRequest},
		{name: "repeated key", body: `{"name":"token","value":"private-value"}`, keys: []string{"one", "two"}, skipAuth: true, wantCode: http.StatusBadRequest},
		{name: "body limit", body: strings.Repeat(" ", maxUploadLimit+1), skipAuth: true, wantCode: http.StatusRequestEntityTooLarge},
		{name: "chunked body limit", body: strings.Repeat(" ", maxUploadLimit+1), skipAuth: true, chunked: true, wantCode: http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec, err := api.GetSpec()
			require.NoError(t, err)
			if tc.skipAuth {
				spec.Paths.Value("/secrets").Post.Security = nil
			}
			flags, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
			require.NoError(t, err)
			t.Cleanup(func() { _ = flags.Close(context.WithoutCancel(t.Context())) })
			server := NewGinServer(t.Context(), cfg.Config{}, telemetry.NewNoopClient(), logger.NewNopLogger(), nil, nil, nil, flags, spec)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/secrets", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			for _, key := range tc.keys {
				req.Header.Add("Idempotency-Key", key)
			}
			if tc.chunked {
				req.ContentLength = -1
			}
			recorder := httptest.NewRecorder()
			server.Handler.ServeHTTP(recorder, req)
			require.Equal(t, tc.wantCode, recorder.Code, recorder.Body.String())
			require.Empty(t, recorder.Result().Header.Values(apierrors.ShouldRetryHeader))
			require.NotContains(t, recorder.Body.String(), "private-value")
		})
	}
}
