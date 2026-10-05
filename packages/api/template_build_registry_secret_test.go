package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A template build start body can carry the caller's private registry
// credentials. A request the spec rejects must not put them in the response or
// in the errors it records, since those reach the access log and the span.
func TestRejectedTemplateBuildNeverRevealsRegistryCredentials(t *testing.T) {
	t.Parallel()

	const (
		secret = "registry-secret-DO-NOT-LOG"
		target = "/v2/templates/tpl/builds/00000000-0000-0000-0000-000000000000"
	)

	registries := map[string]string{
		"registry": `{"type":"registry","username":"user","password":"` + secret + `"}`,
		"aws":      `{"type":"aws","awsAccessKeyId":"id","awsSecretAccessKey":"` + secret + `","awsRegion":"us-east-1"}`,
		"gcp":      `{"type":"gcp","serviceAccountJson":"` + secret + `"}`,
	}
	rejections := map[string]func(registry string) string{
		"malformed files hash": func(registry string) string {
			return `{"fromImage":"alpine","fromImageRegistry":` + registry + `,"steps":[{"type":"COPY","filesHash":""}]}`
		},
		"empty fromImage": func(registry string) string {
			return `{"fromImage":"","fromImageRegistry":` + registry + `}`
		},
		"malformed force": func(registry string) string {
			return `{"fromImage":"alpine","fromImageRegistry":` + registry + `,"force":"yes"}`
		},
	}

	for registryName, registry := range registries {
		for rejectionName, body := range rejections {
			t.Run(registryName+"/"+rejectionName, func(t *testing.T) {
				t.Parallel()

				assertRejectedWithoutSecret(t, target, body(registry), secret)
			})
		}
	}

	t.Run("malformed registry", func(t *testing.T) {
		t.Parallel()

		assertRejectedWithoutSecret(t, target,
			`{"fromImage":"alpine","fromImageRegistry":{"type":"registry","password":"`+secret+`"}}`, secret)
	})
}

func assertRejectedWithoutSecret(t *testing.T, target, body, secret string) {
	t.Helper()

	router, recorded := newValidatingRouter(t)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-API-Key", "e2b_key")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	assert.NotContains(t, recorder.Body.String(), secret, "the response echoed a registry credential")

	require.NotEmpty(t, *recorded, "the rejection was not recorded at all")
	for _, err := range *recorded {
		assert.NotContains(t, err.Error(), secret, "a recorded error carried a registry credential")
	}
}
