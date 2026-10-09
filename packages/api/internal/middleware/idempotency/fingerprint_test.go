package idempotency

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func jsonRequest(t *testing.T) *http.Request {
	t.Helper()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/sandboxes", nil)
	request.Header.Set("Content-Type", "application/json")

	return request
}

func TestFingerprint(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, first, second string
		equal               bool
	}{
		{"object order", `{"templateID":"base","metadata":{"a":"1","b":"2"}}`, "{ \"metadata\":{\"b\":\"2\",\"a\":\"1\"}, \"templateID\":\"base\" }", true},
		{"case sensitive env map order", `{"templateID":"base","envVars":{"FOO":"1","foo":"2"}}`, `{"envVars":{"foo":"2","FOO":"1"},"templateID":"base"}`, true},
		{"case sensitive metadata map order", `{"metadata":{"FOO":"1","foo":"2"}}`, `{"metadata":{"foo":"2","FOO":"1"}}`, true},
		{"nested struct alias order", `{"network":{"allowOut":["a"],"AllowOut":["b"]}}`, `{"network":{"AllowOut":["b"],"allowOut":["a"]}}`, false},
		{"string escapes", `{"metadata":{"a":"\u0061"}}`, `{"metadata":{"a":"a"}}`, true},
		{"unknown fields", `{"extra":9007199254740992}`, `{"extra":9007199254740993}`, true},
		{"numeric values", `{"timeout":60}`, `{"timeout":61}`, false},
		{"dynamic number spelling", `{"mcp":{"n":1}}`, `{"mcp":{"n":1.0}}`, true},
		{"array order", `{"network":{"allowOut":["a","b"]}}`, `{"network":{"allowOut":["b","a"]}}`, false},
		{"null versus absent", `{"network":null}`, `{}`, true},
		{"empty object versus absent", `{"network":{}}`, `{}`, false},
		{"omitted pointer versus explicit false", `{}`, `{"autoPause":false}`, false},
		{"duplicate merge", `{"network":{"allowOut":["a"]},"network":{"denyOut":["b"]}}`, `{"network":{"denyOut":["b"]}}`, false},
		{"duplicate order", `{"templateID":"a","templateID":"b"}`, `{"templateID":"b","templateID":"a"}`, false},
		{"duplicate result", `{"templateID":"a","templateID":"b"}`, `{"templateID":"b"}`, true},
		{"field alias", `{"TemplateID":"a"}`, `{"templateID":"a"}`, true},
		{"case alias order", `{"templateID":"a","TemplateID":"b"}`, `{"TemplateID":"b","templateID":"a"}`, false},
		{"unknown unicode key order", `{"k":1,"\u212a":2}`, `{"\u212a":2,"k":1}`, true},
		{"unicode struct alias order", `{"network":{"maskRequestHost":"a","mas\u212aRequestHost":"b"}}`, `{"network":{"mas\u212aRequestHost":"b","maskRequestHost":"a"}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var firstBody, secondBody api.NewSandboxV2
			require.NoError(t, json.Unmarshal([]byte(tc.first), &firstBody))
			require.NoError(t, json.Unmarshal([]byte(tc.second), &secondBody))
			first, err := fingerprint(jsonRequest(t), firstBody)
			require.NoError(t, err)
			second, err := fingerprint(jsonRequest(t), secondBody)
			require.NoError(t, err)
			require.Equal(t, tc.equal, first == second)
		})
	}
}

func TestFingerprintIgnoresContentType(t *testing.T) {
	t.Parallel()
	body := api.NewSandboxV2{TemplateID: "base"}
	want, err := fingerprint(jsonRequest(t), body)
	require.NoError(t, err)
	for _, contentType := range []string{
		"",
		"application/json; charset=utf-8",
		`Application/JSON; profile="one"; charset=UTF-8`,
		"application/json; profile=two",
		"application/octet-stream",
		"application/json; charset",
	} {
		t.Run(contentType, func(t *testing.T) {
			t.Parallel()
			request := jsonRequest(t)
			if contentType == "" {
				request.Header.Del("Content-Type")
			} else {
				request.Header.Set("Content-Type", contentType)
			}
			got, err := fingerprint(request, body)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}
