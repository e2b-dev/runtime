package pool

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Must stay aligned with the suggested edge Content-Type matchers in grpc.go.
var edgeNativeGRPCContentType = regexp.MustCompile(`(?i)^application/grpc($|[+;].*)`)

func TestIsGRPCRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		contentType string
		want        bool
	}{
		{contentType: "application/grpc", want: true},
		{contentType: "application/grpc+proto", want: true},
		{contentType: "application/grpc+json", want: true},
		{contentType: "application/grpc; charset=utf-8", want: true},
		{contentType: "Application/Grpc", want: true},
		{contentType: "application/grpc-web", want: false},
		{contentType: "application/grpc-web+proto", want: false},
		{contentType: "application/connect+proto", want: false},
		{contentType: "application/json", want: false},
		{contentType: "text/event-stream", want: false},
		{contentType: "", want: false},
	}

	for _, test := range tests {
		t.Run(test.contentType, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(http.MethodPost, "http://sandbox/service/Method", nil)
			require.NoError(t, err)
			if test.contentType != "" {
				req.Header.Set("Content-Type", test.contentType)
			}

			assert.Equal(t, test.want, IsGRPCRequest(req))
		})
	}
}

func TestIsGRPCRequestNil(t *testing.T) {
	t.Parallel()

	assert.False(t, IsGRPCRequest(nil))
}

func TestEdgeGRPCContentTypeRegexpAgreesWithProxy(t *testing.T) {
	t.Parallel()

	tests := []string{
		"application/grpc",
		"application/grpc+proto",
		"application/grpc+json",
		"application/grpc; charset=utf-8",
		"Application/Grpc",
		"application/grpc-web",
		"application/grpc-web+proto",
		"application/connect+proto",
		"application/json",
		"",
	}

	for _, contentType := range tests {
		t.Run(contentType, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(http.MethodPost, "http://sandbox/service/Method", nil)
			require.NoError(t, err)
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}

			assert.Equal(t, IsGRPCRequest(req), edgeNativeGRPCContentType.MatchString(contentType))
		})
	}
}
