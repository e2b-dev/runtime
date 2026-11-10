package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

func TestNewVerifierConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		issuerURL string
	}{
		{
			name: "empty config returns noop verifier",
		},
		{
			name:      "trims empty config",
			issuerURL: " ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			verifier, err := NewVerifier(t.Context(), tt.issuerURL)
			require.NoError(t, err)
			require.NotNil(t, verifier)
		})
	}
}

func TestNoopVerifierRejectsClaims(t *testing.T) {
	t.Parallel()

	verifier, err := NewVerifier(t.Context(), "")
	require.NoError(t, err)

	claims, err := verifier.VerifyClaims(t.Context(), "token")
	require.Error(t, err)
	require.Empty(t, claims)
}

func TestNewVerifierLoadsOIDCProvider(t *testing.T) {
	t.Parallel()

	issuerURL := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			writeJSON(t, w, map[string]string{
				"issuer":   issuerURL,
				"jwks_uri": issuerURL + "/jwks",
			})
		case "/jwks":
			writeJSON(t, w, map[string]any{"keys": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	issuerURL = server.URL

	verifier, err := NewVerifier(t.Context(), server.URL)
	require.NoError(t, err)
	require.NotNil(t, verifier)
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}

type fakeVerifier struct {
	claims Claims
	err    error
}

func (v fakeVerifier) VerifyClaims(context.Context, string) (Claims, error) {
	return v.claims, v.err
}

func TestRequireBearerToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		md        metadata.MD
		wantToken string
		wantCode  codes.Code
	}{
		{
			name:     "missing authorization",
			md:       metadata.MD{},
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "malformed authorization",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "token"),
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "empty bearer token",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer "),
			wantCode: codes.Unauthenticated,
		},
		{
			name:      "bearer token",
			md:        metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"),
			wantToken: "token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token, err := requireBearerToken(tt.md)
			if tt.wantCode != codes.OK {
				require.Equal(t, tt.wantCode, status.Code(err))

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantToken, token)
		})
	}
}

func TestRequireClaims(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		md       metadata.MD
		verifier Verifier
		want     Claims
		wantCode codes.Code
	}{
		{
			name:     "valid bearer token",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"),
			verifier: fakeVerifier{claims: Claims{Subject: "client_123", OrgID: "org_123", Scopes: []string{RequiredScope}}},
			want:     Claims{Subject: "client_123", OrgID: "org_123", Scopes: []string{RequiredScope}},
		},
		{
			name:     "missing bearer token",
			md:       metadata.MD{},
			verifier: fakeVerifier{claims: Claims{Subject: "client_123", OrgID: "org_123", Scopes: []string{RequiredScope}}},
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "configured auth requires verifier",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"),
			wantCode: codes.Internal,
		},
		{
			name:     "verifier error",
			md:       metadata.Pairs(proxygrpc.MetadataAuthorization, "Bearer token"),
			verifier: fakeVerifier{err: errors.New("invalid token")},
			wantCode: codes.Unauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			claims, err := RequireClaims(t.Context(), tt.md, tt.verifier)
			if tt.wantCode != codes.OK {
				require.Equal(t, tt.wantCode, status.Code(err))

				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, claims)
		})
	}
}

func TestRequireScopeClaims(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		claims        Claims
		requiredScope string
		wantCode      codes.Code
	}{
		{
			name:          "matching scope",
			claims:        Claims{Scopes: []string{"other", RequiredScope}},
			requiredScope: RequiredScope,
		},
		{
			name:          "trims required scope",
			claims:        Claims{Scopes: []string{RequiredScope}},
			requiredScope: " " + RequiredScope + " ",
		},
		{
			name:          "missing scope",
			claims:        Claims{Scopes: []string{"other"}},
			requiredScope: RequiredScope,
			wantCode:      codes.Unauthenticated,
		},
		{
			name:          "missing required scope",
			claims:        Claims{Scopes: []string{RequiredScope}},
			requiredScope: "",
			wantCode:      codes.Unauthenticated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := RequireScopeClaims(tt.claims, tt.requiredScope)
			if tt.wantCode != codes.OK {
				require.Equal(t, tt.wantCode, status.Code(err))

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRequireOrgClaims(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		claims        Claims
		expectedOrgID string
		wantCode      codes.Code
	}{
		{
			name:          "matching org",
			claims:        Claims{OrgID: "org_123"},
			expectedOrgID: "org_123",
		},
		{
			name:          "trims expected org",
			claims:        Claims{OrgID: "org_123"},
			expectedOrgID: " org_123 ",
		},
		{
			name:          "wrong org",
			claims:        Claims{OrgID: "org_456"},
			expectedOrgID: "org_123",
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "missing expected org",
			claims:        Claims{OrgID: "org_123"},
			expectedOrgID: "",
			wantCode:      codes.PermissionDenied,
		},
		{
			name:          "missing claim org",
			claims:        Claims{},
			expectedOrgID: "org_123",
			wantCode:      codes.PermissionDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := RequireOrgClaims(tt.claims, tt.expectedOrgID)
			if tt.wantCode != codes.OK {
				require.Equal(t, tt.wantCode, status.Code(err))

				return
			}
			require.NoError(t, err)
		})
	}
}
