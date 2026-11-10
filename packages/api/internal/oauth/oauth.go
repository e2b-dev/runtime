package oauth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

const RequiredScope = proxygrpc.ScopeSandboxLifecycle

type Verifier interface {
	VerifyClaims(ctx context.Context, rawToken string) (Claims, error)
}

type Claims struct {
	Subject string
	OrgID   string
	Scopes  []string
}

type tokenClaims struct {
	OrgID string `json:"org_id"`
	Scope string `json:"scope"`
}

type oidcVerifier struct {
	verifier *oidc.IDTokenVerifier
}

type noopVerifier struct{}

func NewVerifier(ctx context.Context, issuerURL string) (Verifier, error) {
	issuerURL = strings.TrimSpace(issuerURL)

	if issuerURL == "" {
		return noopVerifier{}, nil
	}

	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("create client proxy OIDC provider: %w", err)
	}

	return &oidcVerifier{
		verifier: provider.Verifier(&oidc.Config{SkipClientIDCheck: true}),
	}, nil
}

func Configured(issuerURL string) bool {
	return strings.TrimSpace(issuerURL) != ""
}

func (noopVerifier) VerifyClaims(context.Context, string) (Claims, error) {
	return Claims{}, errors.New("client proxy OIDC verifier is not configured")
}

func (v *oidcVerifier) VerifyClaims(ctx context.Context, rawToken string) (Claims, error) {
	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Claims{}, fmt.Errorf("verify client proxy OIDC token: %w", err)
	}

	claims := tokenClaims{}
	if err := idToken.Claims(&claims); err != nil {
		return Claims{}, fmt.Errorf("parse client proxy OIDC claims: %w", err)
	}

	return Claims{
		Subject: idToken.Subject,
		OrgID:   claims.OrgID,
		Scopes:  strings.Fields(claims.Scope),
	}, nil
}

func bearerToken(md metadata.MD) (string, bool) {
	vals := md.Get(proxygrpc.MetadataAuthorization)
	if len(vals) == 0 {
		return "", false
	}

	scheme, token, ok := strings.Cut(vals[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", false
	}

	return strings.TrimSpace(token), true
}

func requireBearerToken(md metadata.MD) (string, error) {
	rawToken, found := bearerToken(md)
	if !found {
		return "", denyCaller()
	}

	return rawToken, nil
}

func RequireClaims(ctx context.Context, md metadata.MD, verifier Verifier) (Claims, error) {
	if verifier == nil {
		return Claims{}, status.Error(codes.Internal, "client proxy OIDC verifier is not configured")
	}

	rawToken, err := requireBearerToken(md)
	if err != nil {
		return Claims{}, err
	}

	claims, err := verifier.VerifyClaims(ctx, rawToken)
	if err != nil {
		return Claims{}, denyCaller()
	}

	return claims, nil
}

func RequireOrgClaims(claims Claims, expectedOrgID string) error {
	expectedOrgID = strings.TrimSpace(expectedOrgID)
	if expectedOrgID == "" {
		return denyPermission()
	}
	if subtle.ConstantTimeCompare([]byte(claims.OrgID), []byte(expectedOrgID)) != 1 {
		return denyPermission()
	}

	return nil
}

func RequireScopeClaims(claims Claims, requiredScope string) error {
	requiredScope = strings.TrimSpace(requiredScope)
	if requiredScope == "" {
		return denyCaller()
	}
	if !slices.Contains(claims.Scopes, requiredScope) {
		return denyCaller()
	}

	return nil
}

func denyPermission() error {
	return status.Error(codes.PermissionDenied, "permission denied")
}

func denyCaller() error {
	return status.Error(codes.Unauthenticated, "unauthenticated")
}
