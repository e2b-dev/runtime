package auth

import (
	"context"
	"net/http"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/token"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/token/jwks"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/token/oidc"
)

type ProviderConfig = token.ProviderConfig

type JWTConfig = jwks.Config

type JWTIssuer = jwks.Issuer

type AudienceMatchPolicy = jwks.AudienceMatchPolicy

const AudienceMatchAny = jwks.AudienceMatchAny

// The three verifiers below sit on one axis: how a token's keys are found,
// and how far the result is carried.
//
//	JWKSVerifier        keys from the issuer's JWKS path; claims
//	OIDCVerifier        keys via OIDC discovery; what the token asserts
//	LinkedOIDCVerifier  the above, resolved to an internal user
//
// Each adds to the one before it. Picking a lower one is a decision about
// what the caller needs, never a weakening of the token: discovery and issuer
// validation are identical across both OIDC levels.

// JWKSVerifier verifies a JWT against keys published at each issuer's
// conventional JWKS path, without an OIDC discovery document.
//
// Suited to a token minted by a peer service, where there is no discovery to
// perform. Not suited to one from an identity provider: it neither fetches
// the discovery document nor cross-checks the issuer that document declares.
type JWKSVerifier = token.JWKSVerifier

func ParseProviderConfig(value string) (ProviderConfig, error) {
	return token.ParseProviderConfig(value)
}

// NewJWKSVerifier builds a verifier for every issuer in the config, returning
// nil when it declares none. A nil verifier denies everything.
func NewJWKSVerifier(ctx context.Context, config ProviderConfig, httpClient *http.Client) (*JWKSVerifier, error) {
	return token.NewJWKSVerifier(ctx, config, httpClient)
}

// TokenIdentity is what a verified token asserts about itself: the issuer and
// subject it names, and the claims it carried.
type TokenIdentity = oidc.TokenIdentity

// OIDCVerifier verifies auth-provider tokens across every configured issuer
// and reports what they assert, linking them to nobody.
//
// For a caller that must read a subject before that subject is a user here —
// signup, or anything else creating the identity it authenticates. The
// alternative, giving a linked verifier a lookup that answers "nobody", makes
// not-found a value and turns any missed check into an authenticated nobody.
type OIDCVerifier = token.OIDCVerifier

// NewOIDCVerifier builds a verifier that reports what tokens assert without
// resolving them.
func NewOIDCVerifier(ctx context.Context, config ProviderConfig, httpClient *http.Client) (*OIDCVerifier, error) {
	return token.NewOIDCVerifier(ctx, config, httpClient)
}

// IdentityLookup resolves an OIDC identity (issuer + subject) to an internal
// user UUID. Implementations should return ErrIdentityNotFound when no
// matching identity exists.
type IdentityLookup = oidc.IdentityLookup

// ErrIdentityNotFound is returned by OIDC verification when the token is
// valid but no matching identity exists.
var ErrIdentityNotFound = oidc.ErrIdentityNotFound

// LinkedOIDCVerifier verifies auth-provider user JWTs across every issuer in
// a ProviderConfig and resolves each to the internal user it names.
type LinkedOIDCVerifier = token.LinkedOIDCVerifier

// NewLinkedOIDCVerifier constructs a verifier for the given provider config.
// When the config has no JWT issuers it returns (nil, nil); a nil verifier
// denies all verification attempts at runtime.
func NewLinkedOIDCVerifier(ctx context.Context, config ProviderConfig, httpClient *http.Client, identities IdentityLookup) (*LinkedOIDCVerifier, error) {
	return token.NewLinkedOIDCVerifier(ctx, config, httpClient, identities)
}
