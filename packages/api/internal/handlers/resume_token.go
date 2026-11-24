package handlers

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/oauth"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/resumetoken"
)

// Only exchange requests traverse the internal registry connection.
type resumeExchange struct {
	proxygrpc.UnimplementedResumeTokenServiceServer

	registry proxygrpc.ResumeTokenServiceClient
}

func (a *APIStore) RegisterResumeExchange(s *grpc.Server) error {
	if a.clusterServiceConn == nil {
		return status.Error(codes.FailedPrecondition, "resume token exchange requires a registry connection")
	}
	proxygrpc.RegisterResumeTokenServiceServer(s, &resumeExchange{registry: proxygrpc.NewResumeTokenServiceClient(a.clusterServiceConn)})

	return nil
}

func (s *resumeExchange) ExchangeResumeToken(ctx context.Context, req *proxygrpc.ExchangeResumeTokenRequest) (*proxygrpc.ExchangeResumeTokenResponse, error) {
	headers := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(headers) != 1 || headers[0] == "" {
		return nil, status.Error(codes.Unauthenticated, "missing workload credential")
	}

	return s.registry.ExchangeResumeToken(metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", headers[0])), req)
}

type regionalVerifier struct {
	keys   resumetoken.Verifier
	legacy oauth.Verifier
}

func NewRegionalResumeVerifier(ctx context.Context, versions []string, keys string, legacy oauth.Verifier) (oauth.Verifier, error) {
	verifier := make(resumetoken.Verifier)
	if keys != "" {
		var err error
		verifier, err = resumetoken.NewVerifier(keys)
		if err != nil {
			return nil, err
		}
	}
	if err := addKMSResumeKeys(ctx, verifier, versions, loadResumePublicKey); err != nil {
		return nil, err
	}

	return &regionalVerifier{keys: verifier, legacy: legacy}, nil
}

func addKMSResumeKeys(ctx context.Context, verifier resumetoken.Verifier, versions []string, load func(context.Context, string) (*ecdsa.PublicKey, string, error)) error {
	for _, version := range versions {
		pub, kid, err := load(ctx, strings.TrimSpace(version))
		if err != nil {
			return err
		}
		if _, exists := verifier[kid]; exists {
			return errors.New("duplicate resume key ID")
		}
		verifier[kid] = pub
	}

	return nil
}

func (v *regionalVerifier) VerifyClaims(ctx context.Context, raw string) (oauth.Claims, error) {
	id, regional, err := v.keys.Verify(raw)
	if err != nil {
		return oauth.Claims{}, err
	}
	if regional {
		return oauth.Claims{ClusterID: id, Scopes: []string{oauth.RequiredScope}}, nil
	}

	return v.legacy.VerifyClaims(ctx, raw)
}
