package dockerhub

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	artifactregistry "cloud.google.com/go/artifactregistry/apiv1"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	registrygoogle "github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/singleflight"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

type GCPRemoteRepository struct {
	repositoryURL string
	registry      *artifactregistry.Client
	authToken     authn.Authenticator
}

func NewGCPRemoteRepository(ctx context.Context, repositoryURL string) (*GCPRemoteRepository, error) {
	registry, err := artifactregistry.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating artifact registry client: %w", err)
	}

	authToken, err := getAuthToken(ctx, consts.DockerAuthConfig, consts.GoogleServiceAccountSecret)
	if err != nil {
		_ = registry.Close()

		return nil, fmt.Errorf("error getting auth token: %w", err)
	}

	return &GCPRemoteRepository{repositoryURL: repositoryURL, registry: registry, authToken: authToken}, nil
}

func (g *GCPRemoteRepository) GetImage(ctx context.Context, tag string, platform containerregistry.Platform) (containerregistry.Image, error) {
	tagWithoutRegistry, err := removeRegistryFromTag(tag)
	if err != nil {
		return nil, fmt.Errorf("error removing registry from tag: %w", err)
	}

	ref, err := name.ParseReference(g.repositoryURL + "/" + tagWithoutRegistry)
	if err != nil {
		return nil, fmt.Errorf("invalid image reference: %w", err)
	}

	img, err := remote.Image(ref, remote.WithAuth(g.authToken), remote.WithPlatform(platform), remote.WithContext(ctx))
	if err != nil {
		if registryErr, ok := errors.AsType[*transport.Error](err); ok &&
			(registryErr.StatusCode == http.StatusUnauthorized || registryErr.StatusCode == http.StatusForbidden) {
			return nil, fmt.Errorf("%w: %w", ErrRepositoryAccessDenied, err)
		}

		return nil, fmt.Errorf("error pulling image: %w", err)
	}

	return img, nil
}

func getAuthToken(ctx context.Context, authCfg, serviceAccountKey string) (authn.Authenticator, error) {
	if authCfg == "" {
		if serviceAccountKey != "" {
			return &authn.Basic{Username: "_json_key_base64", Password: serviceAccountKey}, nil
		}

		// The SDK retains this context for token refresh after the setup context
		// is canceled. Bound token requests independently; registry requests
		// still use their own caller context.
		client := &http.Client{Timeout: 10 * time.Second}
		authCtx := context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient, client)

		source, err := google.DefaultTokenSource(authCtx, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			return nil, err
		}

		// Discover credentials at setup, but keep transient token failures scoped
		// to image pulls instead of preventing the runtime from starting.
		auth := registrygoogle.NewTokenSourceAuthenticator(oauth2.ReuseTokenSource(nil, source))

		return &gcpRefreshAuthenticator{auth: auth}, nil
	}

	decoded, err := base64.URLEncoding.DecodeString(authCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode auth config: %w", err)
	}

	var cfg struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.Unmarshal(decoded, &cfg); err != nil {
		return nil, fmt.Errorf("invalid JSON auth config: %w", err)
	}

	return &authn.Basic{
		Username: cfg.Username,
		Password: cfg.Password,
	}, nil
}

// Token refresh survives individual pull cancellation, but canceled pulls must
// not wait behind the OAuth token source's refresh mutex.
type gcpRefreshAuthenticator struct {
	auth    authn.Authenticator
	refresh singleflight.Group
}

func (a *gcpRefreshAuthenticator) Authorization() (*authn.AuthConfig, error) {
	return a.AuthorizationContext(context.Background())
}

func (a *gcpRefreshAuthenticator) AuthorizationContext(ctx context.Context) (*authn.AuthConfig, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	result := a.refresh.DoChan("token", func() (any, error) {
		return a.auth.Authorization()
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case refreshed := <-result:
		if refreshed.Err != nil {
			return nil, refreshed.Err
		}

		return refreshed.Val.(*authn.AuthConfig), nil
	}
}

func (g *GCPRemoteRepository) Close() error {
	return g.registry.Close()
}
