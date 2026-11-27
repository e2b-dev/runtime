package setup

import (
	"context"
	"net/http"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
)

func GetAPIClient() *api.ClientWithResponses {
	hc := http.Client{
		Timeout: apiTimeout,
	}

	c, err := api.NewClientWithResponses(APIServerURL, api.WithHTTPClient(&hc))
	if err != nil {
		panic(err)
	}

	return c
}

func WithAPIKey(apiKey ...string) func(ctx context.Context, req *http.Request) error {
	return func(_ context.Context, req *http.Request) error {
		apiKey_ := APIKey
		if len(apiKey) > 0 {
			apiKey_ = apiKey[0]
		}
		req.Header.Set("X-API-Key", apiKey_)

		return nil
	}
}

func WithTestsUserAgent() api.RequestEditorFn {
	return WithUserAgent("e2b-tests/infra")
}

func WithUserAgent(userAgent string) api.RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("User-Agent", userAgent)

		return nil
	}
}

func WithTeamID(teamID ...string) func(ctx context.Context, req *http.Request) error {
	teamID_ := TeamID
	if len(teamID) > 0 {
		teamID_ = teamID[0]
	}

	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("X-Team-ID", teamID_)

		return nil
	}
}
