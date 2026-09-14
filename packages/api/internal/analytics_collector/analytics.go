package analyticscollector

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/posthog/posthog-go"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	teamGroup                = "team"
	placeholderTeamGroupUser = "backend"
	placeholderProperty      = "first interaction"

	infraVersionKey = "infra_version"
	infraVersion    = "v1"

	// duplicates the team group key: PostHog is not promoting $groups.team to $group_0
	teamIDKey = "team_id"

	jsSDKUserAgentPrefix     = "e2b-js-sdk/"
	pythonSDKUserAgentPrefix = "e2b-python-sdk/"
)

type PosthogClient struct {
	client posthog.Client
}

func NewPosthogClient(ctx context.Context, posthogAPIKey string) (*PosthogClient, error) {
	posthogLogger := posthog.StdLogger(log.New(os.Stderr, "posthog ", log.LstdFlags))

	if strings.TrimSpace(posthogAPIKey) == "" {
		logger.L().Info(ctx, "No Posthog API key provided, silencing logs")

		writer := &utils.NoOpWriter{}
		posthogLogger = posthog.StdLogger(log.New(writer, "posthog ", log.LstdFlags))
	}

	client, err := posthog.NewWithConfig(posthogAPIKey, posthog.Config{
		Interval:  30 * time.Second,
		BatchSize: 100,
		Verbose:   false,
		Logger:    posthogLogger,
	})
	if err != nil {
		logger.L().Fatal(ctx, "error initializing Posthog client", zap.Error(err))
	}

	return &PosthogClient{
		client: client,
	}, nil
}

func (p *PosthogClient) Close() error {
	return p.client.Close()
}

func (p *PosthogClient) IdentifyAnalyticsTeam(ctx context.Context, teamID string, teamName string) {
	err := p.client.Enqueue(posthog.GroupIdentify{
		Type: teamGroup,
		Key:  teamID,
		Properties: posthog.NewProperties().
			Set(placeholderProperty, true).
			Set("name", teamName),
	},
	)
	if err != nil {
		logger.L().Error(ctx, "error when setting group property in Posthog", zap.Error(err))
	}
}

func (p *PosthogClient) CreateAnalyticsTeamEvent(ctx context.Context, teamID, event string, properties posthog.Properties) {
	err := p.client.Enqueue(posthog.Capture{
		DistinctId: placeholderTeamGroupUser,
		Event:      event,
		Properties: properties.Set(infraVersionKey, infraVersion).Set(teamIDKey, teamID),
		Groups: posthog.NewGroups().
			Set("team", teamID),
	})
	if err != nil {
		logger.L().Error(ctx, "error when sending event to Posthog", zap.Error(err))
	}
}

func (p *PosthogClient) GetPackageToPosthogProperties(header *http.Header) posthog.Properties {
	properties := posthog.NewProperties().
		Set("browser", header.Get("browser")).
		Set("lang", header.Get("lang")).
		Set("lang_version", header.Get("lang_version")).
		Set("machine", header.Get("machine")).
		Set("os", header.Get("os")).
		Set("package_version", header.Get("package_version")).
		Set("processor", header.Get("processor")).
		Set("publisher", header.Get("publisher")).
		Set("release", header.Get("release")).
		Set("sdk_runtime", header.Get("sdk_runtime")).
		Set("system", header.Get("system"))

	if userAgent := header.Get("User-Agent"); userAgent != "" {
		properties = properties.Set("user_agent", userAgent)

		if name, version, ok := integrationFromUserAgent(userAgent); ok {
			properties = properties.
				Set("integration", name).
				Set("integration_version", version)
		}
	}

	return properties
}

// integrationFromUserAgent extracts the integration wrapping the E2B SDK from
// a User-Agent like "e2b-js-sdk/1.2.3 e2b-cli/1.0.5": the first "name/version"
// token following an SDK token. Requiring the SDK token first prevents
// misreading browser User-Agents (e.g. "Mozilla/5.0 ...") as integrations.
func integrationFromUserAgent(userAgent string) (name, version string, ok bool) {
	sawSDK := false

	for token := range strings.FieldsSeq(userAgent) {
		if strings.HasPrefix(token, jsSDKUserAgentPrefix) || strings.HasPrefix(token, pythonSDKUserAgentPrefix) {
			sawSDK = true

			continue
		}

		if !sawSDK {
			continue
		}

		if name, version, found := strings.Cut(token, "/"); found && name != "" && version != "" {
			return name, version, true
		}
	}

	return "", "", false
}
