package analyticscollector

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jellydator/ttlcache/v3"
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

	// teamIdentifyTTL bounds how long IdentifyAnalyticsTeam suppresses an
	// unchanged identify for a team. PostHog persists group properties, so
	// re-sending an identical $groupidentify only adds ingestion volume; the
	// periodic refresh covers properties lost or edited on the PostHog side.
	teamIdentifyTTL = 24 * time.Hour

	// identifiedTeamsCapacity bounds the identify cache, which has no eviction
	// goroutine: expired entries read as misses and are refreshed in place, so
	// the bound only matters for teams the process never sees again.
	identifiedTeamsCapacity = 100_000

	jsSDKUserAgentPrefix     = "e2b-js-sdk/"
	pythonSDKUserAgentPrefix = "e2b-python-sdk/"
)

type PosthogClient struct {
	client posthog.Client

	// identifiedTeams maps a team ID to the name last sent to PostHog.
	identifiedTeams *ttlcache.Cache[string, string]
	// identifying marks the teams with an identify in flight. Concurrent
	// callers for such a team return instead of waiting, and one identify at
	// a time per team keeps the cached name equal to the one enqueued last.
	identifying sync.Map
}

func NewPosthogClient(ctx context.Context, posthogAPIKey string) (*PosthogClient, error) {
	posthogLogger := posthog.StdLogger(log.New(os.Stderr, "posthog ", log.LstdFlags))

	if strings.TrimSpace(posthogAPIKey) == "" {
		logger.L().Info(ctx, "No Posthog API key provided, silencing logs")

		writer := &utils.NoOpWriter{}
		posthogLogger = posthog.StdLogger(log.New(writer, "posthog ", log.LstdFlags))
	}

	identifiedTeams := newIdentifiedTeams(teamIdentifyTTL)

	client, err := posthog.NewWithConfig(posthogAPIKey, posthog.Config{
		Interval:  30 * time.Second,
		BatchSize: 100,
		Verbose:   false,
		Logger:    posthogLogger,
		Callback:  identifyFailures{identifiedTeams: identifiedTeams},
	})
	if err != nil {
		logger.L().Fatal(ctx, "error initializing Posthog client", zap.Error(err))
	}

	return &PosthogClient{
		client:          client,
		identifiedTeams: identifiedTeams,
	}, nil
}

// newPosthogClient wraps an enqueuing client. Tests inject a recording client
// and a short identify TTL through it.
func newPosthogClient(client posthog.Client, identifyTTL time.Duration) *PosthogClient {
	return &PosthogClient{
		client:          client,
		identifiedTeams: newIdentifiedTeams(identifyTTL),
	}
}

// newIdentifiedTeams builds the identify cache. It is deliberately never
// started: reads treat expired entries as misses and writes refresh them in
// place, so the eviction goroutine would only reclaim memory, and its Stop is
// a no-op when Close runs before that goroutine has been scheduled.
func newIdentifiedTeams(ttl time.Duration) *ttlcache.Cache[string, string] {
	return ttlcache.New(
		ttlcache.WithTTL[string, string](ttl),
		ttlcache.WithDisableTouchOnHit[string, string](),
		ttlcache.WithCapacity[string, string](identifiedTeamsCapacity),
	)
}

// identifyFailures is the posthog-go upload callback. A $groupidentify that
// PostHog failed to ingest forgets its team, so the next request re-sends it
// instead of waiting out the TTL.
type identifyFailures struct {
	identifiedTeams *ttlcache.Cache[string, string]
}

func (identifyFailures) Success(posthog.APIMessage) {}

func (f identifyFailures) Failure(msg posthog.APIMessage, _ error) {
	identify, ok := msg.(posthog.GroupIdentifyInApi)
	if !ok || identify.Properties["$group_type"] != teamGroup {
		return
	}

	if teamID, ok := identify.Properties["$group_key"].(string); ok {
		f.identifiedTeams.Delete(teamID)
	}
}

func (p *PosthogClient) Close() error {
	return p.client.Close()
}

// IdentifyAnalyticsTeam sets the team group's properties in PostHog. Handlers
// call it on every request, so the identify is sent only when the cached name
// for the team is missing or differs: for a new or renamed team, after a
// restart, once per TTL, and again after PostHog reports a failed upload.
// Nothing here waits: a caller that finds an identify for the team already in
// flight returns, and the next request re-checks the cache.
// Any property added here that can vary per team must join the cached value.
func (p *PosthogClient) IdentifyAnalyticsTeam(ctx context.Context, teamID string, teamName string) {
	if p.teamIdentified(teamID, teamName) {
		return
	}

	if _, inFlight := p.identifying.LoadOrStore(teamID, struct{}{}); inFlight {
		return
	}
	defer p.identifying.Delete(teamID)

	if p.teamIdentified(teamID, teamName) {
		return
	}

	// Record the identify before enqueueing: posthog-go can report an upload
	// failure from its own goroutine as soon as the message is handed off, and
	// that Delete has to land after this Set.
	p.identifiedTeams.Set(teamID, teamName, ttlcache.DefaultTTL)

	err := p.client.Enqueue(posthog.GroupIdentify{
		Type: teamGroup,
		Key:  teamID,
		Properties: posthog.NewProperties().
			Set(placeholderProperty, true).
			Set("name", teamName),
	},
	)
	if err != nil {
		p.identifiedTeams.Delete(teamID)
		logger.L().Error(ctx, "error when setting group property in Posthog", zap.Error(err))
	}
}

func (p *PosthogClient) teamIdentified(teamID, teamName string) bool {
	sent := p.identifiedTeams.Get(teamID)

	return sent != nil && sent.Value() == teamName
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
