// Command storeseed writes synthetic sandbox records for one team into the
// sandbox store, in the key form chosen, so a list can be measured against a
// known population, and -delete removes the ones this tool wrote.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redisbackend "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/factories"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	// seedMarker is the metadata key of a seeded record, so -delete removes
	// only what this tool wrote and the list endpoint's metadata filter
	// (storeseed=true) finds them.
	seedMarker = "storeseed"

	// concurrency bounds the store operations in flight; each is a handful
	// of round trips under its own sandbox lock.
	concurrency = 16

	connectTimeout = 30 * time.Second
)

const usage = `Usage: storeseed [flags]

Writes synthetic sandbox records for one team into the sandbox store in the
key form chosen; -delete removes the ones this tool wrote.

The connection comes from the API's environment (REDIS_URL or
REDIS_CLUSTER_URL, REDIS_TLS_CA_BASE64, REDIS_TLS_ENABLED, REDIS_PASSWORD), so
the tool runs as a job with the API's ConfigMap and Secret. A cluster client
follows MOVED replies to the nodes' private addresses: run it inside the VPC,
never over a port-forward.

Seeded records are real to every reader: they count toward the team's
concurrency limit, appear in its lists, and once their end time passes the
evictor removes them like sandboxes whose node is gone, emitting a stopped
sandbox's usage events for each. Seed a dedicated, non-billed test team and
run -delete before the end time. The healer's census, the
api.redis_storage.records.form gauge, shows the records of each form after
its next sweep.

Sandbox-form records are refused unless the store carries the scheme marker
an API that knows the per-sandbox key scheme writes on start: an older API
would kill every such record's sandbox as an orphan.

Flags:
`

// redisEnv is the API's Redis connection environment.
type redisEnv struct {
	URL         string `env:"REDIS_URL"`
	ClusterURL  string `env:"REDIS_CLUSTER_URL"`
	TLSCABase64 string `env:"REDIS_TLS_CA_BASE64"`
	TLSEnabled  bool   `env:"REDIS_TLS_ENABLED"`
	Password    string `env:"REDIS_PASSWORD"`
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), usage)
		flag.PrintDefaults()
	}
	team := flag.String("team", "", "ID of the team the records belong to")
	count := flag.Int("count", 100, "records to write")
	form := flag.String("form", "team", "key form of the records written: team or sandbox")
	lifetime := flag.Duration("lifetime", 4*time.Hour, "how long the records written run before the evictor removes them")
	del := flag.Bool("delete", false, "remove the team's seeded records instead of writing")
	flag.Parse()

	e, err := env.ParseAs[redisEnv]()
	if err != nil {
		log.Fatal(err)
	}
	config := factories.RedisConfig{RedisURL: e.URL, RedisClusterURL: e.ClusterURL, RedisTLSCABase64: e.TLSCABase64, RedisTLSEnabled: e.TLSEnabled, RedisPassword: e.Password}
	if (config.RedisURL == "") == (config.RedisClusterURL == "") {
		log.Fatal("exactly one of REDIS_URL and REDIS_CLUSTER_URL is required")
	}
	teamID, err := uuid.Parse(*team)
	if err != nil {
		log.Fatalf("-team: %s", err)
	}
	if *form != "team" && *form != "sandbox" {
		log.Fatal("-form must be team or sandbox")
	}

	err = run(context.Background(), config, func(ctx context.Context, client redis.UniversalClient, store *redisbackend.Storage) error {
		store.SetPerSandboxWrites(*form == "sandbox")
		if *del {
			return deleteSeeded(ctx, store, teamID)
		}
		if *form == "sandbox" {
			if err := requireSchemeMarker(ctx, client); err != nil {
				return err
			}
		}

		return seed(ctx, store, teamID, *count, *form, *lifetime)
	})
	if err != nil {
		log.Fatal(err)
	}
}

// run opens the store as the API does and calls op with it, closing
// everything on return.
func run(ctx context.Context, config factories.RedisConfig, op func(context.Context, redis.UniversalClient, *redisbackend.Storage) error) error {
	// The store logs what it leaves for the healer; without a global logger
	// those lines would be dropped.
	l, err := logger.NewLogger(logger.LoggerConfig{ServiceName: "storeseed", IsInternal: true, EnableConsole: true})
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	defer l.Sync()
	logger.ReplaceGlobals(ctx, l)

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	client, err := factories.NewRedisClient(connectCtx, config)
	cancel()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer client.Close()

	store, err := redisbackend.NewStorage(client, noop.NewMeterProvider())
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}

	return op(ctx, client, store)
}

// requireSchemeMarker fails when the store carries no scheme marker: no API
// that knows the per-sandbox key scheme has started on it, and an older one
// would kill every sandbox-form record's sandbox as an orphan.
func requireSchemeMarker(ctx context.Context, client redis.UniversalClient) error {
	err := client.Get(ctx, redisbackend.SchemeMarkerKey).Err()
	if errors.Is(err, redis.Nil) {
		return fmt.Errorf("the store carries no scheme marker, so no API that knows the per-sandbox key scheme has started on it; seed the team form, or, if no older API will ever run against this store, write the marker by hand: SET %s %s", redisbackend.SchemeMarkerKey, redisbackend.SchemeMinRelease)
	}
	if err != nil {
		return fmt.Errorf("scheme marker: %w", err)
	}

	return nil
}

// seed writes count records in the store's write form, each under its own
// lock, as the API writes a new sandbox.
func seed(ctx context.Context, store *redisbackend.Storage, teamID uuid.UUID, count int, form string, lifetime time.Duration) error {
	if count <= 0 {
		return errors.New("-count must be positive")
	}

	start := time.Now()
	end := start.Add(lifetime)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	for range count {
		g.Go(func() error {
			return store.Add(ctx, seedRecord(teamID, start, end))
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	fmt.Printf("wrote %d %s-form records for team %s\n", count, form, teamID)

	return nil
}

// seedRecord is a running sandbox of the team that exists in the store only:
// its node, template and client name this tool, its metadata carries the
// marker -delete selects on, and its ID has the API's sandbox ID shape. It
// is real to every reader, as the usage text warns.
func seedRecord(teamID uuid.UUID, start, end time.Time) sandboxtypes.Sandbox {
	return sandboxtypes.Sandbox{
		SandboxID:         "i" + id.Generate(),
		TemplateID:        seedMarker,
		BaseTemplateID:    seedMarker,
		ClientID:          seedMarker,
		ExecutionID:       uuid.NewString(),
		TeamID:            teamID,
		BuildID:           uuid.New(),
		Metadata:          map[string]string{seedMarker: "true"},
		MaxInstanceLength: end.Sub(start),
		StartTime:         start,
		EndTime:           end,
		VCpu:              1,
		RamMB:             512,
		TotalDiskSizeMB:   1024,
		NodeID:            seedMarker,
		State:             sandboxtypes.StateRunning,
	}
}

// deleteSeeded removes every record of the team that carries the seed
// marker, each by the execution it was written with.
func deleteSeeded(ctx context.Context, store *redisbackend.Storage, teamID uuid.UUID) error {
	items, err := store.TeamItems(ctx, teamID, nil)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	seeded := 0
	for _, sbx := range items {
		if sbx.Metadata[seedMarker] != "true" {
			continue
		}
		seeded++
		g.Go(func() error {
			return store.Remove(ctx, teamID, sbx.SandboxID, sbx.ExecutionID)
		})
	}
	if err := g.Wait(); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	fmt.Printf("deleted %d seeded records of the %d listed for team %s\n", seeded, len(items), teamID)

	return nil
}
