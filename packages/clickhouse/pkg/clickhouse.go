package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type SandboxQueriesProvider interface {
	QuerySandboxTimeRange(ctx context.Context, sandboxID, teamID string) (start time.Time, end time.Time, err error)
	QuerySandboxMetrics(ctx context.Context, sandboxID, teamID string, start time.Time, end time.Time, step time.Duration) ([]Metrics, error)
	QueryLatestMetrics(ctx context.Context, sandboxIDs []string, teamID string) ([]Metrics, error)
}

type Clickhouse interface {
	SandboxQueriesProvider

	Close(ctx context.Context) error

	// Team metrics queries
	QueryTeamMetrics(ctx context.Context, teamID string, start time.Time, end time.Time, step time.Duration) ([]TeamMetrics, error)
	QueryMaxStartRateTeamMetrics(ctx context.Context, teamID string, start time.Time, end time.Time, step time.Duration) (MaxTeamMetric, error)
	QueryMaxConcurrentTeamMetrics(ctx context.Context, teamID string, start time.Time, end time.Time) (MaxTeamMetric, error)

	QuerySandboxLifecycle(ctx context.Context, sandboxID string) (SandboxLifecycle, error)
}

type Client struct {
	conn driver.Conn
}

func NewDriver(connectionString string) (driver.Conn, error) {
	options, err := clickhouse.ParseDSN(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ClickHouse DSN: %w", err)
	}

	options.MaxOpenConns = 10
	options.MaxIdleConns = 3

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	return conn, nil
}

func New(connectionString string) (*Client, error) {
	conn, err := NewDriver(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse driver: %w", err)
	}

	return &Client{conn: conn}, nil
}

// EndpointFromDSN returns the credential-stripped host:port for use in logs
// and metric attributes. Never use the raw DSN there — it contains the password.
// On parse failure returns a fixed sentinel so the DSN (which clickhouse-go's
// url.Error embeds verbatim) never reaches a log line.
func EndpointFromDSN(dsn string) (string, error) {
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return "", errors.New("parse DSN")
	}
	if len(options.Addr) == 0 {
		return "", errors.New("DSN has no addresses")
	}

	return options.Addr[0], nil
}

// Close drains the queue and flushes remaining items
func (c *Client) Close(context.Context) error {
	return c.conn.Close()
}
