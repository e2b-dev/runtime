package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/queries"
)

type noRowsDB struct{}

func (d noRowsDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (d noRowsDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (d noRowsDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return noRowsRow{}
}

type noRowsRow struct{}

func (r noRowsRow) Scan(...any) error {
	return pgx.ErrNoRows
}

// recordDB returns a single sandbox record row whose stopped_at is configurable,
// so we can exercise the retentionExpired computation in the handler.
type recordDB struct {
	stoppedAt *time.Time
}

func (d recordDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (d recordDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, nil
}

func (d recordDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return recordRow(d)
}

type recordRow struct {
	stoppedAt *time.Time
}

// Scan fills the destinations in the exact order produced by the generated
// GetSandboxRecordByTeamAndSandboxID query.
func (r recordRow) Scan(dest ...any) error {
	*dest[0].(*string) = "sbx_1"         // sandbox_id
	*dest[1].(*string) = "tmpl_1"        // template_id
	*dest[2].(*int64) = 1                // vcpu
	*dest[3].(*int64) = 512              // ram_mb
	*dest[4].(*int64) = 1024             // total_disk_size_mb
	*dest[5].(*time.Time) = time.Now()   // started_at
	*dest[6].(**time.Time) = r.stoppedAt // stopped_at
	*dest[7].(**string) = nil            // domain
	*dest[8].(*string) = ""              // alias

	return nil
}

func TestGetSandboxesSandboxIDRecordReturns404WhenNotFound(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes/sbx_1/record", nil)

	teamID := uuid.New()
	auth.SetTeamInfoForTest(t, ctx, &authtypes.Team{
		Team: &authqueries.Team{
			ID: teamID,
		},
	})

	store := &APIStore{
		db: &sqlcdb.Client{
			Queries: queries.New(noRowsDB{}),
		},
	}

	store.GetSandboxesSandboxIDRecord(ctx, api.SandboxID("sbx_1"))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, recorder.Code)
	}

	var response struct {
		Code    int32  `json:"code"`
		Message string `json:"message"`
	}

	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	if response.Code != int32(http.StatusNotFound) {
		t.Fatalf("expected code %d, got %d", http.StatusNotFound, response.Code)
	}
}

func TestGetSandboxesSandboxIDRecordRetentionExpired(t *testing.T) {
	t.Parallel()

	stoppedLongAgo := time.Now().Add(-8 * 24 * time.Hour)
	stoppedRecently := time.Now().Add(-24 * time.Hour)
	stoppedOverAYearAgo := time.Now().Add(-366 * 24 * time.Hour)

	testCases := []struct {
		name                   string
		stoppedAt              *time.Time
		limits                 *authtypes.TeamLimits
		retentionExpired       bool
		eventsRetentionExpired bool
	}{
		{name: "ended more than retention ago", stoppedAt: &stoppedLongAgo, retentionExpired: true, eventsRetentionExpired: true},
		{name: "ended within retention", stoppedAt: &stoppedRecently, retentionExpired: false, eventsRetentionExpired: false},
		{name: "still running", stoppedAt: nil, retentionExpired: false, eventsRetentionExpired: false},
		{name: "extended team retention keeps events, monitoring expired", stoppedAt: &stoppedLongAgo, limits: &authtypes.TeamLimits{EventsTTLDays: 30}, retentionExpired: true, eventsRetentionExpired: false},
		{name: "team retention from limits expires events", stoppedAt: &stoppedLongAgo, limits: &authtypes.TeamLimits{EventsTTLDays: 7}, retentionExpired: true, eventsRetentionExpired: true},
		{name: "events retention capped at max", stoppedAt: &stoppedOverAYearAgo, limits: &authtypes.TeamLimits{EventsTTLDays: 1000}, retentionExpired: true, eventsRetentionExpired: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes/sbx_1/record", nil)

			auth.SetTeamInfoForTest(t, ctx, &authtypes.Team{
				Team: &authqueries.Team{
					ID: uuid.New(),
				},
				Limits: tc.limits,
			})

			store := &APIStore{
				db: &sqlcdb.Client{
					Queries: queries.New(recordDB{stoppedAt: tc.stoppedAt}),
				},
			}

			store.GetSandboxesSandboxIDRecord(ctx, api.SandboxID("sbx_1"))

			if recorder.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d (body: %s)", http.StatusOK, recorder.Code, recorder.Body.String())
			}

			var record api.SandboxRecord
			if err := json.Unmarshal(recorder.Body.Bytes(), &record); err != nil {
				t.Fatalf("failed to parse response body: %v", err)
			}

			if record.RetentionExpired != tc.retentionExpired {
				t.Fatalf("expected retentionExpired=%v, got %v", tc.retentionExpired, record.RetentionExpired)
			}

			if record.EventsRetentionExpired != tc.eventsRetentionExpired {
				t.Fatalf("expected eventsRetentionExpired=%v, got %v", tc.eventsRetentionExpired, record.EventsRetentionExpired)
			}
		})
	}
}
