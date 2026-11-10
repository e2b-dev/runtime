package clickhouse

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MaxDate64 representable in Clickhouse https://clickhouse.com/docs/sql-reference/data-types/datetime64
var MaxDate64 = time.Date(2299, 12, 31, 23, 59, 59, 999999999, time.UTC)

// minQueryDate is the earliest bound sent to ClickHouse. clickhouse-go sends a
// DateNamed value as epoch seconds, and ClickHouse rejects a DateTime64
// parameter of fewer than five digits. No row predates it, so raising an
// earlier bound to it changes no result.
var minQueryDate = time.Unix(10_000, 0)

// dateParam binds t to a {name:DateTime64} query parameter at whole seconds.
func dateParam(name string, t time.Time) driver.NamedDateValue {
	if t.Before(minQueryDate) {
		t = minQueryDate
	}

	return clickhouse.DateNamed(name, t, clickhouse.Seconds)
}
