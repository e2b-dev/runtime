package host

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsJSONKeys(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(Metrics{})
	require.NoError(t, err)

	var fields map[string]any
	require.NoError(t, json.Unmarshal(data, &fields))

	assert.ElementsMatch(t, []string{
		"ts",
		"cpu_count",
		"cpu_used_pct",
		"mem_total",
		"mem_used",
		"mem_cache",
		"disk_used",
		"disk_total",
	}, slices.Collect(maps.Keys(fields)))
}
