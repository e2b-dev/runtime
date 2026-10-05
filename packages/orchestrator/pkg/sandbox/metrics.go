//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// Caps the guest-supplied /metrics body; envd's is a few KB even with its 32 OOM kills.
const maxMetricsBodySize = 1 << 20

type Metrics struct {
	Timestamp int64 `json:"ts"` // Unix Timestamp in UTC

	CPUCount       int64   `json:"cpu_count"`    // Total CPU cores
	CPUUsedPercent float64 `json:"cpu_used_pct"` // Percent rounded to 2 decimal places

	MemTotal int64 `json:"mem_total"` // Total virtual memory in bytes
	MemUsed  int64 `json:"mem_used"`  // Used virtual memory in bytes
	MemCache int64 `json:"mem_cache"` // Cached memory (page cache) in bytes

	DiskUsed  int64 `json:"disk_used"`  // Used disk space in bytes
	DiskTotal int64 `json:"disk_total"` // Total disk space in bytes

	// Latest OOM kills, oldest first; nil when envd doesn't know them.
	OomKills *[]envd.OOMKill `json:"oom_kills,omitempty"`
}

func (c *Checks) GetMetrics(ctx context.Context, timeout time.Duration) (*Metrics, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	address := fmt.Sprintf("http://%s:%d/metrics", c.sandbox.Slot.HostIPString(), consts.DefaultEnvdServerPort)

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}

	if c.sandbox.Config.Envd.AccessToken != nil {
		request.Header.Set("X-Access-Token", *c.sandbox.Config.Envd.AccessToken)
	}

	response, err := sandboxHttpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("unexpected status code: %d", response.StatusCode)

		return nil, err
	}

	var m Metrics
	err = json.NewDecoder(io.LimitReader(response.Body, maxMetricsBodySize)).Decode(&m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}
