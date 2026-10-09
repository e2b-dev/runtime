//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

type Metrics struct {
	Timestamp int64 `json:"ts"` // Unix Timestamp in UTC

	CPUCount       int64   `json:"cpu_count"`    // Online CPU cores
	CPUUsedPercent float64 `json:"cpu_used_pct"` // Percent rounded to 2 decimal places

	// Older envd omits these fields; CPUPossible can also be 0 if sysfs fails.
	CPUPossible       int64 `json:"cpu_possible"`         // CPUs the guest can bring online
	CPUTarget         int64 `json:"cpu_target"`           // Online count last requested, 0 when none was
	CPUTargetAttempts int64 `json:"cpu_target_attempts"`  // Attempts at reaching CPUTarget since it was set
	CPUWritePendingMs int64 `json:"cpu_write_pending_ms"` // How long a CPU online/offline write has been running; growing means stuck

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
	err = decodeEnvdResult(response.Body, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}
