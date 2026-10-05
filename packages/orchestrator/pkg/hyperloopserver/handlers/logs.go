//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var (
	logForwardMeter      = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/hyperloopserver/handlers")
	logForwardWriteCount = mustLogForwardCounter(
		logForwardWriteCountName,
		"Number of hyperloop log forward HTTP attempts by route and result",
	)
	logForwardShadowInflight = mustLogForwardUpDownCounter(
		"hyperloop_log_forward_shadow_inflight",
		"Current number of in-flight best-effort shadow log forwards",
	)
)

const (
	logForwardWriteCountName = "orchestrator.hyperloop.log_forward.write_count"
	// staleLogWarningInterval is a sampler tick bucketed on entry.Time (the wall clock): the sampler
	// stores resetAt = entry.Time + tick, so after a backward clock step warnings stay suppressed until
	// the wall clock catches back up past resetAt -- silence lasting roughly the step size, self-healing
	// once time recovers. Accepted trade-off of using the stock zap sampler; the per-drop metric is
	// unaffected and still records every drop. Emitted warnings deliberately omit a suppressed count;
	// the per-drop metric carries the magnitude.
	staleLogWarningInterval = 30 * time.Second
)

func mustLogForwardCounter(name, description string) metric.Int64Counter {
	counter, err := logForwardMeter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

func mustLogForwardUpDownCounter(name, description string) metric.Int64UpDownCounter {
	counter, err := logForwardMeter.Int64UpDownCounter(name, metric.WithDescription(description))
	if err != nil {
		return nil
	}

	return counter
}

func (h *APIStore) Logs(c *gin.Context) {
	ctx := c.Request.Context()
	sbx, err := h.sandboxes.GetByHostPort(c.Request.RemoteAddr)
	if err != nil {
		h.sendAPIStoreError(c, http.StatusBadRequest, "Error when finding source sandbox")
		ip, _, _ := net.SplitHostPort(c.Request.RemoteAddr)
		h.logger.Error(ctx, "error finding sandbox for source addr", logger.WithSandboxIP(ip), zap.Error(err))

		return
	}

	sbxID := sbx.Runtime.SandboxID

	payload := make(map[string]any)
	if err := c.ShouldBindJSON(&payload); err != nil {
		h.sendAPIStoreError(c, http.StatusBadRequest, "Invalid body for logs")
		h.logger.Error(ctx, "error when parsing sandbox logs request", zap.Error(err), logger.WithSandboxID(sbxID))

		return
	}

	err = h.validatePayloadSandboxID(payload, sbxID)
	if err != nil {
		h.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandboxID in logs payload")
		// Inflight logs with old sandboxID from snapshotted sandbox
		// Change to error once we have a way how to tell sandbox to flush and stop sending logs when being paused
		h.logger.Warn(ctx, "error when parsing sandbox logs request", zap.Error(err), logger.WithSandboxID(sbxID))

		return
	}

	if hasStaleLogTimestamp(payload, sbx.LifecycleStartedAt) {
		h.sendAPIStoreError(c, http.StatusBadRequest, "Log timestamp predates this sandbox's resume")
		recordLogForwardWrite(ctx, "ingest", "dropped", "stale_timestamp")
		h.staleWarnLogger.Warn(
			ctx,
			"dropping envd log with a stale pre-resume timestamp",
			logger.WithSandboxID(sbxID),
		)

		return
	}

	// Downstream normalization relies on one trusted value for each identity field.
	setTrustedLogIdentity(
		payload,
		sbxID,
		sbx.Runtime.TemplateID,
		sbx.Runtime.TeamID,
		sbx.Runtime.BuildID,
	)
	if sbx.Runtime.TeamID == "" {
		recordLogForwardWrite(ctx, "ingest", "degraded", "missing_team_id")
	}

	logs, err := json.Marshal(payload)
	if err != nil {
		h.sendAPIStoreError(c, http.StatusInternalServerError, "Error when parsing logs payload")
		h.logger.Error(ctx, "error when parsing logs payload", zap.Error(err), logger.WithSandboxID(sbxID))

		return
	}

	// Resolve log destinations from LaunchDarkly (cached behind a short TTL),
	// falling back to the fixed collector address. This lets operators retarget
	// logs without a redeploy.
	route := h.logWriteConfig.Resolve(ctx)

	// Fire-and-forget shadow writes: never affect the response. Concurrency is
	// bounded by the resolved route limit; excess writes are dropped silently to avoid
	// unbounded goroutine growth (and a shadow log storm) under high volume.
	maxInflight := route.MaxInflightShadowWrites
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflightShadowWrites
	}
	for _, shadowURL := range route.ShadowURLs {
		if !h.tryAcquireShadow(maxInflight) {
			// Semaphore full: drop this shadow write.
			recordLogForwardWrite(ctx, "shadow", "dropped", "saturated")

			continue
		}
		recordLogForwardShadowInflight(ctx, 1)

		go func(url string, payload []byte) {
			defer func() {
				h.shadowInflight.Add(-1)
				recordLogForwardShadowInflight(context.WithoutCancel(ctx), -1)
			}()

			shadowCtx := context.WithoutCancel(ctx)
			if err := h.forwardLogs(shadowCtx, url, payload, route.Timeout); err != nil {
				recordLogForwardWrite(shadowCtx, "shadow", "failure", "send_error")

				return
			}
			recordLogForwardWrite(shadowCtx, "shadow", "success", "")
		}(shadowURL, logs)
	}

	// The primary write controls the response, preserving today's behavior.
	if err := h.forwardLogs(c.Request.Context(), route.PrimaryURL, logs, route.Timeout); err != nil {
		recordLogForwardWrite(ctx, "primary", "failure", "send_error")
		h.sendAPIStoreError(c, http.StatusInternalServerError, "Error when forwarding sandbox logs")
		h.logger.Error(ctx, "error when forwarding sandbox logs", zap.Error(err), logger.WithSandboxID(sbxID))

		return
	}
	recordLogForwardWrite(ctx, "primary", "success", "")

	c.Status(http.StatusOK)
}

func setTrustedLogIdentity(payload map[string]any, sandboxID, templateID, teamID, buildID string) {
	for _, field := range [...]string{
		"instanceID", "sandboxID", "sandbox_id", "sandbox.id",
		"envID", "env_id", "env.id", "templateID", "template_id", "template.id",
		"teamID", "team_id", "team.id",
		"buildID", "build_id", "build.id",
		// The collector routes on internal; only the platform may set it.
		"internal",
	} {
		delete(payload, field)
	}

	payload["instanceID"] = sandboxID
	payload["envID"] = templateID
	payload["teamID"] = teamID
	if buildID != "" {
		payload["buildID"] = buildID
	}
}

func (h *APIStore) tryAcquireShadow(maxInflight int64) bool {
	for {
		current := h.shadowInflight.Load()
		if current >= maxInflight {
			return false
		}
		if h.shadowInflight.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func recordLogForwardWrite(ctx context.Context, route, result, reason string) {
	if logForwardWriteCount == nil {
		return
	}

	logForwardWriteCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("route", route),
		attribute.String("result", result),
		attribute.String("reason", reason),
	))
}

func recordLogForwardShadowInflight(ctx context.Context, delta int64) {
	if logForwardShadowInflight == nil {
		return
	}

	logForwardShadowInflight.Add(ctx, delta)
}

// forwardLogs POSTs the marshaled logs payload to url, bounded by timeout.
func (h *APIStore) forwardLogs(ctx context.Context, url string, payload []byte, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("error creating request to forward sandbox logs: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")
	response, err := h.collectorClient.Do(request)
	if err != nil {
		return fmt.Errorf("error forwarding sandbox logs: %w", err)
	}
	defer response.Body.Close()

	// Always drain so the transport can reuse the connection; the body itself
	// is never surfaced in the returned error (it may echo request content).
	drainErr := drainLogForwardResponse(response.Body)

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		statusErr := fmt.Errorf("error forwarding sandbox logs: unexpected HTTP status %d", response.StatusCode)

		return errors.Join(statusErr, drainErr)
	}

	return drainErr
}

func drainLogForwardResponse(body io.Reader) error {
	if _, err := io.Copy(io.Discard, body); err != nil {
		return fmt.Errorf("error draining log forward response body: %w", err)
	}

	return nil
}

// validatePayloadSandboxID checks if the payload contains correct instanceID to prevent slow requests to contaminating the logs of other sandboxes.
func (h *APIStore) validatePayloadSandboxID(payload map[string]any, sbxID string) error {
	if payload["instanceID"] == nil {
		return errors.New("missing sandboxID in logs payload")
	}

	payloadSandboxID, ok := payload["instanceID"].(string)
	if !ok {
		return fmt.Errorf("instanceID in logs payload is not a string: %v", payload["instanceID"])
	}

	if payloadSandboxID != sbxID {
		return fmt.Errorf("sandboxID in logs payload does not match the sandboxID of the source sandbox (%s != %s)", payloadSandboxID, sbxID)
	}

	return nil
}

// Matches envd's zerolog timestamp format.
const envdTimestampLayout = time.RFC3339Nano

// Allows normal host/guest clock skew.
const clockSkewTolerance = time.Minute

// True if timestamp predates resume.
func hasStaleLogTimestamp(payload map[string]any, lifecycleStart time.Time) bool {
	if lifecycleStart.IsZero() {
		return false
	}

	raw, ok := payload["timestamp"].(string)
	if !ok {
		return false
	}

	ts, err := time.Parse(envdTimestampLayout, raw)
	if err != nil {
		return false
	}

	return ts.Before(lifecycleStart.Add(-clockSkewTolerance))
}
