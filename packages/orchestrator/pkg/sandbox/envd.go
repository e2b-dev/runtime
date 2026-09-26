//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	loopDelay = 5 * time.Millisecond
)

// envdInitExitType classifies the outcome of an envd init call.
type envdInitExitType string

const (
	envdInitExitSuccess  envdInitExitType = "success"
	envdInitExitTimeout  envdInitExitType = "timeout"
	envdInitExitCanceled envdInitExitType = "canceled"
	envdInitExitOther    envdInitExitType = "other"
	// envdInitExitTransient marks a retried attempt that failed but was not the
	// terminal outcome of the init episode.
	envdInitExitTransient envdInitExitType = "transient"
)

// classifyEnvdInitExit maps an init error to an exit_type.
func classifyEnvdInitExit(err error) envdInitExitType {
	switch {
	case err == nil:
		return envdInitExitSuccess
	case errors.Is(err, ErrWaitForEnvdTimeout), errors.Is(err, context.DeadlineExceeded):
		return envdInitExitTimeout
	case errors.Is(err, ErrFcProcessExited):
		return envdInitExitOther
	case errors.Is(err, context.Canceled):
		return envdInitExitCanceled
	default:
		return envdInitExitOther
	}
}

// envdOp is the path segment of a parameterless envd POST endpoint.
type envdOp string

const (
	envdOpFreeze   envdOp = "freeze"
	envdOpUnfreeze envdOp = "unfreeze"
	envdOpFsfreeze envdOp = "fsfreeze"
	envdOpFsthaw   envdOp = "fsthaw"
)

// doRequestWithInfiniteRetries does a request with infinite retries until the context is done.
// The parent context must be bounded — by a deadline/timeout, or by a cancel
// the caller races against sandbox liveness (WaitForEnvd, bestEffortEnvdReinit).
func (s *Sandbox) doRequestWithInfiniteRetries(
	ctx context.Context,
	method,
	address string,
) (*http.Response, int64, error) {
	requestCount := int64(0)

	jsonBody := &envd.PostInitJSONBody{
		LifecycleID:    s.LifecycleID,
		EnvVars:        s.Config.Envd.Vars,
		HyperloopIP:    s.config.NetworkConfig.OrchestratorInSandboxIPAddress,
		AccessToken:    utils.DerefOrDefault(s.Config.Envd.AccessToken, ""),
		DefaultUser:    utils.DerefOrDefault(s.Config.Envd.DefaultUser, ""),
		DefaultWorkdir: utils.DerefOrDefault(s.Config.Envd.DefaultWorkdir, ""),
		VolumeMounts:   s.convertMounts(s.Config.VolumeMounts),
		CaBundle:       s.CABundle,
	}

	for {
		jsonBody.Timestamp = time.Now()

		body, err := json.Marshal(jsonBody)
		if err != nil {
			return nil, requestCount, err
		}

		requestCount++
		reqCtx, cancel := context.WithTimeout(ctx, s.internalConfig.EnvdInitRequestTimeout)
		request, err := http.NewRequestWithContext(reqCtx, method, address, bytes.NewReader(body))
		if err != nil {
			cancel()

			return nil, requestCount, err
		}

		// make sure request to already authorized envd will not fail
		// this can happen in sandbox resume and in some edge cases when previous request was success, but we continued
		if s.Config.Envd.AccessToken != nil {
			request.Header.Set("X-Access-Token", *s.Config.Envd.AccessToken)
		}

		response, err := sandboxHttpClient.Do(request)
		cancel()

		if err == nil {
			return response, requestCount, nil
		}

		select {
		case <-ctx.Done():
			return nil, requestCount, fmt.Errorf("%w with cause: %w", ctx.Err(), context.Cause(ctx))
		case <-time.After(loopDelay):
		}
	}
}

// callEnvdFreeze issues the pre-pause freeze through envd's native POST /freeze --
// freezing the workload cgroups directly, with no Process.Start and no shell -- and
// returns what envd observed.
//
// We always send maxWaitMs, which is what asks for the structured result. An envd
// predating that parameter answers 204 instead; that is not an error, only an envd whose
// observed counts are unavailable, so the zero result comes back with ok=false.
func (s *Sandbox) callEnvdFreeze(ctx context.Context, timeout time.Duration, hierarchy bool, maxCgroups int) (result envd.FreezeResult, ok bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Tell envd how long to wait, derived from our own timeout minus a round-trip
	// margin, so one flag moves both halves and envd never waits past the point where
	// we would have given up on it.
	maxWaitMs := max((timeout - freezeRoundTripMargin).Milliseconds(), 1)

	query := fmt.Sprintf("maxWaitMs=%d", maxWaitMs)
	if hierarchy {
		// The flag lives here because envd has no LaunchDarkly, so the mode travels on
		// the request. FreezeResult echoes back what envd actually ran, which is what
		// makes an envd that ignores this parameter visible rather than assumed.
		query += fmt.Sprintf("&mode=%s&maxCgroups=%d", envd.PostFreezeParamsModeHierarchy, maxCgroups)
	}

	resp, err := s.doEnvdPost(ctx, string(envdOpFreeze), query)
	if err != nil {
		return envd.FreezeResult{}, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return envd.FreezeResult{}, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return envd.FreezeResult{}, false, fmt.Errorf("freeze returned %d: %s", resp.StatusCode, utils.Truncate(string(body), 100))
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return envd.FreezeResult{}, false, fmt.Errorf("decode freeze result: %w", err)
	}

	return result, true, nil
}

// callEnvdUnfreeze calls envd's native POST /unfreeze endpoint. Reserved for
// the pause-failure rollback path; the resume thaw runs via /init's deferred
// unfreeze and does not use this.
func (s *Sandbox) callEnvdUnfreeze(ctx context.Context, timeout time.Duration) error {
	return s.callEnvdPostOp(ctx, timeout, envdOpUnfreeze)
}

// callEnvdFsfreeze calls envd's native POST /fsfreeze endpoint to freeze the
// guest rootfs before a filesystem-only pause, flushing it to a consistent
// on-disk state.
func (s *Sandbox) callEnvdFsfreeze(ctx context.Context, timeout time.Duration) error {
	return s.callEnvdPostOp(ctx, timeout, envdOpFsfreeze)
}

// callEnvdFsthaw calls envd's native POST /fsthaw endpoint. Reserved for the
// pause-failure rollback path so a frozen rootfs can't leave the live VM
// deadlocked.
func (s *Sandbox) callEnvdFsthaw(ctx context.Context, timeout time.Duration) error {
	return s.callEnvdPostOp(ctx, timeout, envdOpFsthaw)
}

func (s *Sandbox) callEnvdPostOp(ctx context.Context, timeout time.Duration, op envdOp) error {
	return s.postEnvd(ctx, timeout, string(op))
}

// callEnvdCollapse calls envd's native POST /collapse endpoint, which compacts
// envd's own anonymous heap into 2 MiB hugepages before pause so it faults
// fewer distinct frames on resume. Unlike freeze/unfreeze it returns a body:
// the per-call collapse stats, which the caller records as metrics and span
// attributes.
func (s *Sandbox) callEnvdCollapse(ctx context.Context, timeout time.Duration) (envd.CollapseResult, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := s.doEnvdPost(ctx, "collapse")
	if err != nil {
		return envd.CollapseResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)

		return envd.CollapseResult{}, fmt.Errorf("collapse returned %d: %s", resp.StatusCode, string(body))
	}

	var result envd.CollapseResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return envd.CollapseResult{}, fmt.Errorf("decode collapse result: %w", err)
	}

	return result, nil
}

// postEnvd issues an authenticated POST to envd's /<path> endpoint with a tight,
// dedicated deadline and expects 204 No Content.
func (s *Sandbox) postEnvd(ctx context.Context, timeout time.Duration, path string) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := s.doEnvdPost(ctx, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, string(body))
	}

	return nil
}

// CallEnvdUpgrade triggers envd's POST /upgrade — the orchestrator-driven
// live-upgrade trigger. It streams the new envd binary
// from localSrcPath as the (authenticated) request body; envd writes it to
// guestBinPath inside the guest and then same-PID re-execs into it. Delivering
// over the token-authenticated /upgrade endpoint avoids the unauthenticated
// /files path that a runtime (post-/init) sandbox rejects.
//
// envd reads the whole body, then execs and never responds, so the connection
// drops without a reply: a transport error after the body was sent is the
// expected success path. The caller must follow with WaitForEnvd.
//
// execConfirmed reports whether the same-PID exec is CONFIRMED to have happened
// (a connection reset/EOF after the body was sent). A deadline OR a cancelled
// ctx returns (false, nil): ambiguous — envd may still be mid-handover on the
// old binary — so the caller must NOT treat a follow-up not-ready as an
// unrecoverable brick. The request is bounded by the timeout ctx deadline, not
// sandboxHttpClient's shorter client-level Timeout.
func (s *Sandbox) CallEnvdUpgrade(ctx context.Context, localSrcPath, guestBinPath string, timeout time.Duration) (execConfirmed bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	f, err := os.Open(localSrcPath)
	if err != nil {
		return false, fmt.Errorf("open envd source %s: %w", localSrcPath, err)
	}
	defer f.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.envdServerURL()+"/upgrade", f)
	if err != nil {
		return false, fmt.Errorf("build upgrade request: %w", err)
	}
	if fi, statErr := f.Stat(); statErr == nil {
		req.ContentLength = fi.Size()
	}
	if s.Config.Envd.AccessToken != nil {
		req.Header.Set("X-Access-Token", *s.Config.Envd.AccessToken)
	}
	req.Header.Set("X-Envd-Upgrade-Bin", guestBinPath)

	// Reuse the shared transport but drop sandboxHttpClient's short client-level
	// Timeout (10s) — it would preempt the deliverTimeout ctx above and cut the
	// upgrade off before envd finishes reading the body and exec'ing. The ctx
	// deadline is the sole delivery budget.
	resp, err := (&http.Client{Transport: sandboxHttpClient.Transport}).Do(req)
	if err != nil {
		// envd reads the whole body, then execs without responding, so a
		// transport error AFTER the request reached it (connection reset/EOF) is
		// the expected success path. But a failure to even reach a running envd
		// (connection refused, or a dial-phase failure) means the upgrade was
		// never delivered — surface it so the caller doesn't record a false
		// success. A deadline is deliberately NOT treated as delivery failure
		// (ambiguous — see isUpgradeDeliveryFailure — left to version confirm).
		if isUpgradeDeliveryFailure(err) {
			return false, fmt.Errorf("deliver upgrade to envd: %w", err)
		}
		// A context error — the deliverTimeout deadline OR a cancelled parent ctx
		// (e.g. the resume budget ran out) — means we gave up before observing the
		// exec, so it is NOT confirmed: envd may still be mid-handover on the old
		// binary. Report success-but-unconfirmed so the caller keeps a follow-up
		// not-ready recoverable rather than a fatal brick.
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false, nil
		}
		// Anything else here reached envd (it passed isUpgradeDeliveryFailure) and
		// isn't a context error, so it's the expected post-send connection
		// reset/EOF = the same-PID exec fired.
		return true, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)

		return false, fmt.Errorf("upgrade returned %d: %s", resp.StatusCode, string(body))
	}

	// envd answered instead of exec'ing — no swap happened, exec not confirmed.
	return false, nil
}

// isUpgradeDeliveryFailure reports whether an error from the /upgrade request
// means the binary never reached a running envd — a genuine failure — as opposed
// to the expected post-send connection drop when envd execs mid-response.
//
// A deadline is deliberately NOT treated as failure: it's ambiguous (envd may
// have exec'd and simply never answered), so it's left to the post-upgrade
// version confirmation to decide the true outcome.
func isUpgradeDeliveryFailure(err error) bool {
	// Covered (=> true, "the binary never reached a running envd, so no swap"):
	//   - syscall.ECONNREFUSED: nothing is listening on the envd port.
	//   - net.OpError with Op == "dial": the connection could not be established
	//     (DNS / dial-phase failure) before any byte was sent.
	// Deliberately NOT covered (=> false, i.e. treated as the expected
	// post-send drop): connection reset / EOF / unexpected EOF after the body
	// was sent (envd exec'd mid-response), and context deadline / timeout — the
	// latter is ambiguous (envd may have exec'd and simply never answered), so
	// it is left to the post-upgrade version confirmation to decide the outcome.
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}

	return false
}

// setLiveEnvdVersion records the version the running envd last reported.
func (s *Sandbox) setLiveEnvdVersion(v string) {
	s.liveEnvdVersion.Store(&v)
}

// LiveEnvdVersion returns the version the running envd last reported on /init,
// or "" if none has been captured yet.
func (s *Sandbox) LiveEnvdVersion() string {
	if p := s.liveEnvdVersion.Load(); p != nil {
		return *p
	}

	return ""
}

// EnvdHandoverResult is the live-upgrade handover outcome the running envd
// reported on /init (X-Envd-Handover). Its JSON tags match envd's
// api.handoverResult so the header unmarshals directly.
type EnvdHandoverResult struct {
	// Failed is true when the in-guest handover itself failed post-execve (the
	// workload was not re-adopted), so a version flip to the target is NOT a
	// healthy upgrade — the trigger fails the resume on it.
	Failed bool `json:"failed"`
	// Every item is total-carried + failed-subset (ok = total - failed).
	Procs          int `json:"procs"`
	ProcsFailed    int `json:"procs_failed"`
	Retained       int `json:"retained"`
	RetainedFailed int `json:"retained_failed"`
	Watchers       int `json:"watchers"`
	WatchersFailed int `json:"watchers_failed"`
}

// setHandoverResult records the handover outcome the running envd last reported.
func (s *Sandbox) setHandoverResult(h *EnvdHandoverResult) {
	s.handoverResult.Store(h)
}

// HandoverResult returns the last handover outcome the running envd reported on
// /init, or nil if it never booted from a live-upgrade handover.
func (s *Sandbox) HandoverResult() *EnvdHandoverResult {
	return s.handoverResult.Load()
}

// EnvdReportedDefaults is what the running envd last said it is effectively serving with,
// or nil if it never reported any.
//
// Nil is a capability answer rather than an empty one, and callers rely on that: the header
// and the handover blob's defaults field ship in the same envd, so an envd that reports
// nothing here is also one whose blob cannot carry the exec context across a swap.
func (s *Sandbox) EnvdReportedDefaults() *EnvdEffectiveDefaults {
	return s.envdReportedDefaults.Load()
}

// EnvdWorkdirWithheld reports that a recorded default workdir was not re-sent at start, so
// it lives only in the running envd's memory. False on every path that sends one.
func (s *Sandbox) EnvdWorkdirWithheld() bool {
	return s.envdWorkdirWithheld
}

// envdWorkdirWithheld decides whether a recorded workdir is being left in the running
// process instead of re-sent. BOTH terms are required: a recorded value alone is not
// withheld if the start path is also sending it, which is exactly the filesystem-only cold
// boot — it reconstructs and sends the recorded workdir, and it reaches the same
// live-upgrade gate as a memory resume.
func envdWorkdirWithheld(meta metadata.Template, sentWorkdir *string) bool {
	return meta.Context.WorkDir != nil && sentWorkdir == nil
}

// envdServerURL returns the base URL (scheme://host:port) of the sandbox's envd
// HTTP server. A non-empty internalConfig.envdServerURLOverride redirects it
// (test-only; production always uses the slot IP and the default envd port).
func (s *Sandbox) envdServerURL() string {
	if s.internalConfig.envdServerURLOverride != "" {
		return s.internalConfig.envdServerURLOverride
	}

	return fmt.Sprintf("http://%s:%d", s.Slot.HostIPString(), consts.DefaultEnvdServerPort)
}

// doEnvdPost builds and sends an authenticated POST to envd's /<path> endpoint.
// The caller owns the returned response and must close its body. Status handling
// is left to the caller because the endpoints disagree on success: /collapse
// returns 200 with a body, while the cgroup ops return 204 No Content. The
// deadline must live on ctx (callers set it via context.WithTimeout) so it
// stays in force while the caller reads the body.
func (s *Sandbox) doEnvdPost(ctx context.Context, path string, query ...string) (*http.Response, error) {
	address := s.envdServerURL() + "/" + path
	if len(query) > 0 && query[0] != "" {
		address += "?" + query[0]
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address, nil)
	if err != nil {
		return nil, fmt.Errorf("build %s request: %w", path, err)
	}
	if s.Config.Envd.AccessToken != nil {
		req.Header.Set("X-Access-Token", *s.Config.Envd.AccessToken)
	}

	resp, err := sandboxHttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s request: %w", path, err)
	}

	return resp, nil
}

func (s *Sandbox) convertMounts(mounts []VolumeMountConfig) []envd.VolumeMount {
	results := make([]envd.VolumeMount, 0, len(mounts))

	for _, mount := range mounts {
		results = append(results, envd.VolumeMount{
			NfsTarget: fmt.Sprintf("%s:/%s", s.config.NetworkConfig.OrchestratorInSandboxIPAddress, mount.Name),
			Path:      mount.Path,
		})
	}

	return results
}

func (s *Sandbox) initEnvd(ctx context.Context, startType StartType, recordMetrics bool) (e error) {
	ctx, span := tracer.Start(ctx, "envd-init", trace.WithAttributes(telemetry.WithEnvdVersion(s.Config.Envd.Version)))
	defer func() {
		if e != nil {
			span.SetStatus(codes.Error, e.Error())
		}

		span.End()
	}()

	attributes := []attribute.KeyValue{
		telemetry.WithEnvdVersion(s.Config.Envd.Version),
		attribute.Int64("timeout_ms", s.internalConfig.EnvdInitRequestTimeout.Milliseconds()),
		attribute.String("start_type", string(startType)),
	}

	// success is kept for backward compatibility until consumers move to exit_type.
	callAttributes := func(exit envdInitExitType) []attribute.KeyValue {
		attrs := append(attributes,
			attribute.Bool("success", exit == envdInitExitSuccess),
			attribute.String("exit_type", string(exit)),
		)

		// The cohort is read here rather than appended to attributes at the point the
		// header decodes, so that every call site yields the same attribute set instead
		// of one that depends on where in this function it sits. The error path below
		// returns before any header is read, and only the first WaitForEnvd of a start
		// records these counters, so on that path there is no report to find.
		return append(attrs, s.envdProtectionAttrs()...)
	}

	address := s.envdServerURL() + "/init"

	response, count, err := s.doRequestWithInfiniteRetries(ctx, http.MethodPost, address)
	if err != nil {
		s.log().Error(ctx, "failed to init envd after retries",
			logger.WithEnvdVersion(s.Config.Envd.Version),
			zap.Int64("timeout_ms", s.internalConfig.EnvdInitRequestTimeout.Milliseconds()),
			zap.Int64("attempts", count),
			zap.Error(err),
		)

		exit := classifyEnvdInitExit(err)
		// Count only on the first WaitForEnvd (the real start); a later re-check
		// on the same handler (post-upgrade readiness, template-build swap) must
		// not double-count the resume KPI.
		if recordMetrics {
			envdInitCalls.Add(ctx, count, metric.WithAttributes(callAttributes(exit)...))
		}

		return fmt.Errorf("failed to init envd: %w", err)
	}

	defer response.Body.Close()
	// The response headers are read before the init counters are recorded, so that
	// anything the guest reports on /init can label the counters for this start.
	//
	// Capture the version the running envd reports (X-Envd-Version). This rides
	// on the /init call the resume path already makes — before and after an
	// upgrade — so the upgrade trigger can decide/label/confirm against the live
	// version with no extra round-trip.
	if v := response.Header.Get("X-Envd-Version"); v != "" {
		s.setLiveEnvdVersion(v)
	}
	// The resume-time audit of the frozen cgroup set (X-Envd-Freeze-Audit). envd exports
	// no metrics of its own, so this header is what turns the audit into a fleet-wide
	// number instead of a guest log line nothing joins against.
	if a := response.Header.Get("X-Envd-Freeze-Audit"); a != "" {
		s.recordFreezeAudit(ctx, a)
	}
	// Alongside the version, capture the handover outcome the new envd advertises
	// after a live upgrade (X-Envd-Handover) so the trigger can record it.
	if h := response.Header.Get("X-Envd-Handover"); h != "" {
		var hr EnvdHandoverResult
		if err := json.Unmarshal([]byte(h), &hr); err == nil {
			s.setHandoverResult(&hr)
		}
	}
	// What envd reports it is EFFECTIVELY serving with (X-Envd-Defaults), compared
	// against what we just sent. Absent on an envd that predates the header, in which
	// case there is nothing to compare and nothing is counted.
	if d := response.Header.Get("X-Envd-Defaults"); d != "" {
		s.compareEnvdDefaults(ctx, d)
	}
	// The memory protection configured on envd's cgroup chain (X-Envd-Memory). Read
	// before the counters below so the cohort it derives labels them for this start.
	if m := response.Header.Get(envdMemoryHeader); m != "" {
		s.recordEnvdMemoryProtection(ctx, m, startType, recordMetrics)
	}

	if recordMetrics && count > 1 {
		// Retried attempts were transient per-request failures that preceded the success.
		envdInitCalls.Add(ctx, count-1, metric.WithAttributes(callAttributes(envdInitExitTransient)...))
	}

	// Track successful envd init (first WaitForEnvd only — see recordMetrics).
	if recordMetrics {
		envdInitCalls.Add(ctx, 1, metric.WithAttributes(callAttributes(envdInitExitSuccess)...))
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read envd init response body: %w", err)
	}

	if response.StatusCode != http.StatusNoContent {
		s.log().Error(ctx, "envd init request failed",
			logger.WithEnvdVersion(s.Config.Envd.Version),
			zap.Int("status_code", response.StatusCode),
			zap.String("response_body", string(body)),
		)

		return fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	s.log().Debug(ctx, "succeeded to init envd",
		logger.WithEnvdVersion(s.Config.Envd.Version),
		zap.Int64("timeout_ms", s.internalConfig.EnvdInitRequestTimeout.Milliseconds()),
		zap.Int64("attempts", count),
	)

	span.SetStatus(codes.Ok, fmt.Sprintf("envd init returned %d", response.StatusCode))

	return nil
}

// EnvdEffectiveDefaults is what the running envd reports it is ACTUALLY serving
// with, from the X-Envd-Defaults header on /init. Its JSON tags match envd's
// api.effectiveDefaults so the header unmarshals directly.
//
// Absent from an envd that predates the header, which is the common case during a
// rollout and is not an error. The caller skips the comparison on an empty header
// rather than decoding one, so this type never holds a "no report" state: a zero value
// reaching the comparison with a user sent would report a mismatch, and it must not be
// reachable that way. Counting "envd did not tell us" as "envd disagrees" would make
// the mismatch counter unusable exactly while it is most needed.
type EnvdEffectiveDefaults struct {
	User    string  `json:"user"`
	Workdir *string `json:"workdir,omitempty"`
	// Fallback is true when envd is still on its compiled-in default user, i.e.
	// nothing ever told it who to run as.
	Fallback bool `json:"fallback"`
}

// Bounds on the guest-written X-Envd-Defaults header. envd runs inside a guest the customer
// controls, so every byte here is untrusted input that reaches a retained struct, a span
// attribute and log lines on every /init.
//
// The numbers come from what the two fields can legitimately hold, not from the transport:
// a username (useradd's own limit is 32; glibc LOGIN_NAME_MAX, the loosest bound at which a
// name still resolves, is 256) and an absolute path (PATH_MAX, 4096). 256 + 4096 + ~40 bytes
// of JSON envelope rounds up to 8 KiB, which is also the conventional per-header limit. Go's
// transport default is 10 MiB and covers the whole header block, so it bounds neither.
const (
	// envdDefaultsHeaderMaxBytes is the admissible bound, in BYTES, refused before decoding.
	// This is the one that limits retention: nothing that decodes can exceed it.
	envdDefaultsHeaderMaxBytes = 8 << 10
	// envdDefaultsUserMaxLen and envdDefaultsWorkdirMaxLen are display bounds, in RUNES,
	// applied where a value escapes into a log line or a span attribute. They are not
	// applied before the comparison: shortening one side of an equality would report a
	// header that agreed as a mismatch. Multi-byte input can sit under these and still be
	// refused by the byte cap above, which is the safe direction.
	envdDefaultsUserMaxLen    = 256
	envdDefaultsWorkdirMaxLen = 4096
)

// The source values envdDefaultsApplied reports. Named so the emitted vocabulary is
// greppable from one place and a test can pin it: the metric's own description
// deliberately does not enumerate them, because a list in a description is a claim
// nothing checks.
const (
	// defaultsSourceMetadata: the metadata established what the build sent, and we sent it.
	defaultsSourceMetadata = "metadata"
	// defaultsSourceIndeterminate: the metadata could not establish it (see
	// metadata.Template.EnvdDefaultUser), so nothing was sent and the restored process
	// keeps serving what it holds. A population a rollout needs to size, not an error.
	defaultsSourceIndeterminate = "indeterminate"
	// defaultsSourceInherited: the config handed to this resume already carried a default
	// user, so the derivation did not run and the inherited value was sent. Reachable
	// because ResumeSandbox mutates the config it is given and the checkpoint path hands
	// the same one back for the sandbox's next life — so the value came from the metadata
	// originally, but not on this start. Counted, because the resume still sends a user
	// and the withheld term is still recorded for the live-upgrade gate; kept separate,
	// because it is not evidence that this start's derivation worked.
	defaultsSourceInherited = "inherited"
)

// resolveEnvdDefaultUser decides which default user a resume sends and, in the same
// return, the source it is counted under. One function for both, so what was sent and
// what was reported cannot disagree: two derivations of the same decision are how the
// counter came to describe a different population than the code took.
//
// A configured value wins without consulting the metadata. It is the metadata's own
// value one life earlier — this function's caller writes it back into the config, and
// the checkpoint path hands that config to the next resume — but this start did not
// derive it, so it is not evidence the derivation works.
func resolveEnvdDefaultUser(meta metadata.Template, configured *string) (*string, string) {
	if configured != nil {
		return configured, defaultsSourceInherited
	}

	if user, ok := meta.EnvdDefaultUser(); ok {
		return &user, defaultsSourceMetadata
	}

	return nil, defaultsSourceIndeterminate
}

// recordEnvdDefaults counts where the default user this resume sent came from, and whether a
// recorded workdir had to be withheld.
//
// Split by sandbox type because this path is not only customer starts: a template build
// resumes its own sandbox once per uncached layer and again for each prefetch iteration, and
// those never live-upgrade. Without the split the denominator answers no question.
//
// The withheld count sizes what this fix does NOT repair, as an UPPER BOUND rather than the
// population. Two things make it loose, and the second dominates: a workdir equal to the
// resolved user's home is withheld to no effect, and — for any build below
// templates.TemplateV2ReleaseVersion — finalize sent no workdir at all, so the recorded one
// was never in effect and withholding it loses nothing. Those are precisely the builds the
// metadata cannot identify, so the counter cannot separate them either.
//
// Both labels and the withheld term are passed in rather than derived here, and the metadata
// is deliberately NOT a parameter: the live-upgrade gate reads the same withheld term, and
// this counter is the denominator its decline is read against. Deriving it twice is how the
// two came to disagree — this counted a recorded workdir, the gate counted a recorded workdir
// that was also not re-sent. With no metadata in scope the divergence cannot come back.
func recordEnvdDefaults(ctx context.Context, source string, sbxType sandboxtypes.SandboxType, workdirWithheld bool) {
	attrs := metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("sandbox_type", string(sbxType)),
	)
	envdDefaultsApplied.Add(ctx, 1, attrs)

	if workdirWithheld {
		envdDefaultsWorkdirWithheld.Add(ctx, 1, attrs)
	}
}

// envdDefaultsField names a field the comparison can disagree on. Values become the
// `field` label on envdDefaultsMismatch.
const (
	envdDefaultsFieldUser    = "user"
	envdDefaultsFieldWorkdir = "workdir"
)

// envdDefaultsMismatches reports which fields envd's EFFECTIVE defaults disagree with,
// given what the orchestrator sent. Pure, so the skip rules are testable without a
// metric reader — they are the subtle half:
//
//   - A value we never sent cannot be violated, so an empty `sent` is skipped rather
//     than counted. Otherwise every legacy start would report a mismatch against a
//     value nobody asked for.
//   - "unset" is normalised to empty on BOTH sides. envd omits the workdir when it
//     holds none and the orchestrator sends the empty string for the same state, so
//     comparing the representations rather than the states would fire on every
//     workdir-less template.
func envdDefaultsMismatches(eff EnvdEffectiveDefaults, sentUser, sentWorkdir string) []string {
	var fields []string
	if sentUser != "" && eff.User != sentUser {
		fields = append(fields, envdDefaultsFieldUser)
	}
	if sentWorkdir != "" && utils.DerefOrDefault(eff.Workdir, "") != sentWorkdir {
		fields = append(fields, envdDefaultsFieldWorkdir)
	}

	return fields
}

// decodeEnvdEffectiveDefaults parses the guest-written header and bounds it, so the size
// rules are exercised directly by a test rather than only through a sandbox.
//
// Over the byte cap is refused rather than parsed: a value that long is not a report we can
// trust, so it takes the same path as one that fails to decode. That single byte bound is what
// actually limits retention — everything the header can produce is under it.
//
// It deliberately does NOT shorten the values. compareEnvdDefaults tests them against what the
// host sent, and shortening one side of an equality turns a header that agreed into a reported
// mismatch. Display bounds belong at the points where a value escapes into a log line or a
// span, and that is where envdDefaultsField*MaxLen are applied.
func decodeEnvdEffectiveDefaults(header string) (EnvdEffectiveDefaults, error) {
	return decodeEnvdHeader[EnvdEffectiveDefaults](header, envdDefaultsHeaderMaxBytes)
}

// decodeEnvdHeader decodes one guest-written JSON header into T, refusing anything over
// maxBytes before parsing it. A header envd sets is input from a guest the customer
// controls, so the byte bound is what limits what one can push through the decoder, and
// the shape of T does the rest: a field the guest omits is its zero value, a field this
// orchestrator does not know is ignored. X-Envd-Defaults and X-Envd-Memory decode through
// it; the older headers keep their own paths.
func decodeEnvdHeader[T any](header string, maxBytes int) (T, error) {
	var zero T
	if len(header) > maxBytes {
		return zero, fmt.Errorf("header is %d bytes, over the %d-byte cap", len(header), maxBytes)
	}

	var v T
	if err := json.Unmarshal([]byte(header), &v); err != nil {
		return zero, err
	}

	return v, nil
}

// effectiveUserForDisplay and effectiveWorkdirForDisplay bound a guest-written value on its way
// into a log line or a span attribute. Separate from the decode cap because they serve a
// different purpose: the byte cap decides what is admissible, these decide what is printable.
func (eff EnvdEffectiveDefaults) effectiveUserForDisplay() string {
	return utils.Truncate(eff.User, envdDefaultsUserMaxLen)
}

func (eff EnvdEffectiveDefaults) effectiveWorkdirForDisplay() string {
	return utils.Truncate(utils.DerefOrDefault(eff.Workdir, ""), envdDefaultsWorkdirMaxLen)
}

// compareEnvdDefaults checks what envd reports as effective against what we sent, and
// counts any divergence per field.
//
// This is the signal whose absence made the identity loss invisible: the orchestrator
// could see what it SENT but never what the guest ended up with, and every RPC returned
// 200 either way. A non-zero count here means a sandbox is running requests as somebody
// other than its template's user.
//
// The header is written by envd, which runs inside a guest the customer controls, so it
// is untrusted input. It reaches log fields, a span attribute, the retained struct this
// function stores, and the `field` label — and `field` is ours, not the guest's, so a
// hostile value cannot inflate label cardinality. The two string fields are bounded in
// bytes before they decode and in runes wherever they escape (see the display helpers);
// the retained copy is deliberately whole, so the comparison reads what envd reported.
func (s *Sandbox) compareEnvdDefaults(ctx context.Context, header string) {
	eff, err := decodeEnvdEffectiveDefaults(header)
	if err != nil {
		logger.L().Warn(ctx, "could not decode the envd effective-defaults header",
			logger.WithSandboxID(s.Runtime.SandboxID),
			zap.Error(err),
		)

		return
	}

	// Retain it before comparing: the live-upgrade gate reads this to tell an envd that
	// can carry the exec context itself from one that cannot.
	s.envdReportedDefaults.Store(&eff)

	sentUser := utils.DerefOrDefault(s.Config.Envd.DefaultUser, "")
	sentWorkdir := utils.DerefOrDefault(s.Config.Envd.DefaultWorkdir, "")

	for _, field := range envdDefaultsMismatches(eff, sentUser, sentWorkdir) {
		envdDefaultsMismatch.Add(ctx, 1, metric.WithAttributes(attribute.String("field", field)))
		logger.L().Error(ctx, "envd is serving different defaults than the ones sent",
			logger.WithSandboxID(s.Runtime.SandboxID),
			logger.WithEnvdVersion(s.Config.Envd.Version),
			zap.String("field", field),
			zap.String("sent_user", sentUser),
			zap.String("effective_user", eff.effectiveUserForDisplay()),
			zap.String("sent_workdir", sentWorkdir),
			zap.String("effective_workdir", eff.effectiveWorkdirForDisplay()),
			zap.Bool("envd_on_builtin_fallback", eff.Fallback),
		)
	}

	// The realized loss, and the only signal that can see one. A mismatch on the user is
	// close to unfireable by construction: SetData stores exactly what /init carried and
	// reportEffectiveDefaults reads that same field back, so the two agree whenever
	// anything was sent. What a sandbox actually losing its identity looks like is envd
	// reporting it was never told at all.
	//
	// Labelled by whether we sent a user, because the two cases mean opposite things:
	// not sent is the indeterminate population realizing its loss, which is expected and
	// worth sizing; sent is a defect, since a delivered user must never read back as
	// never-told.
	//
	// Skipped for a resume whose start does not describe a customer start, the same rule
	// recordEnvdDefaults follows: this runs on every /init, and the prefetch harvest resumes
	// every memory pause, so counting throwaways here while applied excludes them would put
	// roughly twice the samples in this counter for one underlying population. The mismatch
	// loop above is deliberately NOT skipped — it reports a defect rather than sizing a
	// population, and a defect on a throwaway is still a defect.
	if eff.Fallback && !s.skipStartupMetrics {
		// Split by sandbox type for the same reason recordEnvdDefaults is, and more
		// strongly: this runs on every /init, creates included, and the build tree starts
		// sandboxes with no default user in their config at all — so without the split
		// sent="false" tracks build volume rather than the population at risk. Build
		// sandboxes also run the host envd, so they emit this header from the first day of
		// the rollout, before any customer template carries one.
		envdDefaultsBuiltinFallback.Add(ctx, 1, metric.WithAttributes(
			attribute.Bool("sent", sentUser != ""),
			attribute.String("sandbox_type", string(s.Runtime.SandboxType))))
		if sentUser != "" {
			logger.L().Error(ctx, "envd reports it was never told a default user, but one was sent",
				logger.WithSandboxID(s.Runtime.SandboxID),
				logger.WithEnvdVersion(s.Config.Envd.Version),
				zap.String("sent", sentUser),
				zap.String("effective", eff.effectiveUserForDisplay()),
			)
		}
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(
			attribute.String("envd.defaults.effective_user", eff.effectiveUserForDisplay()),
			attribute.Bool("envd.defaults.builtin_fallback", eff.Fallback),
		)
	}
}
