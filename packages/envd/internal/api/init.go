package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/awnumar/memguard"
	"github.com/rs/zerolog"
	"github.com/txn2/txeh"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/envd/internal/host"
	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/logs/ratelimit"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/pkg"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

// /init is hammered by the orchestrator's infinite retry loop, so a
// persistent pin failure would otherwise flood the log.
var pinMMDSWarnLimit = ratelimit.New(10 * time.Second)

var (
	ErrAccessTokenMismatch           = errors.New("access token validation failed")
	ErrAccessTokenResetNotAuthorized = errors.New("access token reset not authorized")
)

const (
	maxTimeInPast   = 50 * time.Millisecond
	maxTimeInFuture = 5 * time.Second
	logFlushTimeout = 2 * time.Second
)

// validateInitAccessToken validates the access token for /init requests.
// Token is valid if it matches the existing token OR the MMDS hash.
// If neither exists, first-time setup is allowed.
func (a *API) validateInitAccessToken(ctx context.Context, requestToken *SecureToken) error {
	requestTokenSet := requestToken.IsSet()

	// Fast path: token matches existing
	if a.accessToken.IsSet() && requestTokenSet && a.accessToken.EqualsSecure(requestToken) {
		return nil
	}

	// Check MMDS only if token didn't match existing
	matchesMMDS, mmdsExists := a.checkMMDSHash(ctx, requestToken)

	switch {
	case matchesMMDS:
		return nil
	case !a.accessToken.IsSet() && !mmdsExists:
		return nil // first-time setup
	case !requestTokenSet:
		return ErrAccessTokenResetNotAuthorized
	default:
		return ErrAccessTokenMismatch
	}
}

// checkMMDSHash checks if the request token matches the MMDS hash.
// Returns (matches, mmdsExists).
//
// The MMDS hash is set by the orchestrator during Resume:
//   - hash(token): requires this specific token
//   - hash(""): explicitly allows nil token (token reset authorized)
//   - "": MMDS not properly configured, no authorization granted
func (a *API) checkMMDSHash(ctx context.Context, requestToken *SecureToken) (bool, bool) {
	if a.isNotFC {
		return false, false
	}

	mmdsHash, err := a.mmdsClient.GetAccessTokenHash(ctx)
	if err != nil {
		// Self-heal: a user-installed PREROUTING/OUTPUT redirect on
		// 169.254.169.254:80 in the same netns can shadow our route.
		// Re-pin our RETURN rule at position 1 of nat PREROUTING and
		// OUTPUT, then retry once.
		if pinErr := host.PinMMDSRoute(ctx); pinErr != nil {
			if ok, suppressed := pinMMDSWarnLimit.Allow(); ok {
				a.logger.Warn().Err(pinErr).Int64("suppressed", suppressed).Msg("failed to pin MMDS iptables route")
			}
		}
		mmdsHash, err = a.mmdsClient.GetAccessTokenHash(ctx)
	}
	if err != nil {
		return false, false
	}

	if mmdsHash == "" {
		return false, false
	}

	if !requestToken.IsSet() {
		return mmdsHash == keys.HashAccessToken(""), true
	}

	tokenBytes, err := requestToken.Bytes()
	if err != nil {
		return false, true
	}
	defer memguard.WipeBytes(tokenBytes)

	return keys.HashAccessTokenBytes(tokenBytes) == mmdsHash, true
}

// freezeAuditHeader carries the resume-time audit of the frozen cgroup set. A header
// rather than a response body because /init answers 204 and the orchestrator already reads
// headers off this call.
//
// The value is JSON, like X-Envd-Handover on the same response, rather than a bespoke k=v
// string. Both ends then get field-by-field decoding for free: a reader on an older
// orchestrator ignores a field this envd adds instead of failing the whole parse, and a
// reader on a newer one sees a field this envd omits as its zero value. A single header
// format across the resume path is also one fewer thing to get right.
const freezeAuditHeader = "X-Envd-Freeze-Audit"

// freezeAudit is the wire form of the audit. Counts are pointers-free and omitempty-free on
// purpose: a zero violation count is a meaningful report, not an absent one.
type freezeAudit struct {
	Visited    int  `json:"visited"`
	Frozen     int  `json:"frozen"`
	Escaped    int  `json:"escaped"`
	Violations int  `json:"violations"`
	Truncated  bool `json:"truncated"`
}

// defaultsHeader reports the defaults this envd is EFFECTIVELY serving with, after
// SetData has applied whatever /init carried. Same JSON-header convention as
// X-Envd-Handover and X-Envd-Freeze-Audit, on a call the orchestrator already makes.
//
// It exists because the orchestrator can otherwise only see what it SENT, never what
// the guest ended up with — and the two diverging silently is exactly the failure this
// reports. A re-exec'd envd rebuilds defaults from scratch and depends on /init to
// repopulate them; if that ever fails to happen, every request that omits a user
// resolves to the built-in fallback, with a 200 on every RPC and nothing in any log.
const defaultsHeader = "X-Envd-Defaults"

// effectiveDefaults is the wire form. Workdir is a pointer so "unset" and "set to
// empty" stay distinguishable — the orchestrator compares against what it sent, and
// collapsing those two would make a real mismatch look like agreement.
type effectiveDefaults struct {
	User    string  `json:"user"`
	Workdir *string `json:"workdir,omitempty"`
	// Fallback is true when nothing ever told this envd who to run as, so User is
	// whatever the binary was compiled with. Deliberately provenance and not
	// User == BuiltinDefaultUser: a template whose default user genuinely is that value
	// had its identity DELIVERED, and reporting it as a fallback would both cry wolf on
	// every resume and hide a real loss behind a value that happens to match.
	Fallback bool `json:"fallback"`
}

// memoryHeader reports the memory protection configured on envd's cgroup chain, on
// every /init response. Same JSON-header convention as its siblings above.
//
// It exists because the kernel exposes no file with a cgroup's EFFECTIVE protection:
// a memory.min that reads back as set on envd.service proves nothing about what envd is
// granted, since protection is handed down the chain and an ancestor without a value
// leaves the leaf with none. The orchestrator turns this into a per-start cohort, which
// is how a resume that ran with envd protected is told apart, fleet-wide, from one that did
// not.
//
// The wire form is cgroups.MemoryProtection itself, whose fields carry the JSON tags:
// unlike the freeze audit there is no log-only field to hide here, so a copy would only
// be a second declaration to keep in step.
const memoryHeader = "X-Envd-Memory"

func (a *API) PostInit(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Report the running envd version on every /init response (even error/stale
	// ones) so the orchestrator can read the live version off the resume-path
	// call it already makes — no extra round-trip. Set before any WriteHeader.
	w.Header().Set("X-Envd-Version", pkg.Version)

	// The memory protection on envd's cgroup chain, read once at construction and only
	// serialised here, so the resume path does no extra reading. On every response,
	// error ones included, for the same reason as the version above.
	a.reportMemoryProtection(w)

	// If this envd booted from a live-upgrade handover, advertise its outcome on
	// /init so the orchestrator can record it — the envd-side result (re-adopted
	// procs, restored retained exits, watcher re-arm success/failures) is
	// otherwise only logged in-guest and invisible fleet-wide. Set before any
	// WriteHeader.
	if a.handover != nil {
		if b, err := json.Marshal(a.handover); err == nil {
			w.Header().Set("X-Envd-Handover", string(b))
		}
	}

	ctx := r.Context()

	operationID := logs.AssignOperationID()
	logger := a.logger.With().Str(string(logs.OperationIDKey), operationID).Logger()

	if r.Body != nil {
		// Read raw body so we can wipe it after parsing
		body, err := io.ReadAll(r.Body)
		// Ensure body is wiped after we're done
		defer memguard.WipeBytes(body)
		if err != nil {
			logger.Error().Msgf("Failed to read request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		var initRequest PostInitJSONBody
		if len(body) > 0 {
			err = json.Unmarshal(body, &initRequest)
			if err != nil {
				logger.Error().Msgf("Failed to decode request: %v", err)
				w.WriteHeader(http.StatusBadRequest)

				return
			}
		}

		// Ensure request token is destroyed if not transferred via TakeFrom.
		// This handles: validation failures, timestamp-based skips, and any early returns.
		// Safe because Destroy() is nil-safe and TakeFrom clears the source.
		defer initRequest.AccessToken.Destroy()

		// After the body is read, and before auth or SetData. Acquiring earlier would let
		// an unauthenticated caller hold this semaphore for as long as it withholds the
		// rest of its body: /init is auth-excluded (MMDS-validated instead) and the server
		// sets no ReadTimeout, and the orchestrator's own /init would then block here.
		// The Release is deferred to function exit, not block exit, so the lock is still
		// held across reportEffectiveDefaults below -- which is what serialises that read
		// against SetData.
		if err := a.initLock.Acquire(ctx, 1); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}
		defer a.initLock.Release(1)

		// Validate auth before installing the unfreeze defer or running SetData,
		// so stale/replayed but unauthorized requests can't thaw cgroups.
		if err := a.validateInitAccessToken(ctx, initRequest.AccessToken); err != nil {
			writeInitError(w, logger, err)

			return
		}

		// Audit the frozen set BEFORE anything thaws it, and advertise the counts on the
		// response header so the orchestrator can record them fleet-wide. envd exports no
		// metrics of its own, and the guest's logs land somewhere the freeze dashboards
		// cannot join against, so the header is the only channel that turns this into a
		// number rather than a story. It rides a call the orchestrator already makes, the
		// same way X-Envd-Version does.
		//
		// Two things it can see that nothing else can: a cgroup that was never frozen
		// although it was the sweep's business (created in the window between the sweep and
		// the snapshot, which no freeze-on-create semantics exist to prevent), and a cgroup
		// frozen that must not have been. The second is the one that catches bugs.
		a.auditFrozenSet(w, logger)

		// Run on every /init regardless of the Timestamp guard, so stale/replayed
		// requests still thaw cgroups after pre-pause freeze.
		defer a.unfreezeUserCgroups(ctx, logger)

		// Restore the access token (and env) BEFORE marking the envd initialized.
		// On a live-upgraded envd, WithAuthorization only fails CLOSED while
		// !initialized; flipping initialized first, with the token not yet
		// restored, falls through to the fail-OPEN path and lets a re-adopted
		// (and possibly hostile) guest process reach control endpoints — including
		// /upgrade — unauthenticated in that window. Update only if the request is
		// newer (or carries no timestamp); a stale/replayed /init keeps the token
		// already set by the first one.
		if initRequest.Timestamp == nil || a.lastSetTime.SetToGreater(initRequest.Timestamp.UnixNano()) {
			if err := a.SetData(ctx, logger, initRequest); err != nil {
				writeInitError(w, logger, err)

				return
			}
		}

		// Auth passed and token restored: mark the envd initialized so the
		// live-upgrade /upgrade endpoint and the post-upgrade fallback thaw open
		// up — a guest process that can't pass auth can't flip this and drive an
		// unauthenticated upgrade.
		a.initialized.Store(true)
	}

	go func() { //nolint:contextcheck // TODO: fix this later
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		host.PollForMMDSOpts(ctx, a.mmdsChan, a.defaults.EnvVars)
	}()

	// After SetData, so this reports what is actually in effect rather than what was
	// requested. Set before WriteHeader.
	a.reportEffectiveDefaults(w, logger)

	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(http.StatusNoContent)
}

// reportEffectiveDefaults advertises the in-effect defaults on the /init response, and
// warns when a live-upgraded envd ends up on the built-in fallback.
//
// The warn is deliberately scoped to a handover boot. On a cold boot the fallback is
// legitimate and momentary — /init has not necessarily arrived yet — so warning there
// would be noise. On a handover boot it means the swap dropped an identity the outgoing
// process was holding, which is never expected and never self-heals.
func (a *API) reportEffectiveDefaults(w http.ResponseWriter, logger zerolog.Logger) {
	eff := effectiveDefaults{
		User:     a.defaults.User,
		Workdir:  a.defaults.Workdir,
		Fallback: !a.defaults.UserDelivered,
	}

	if eff.Fallback && a.handover != nil {
		logger.Warn().
			Str("user", eff.User).
			Msg("live-upgraded envd was never told which user to run as: neither /init nor the " +
				"handover blob carried one, so every request that omits a user runs as the " +
				"built-in default")
	}

	b, err := json.Marshal(eff)
	if err != nil {
		logger.Error().Err(err).Msg("failed to marshal effective defaults")

		return
	}
	w.Header().Set(defaultsHeader, string(b))
}

// reportMemoryProtection advertises the cached chain protection on the /init response.
// Marshalling a struct of integers and a bool cannot fail, but the report is advisory and
// must never fail the /init it rides on, so an error still degrades to no header.
func (a *API) reportMemoryProtection(w http.ResponseWriter) {
	b, err := json.Marshal(a.memory)
	if err != nil {
		a.logger.Warn().Err(err).Msg("could not encode the memory protection report")

		return
	}
	w.Header().Set(memoryHeader, string(b))
}

func (a *API) SetData(ctx context.Context, logger zerolog.Logger, data PostInitJSONBody) error {
	if data.Timestamp != nil {
		// Check if current time differs significantly from the received timestamp
		if shouldSetSystemTime(time.Now(), *data.Timestamp) {
			logger.Debug().Msgf("Setting sandbox start time to: %v", *data.Timestamp)
			if err := setSystemTime(*data.Timestamp); err != nil {
				logger.Error().Msgf("Failed to set system time: %v", err)
			}
		} else {
			logger.Debug().Msgf("Current time is within acceptable range of timestamp %v, not setting system time", *data.Timestamp)
		}
	}

	if data.EnvVars != nil {
		keys := slices.Collect(maps.Keys(*data.EnvVars))
		logger.Debug().Msgf("Setting %d env vars: %s", len(keys), strings.Join(keys, ", "))
		a.defaults.EnvVars.ReplaceUserVars(*data.EnvVars)
	}

	if data.AccessToken.IsSet() {
		logger.Debug().Msg("Setting access token")
		a.accessToken.TakeFrom(data.AccessToken)
	} else if a.accessToken.IsSet() {
		logger.Debug().Msg("Clearing access token")
		a.accessToken.Destroy()
	}

	if data.HyperloopIP != nil {
		go a.SetupHyperloop(*data.HyperloopIP)
	}

	if data.DefaultUser != nil && *data.DefaultUser != "" {
		logger.Debug().Msgf("Setting default user to: %s", *data.DefaultUser)
		a.defaults.User = *data.DefaultUser
		a.defaults.UserDelivered = true
	}

	if data.DefaultWorkdir != nil && *data.DefaultWorkdir != "" {
		logger.Debug().Msgf("Setting default workdir to: %s", *data.DefaultWorkdir)
		a.defaults.Workdir = data.DefaultWorkdir
	}

	if data.CaBundle != nil && *data.CaBundle != "" {
		if err := a.caCertInstaller.Install(ctx, *data.CaBundle); err != nil {
			return fmt.Errorf("failed to install CA bundle: %w", err)
		}
	}

	if data.VolumeMounts != nil {
		if err := a.setupNFS(ctx, logger, data.LifecycleID, *data.VolumeMounts); err != nil {
			return fmt.Errorf("failed to setup NFS volumes: %w", err)
		}
	}

	return nil
}

// userCgroupsToFreeze is the cgroup set frozen pre-pause and thawed on /init.
// PostFreeze freezes user/pty cgroups directly (no Process.Start / shell).
// Orchestrator calls this just before pause; the frozen state persists into the
// snapshot and /init thaws on resume. Best-effort: tries every cgroup even if
// one fails so we freeze as many as possible before the snapshot.
func (a *API) PostFreeze(w http.ResponseWriter, r *http.Request, params PostFreezeParams) {
	handlerStart := time.Now()
	defer r.Body.Close()

	logger := a.logger.With().Str(string(logs.OperationIDKey), logs.AssignOperationID()).Logger()

	// maxWaitMs is what tells the two callers apart. One that sends it wants the
	// structured result; one that does not is an orchestrator predating it, which treats
	// any status but 204 as a failed freeze. So an absent parameter keeps the original
	// contract exactly: no wait at all (that caller's request timeout is shorter than a
	// useful one anyway) and a bare 204. The budget belongs to the caller either way,
	// because the caller owns the request timeout and a wait outliving it cannot be
	// observed.
	report := params.MaxWaitMs != nil && *params.MaxWaitMs > 0
	var maxWait time.Duration
	if report {
		maxWait = time.Duration(*params.MaxWaitMs) * time.Millisecond
	}

	// Pause stays best-effort: report an incomplete freeze, never fail the pause on it.
	// The mode is the caller's choice because the flag that selects it lives there. An
	// absent or unknown value means legacy, so an orchestrator predating the parameter
	// keeps today's frozen set.
	mode := cgroups.ModeLegacy
	if params.Mode != nil && *params.Mode == PostFreezeParamsModeHierarchy {
		mode = cgroups.ModeHierarchy
	}
	var maxCgroups int
	if params.MaxCgroups != nil {
		maxCgroups = *params.MaxCgroups
	}

	res, err := a.workloadFreezer.Freeze(r.Context(), cgroups.FreezeOptions{
		MaxWait:    maxWait,
		Mode:       mode,
		MaxCgroups: maxCgroups,
	})

	// A failed lock acquire surfaces the ctx error itself, which is what distinguishes it
	// from a sweep that ran and collected per-cgroup errors -- those are joined, and a
	// joined write error never matches a ctx cause. Testing the error rather than "some
	// error happened while ctx also happens to be done" keeps a real result from being
	// discarded as contention.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		w.WriteHeader(http.StatusServiceUnavailable)

		return
	}
	if err != nil {
		// Per-cgroup failures are counted in res.Failed and reported in the body, not
		// raised as a 500: a cgroup that refuses the write is expected, and answering
		// with an error would hide the very count that makes it visible.
		//
		// A WALK-DISCOVERED cgroup that vanished does not reach here -- it is counted
		// Vanished and raises nothing. One of envd's own static cgroups still does, on
		// purpose: in legacy mode those are the whole poll set, and a vanish there means
		// something removed the cgroup from under our tasks rather than a transient unit
		// exiting. See freezeTarget.tolerateVanish.
		logger.Error().Err(err).
			Int("failed", res.Failed).
			Msg("some cgroups failed to freeze; reporting them in the result")
	}
	// The caller's maxWaitMs covers the whole handler, not just the cgroup wait.
	// Spend only its remainder on sending logs. A legacy caller provides no
	// observable budget, so an immediately expired context makes this purge-only.
	flushBudget := time.Duration(0)
	if report {
		flushBudget = min(logFlushTimeout, max(maxWait-time.Since(handlerStart), 0))
	}
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), flushBudget)
	flushErr := a.logFlusher.FlushAndPurge(flushCtx)
	cancel()
	if flushErr != nil {
		logger.Warn().Err(flushErr).Msg("flush and purge logs before pause")
	}
	if !report {
		// Nothing was observed, so there is nothing to say about whether the workload
		// stopped: a zero budget means the state was never read, which leaves every
		// written cgroup counted as NotFrozen. Warning on that would fire for every pause
		// from an orchestrator predating maxWaitMs -- claiming a running workload on the
		// strength of a check we deliberately skipped. Hence the observation warnings live
		// below this return, not above it.
		w.WriteHeader(http.StatusNoContent)

		return
	}

	// An interrupted wait is the same situation as a skipped one: we stopped looking
	// early, so the counts describe what we managed to read rather than what the workload
	// did, and warning on them would be a claim we cannot support. The caller has gone,
	// so the status is for the record only.
	if r.Context().Err() != nil {
		w.WriteHeader(http.StatusServiceUnavailable)

		return
	}

	if !res.AllFrozen() {
		logger.Warn().
			Int("requested", res.Requested).
			Int("frozen", res.Frozen).
			Int("not_frozen", res.NotFrozen).
			Int("failed", res.Failed).
			// Context rather than a cause: a vanished cgroup never makes this warning
			// fire. Logged because it explains a requested count the other outcomes do
			// not reach -- but only for one that vanished during the settle poll. One
			// that vanished at the write was never requested, so it raises the total
			// instead. FreezeResult's accounting note has the two lines that balance.
			Int("vanished", res.Vanished).
			Dur("wait_duration", res.WaitDuration).
			Msg("pre-pause freeze did not stop the whole workload; snapshot may capture it running")
	}
	if res.Vanished > 0 {
		// The only record of WHICH cgroups went away. A vanish raises no error and does not
		// hold back AllFrozen, so a sweep that is otherwise clean logs nothing on either
		// side of the call and the paths would be lost. Debug because it is routine -- the
		// fleet sees it on roughly 0.02% of pauses -- and because envd's logger has no level
		// gate, so Debug ships and is retained exactly like Info.
		logger.Debug().
			Int("vanished", res.Vanished).
			Int("requested", res.Requested).
			Int("frozen", res.Frozen).
			Strs("vanished_paths", res.VanishedPaths).
			Msg("cgroups vanished while the freeze was working on them; counted, not failed")
	}
	if res.ScanFailed > 0 {
		// Not an error, but not silent either: the guest-freeze record is incomplete by this
		// many cgroups, so that many of the guest's own freezes get cleared on resume.
		logger.Warn().
			Int("scan_failed", res.ScanFailed).
			Int("pre_frozen", res.PreFrozen).
			Msg("could not classify some cgroups as the guest's own; those will be thawed on resume")
	}

	if res.Truncated {
		// Coverage is incomplete: cgroups past the bound were never examined, so some of
		// the workload may still be running. A degradation rather than a failure, but
		// never a silent one -- if this fires the bound is wrong, and that is the finding.
		logger.Warn().
			Int("visited", res.Visited).
			Int("requested", res.Requested).
			Msg("freeze walk hit its bound; some cgroups were never examined")
	}
	if res.Unobservable > 0 {
		// Not a failure, but never silent: this guest cannot report freeze state at all,
		// so every pause here snapshots without the guarantee the rest of the fleet has.
		logger.Warn().
			Int("unobservable", res.Unobservable).
			Msg("cgroup freeze state is unobservable on this guest; freeze issued but unverifiable")
	}

	sweepMs := res.SweepDuration.Milliseconds()
	waitMs := res.WaitDuration.Milliseconds()
	resultMode := FreezeResultMode(res.Mode)
	result := FreezeResult{
		Mode:         &resultMode,
		Visited:      &res.Visited,
		Allowlisted:  &res.Allowlisted,
		Truncated:    &res.Truncated,
		PreFrozen:    &res.PreFrozen,
		Requested:    &res.Requested,
		Frozen:       &res.Frozen,
		NotFrozen:    &res.NotFrozen,
		Failed:       &res.Failed,
		Vanished:     &res.Vanished,
		Unobservable: &res.Unobservable,
		SweepMs:      &sweepMs,
		WaitMs:       &waitMs,
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(result); err != nil {
		logger.Error().Err(err).Msg("encode freeze result")
	}
}

// PostUnfreeze thaws user/pty cgroups directly. Exists ONLY for the
// orchestrator's pause-failure rollback path; the resume thaw runs via /init's
// deferred unfreeze and must not be replaced by this endpoint. Best-effort: tries
// every cgroup even if one fails so a partial failure cannot leave the rest frozen.
func (a *API) PostUnfreeze(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	logger := a.logger.With().Str(string(logs.OperationIDKey), logs.AssignOperationID()).Logger()

	if err := a.workloadFreezer.Unfreeze(r.Context()); err != nil {
		logger.Error().Err(err).Msg("unfreeze workload cgroups")
		jsonError(w, http.StatusInternalServerError, err)

		return
	}

	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

// unfreezeUserCgroups unfreezes user/pty cgroups (idempotent if not frozen).
// The freezer detaches the wait from ctx cancellation so the unfreeze always
// completes.
func (a *API) unfreezeUserCgroups(ctx context.Context, logger zerolog.Logger) {
	if err := a.workloadFreezer.Unfreeze(ctx); err != nil {
		logger.Warn().Err(err).Msg("unfreeze workload cgroups")
	}
}

func writeInitError(w http.ResponseWriter, logger zerolog.Logger, err error) {
	switch {
	case errors.Is(err, ErrAccessTokenMismatch), errors.Is(err, ErrAccessTokenResetNotAuthorized):
		w.WriteHeader(http.StatusUnauthorized)
	case errors.Is(err, host.ErrCAInstallInProgress):
		// Not a failure, a concurrent install still holds the CA lock.
		logger.Warn().Err(err).Msg("CA initialization still in progress, retrying")
		w.WriteHeader(http.StatusServiceUnavailable)
	default:
		logger.Error().Msgf("Failed to set data: %v", err)
		w.WriteHeader(http.StatusBadRequest)
	}
	w.Write([]byte(err.Error()))
}

var nfsOptions = strings.Join([]string{
	// wait for data to be sent to proxy server before returning.
	// async might cause issues if the sandbox is shut down suddenly.
	"sync",

	"rsize=1048576",  // 1 MB read buffer
	"wsize=1048576",  // 1 MB write buffer
	"mountproto=tcp", // nfs proxy only supports tcp
	"mountport=2049", // nfs proxy only supports mounting on port 2049
	"proto=tcp",      // nfs proxy only supports tcp
	"port=2049",      // nfs proxy only supports mounting on port 2049
	"nfsvers=3",      // nfs proxy is nfs version 3
	"noacl",          // no reason for acl in the sandbox

	// Resolve locks node-locally. The nfs proxy has no lock manager and its
	// portmap never registers NLM, so without this a hard-mounted lock request
	// (e.g. flock) is sent to the proxy, gets port 0, and retries forever in
	// uninterruptible sleep. See github.com/e2b-dev/infra issue #3619.
	"nolock",

	// disable caching so that pause/resume works correctly
	"noac",
	"lookupcache=none",
}, ",")

const nfsMountTimeout = 10 * time.Second

func (a *API) setupNFS(ctx context.Context, logger zerolog.Logger, lifecycleID *string, mounts []VolumeMount) (e error) {
	// Prevent concurrent mounting attempts
	if !a.isMountingNFS.CompareAndSwap(false, true) {
		logger.Debug().Msg("NFS volumes already mounting")

		return e
	}
	defer a.isMountingNFS.Store(false)

	logger.Debug().Msg("Setting up NFS volumes")

	ctx = context.WithoutCancel(ctx)                         // don't allow request context cancellation to propagate
	ctx, cancel := context.WithTimeout(ctx, nfsMountTimeout) // don't let the nfs mount run forever
	defer cancel()

	wg, wgCtx := errgroup.WithContext(ctx)

	requestLifecycleID := derefString(lifecycleID)

	for _, volume := range mounts {
		// Check if this path is already mounted for the current lifecycle
		mountedLifecycle, isMounted := a.mountedPaths.Load(volume.Path)
		mountedLifecycleID := asString(mountedLifecycle)
		if !shouldRemountNFS(isMounted, mountedLifecycleID, requestLifecycleID) {
			logger.Debug().Msgf("Skipping %q, already mounted for lifecycle %q", volume.Path, requestLifecycleID)

			continue
		}

		if isMounted {
			logger.Debug().Msgf("Lifecycle changed for %q: %q -> %q", volume.Path, mountedLifecycleID, requestLifecycleID)
		}

		logger.Debug().Msgf("Setting up %s at %q", volume.NfsTarget, volume.Path)

		wg.Go(func() error {
			// Unmount if currently mounted (handles stale mounts from previous lifecycle)
			if err := a.unmountNFS(wgCtx, logger, volume.Path); err != nil {
				return fmt.Errorf("failed to unmount stale NFS mount at %q: %w", volume.Path, err)
			}

			if err := a.mountNFS(wgCtx, volume.NfsTarget, volume.Path); err != nil {
				return fmt.Errorf("failed to mount NFS at %q: %w", volume.Path, err)
			}

			a.mountedPaths.Store(volume.Path, requestLifecycleID)

			return nil
		})
	}

	return wg.Wait()
}

func (a *API) unmountNFS(ctx context.Context, logger zerolog.Logger, path string) error {
	// Check if actually mounted before trying to unmount.
	// findmnt returns exit code 1 when path is not a mount point - that's not an error.
	data, err := exec.CommandContext(ctx, "findmnt", "--noheadings", "--output", "SOURCE", path).CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			// Not mounted - nothing to unmount
			return nil
		}

		return fmt.Errorf("failed to check if %q is mounted: %w", path, err)
	}

	source := strings.TrimSpace(string(data))
	if source == "" {
		return nil // already unmounted
	}

	logger.Debug().Msgf("Unmounting stale NFS mount at %q (was: %s)", path, source)

	if data, err = exec.CommandContext(ctx, "umount", "--force", path).CombinedOutput(); err != nil {
		logger.Warn().Err(err).Str("path", path).Str("output", string(data)).Msg("Forced NFS umount failed, falling back to lazy")
		// Fresh ctx so the forced umount running out of budget doesn't kill the fallback.
		lazyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if data, err = exec.CommandContext(lazyCtx, "umount", "--lazy", path).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to unmount stale NFS mount at %q: %w\n%s", path, err, string(data))
		}
	}

	a.mountedPaths.Delete(path)

	return nil
}

func (a *API) mountNFS(ctx context.Context, nfsTarget, path string) error {
	commands := [][]string{
		{"mkdir", "-p", path},
		{"mount", "-v", "-t", "nfs", "-o", "fg,hard," + nfsOptions, nfsTarget, path},
	}

	for _, command := range commands {
		data, err := exec.CommandContext(ctx, command[0], command[1:]...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("`%s` failed: %w\n%s", strings.Join(command, " "), err, string(data))
		}
	}

	return nil
}

// shouldRemountNFS determines if an NFS volume should be remounted based on lifecycle IDs.
// Returns true if remount is needed, false if we should skip (already mounted for this lifecycle).
//
// Truth table (treating nil/empty as equivalent):
//   - mounted="" + request="" → false (no remount - would cause infinite loop)
//   - mounted="abc" + request="" → true (remount - lifecycle cleared)
//   - mounted="" + request="abc" → true (remount - new lifecycle)
//   - mounted="abc" + request="abc" → false (no remount - same lifecycle)
//   - mounted="abc" + request="xyz" → true (remount - different lifecycle)
func shouldRemountNFS(isMounted bool, mountedLifecycleID, requestLifecycleID string) bool {
	if !isMounted {
		return true // not mounted yet, need to mount
	}

	return mountedLifecycleID != requestLifecycleID
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func asString(v any) string {
	if v == nil {
		return ""
	}

	s, _ := v.(string)

	return s
}

func (a *API) SetupHyperloop(address string) {
	a.hyperloopLock.Lock()
	defer a.hyperloopLock.Unlock()

	if err := rewriteHostsFile(address, "/etc/hosts"); err != nil {
		a.logger.Error().Err(err).Msg("failed to modify hosts file")
	} else {
		a.defaults.EnvVars.Store("E2B_EVENTS_ADDRESS", fmt.Sprintf("http://%s", address))
	}
}

const eventsHost = "events.e2b.local"

func rewriteHostsFile(address, path string) error {
	hosts, err := txeh.NewHosts(&txeh.HostsConfig{
		ReadFilePath:  path,
		WriteFilePath: path,
	})
	if err != nil {
		return fmt.Errorf("failed to create hosts: %w", err)
	}

	// Update /etc/hosts to point events.e2b.local to the hyperloop IP
	// This will remove any existing entries for events.e2b.local first
	ipFamily, err := getIPFamily(address)
	if err != nil {
		return fmt.Errorf("failed to get ip family: %w", err)
	}

	if ok, current, _ := hosts.HostAddressLookup(eventsHost, ipFamily); ok && current == address {
		return nil // nothing to be done
	}

	hosts.AddHost(address, eventsHost)

	return hosts.Save()
}

var (
	ErrInvalidAddress       = errors.New("invalid IP address")
	ErrUnknownAddressFormat = errors.New("unknown IP address format")
)

func getIPFamily(address string) (txeh.IPFamily, error) {
	addressIP, err := netip.ParseAddr(address)
	if err != nil {
		return txeh.IPFamilyV4, fmt.Errorf("failed to parse IP address: %w", err)
	}

	switch {
	case addressIP.Is4():
		return txeh.IPFamilyV4, nil
	case addressIP.Is6():
		return txeh.IPFamilyV6, nil
	default:
		return txeh.IPFamilyV4, fmt.Errorf("%w: %s", ErrUnknownAddressFormat, address)
	}
}

// shouldSetSystemTime returns true if the current time differs significantly from the received timestamp,
// indicating the system clock should be adjusted. Returns true when the sandboxTime is more than
// maxTimeInPast before the hostTime or more than maxTimeInFuture after the hostTime.
func shouldSetSystemTime(sandboxTime, hostTime time.Time) bool {
	return sandboxTime.Before(hostTime.Add(-maxTimeInPast)) || sandboxTime.After(hostTime.Add(maxTimeInFuture))
}

// auditFrozenSet classifies the frozen cgroups at resume and reports the counts on the
// /init response header. Advisory throughout: an observer must never be able to fail the
// resume it is observing, so every error path here degrades to "say nothing".
func (a *API) auditFrozenSet(w http.ResponseWriter, logger zerolog.Logger) {
	pm, ok := a.workloadFreezer.Manager().(cgroups.PathManager)
	if !ok {
		// No hierarchy to walk (no-op manager, or a non-Linux build). Nothing to audit.
		return
	}

	// AuditFrozenSet rather than AuditFrozenState: the freezer owns both the mode and the
	// guard against a thaw landing mid-walk, so the decision belongs next to that state
	// rather than here.
	res, err := a.workloadFreezer.AuditFrozenSet(pm, cgroups.ProcSelfCgroup, cgroups.DefaultThawMaxCgroups)
	if err != nil {
		logger.Warn().Err(err).Msg("could not audit the frozen cgroup set at resume")

		return
	}
	if !res.Applicable {
		// Nothing was frozen, so this guest was not resumed from a paused snapshot that
		// froze anything -- a fresh create, most likely. Reporting escapes here would
		// count every legitimately running cgroup as one.
		return
	}

	// Marshal failure cannot happen for a struct of ints and a bool, but the audit is advisory
	// and must never fail the resume it observes, so an error still degrades to no header.
	if b, err := json.Marshal(freezeAudit{
		Visited:    res.Visited,
		Frozen:     res.Frozen,
		Escaped:    res.Escaped,
		Violations: res.Violations,
		Truncated:  res.Truncated,
	}); err == nil {
		w.Header().Set(freezeAuditHeader, string(b))
	} else {
		logger.Warn().Err(err).Msg("could not encode the resume freeze audit")
	}

	if res.Truncated {
		// Every count above is a floor, so a zero-violations audit says nothing here.
		logger.Warn().
			Int("visited", res.Visited).
			Msg("the resume audit stopped at its bound; the rest of the hierarchy was not classified")
	}

	if res.Violations > 0 {
		// A frozen cgroup that the resume depends on. Not a race and not tolerable: it
		// means either the allowlist is missing a name or the walk froze a parent of one.
		logger.Error().
			Int("violations", res.Violations).
			Int("frozen", res.Frozen).
			Int("visited", res.Visited).
			// Joined into ONE string rather than logged as an array: envd's log pipeline
			// carries scalar fields and drops the rest, so a Strs() field never reaches
			// the place an operator reads it -- which was measured on a dev guest, where
			// the counts arrived and the paths did not. The paths are the whole reason
			// this line names anything, so they have to survive the transport.
			Str("paths", strings.Join(res.ViolationPaths, " ")).
			Msg("cgroups the resume depends on were frozen; the allowlist did not hold")
	}
	if res.Escaped > 0 {
		logger.Warn().
			Int("escaped", res.Escaped).
			Int("frozen", res.Frozen).
			Str("paths", strings.Join(res.EscapedPaths, " ")).
			Msg("cgroups were running at resume that the pre-pause sweep should have stopped; created after it, or missed by a truncated or failed sweep")
	}
}
