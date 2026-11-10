//go:build linux

package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// errNoHeldConn is the transport's dial failing because no connection was handed off: the
// request was never written.
var errNoHeldConn = errors.New("no envd connection handed off")

const (
	// envdThawHeader is how envd says, on a /init response, that it honoured thaw: "skip"
	// and left the workload frozen. Absent after an inline thaw and from an envd that
	// predates the field.
	envdThawHeader = "X-Envd-Thaw"

	// envdInitMinConnectTimeout is the floor under envd-init-connect-timeout-milliseconds.
	// A connect abandoned before the guest answers its SYN carries no request, so a
	// deadline above the guest's SYN latency costs only attempts; one below it would
	// abandon every dial, and a stalled guest takes 0.5-2 s to answer one.
	envdInitMinConnectTimeout = time.Second

	// envdInitSlowDial is the dial duration above which a held attempt is counted in
	// slow_dials: a SYN the guest dropped costs the client's first retransmit, about 1 s.
	envdInitSlowDial = 500 * time.Millisecond
)

// useHeldInit decides the readiness loop for one start: the held request when
// envd-init-held-request is on for this sandbox and the gate version is at or above
// MinEnvdVersionForHeldInit. An unparseable version reads as below it.
func (s *Sandbox) useHeldInit(ctx context.Context) bool {
	if s.featureFlags == nil {
		return false
	}
	if !s.featureFlags.BoolFlag(ctx, featureflags.EnvdInitHeldRequestFlag, sandboxLDContext(s.Runtime, s.Config)) {
		return false
	}

	ok, err := utils.IsGTEVersion(s.envdGateVersion(), utils.MinEnvdVersionForHeldInit)

	return err == nil && ok
}

// envdGateVersion is the envd version the held-init gate reads, for the rest of this life:
// the version a live upgrade this orchestrator confirmed delivered, else the version an
// offline upgrade swapped onto the rootfs before this boot, else the version the last served
// /init reported when it is admissible, else the version of the envd the resumed snapshot
// was taken from (resumeGateEnvdVersion), else the template-baked one. The first two are the
// orchestrator's own binaries; the third is the guest's word, and so may the fourth be, when
// an earlier life's envd reported it. A false claim only chooses between two init paths for
// that guest.
func (s *Sandbox) envdGateVersion() string {
	if v := s.stagedEnvdVersion(); v != "" {
		return v
	}
	if v, ok := admissibleEnvdVersion(s.LiveEnvdVersion()); ok {
		return v
	}
	if s.inheritedEnvdVersion != "" {
		return s.inheritedEnvdVersion
	}

	return s.Config.Envd.Version
}

// stagedEnvdVersion is the version of the envd binary this orchestrator staged for this
// life: the delivered mark, else the offline upgrade's version, else "".
func (s *Sandbox) stagedEnvdVersion() string {
	if p := s.deliveredEnvdVersion.Load(); p != nil {
		return *p
	}
	if p := s.offlineEnvdVersion.Load(); p != nil {
		return *p
	}

	return ""
}

// RunningEnvdVersion is the version of the envd that answered this life's last /init when
// that report is admissible, else the version of the binary the orchestrator staged for
// this life, else "". Throwaway inheritance (WithInheritedEnvdVersion), the pause record
// (snapshotEnvdVersion) and CA selection (ExpectedEnvdVersion) all read it. The report comes
// ahead of the mark: a guest whose delivered envd died and restarted from its rootfs binary
// runs that binary, and a throwaway gated by the mark would send it a thaw skip it ignores.
func (s *Sandbox) RunningEnvdVersion() string {
	if v, ok := admissibleEnvdVersion(s.LiveEnvdVersion()); ok {
		return v
	}

	return s.stagedEnvdVersion()
}

// resumeGateEnvdVersion is the version a memory resume's gate reads before its envd reports
// one: the source handle's version a throwaway is given (WithInheritedEnvdVersion), else the
// version the snapshot recorded at pause when it is admissible, else "". The recorded version
// is checked here because it may be what an earlier life's guest reported.
func resumeGateEnvdVersion(given, recorded string) string {
	if given != "" {
		return given
	}
	if v, ok := admissibleEnvdVersion(recorded); ok {
		return v
	}

	return ""
}

// snapshotEnvdVersion is the version a pause records for the next life's gate
// (metadata.Template.RunningEnvdVersion): the version the running envd last reported when
// it is admissible, else the version this life's gate reads, or "" when that is the
// template-baked one or the snapshot is filesystem-only, whose cold boot runs the rootfs
// binary. The report comes ahead of the mark here, unlike in the gate: it names the envd
// that answered this life's last /init, so a guest whose envd restarted from the rootfs
// binary after an upgrade, and answered a later /init, records the rootfs binary's version,
// which is nothing when that is the template-baked one.
func (s *Sandbox) snapshotEnvdVersion(filesystemOnly bool) string {
	if filesystemOnly {
		return ""
	}
	v := s.RunningEnvdVersion()
	if v == "" {
		v = s.envdGateVersion()
	}
	if v == s.Config.Envd.Version {
		return ""
	}

	return v
}

// MarkEnvdDelivered records that a live upgrade delivered envd version v and its exec was
// confirmed. The held-init gate reads it ahead of what the running envd reports, so the
// post-upgrade re-check and every later /init of this life route by the binary the
// orchestrator itself staged. The mark is not cleared when the running envd then reports a
// different version: a held start on an envd below MinEnvdVersionForHeldInit still ends
// thawed by its handler, but lacks that release's clock correction, thaw skip and withheld
// thaw of an /init a freeze overtook; the missing correction shows as a held start with no
// clock_correction sample. A new life starts unmarked; a memory pause records the running
// envd's version for the next life's gate instead (snapshotEnvdVersion), which is the mark
// only when no admissible report differs from it, and a throwaway inherits the same
// (RunningEnvdVersion).
func (s *Sandbox) MarkEnvdDelivered(v string) {
	s.deliveredEnvdVersion.Store(&v)
}

// envdInitConnectTimeout is the held request's connect deadline for this start. It is
// derived from the start's context, so a deadline above the start's budget is cut by it.
func (s *Sandbox) envdInitConnectTimeout(ctx context.Context) time.Duration {
	ms := s.featureFlags.IntFlag(ctx, featureflags.EnvdInitConnectTimeoutMilliseconds, sandboxLDContext(s.Runtime, s.Config))
	if ms <= 0 {
		ms = featureflags.EnvdInitConnectTimeoutMilliseconds.Fallback()
	}

	return max(time.Duration(ms)*time.Millisecond, envdInitMinConnectTimeout)
}

// heldInitClient is one start's dedicated keep-alive client with a single connection. It
// is separate from sandboxHttpClient, whose transport disables keep-alives and whose 10 s
// timeout would cut a held request short.
type heldInitClient struct {
	addr           string
	connectTimeout time.Duration
	dialer         net.Dialer
	transport      *http.Transport
	client         *http.Client

	mu   sync.Mutex
	next net.Conn
}

// errNoHeldBudget is a held start asked for with no envd-wait budget. A zero
// ExpectContinueTimeout would send the body with the headers, and any other stand-in
// would send it unasked once that passed: the legacy loop's aging timestamp under the
// held loop's label.
var errNoHeldBudget = errors.New("held init without an envd-wait budget")

// newHeldInitClient builds the client for one start. budget is the start's envd-wait
// budget: the transport holds a request's body until envd's 100 Continue for that long, so
// the wait never ends before the start's context does.
func newHeldInitClient(baseURL string, connectTimeout, budget time.Duration) (*heldInitClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse envd address: %w", err)
	}
	if budget <= 0 {
		return nil, errNoHeldBudget
	}

	h := &heldInitClient{addr: u.Host, connectTimeout: connectTimeout}
	h.transport = &http.Transport{
		DialContext:           h.dialContext,
		MaxConnsPerHost:       1,
		MaxIdleConnsPerHost:   1,
		ExpectContinueTimeout: budget,
		DisableCompression:    true,
	}
	client := envdHTTPClient(otelhttp.NewTransport(h.transport), 0)
	h.client = &client

	return h, nil
}

// dial connects to envd under the connect deadline.
func (h *heldInitClient) dial(ctx context.Context) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, h.connectTimeout)
	defer cancel()

	return h.dialer.DialContext(ctx, "tcp", h.addr)
}

// handOff makes conn the connection the transport's next dial returns, after closing the
// idle one, so the next request goes over conn.
func (h *heldInitClient) handOff(conn net.Conn) {
	h.transport.CloseIdleConnections()

	h.mu.Lock()
	prev := h.next
	h.next = conn
	h.mu.Unlock()

	if prev != nil {
		prev.Close()
	}
}

// dialContext returns the connection handed off by the loop. It never dials one itself, so
// the connect deadline and the connect-failure retry stay the loop's.
func (h *heldInitClient) dialContext(context.Context, string, string) (net.Conn, error) {
	h.mu.Lock()
	conn := h.next
	h.next = nil
	h.mu.Unlock()

	if conn == nil {
		return nil, errNoHeldConn
	}

	return conn, nil
}

// close releases the idle connection, which neither side would expire on its own: the
// transport sets no idle timeout and envd's server holds one for minutes.
func (h *heldInitClient) close() {
	h.transport.CloseIdleConnections()

	h.mu.Lock()
	conn := h.next
	h.next = nil
	h.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
}

// do sends req with no GetBody, so net/http never re-sends it on its own after a reused
// connection fails: a request the peer rejects is a transport failure the loop handles.
func (h *heldInitClient) do(req *http.Request) (*http.Response, error) {
	req.GetBody = nil

	return h.client.Do(req)
}

// mintedInitBody is a held /init's body, which does not exist until the transport reads it.
// The request carries Expect: 100-continue, so the transport reads it only once envd's
// handler has asked for the body; its timestamp is minted then, after the guest's accept
// queue, the header parse and the dispatch, which on a slow resume are most of the time a
// request spends before envd applies it. Its length is unknown up front, so the body is
// sent chunked.
type mintedInitBody struct {
	mu     sync.Mutex
	body   envd.PostInitJSONBody
	minted time.Time
	r      *bytes.Reader
	err    error
}

func newMintedInitBody(body envd.PostInitJSONBody) *mintedInitBody {
	return &mintedInitBody{body: body}
}

func (b *mintedInitBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.r == nil && b.err == nil {
		b.minted = time.Now()
		b.body.Timestamp = b.minted

		encoded, err := json.Marshal(b.body)
		if err != nil {
			b.err = err
		} else {
			b.r = bytes.NewReader(encoded)
		}
	}
	if b.err != nil {
		return 0, b.err
	}

	return b.r.Read(p)
}

func (b *mintedInitBody) Close() error { return nil }

// mintedAt is when the body's timestamp was minted, and false when the transport never read
// the body: envd answered before it asked for one.
func (b *mintedInitBody) mintedAt() (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.minted, b.r != nil
}

// newHeldHTTPRequest builds one held attempt's POST /init with a body minted when read.
func (r *envdInitRequest) newHeldHTTPRequest(ctx context.Context) (*http.Request, *mintedInitBody, error) {
	body := newMintedInitBody(r.body)

	request, err := http.NewRequestWithContext(WithEnvdSandbox(ctx, r.sandbox), http.MethodPost, r.address, body)
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Expect", "100-continue")
	if r.accessToken != nil {
		request.Header.Set("X-Access-Token", *r.accessToken)
	}

	return request, body, nil
}

// doHeldInit sends one /init and waits for it under the parent context alone, re-sending
// only after a transport failure. Each attempt dials first, so the request's headers are
// written on a connection that is already up, and its body, timestamp included, only once
// envd's handler asks for it. A failed connect is retried after loopDelay, as the legacy
// loop does while envd is not yet listening; a failure after the connection was up is
// retried on a new connection.
func (s *Sandbox) doHeldInit(
	ctx context.Context,
	h *heldInitClient,
	initRequest *envdInitRequest,
	countStart bool,
	reinit *envdReinit,
) (*http.Response, envdInitAttempts, error) {
	var attempts envdInitAttempts

	for {
		if reinit != nil && !reinit.mayWrite() {
			return nil, attempts, errEnvdReinitCut
		}
		attempts.count++

		dialStart := time.Now()
		conn, err := h.dial(ctx)
		dialed := time.Since(dialStart)
		attempts.dialed(dialed)
		if err != nil {
			attempts.fail(envdInitPhaseConnect)
			s.recordSlowDial(ctx, countStart, dialed, string(envdInitPhaseConnect))
		} else {
			h.handOff(conn)

			var phase attemptPhase
			request, body, err := initRequest.newHeldHTTPRequest(phase.trace(ctx))
			if err != nil {
				return nil, attempts, err
			}

			response, err := h.do(request)
			if err == nil {
				if minted, ok := body.mintedAt(); ok {
					attempts.servedAge = time.Since(minted)
				}
				attempts.servedDial = dialed
				s.recordSlowDial(ctx, countStart, dialed, "success")

				return response, attempts, nil
			}

			failed := phase.heldPhase()
			attempts.fail(failed)
			s.recordSlowDial(ctx, countStart, dialed, string(failed))
		}

		select {
		case <-ctx.Done():
			return nil, attempts, fmt.Errorf("%w with cause: %w", ctx.Err(), context.Cause(ctx))
		case <-time.After(loopDelay):
		}
	}
}

// recordSlowDial counts a held attempt whose dial took over envdInitSlowDial, by how the
// attempt then ended.
func (s *Sandbox) recordSlowDial(ctx context.Context, countStart bool, dialed time.Duration, phase string) {
	if !countStart || dialed <= envdInitSlowDial {
		return
	}

	envdInitSlowDials.Add(ctx, 1, metric.WithAttributes(attribute.String("phase", phase), s.teamAttr()))
}

// recordHarvestThaw counts a throwaway's served held /init by whether envd answered that it
// skipped the thaw. A throwaway the gate admitted whose envd thawed inline anyway ran its
// workload into the harvest's trace; its envd claimed a version that does not skip. The
// version label is gateVersion, the one the gate read for this /init, bounded like the
// running-version label.
func (s *Sandbox) recordHarvestThaw(ctx context.Context, header http.Header, gateVersion string) {
	v := gateVersion
	if !s.holdsEnvdVersion(v) {
		v = envdRunningVersionOther
	}

	harvestThaw.Add(ctx, 1, metric.WithAttributes(
		attribute.Bool("skipped", header.Get(envdThawHeader) == string(envd.Skip)),
		envdRunningVersionKey.String(v),
		s.teamAttr(),
	))
}
