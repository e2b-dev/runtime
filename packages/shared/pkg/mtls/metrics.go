package mtls

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// meterName is the package import path, the convention for shared meters.
const meterName = "github.com/e2b-dev/infra/packages/shared/pkg/mtls"

// Instrument names. The prefix is what the collectors' include lists carry.
const (
	MetricHandshakes        = "e2b_mtls_handshakes_total"
	MetricRefused           = "e2b_mtls_refused_total"
	MetricWouldReject       = "e2b_mtls_would_reject_total"
	MetricPlaintextRequests = "e2b_mtls_plaintext_requests_total"
	MetricPlaintextOpen     = "e2b_mtls_plaintext_connections"
	MetricConnections       = "e2b_mtls_connections_total"
	MetricMode              = "e2b_mtls_mode"
	MetricClientMode        = "e2b_mtls_client_mode"
	MetricNotAfter          = "e2b_mtls_certificate_not_after_seconds"
	MetricReloads           = "e2b_mtls_reloads_total"
	MetricRootsLoaded       = "e2b_mtls_roots_loaded"
)

// Attribute keys.
const (
	AttrListener    = "listener"
	AttrClientHop   = "client_hop"
	AttrOutcome     = "outcome"
	AttrReason      = "reason"
	AttrKind        = "kind"
	AttrCaller      = "caller"
	AttrMode        = "mode"
	AttrSource      = "source"
	AttrFingerprint = "fingerprint"
)

// Attribute values.
const (
	// OutcomeTLS: a TLS handshake completed and the peer was admitted.
	OutcomeTLS = "tls"
	// OutcomePlaintext: the connection was admitted as plaintext.
	OutcomePlaintext = "plaintext"
	// OutcomeRefused: the policy refused a TLS peer at the handshake.
	OutcomeRefused = "refused"
	// OutcomeFailed: the TLS handshake itself failed.
	OutcomeFailed = "failed"
	// OutcomeTimeout: the first byte or the handshake did not arrive in time.
	OutcomeTimeout = "timeout"

	KindHandshake = "handshake"
	KindGRPC      = "grpc"
	KindHTTP      = "http"
	KindLeaf      = "leaf"
	KindChain     = "chain"

	ReloadLoaded    = "loaded"
	ReloadUnchanged = "unchanged"
	ReloadFailed    = "failed"
)

// Metrics holds the package instruments. A nil *Metrics records nothing.
type Metrics struct {
	handshakes        metric.Int64Counter
	refusedTotal      metric.Int64Counter
	wouldRejectTotal  metric.Int64Counter
	plaintextRequests metric.Int64Counter
	plaintextOpen     metric.Int64UpDownCounter
	connections       metric.Int64Counter
	mode              metric.Int64Gauge
	clientMode        metric.Int64Gauge
	notAfter          metric.Int64Gauge
	reloads           metric.Int64Counter
	rootsLoaded       metric.Int64Gauge

	// lastMode and lastHop map a listener or hop name to the attribute set
	// its mode gauge reads 1 on, under mu.
	mu       sync.Mutex
	lastMode map[string]attribute.Set
	lastHop  map[string]attribute.Set
}

// NewMetrics creates the instruments on provider's meter named after this package.
func NewMetrics(provider metric.MeterProvider) (*Metrics, error) {
	meter := provider.Meter(meterName)
	m := &Metrics{lastMode: map[string]attribute.Set{}, lastHop: map[string]attribute.Set{}}

	var err error
	if m.handshakes, err = meter.Int64Counter(MetricHandshakes,
		metric.WithDescription("Connections classified by a listener or dialled by a client hop, by outcome"), metric.WithUnit("{connection}")); err != nil {
		return nil, err
	}
	if m.refusedTotal, err = meter.Int64Counter(MetricRefused,
		metric.WithDescription("Peers, requests and RPCs refused on a required listener, by reason"), metric.WithUnit("{refusal}")); err != nil {
		return nil, err
	}
	if m.wouldRejectTotal, err = meter.Int64Counter(MetricWouldReject,
		metric.WithDescription("Peers, requests and RPCs a permissive listener would have refused, by reason"), metric.WithUnit("{refusal}")); err != nil {
		return nil, err
	}
	if m.plaintextRequests, err = meter.Int64Counter(MetricPlaintextRequests,
		metric.WithDescription("Plaintext requests and RPCs other than health checks"), metric.WithUnit("{request}")); err != nil {
		return nil, err
	}
	if m.plaintextOpen, err = meter.Int64UpDownCounter(MetricPlaintextOpen,
		metric.WithDescription("Plaintext connections currently open"), metric.WithUnit("{connection}")); err != nil {
		return nil, err
	}
	if m.connections, err = meter.Int64Counter(MetricConnections,
		metric.WithDescription("Mutual TLS connections admitted, by caller name"), metric.WithUnit("{connection}")); err != nil {
		return nil, err
	}
	if m.mode, err = meter.Int64Gauge(MetricMode,
		metric.WithDescription("1 for the mode in force on a listener and where it came from")); err != nil {
		return nil, err
	}
	if m.clientMode, err = meter.Int64Gauge(MetricClientMode,
		metric.WithDescription("1 for the mode in force on a client hop and where it came from")); err != nil {
		return nil, err
	}
	if m.notAfter, err = meter.Int64Gauge(MetricNotAfter,
		metric.WithDescription("Expiry of the process's own leaf and of the earliest certificate in its chain"), metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.reloads, err = meter.Int64Counter(MetricReloads,
		metric.WithDescription("Certificate file reloads, by outcome"), metric.WithUnit("{reload}")); err != nil {
		return nil, err
	}
	if m.rootsLoaded, err = meter.Int64Gauge(MetricRootsLoaded,
		metric.WithDescription("1 for each root in the loaded trust bundle, by SHA-256 fingerprint")); err != nil {
		return nil, err
	}

	return m, nil
}

var (
	defaultMetricsOnce sync.Once
	defaultMetrics     *Metrics
)

// DefaultMetrics builds the instruments on the global meter provider once.
// On failure it logs and returns nil, which records nothing.
func DefaultMetrics() *Metrics {
	defaultMetricsOnce.Do(func() {
		m, err := NewMetrics(otel.GetMeterProvider())
		if err != nil {
			logger.L().Warn(context.Background(), "mtls: metrics disabled", zap.Error(err))

			return
		}
		defaultMetrics = m
	})

	return defaultMetrics
}

func (m *Metrics) handshake(ctx context.Context, listener, outcome string) {
	if m == nil {
		return
	}
	m.handshakes.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrListener, listener), attribute.String(AttrOutcome, outcome)))
}

func (m *Metrics) clientHandshake(ctx context.Context, hop, outcome string) {
	if m == nil {
		return
	}
	m.handshakes.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrClientHop, hop), attribute.String(AttrOutcome, outcome)))
}

func (m *Metrics) refused(ctx context.Context, listener string, reason Reason, kind string) {
	if m == nil {
		return
	}
	m.refusedTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrListener, listener), attribute.String(AttrReason, string(reason)), attribute.String(AttrKind, kind)))
}

func (m *Metrics) wouldReject(ctx context.Context, listener string, reason Reason, kind string) {
	if m == nil {
		return
	}
	m.wouldRejectTotal.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrListener, listener), attribute.String(AttrReason, string(reason)), attribute.String(AttrKind, kind)))
}

func (m *Metrics) plaintextRequest(ctx context.Context, listener, kind string) {
	if m == nil {
		return
	}
	m.plaintextRequests.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrListener, listener), attribute.String(AttrKind, kind)))
}

func (m *Metrics) plaintextConn(ctx context.Context, listener string, delta int64) {
	if m == nil {
		return
	}
	m.plaintextOpen.Add(ctx, delta, metric.WithAttributes(attribute.String(AttrListener, listener)))
}

func (m *Metrics) connection(ctx context.Context, listener, caller string) {
	if m == nil {
		return
	}
	m.connections.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrListener, listener), attribute.String(AttrCaller, caller)))
}

func (m *Metrics) recordMode(ctx context.Context, listener string, mode Mode, source string) {
	if m == nil {
		return
	}
	set := attribute.NewSet(attribute.String(AttrListener, listener), attribute.String(AttrMode, mode.String()), attribute.String(AttrSource, source))
	m.flip(ctx, m.mode, m.lastMode, listener, set)
}

func (m *Metrics) recordClientMode(ctx context.Context, hop string, mode ClientMode, source string) {
	if m == nil {
		return
	}
	set := attribute.NewSet(attribute.String(AttrClientHop, hop), attribute.String(AttrMode, mode.String()), attribute.String(AttrSource, source))
	m.flip(ctx, m.clientMode, m.lastHop, hop, set)
}

// flip moves the 1 on a mode gauge: set is the series for the mode now in
// force, and last remembers which series each key last set to 1. When they
// differ, the old series is written 0 and the new one 1; when they are the
// same, nothing is written. The lock is held across both writes, so two
// concurrent changes of one key cannot leave two series reading 1.
func (m *Metrics) flip(ctx context.Context, gauge metric.Int64Gauge, last map[string]attribute.Set, key string, set attribute.Set) {
	m.mu.Lock()
	defer m.mu.Unlock()

	previous, had := last[key]
	if had && previous.Equals(&set) {
		return
	}
	last[key] = set

	if had {
		gauge.Record(ctx, 0, metric.WithAttributeSet(previous))
	}
	gauge.Record(ctx, 1, metric.WithAttributeSet(set))
}

func (m *Metrics) reload(ctx context.Context, outcome string) {
	if m == nil {
		return
	}
	m.reloads.Add(ctx, 1, metric.WithAttributes(attribute.String(AttrOutcome, outcome)))
}

// bundleLoaded reports the expiry of the new set and the roots it holds,
// and zeroes the roots that left since previous.
func (m *Metrics) bundleLoaded(ctx context.Context, previous, current *Bundle) {
	if m == nil || current == nil {
		return
	}

	leaf := current.Certificate.Leaf
	m.notAfter.Record(ctx, leaf.NotAfter.Unix(), metric.WithAttributes(attribute.String(AttrKind, KindLeaf)))
	m.notAfter.Record(ctx, chainNotAfter(current.Certificate.Leaf, current.Certificate.Certificate[1:]).Unix(), metric.WithAttributes(attribute.String(AttrKind, KindChain)))

	loaded := map[string]struct{}{}
	for _, root := range current.RootCerts {
		fp := fingerprintOf(root)
		loaded[fp] = struct{}{}
		m.rootsLoaded.Record(ctx, 1, metric.WithAttributes(attribute.String(AttrFingerprint, fp)))
	}
	if previous == nil {
		return
	}
	for _, root := range previous.RootCerts {
		fp := fingerprintOf(root)
		if _, still := loaded[fp]; still {
			continue
		}
		m.rootsLoaded.Record(ctx, 0, metric.WithAttributes(attribute.String(AttrFingerprint, fp)))
	}
}

// chainNotAfter is the earliest expiry among the leaf and the intermediates
// the certificate file carried; an intermediate that does not parse is skipped.
func chainNotAfter(leaf *x509.Certificate, intermediatesDER [][]byte) time.Time {
	earliest := leaf.NotAfter
	for _, der := range intermediatesDER {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			continue
		}
		if cert.NotAfter.Before(earliest) {
			earliest = cert.NotAfter
		}
	}

	return earliest
}

func fingerprintOf(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)

	return hex.EncodeToString(sum[:])
}
