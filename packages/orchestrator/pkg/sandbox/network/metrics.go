//go:build linux

package network

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	datapathMetricName  = "orchestrator.network.datapath"
	nonDNSUDPMetricName = "orchestrator.network.slot_firewall.non_dns_udp"
)

var nonDNSUDPCounter = utils.Must(meter.Int64Counter(nonDNSUDPMetricName,
	metric.WithDescription("Guest UDP datagrams to ports other than 53 seen by a slot firewall, read when the slot is released."),
	metric.WithUnit("{packet}"),
))

// recordNonDNSUDP adds a released slot's tally under the mode and posture of
// the chain that counted it. No sandbox, team or template attribute: the
// cardinality stays at modes times postures times datapaths.
func recordNonDNSUDP(ctx context.Context, packets uint64, chain ChainDescriptor, networkVersion int) {
	nonDNSUDPCounter.Add(ctx, int64(packets), metric.WithAttributes(
		attribute.String("mode", string(chain.Mode)),
		attribute.String("posture", chain.Posture),
		attribute.String("network_version", strconv.Itoa(networkVersion)),
	))
}

// RegisterDatapathMetric reports the selected network datapath for this node.
// The caller owns the returned registration and must unregister it on shutdown.
func RegisterDatapathMetric(meterProvider metric.MeterProvider, networkVersion int) (metric.Registration, error) {
	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network")
	gauge, err := meter.Int64ObservableGauge(
		datapathMetricName,
		metric.WithDescription("Selected orchestrator network datapath on this node."),
		metric.WithUnit("{node}"),
	)
	if err != nil {
		return nil, err
	}

	attrs := metric.WithAttributes(attribute.String("network_version", strconv.Itoa(networkVersion)))

	return meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(gauge, 1, attrs)

		return nil
	}, gauge)
}
