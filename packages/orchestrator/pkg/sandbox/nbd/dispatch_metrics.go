//go:build linux

package nbd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Both instruments read the in-flight set at collection, so the hot path only
// adds and removes its call there.
var (
	writeOps = []string{opWrite, opWriteZeroes, opTrim}

	// nbdInflight holds every backend call this process is waiting on.
	nbdInflight = newInflightTracker()
	opAttrs     = attrSets([]string{opRead, opWrite, opWriteZeroes, opTrim})

	_ = utils.Must(meter.Int64ObservableUpDownCounter("orchestrator.nbd.dispatch.write.concurrent",
		metric.WithDescription("Number of NBD write, write-zeroes and trim backend calls currently waiting, by op."),
		metric.WithUnit("{command}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			stats := nbdInflight.stats(time.Now())
			for _, op := range writeOps {
				o.Observe(stats[op].waiting, opAttrs[op])
			}

			return nil
		}),
	))

	_ = utils.Must(meter.Int64ObservableGauge("orchestrator.nbd.dispatch.oldest_inflight.age",
		metric.WithDescription("Age of the oldest NBD backend call still waiting, by op; absent when none waits."),
		metric.WithUnit("ms"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for op, stat := range nbdInflight.stats(time.Now()) {
				o.Observe(stat.oldest.Milliseconds(), opAttrs[op])
			}

			return nil
		}),
	))
)

func attrSets(ops []string) map[string]metric.MeasurementOption {
	sets := make(map[string]metric.MeasurementOption, len(ops))
	for _, op := range ops {
		sets[op] = metric.WithAttributeSet(attribute.NewSet(attribute.String("op", op)))
	}

	return sets
}

// track runs one backend call, keeping it in the in-flight set until it
// returns. A call that never returns stays there, so its age keeps growing on
// the gauge.
func (d *Dispatch) track(op string, call func() error) error {
	id := d.inflight.begin(op)
	defer d.inflight.end(id)

	return call()
}

const inflightShards = 64

// inflightTracker holds the start time of every backend call still waiting.
// It is sharded so concurrent dispatchers rarely share a lock.
type inflightTracker struct {
	next   atomic.Uint64
	shards [inflightShards]inflightShard
}

type inflightShard struct {
	mu    sync.Mutex
	calls map[uint64]inflightCall
}

type inflightCall struct {
	op    string
	start time.Time
}

func newInflightTracker() *inflightTracker {
	t := &inflightTracker{}
	for i := range t.shards {
		t.shards[i].calls = make(map[uint64]inflightCall)
	}

	return t
}

// begin records a call of op starting now and returns its id for end.
func (t *inflightTracker) begin(op string) uint64 {
	id := t.next.Add(1)
	shard := &t.shards[id%inflightShards]

	shard.mu.Lock()
	shard.calls[id] = inflightCall{op: op, start: time.Now()}
	shard.mu.Unlock()

	return id
}

func (t *inflightTracker) end(id uint64) {
	shard := &t.shards[id%inflightShards]

	shard.mu.Lock()
	delete(shard.calls, id)
	shard.mu.Unlock()
}

type inflightStat struct {
	waiting int64
	oldest  time.Duration
}

// stats returns, for each op with a call waiting, how many wait and how long
// the oldest has.
func (t *inflightTracker) stats(now time.Time) map[string]inflightStat {
	stats := make(map[string]inflightStat)
	for i := range t.shards {
		shard := &t.shards[i]
		shard.mu.Lock()
		for _, call := range shard.calls {
			stat := stats[call.op]
			stat.waiting++
			stat.oldest = max(stat.oldest, now.Sub(call.start))
			stats[call.op] = stat
		}
		shard.mu.Unlock()
	}

	return stats
}
