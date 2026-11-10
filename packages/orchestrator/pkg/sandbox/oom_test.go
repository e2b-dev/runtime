//go:build linux

package sandbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
)

func TestOOMWatermarkSeedFrom(t *testing.T) {
	t.Parallel()

	errRequest := errors.New("request failed")
	kills := func(seqs ...int64) *Metrics {
		list := []envd.OOMKill{}
		for _, seq := range seqs {
			list = append(list, envd.OOMKill{Seq: seq, Process: "p"})
		}

		return &Metrics{OomKills: &list}
	}
	// fetcher returns the responses in order, then keeps failing.
	fetcher := func(responses ...*Metrics) (func(context.Context) (*Metrics, error), *int) {
		calls := 0

		return func(context.Context) (*Metrics, error) {
			calls++
			if calls > len(responses) || responses[calls-1] == nil {
				return nil, errRequest
			}

			return responses[calls-1], nil
		}, &calls
	}

	t.Run("retries until envd knows the kills", func(t *testing.T) {
		t.Parallel()

		var w OOMWatermark
		fetch, calls := fetcher(nil, &Metrics{}, kills(4))
		require.NoError(t, w.seedFrom(t.Context(), fetch, 0))
		assert.Equal(t, 3, *calls, "a failed request and an omitted list are both retried")
		assert.Equal(t, []envd.OOMKill{{Seq: 7, Process: "p"}}, w.Unseen(*kills(4, 7).OomKills))
	})

	t.Run("gives up and lets the first poll set the watermark", func(t *testing.T) {
		t.Parallel()

		var w OOMWatermark
		fetch, calls := fetcher()
		require.ErrorIs(t, w.seedFrom(t.Context(), fetch, 0), errRequest)
		assert.Equal(t, oomSeedAttempts, *calls)
		require.NoError(t, w.seedFrom(t.Context(), fetch, 0))
		assert.Equal(t, oomSeedAttempts, *calls, "after a checkpoint a seed that gave up isn't retried")
		assert.Empty(t, w.Unseen(*kills(4).OomKills), "the first poll sets the watermark")
		assert.Equal(t, []envd.OOMKill{{Seq: 9, Process: "p"}}, w.Unseen(*kills(4, 9).OomKills))
	})

	t.Run("leaves a set watermark alone", func(t *testing.T) {
		t.Parallel()

		var w OOMWatermark
		w.Seed(*kills(5).OomKills)
		fetch, calls := fetcher(kills(5, 8))
		require.NoError(t, w.seedFrom(t.Context(), fetch, 0))
		assert.Zero(t, *calls, "after a checkpoint the seed doesn't call envd")
		assert.Equal(t, []envd.OOMKill{{Seq: 8, Process: "p"}}, w.Unseen(*kills(5, 8).OomKills))
	})

	t.Run("stops when the checks stop", func(t *testing.T) {
		t.Parallel()

		var w OOMWatermark
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		fetch, _ := fetcher()
		require.ErrorIs(t, w.seedFrom(ctx, fetch, 0), context.Canceled)
		assert.Empty(t, w.Unseen(*kills(4).OomKills), "a stopped seed doesn't hand over to the poll")
	})
}
