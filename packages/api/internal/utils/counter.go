package utils

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type TemplateCounter struct {
	count      int64
	lastUpdate time.Time
}

type TemplateSpawnCounter struct {
	db        *sqlcdb.Client
	counters  map[string]*TemplateCounter
	mu        sync.Mutex
	ticker    *time.Ticker
	done      chan struct{}
	closeOnce sync.Once
}

func NewTemplateSpawnCounter(ctx context.Context, tickerDuration time.Duration, dbClient *sqlcdb.Client) *TemplateSpawnCounter {
	counter := &TemplateSpawnCounter{
		db:       dbClient,
		counters: make(map[string]*TemplateCounter),
		ticker:   time.NewTicker(tickerDuration),
		done:     make(chan struct{}),
	}

	go counter.processUpdates(ctx)

	return counter
}

func (t *TemplateSpawnCounter) IncreaseTemplateSpawnCount(templateID string, time time.Time) {
	t.mu.Lock()
	if _, exists := t.counters[templateID]; !exists {
		t.counters[templateID] = &TemplateCounter{}
	}
	t.counters[templateID].count++
	t.counters[templateID].lastUpdate = time
	t.mu.Unlock()
}

func (t *TemplateSpawnCounter) processUpdates(ctx context.Context) {
	for {
		select {
		case <-t.ticker.C:
			t.flushCounters(ctx)
		case <-ctx.Done():
			t.ticker.Stop()

			return
		case <-t.done:
			t.ticker.Stop()

			return
		}
	}
}

func (t *TemplateSpawnCounter) flushCounters(ctx context.Context) {
	t.mu.Lock()
	updates := make(map[string]*TemplateCounter)
	for templateID, counter := range t.counters {
		if counter.count > 0 {
			updates[templateID] = counter
		}
	}
	// Clear the counters
	t.counters = make(map[string]*TemplateCounter)
	t.mu.Unlock()

	for templateID, counter := range updates {
		err := t.db.UpdateTemplateSpawnCount(ctx, queries.UpdateTemplateSpawnCountParams{
			SpawnCount:    counter.count,
			LastSpawnedAt: &counter.lastUpdate,
			TemplateID:    templateID,
		})
		if err != nil {
			logger.L().Error(ctx, "error updating template spawn count", zap.Error(err))
		}
	}
}

func (t *TemplateSpawnCounter) Close(ctx context.Context) {
	t.closeOnce.Do(func() {
		close(t.done)
		t.flushCounters(ctx)
	})
}
