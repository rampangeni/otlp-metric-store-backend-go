package main

import (
	"context"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel/metric"
)

var identityCacheOverflowCounter metric.Int64Counter

func init() {
	var err error
	identityCacheOverflowCounter, err = meter.Int64Counter(
		"com.dash0.homeexercise.metrics.identity_cache_overflow",
		metric.WithDescription("Number of times the metric identity cache exceeded its configured size and had to be reset"),
		metric.WithUnit("{overflow}"))
	if err != nil {
		panic(err)
	}
}

// MetricIdentityCache tracks which MetricIDs have already been persisted to the
// otel_metric_metadata lookup table, so that a long-running series -- which
// will appear in effectively every subsequent Export request -- doesn't cause
// its (unchanging) metadata row to be re-sent and re-inserted on every batch.
//
// The assignment's stated assumption is that metric cardinality is low, so an
// in-memory set sized for "every distinct series ever seen" is a reasonable
// default. The maxSize cap below is purely a safety net: if that assumption
// turns out to be wrong in production (e.g. a label accidentally carries
// something high-cardinality like a request ID), we would rather degrade
// gracefully -- fall back to re-inserting metadata, which is always safe and
// idempotent, see the ReplacingMergeTree comment on otel_metric_metadata in
// clickhouse_schema.go -- and loudly signal the problem via logs/metrics, than
// grow the process's memory without bound.
type MetricIdentityCache struct {
	mu      sync.Mutex
	seen    map[uint64]struct{}
	maxSize int
}

// NewMetricIdentityCache creates a cache that holds up to maxSize distinct
// MetricIDs before resetting itself.
func NewMetricIdentityCache(maxSize int) *MetricIdentityCache {
	return &MetricIdentityCache{
		seen:    make(map[uint64]struct{}, 1024),
		maxSize: maxSize,
	}
}

// MarkSeen records id as persisted and reports whether it was already known.
// Callers should only (re-)insert metadata for a MetricID when this returns
// false.
func (c *MetricIdentityCache) MarkSeen(ctx context.Context, id uint64) (alreadySeen bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.seen[id]; ok {
		return true
	}

	if len(c.seen) >= c.maxSize {
		slog.WarnContext(ctx, "metric identity cache exceeded configured size, resetting; "+
			"if this happens repeatedly, metric cardinality is higher than expected",
			slog.Int("maxSize", c.maxSize))
		identityCacheOverflowCounter.Add(ctx, 1)
		c.seen = make(map[uint64]struct{}, 1024)
	}

	c.seen[id] = struct{}{}
	return false
}

// Size returns the number of distinct MetricIDs currently tracked. Exposed for
// tests and diagnostics.
func (c *MetricIdentityCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}
