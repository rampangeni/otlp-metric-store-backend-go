package main

// createMetricMetadataTableSQL creates the "lookup" table that this schema
// revolves around: every field that identifies a metric time series (as
// opposed to a single data point) lives here exactly once, keyed by MetricID
// -- a deterministic hash of those very fields, computed in Go by
// MetricIdentity.ID() (see metric_identity.go). The datapoint tables below
// then store only MetricID + timestamp + value, joining back to this table
// for anything metadata-related.
//
// Why ReplacingMergeTree: multiple backend replicas (or a single replica
// after a crash/retry) may concurrently compute and try to insert the same
// MetricID with identical metadata. Rather than deduplicate in application
// code with a distributed lock, we let ClickHouse asynchronously drop
// duplicate rows in the background during merges. CreatedAt is the version
// column so that, if a row for a given MetricID were ever inserted twice with
// different content (which should not happen since MetricID is a pure
// function of this content, but could in theory happen due to a hash
// collision), the most recently inserted row wins deterministically rather
// than silently keeping whichever copy merges first.
//
// Why ORDER BY (ServiceName, MetricName, MetricID) and not something
// time-based: this table has no time dimension of its own worth pruning on
// (FirstSeenTimeUnix is informational, not a query filter), and per the
// assignment cardinality of distinct series is assumed low, so the whole
// table is expected to be small enough that a metadata lookup by MetricID (or
// by ServiceName/MetricName prefix, e.g. for a "list known metrics for this
// service" admin UI) is cheap even without dedicated skip indexes on
// MetricID itself.
const createMetricMetadataTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metric_metadata (
   MetricID UInt64 CODEC(ZSTD(1)),
   MetricType LowCardinality(String) CODEC(ZSTD(1)),
   ServiceName LowCardinality(String) CODEC(ZSTD(1)),
   MetricName LowCardinality(String) CODEC(ZSTD(1)),
   MetricDescription String CODEC(ZSTD(1)),
   MetricUnit String CODEC(ZSTD(1)),
   ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ResourceSchemaUrl String CODEC(ZSTD(1)),
   ScopeName String CODEC(ZSTD(1)),
   ScopeVersion String CODEC(ZSTD(1)),
   ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
   ScopeSchemaUrl String CODEC(ZSTD(1)),
   Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   AggregationTemporality Int32 CODEC(ZSTD(1)),
   IsMonotonic Bool CODEC(ZSTD(1)),
   FirstSeenTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   CreatedAt DateTime DEFAULT now() CODEC(ZSTD(1)),


   INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE ReplacingMergeTree(CreatedAt)
ORDER BY (ServiceName, MetricName, MetricID)
SETTINGS index_granularity = 8192;
`

// createGaugeTableSQL and createSumTableSQL intentionally store nothing but
// the reference to otel_metric_metadata plus the raw sample itself. This is
// the core of the "extract metadata into a lookup table" design: on
// high-throughput ingestion, every data point row is now a fixed, small width
// (8+8+8+8+4 bytes plus codec overhead) instead of carrying two Maps and half
// a dozen strings, which is where the bulk of the storage and network cost of
// the wide-row layout used to go.
//
// Why ORDER BY (TimeUnix, MetricID) and not (MetricID, TimeUnix): the
// assignment states queries always filter on a time range and, other than
// that, have no other mandatory filter -- so a bare "give me everything
// between T1 and T2" must never require a full table scan. Leading the
// sorting key with TimeUnix (combined with PARTITION BY toDate(TimeUnix))
// guarantees that. MetricID is still included in the sorting key (so
// "single series over a time range" queries -- the expected common case --
// benefit from clustering) and is additionally exposed as its own minmax skip
// index below so ClickHouse can prune granules by MetricID within a time
// range without it having to be the primary sort dimension.
const createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
   MetricID UInt64 CODEC(ZSTD(1)),
   StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   Value Float64 CODEC(ZSTD(1)),
   Flags UInt32 CODEC(ZSTD(1)),


   INDEX idx_metric_id MetricID TYPE minmax GRANULARITY 4
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (TimeUnix, MetricID)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createSumTableSQL: AggregationTemporality/IsMonotonic are properties of the
// metric definition, not of any individual data point, so they now live once
// in otel_metric_metadata instead of being repeated on every row here -- see
// createGaugeTableSQL's comment for the rest of the rationale, which applies
// identically to this table.
const createSumTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_sum (
   MetricID UInt64 CODEC(ZSTD(1)),
   StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   Value Float64 CODEC(ZSTD(1)),
   Flags UInt32 CODEC(ZSTD(1)),


   INDEX idx_metric_id MetricID TYPE minmax GRANULARITY 4
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (TimeUnix, MetricID)
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createHistogramTableSQL, createExponentialHistogramTableSQL and
// createSummaryTableSQL are intentionally left on the old wide-row layout.
// Unlike Gauge and Sum, none of these three ever had a mapper or store method
// wired up to begin with (there is no MapHistogramRows/InsertHistogram etc.
// anywhere in this codebase) -- they are pre-existing dead schema, not part
// of the working ingestion path. Migrating dead code to the new
// metadata-lookup design would be scope creep with no observable behavior
// change, so they are left as-is. Extending them later is mechanical: apply
// the exact same MetricID pattern used for otel_metrics_gauge/otel_metrics_sum
// once a mapper for these types actually exists.
const createHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_histogram (
   ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ResourceSchemaUrl String CODEC(ZSTD(1)),
   ScopeName String CODEC(ZSTD(1)),
   ScopeVersion String CODEC(ZSTD(1)),
   ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
   ScopeSchemaUrl String CODEC(ZSTD(1)),
   ServiceName LowCardinality(String) CODEC(ZSTD(1)),
   MetricName LowCardinality(String) CODEC(ZSTD(1)),
   MetricDescription String CODEC(ZSTD(1)),
   MetricUnit String CODEC(ZSTD(1)),
   Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   Count UInt64 CODEC(Delta(8), ZSTD(1)),
   Sum Float64 CODEC(ZSTD(1)),
   BucketCounts Array(UInt64) CODEC(ZSTD(1)),
   ExplicitBounds Array(Float64) CODEC(ZSTD(1)),
   Min Float64 CODEC(ZSTD(1)),
   Max Float64 CODEC(ZSTD(1)),
   Flags UInt32 CODEC(ZSTD(1)),
   AggregationTemporality Int32 CODEC(ZSTD(1)),


   INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createExponentialHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_exponential_histogram (
   ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ResourceSchemaUrl String CODEC(ZSTD(1)),
   ScopeName String CODEC(ZSTD(1)),
   ScopeVersion String CODEC(ZSTD(1)),
   ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
   ScopeSchemaUrl String CODEC(ZSTD(1)),
   ServiceName LowCardinality(String) CODEC(ZSTD(1)),
   MetricName LowCardinality(String) CODEC(ZSTD(1)),
   MetricDescription String CODEC(ZSTD(1)),
   MetricUnit String CODEC(ZSTD(1)),
   Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   Count UInt64 CODEC(Delta(8), ZSTD(1)),
   Sum Float64 CODEC(ZSTD(1)),
   Scale Int32 CODEC(ZSTD(1)),
   ZeroCount UInt64 CODEC(ZSTD(1)),
   PositiveOffset Int32 CODEC(ZSTD(1)),
   PositiveBucketCounts Array(UInt64) CODEC(ZSTD(1)),
   NegativeOffset Int32 CODEC(ZSTD(1)),
   NegativeBucketCounts Array(UInt64) CODEC(ZSTD(1)),
   Min Float64 CODEC(ZSTD(1)),
   Max Float64 CODEC(ZSTD(1)),
   Flags UInt32 CODEC(ZSTD(1)),
   AggregationTemporality Int32 CODEC(ZSTD(1)),


   INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

const createSummaryTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_summary (
   ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ResourceSchemaUrl String CODEC(ZSTD(1)),
   ScopeName String CODEC(ZSTD(1)),
   ScopeVersion String CODEC(ZSTD(1)),
   ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
   ScopeSchemaUrl String CODEC(ZSTD(1)),
   ServiceName LowCardinality(String) CODEC(ZSTD(1)),
   MetricName LowCardinality(String) CODEC(ZSTD(1)),
   MetricDescription String CODEC(ZSTD(1)),
   MetricUnit String CODEC(ZSTD(1)),
   Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
   StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   TimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
   Count UInt64 CODEC(Delta(8), ZSTD(1)),
   Sum Float64 CODEC(ZSTD(1)),
   ValueAtQuantiles Nested(
       Quantile Float64,
       Value Float64
   ) CODEC(ZSTD(1)),
   Flags UInt32 CODEC(ZSTD(1)),


   INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_key mapKeys(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1,
   INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`
