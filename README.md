# OTLP Metric Storage (Go)

## Introduction
This take-home assignment is designed to give you an opportunity to demonstrate your skills and experience in
building a small backend application. We expect you to spend 3-4 hours on this assignment (using AI coding agents).
If you find yourself spending more time than that, please stop and submit what you have. We are not looking for a
complete solution, but rather a demonstration of your skills and experience.

To submit your solution, please create a public GitHub repository and send us the link. Please include a `README.md` file
with instructions on how to run your application.

## Overview
The goal of this assignment is to build a simple backend application that receives [metric datapoints](https://opentelemetry.io/docs/concepts/signals/metrics/)
on a gRPC endpoint and processes them, before storing in ClickHouse.
Current state is that we have a gRPC endpoint for receiving metrics, and Gauge and Sum type get correctly converted to
records and inserted into ClickHouse. This is tested with both unit- and integration-tests.

What we're looking for is to extract meta-data about the metrics into a separate table, which will then act as a 'lookup'
table, and that actual data-points just get stored as value + timestamp and with a reference to the lookup table.

Think about and keep in mind the following things:
- How to do the reference between tables?
- How to efficiently store the meta-data in ClickHouse?
- All data should be stored in such a way that full table scans are never needed, under the assumption data always gets queried for a specific time-frame
- Other than time-frame, there are no other mandatory filters for querying
- While you can assume cardinality of the metrics is 'low', e.g. Resources (Attributes) are likely to change over time 

Your solution should take into account high throughput, both in number of messages and the number of metrics / data-points per message.

Feel free to use the existing scaffoling in this folder. Of course, you can also change anything else as you see fit.

## Technology Constraints
- Your Go program should compile using standard Go SDK, and be compatible with Go 1.26.
- Use any additional libraries you want and need.

## Notes
- As this assignment is for the role of a Staff / Senior Product Engineer, we expect you to pay some attention to maintainability and operability of the solution. For example:
  - Consistent terminology usage
  - Validation of the behaviour
  - Include signals / events to help in debugging
- Assume that this application will be deployed to production. Build it accordingly.

## Solution

### Design

**Reference between tables.** Every metric time series (a metric name + its resource/scope/datapoint attributes,
aggregation temporality, etc. — everything except the raw sample itself) is fingerprinted into a deterministic
64-bit `MetricID` (`xxhash64` over the length-prefixed, sorted-key-order encoding of those fields — see
`metric_identity.go`). The ID is computed once in application code, at ingestion time, and used both as the primary
key of the new `otel_metric_metadata` lookup table and as a plain `UInt64` foreign-key-like column on the datapoint
tables (`otel_metrics_gauge`, `otel_metrics_sum`). Computing it in Go rather than in ClickHouse (e.g. via a
materialized column) keeps the hash cheap, keeps it out of the hot insert path's query planner, and lets the same
identity/dedup logic be reused for the in-memory `MetricIdentityCache` described below.

**Storing the metadata efficiently.** `otel_metric_metadata` uses `ReplacingMergeTree`, keyed by `MetricID`, so
that concurrent replicas (or a retried batch after a transient failure) can both insert the same series' metadata
without coordination — duplicate rows are asynchronously collapsed by ClickHouse during merges, keyed off a
`CreatedAt` version column. Attribute maps keep `bloom_filter` skip indexes on both keys and values (as in the
original schema) for the (secondary) query pattern of "find series with resource/scope/datapoint attribute X=Y".
Given the assignment's "low cardinality" assumption, this table is expected to stay small.

**Avoiding full table scans.** The datapoint tables are `PARTITION BY toDate(TimeUnix)` with
`ORDER BY (TimeUnix, MetricID)`. Since the only *mandatory* filter is a time range, leading the sorting key with
`TimeUnix` guarantees a bare "everything between T1 and T2" query is always partition- and index-pruned, never a
full scan. `MetricID` is included both later in the sorting key and as an explicit `minmax` skip index, so the
common case of "single series over a time range" still benefits from pruning — without making `MetricID` the
primary sort dimension, which would reintroduce full scans for the bare time-range case.

**High throughput.** Datapoint rows are now fixed-width (`MetricID`, two timestamps, `Value`, `Flags`) instead of
carrying two `Map` columns and half a dozen strings on every row, which is where most of the insert/network/storage
cost of the original wide-row design went. Metadata, which changes far less often than data points arrive, is only
inserted once per distinct series per process: `MetricIdentityCache` (`identity_cache.go`) is a bounded in-memory
set of `MetricID`s already known to be persisted, consulted before every `InsertMetadata` call. Because
`otel_metric_metadata` is a `ReplacingMergeTree`, a cache miss or a cold restart is always safe to "fix" by simply
re-inserting — this is treated purely as a throughput optimization, never as a correctness requirement, so the
cache resets (rather than blocking ingestion) on overflow, logging a warning and incrementing an OTel counter as an
operability signal in case the assumed-low cardinality assumption doesn't hold in production.

**Maintainability & operability** (see [Notes](#notes)):
- Consistent terminology: `MetricIdentity`/`MetricID` are used the same way in the Go code, the schema comments, and
  this document.
- Validation: `ExtractMetrics` (`metrics_mapper.go`) rejects data points missing a metric name or a timestamp;
  rejected points are excluded from the batch (not a hard failure) and reported back to the caller via the OTLP
  `ExportMetricsServiceResponse.PartialSuccess` field, per the OTLP spec.
- Debugging signals: structured `slog` logging on rejection and on store errors, plus new OTel counters
  (`...metrics.rejected_data_points`, `...metrics.metadata_inserted`, `...metrics.identity_cache_overflow`)
  alongside the pre-existing `...metrics.received` counter.
- Production readiness: `main()` (`server.go`) now actually connects the gRPC server to ClickHouse (previously it
  was wired up with a `nil` store and silently never persisted anything), retries the initial ClickHouse connection
  with backoff to tolerate ordering during a coordinated rollout, creates tables at startup, and shuts down
  gracefully (`SIGINT`/`SIGTERM` → `GracefulStop` → close the ClickHouse connection → flush OpenTelemetry).

### Scope

`otel_metrics_histogram`, `otel_metrics_exponential_histogram` and `otel_metrics_summary` are left on the original
wide-row schema. None of them ever had a mapper or store method wired up in the pre-existing scaffolding, so they
are dead schema rather than part of the working ingestion path; migrating them would be scope creep with no
observable behavior change. The same `MetricID` pattern applies mechanically once a mapper for these types exists.

## Usage

Build the application:
```shell
go build ./...
```

Run the application (requires a reachable ClickHouse instance; see flags below):
```shell
go run . \
  -listenAddr=localhost:4317 \
  -clickhouseAddr=localhost:9000 \
  -clickhouseDatabase=default \
  -clickhouseUsername=default \
  -clickhousePassword=""
```

Tables are created automatically at startup if they don't already exist.

Run unit tests:
```shell
go test ./...
```

Run integration tests (requires Docker; spins up a real ClickHouse container via testcontainers-go):
```shell
go test -tags integration ./...
```

## References

- [OpenTelemetry Metrics](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)

