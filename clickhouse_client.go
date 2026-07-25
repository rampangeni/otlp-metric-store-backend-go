package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertMetadata(ctx context.Context, rows []MetricMetadataRow) error
	InsertGauge(ctx context.Context, rows []NumberDataPointRow) error
	InsertSum(ctx context.Context, rows []NumberDataPointRow) error
	Close() error
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn driver.Conn
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{conn: conn}, nil
}

// CreateTables executes DDL for the metadata lookup table and all 5 metric
// tables. The metadata table is created first since, while ClickHouse itself
// doesn't enforce foreign keys, it establishes the correct mental model
// (and matters for anyone reading the DDL top-to-bottom) that datapoint rows
// reference it, not the other way around.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	ddls := []string{
		createMetricMetadataTableSQL,
		createGaugeTableSQL,
		createSumTableSQL,
		createHistogramTableSQL,
		createExponentialHistogramTableSQL,
		createSummaryTableSQL,
	}
	for _, ddl := range ddls {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// InsertMetadata batch-inserts metadata rows into otel_metric_metadata. Rows
// are expected to already be deduplicated by MetricID by the caller (see
// ExtractMetrics and MetricIdentityCache) -- ClickHouse's ReplacingMergeTree
// engine on this table would eventually collapse duplicates on its own, but
// there's no reason to pay the network/insert cost for rows we already know
// are redundant.
func (s *ClickHouseMetricsStore) InsertMetadata(ctx context.Context, rows []MetricMetadataRow) error {
	if len(rows) == 0 {
		return nil
	}
	// Explicit column list, omitting CreatedAt: PrepareBatch with a bare
	// "INSERT INTO table" expects a value for every column including ones
	// with a DEFAULT expression, so leaving CreatedAt out of the column list
	// is what lets ClickHouse apply DEFAULT now() instead of erroring on an
	// arg-count mismatch.
	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO otel_metric_metadata (
       MetricID, MetricType, ServiceName, MetricName, MetricDescription, MetricUnit,
       ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes,
       ScopeDroppedAttrCount, ScopeSchemaUrl, Attributes, AggregationTemporality,
       IsMonotonic, FirstSeenTimeUnix
   )`)
	if err != nil {
		return fmt.Errorf("preparing metadata batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricID,
			r.MetricType,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			r.ResourceAttributes,
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			r.ScopeAttributes,
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			r.Attributes,
			r.AggregationTemporality,
			r.IsMonotonic,
			r.FirstSeenTimeUnix,
		); err != nil {
			return fmt.Errorf("appending metadata row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts gauge data points into otel_metrics_gauge.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []NumberDataPointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge")
	if err != nil {
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	return batch.Send()
}

// InsertSum batch-inserts sum data points into otel_metrics_sum.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []NumberDataPointRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum")
	if err != nil {
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.MetricID,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	return batch.Send()
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
