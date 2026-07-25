package main

import "time"

// MetricMetadataRow is one row of the otel_metric_metadata lookup table: it
// captures everything that identifies a metric time series exactly once.
// MetricID is the deterministic fingerprint (see MetricIdentity.ID()) that
// every datapoint row references instead of repeating this data inline.
type MetricMetadataRow struct {
	MetricID               uint64
	MetricType             string
	ServiceName            string
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	ScopeSchemaUrl         string
	Attributes             map[string]string
	AggregationTemporality int32
	IsMonotonic            bool
	FirstSeenTimeUnix      time.Time
}

// NumberDataPointRow is one row of otel_metrics_gauge or otel_metrics_sum: a
// bare sample (value + timestamps + flags) plus the MetricID reference back
// to its metadata row. Gauge and Sum data points share this exact shape once
// AggregationTemporality/IsMonotonic move to the metadata table, since those
// two fields describe the metric, not any individual sample.
type NumberDataPointRow struct {
	MetricID      uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}
