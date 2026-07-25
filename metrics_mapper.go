package main

import (
	"fmt"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// ExtractResult is the outcome of turning a batch of OTLP ResourceMetrics
// into rows ready for ClickHouse insertion.
//
// Metadata is already deduplicated by MetricID *within this batch*: a batch
// touching the same series many times (e.g. one gauge scraped at several
// timestamps) produces exactly one metadata row for it here. Cross-batch
// dedup (skipping metadata we already persisted in a previous Export call)
// is the caller's job via MetricIdentityCache -- ExtractMetrics has no memory
// of previous requests.
//
// Rejected/RejectionReasons record data points that failed validation so the
// caller can report them via ExportMetricsServiceResponse.PartialSuccess (per
// the OTLP spec, malformed data points should not fail the entire batch) and
// so operators have a concrete signal for *why* points were dropped.
type ExtractResult struct {
	Metadata         []MetricMetadataRow
	GaugeRows        []NumberDataPointRow
	SumRows          []NumberDataPointRow
	Rejected         int
	RejectionReasons map[string]int
}

// ExtractMetrics walks a batch of OTLP ResourceMetrics, validates every
// number data point, and produces the metadata + datapoint rows describing
// it.
func ExtractMetrics(resourceMetrics []*metricspb.ResourceMetrics) ExtractResult {
	result := ExtractResult{
		RejectionReasons: make(map[string]int),
	}
	metadataByID := make(map[uint64]MetricMetadataRow)

	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())
		resSchemaUrl := rm.GetSchemaUrl()

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				if gauge := metric.GetGauge(); gauge != nil {
					for _, dp := range gauge.GetDataPoints() {
						if reason, ok := validateNumberDataPoint(metric, dp); !ok {
							result.Rejected++
							result.RejectionReasons[reason]++
							continue
						}
						identity := MetricIdentity{
							MetricType:            "Gauge",
							ServiceName:           svcName,
							ResourceAttributes:    resAttrs,
							ResourceSchemaUrl:     resSchemaUrl,
							ScopeName:             scope.GetName(),
							ScopeVersion:          scope.GetVersion(),
							ScopeAttributes:       scopeAttrs,
							ScopeDroppedAttrCount: scope.GetDroppedAttributesCount(),
							ScopeSchemaUrl:        sm.GetSchemaUrl(),
							MetricName:            metric.GetName(),
							MetricDescription:     metric.GetDescription(),
							MetricUnit:            metric.GetUnit(),
							Attributes:            kvToMap(dp.GetAttributes()),
						}
						id := identity.ID()
						registerMetadata(metadataByID, id, identity, dp.GetTimeUnixNano())
						result.GaugeRows = append(result.GaugeRows, NumberDataPointRow{
							MetricID:      id,
							StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
							TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
							Value:         numberDataPointValue(dp),
							Flags:         dp.GetFlags(),
						})
					}
				}

				if sum := metric.GetSum(); sum != nil {
					for _, dp := range sum.GetDataPoints() {
						if reason, ok := validateNumberDataPoint(metric, dp); !ok {
							result.Rejected++
							result.RejectionReasons[reason]++
							continue
						}
						identity := MetricIdentity{
							MetricType:             "Sum",
							ServiceName:            svcName,
							ResourceAttributes:     resAttrs,
							ResourceSchemaUrl:      resSchemaUrl,
							ScopeName:              scope.GetName(),
							ScopeVersion:           scope.GetVersion(),
							ScopeAttributes:        scopeAttrs,
							ScopeDroppedAttrCount:  scope.GetDroppedAttributesCount(),
							ScopeSchemaUrl:         sm.GetSchemaUrl(),
							MetricName:             metric.GetName(),
							MetricDescription:      metric.GetDescription(),
							MetricUnit:             metric.GetUnit(),
							Attributes:             kvToMap(dp.GetAttributes()),
							AggregationTemporality: int32(sum.GetAggregationTemporality()),
							IsMonotonic:            sum.GetIsMonotonic(),
						}
						id := identity.ID()
						registerMetadata(metadataByID, id, identity, dp.GetTimeUnixNano())
						result.SumRows = append(result.SumRows, NumberDataPointRow{
							MetricID:      id,
							StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
							TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
							Value:         numberDataPointValue(dp),
							Flags:         dp.GetFlags(),
						})
					}
				}
			}
		}
	}

	if len(metadataByID) > 0 {
		result.Metadata = make([]MetricMetadataRow, 0, len(metadataByID))
		for _, row := range metadataByID {
			result.Metadata = append(result.Metadata, row)
		}
	}

	return result
}

// registerMetadata records the metadata row for id the first time it is seen
// within this batch. FirstSeenTimeUnix is set from whichever data point in
// the batch happens to be processed first for that series -- it is
// informational (useful for "when did this series first appear" debugging),
// not a query-critical field, so no effort is made to find the minimum
// timestamp across the batch.
func registerMetadata(metadataByID map[uint64]MetricMetadataRow, id uint64, identity MetricIdentity, timeUnixNano uint64) {
	if _, ok := metadataByID[id]; ok {
		return
	}
	metadataByID[id] = MetricMetadataRow{
		MetricID:               id,
		MetricType:             identity.MetricType,
		ServiceName:            identity.ServiceName,
		MetricName:             identity.MetricName,
		MetricDescription:      identity.MetricDescription,
		MetricUnit:             identity.MetricUnit,
		ResourceAttributes:     identity.ResourceAttributes,
		ResourceSchemaUrl:      identity.ResourceSchemaUrl,
		ScopeName:              identity.ScopeName,
		ScopeVersion:           identity.ScopeVersion,
		ScopeAttributes:        identity.ScopeAttributes,
		ScopeDroppedAttrCount:  identity.ScopeDroppedAttrCount,
		ScopeSchemaUrl:         identity.ScopeSchemaUrl,
		Attributes:             identity.Attributes,
		AggregationTemporality: identity.AggregationTemporality,
		IsMonotonic:            identity.IsMonotonic,
		FirstSeenTimeUnix:      nanosToTime(timeUnixNano),
	}
}

// validateNumberDataPoint rejects data points that cannot be meaningfully
// stored or looked up later. Per the OTLP spec, invalid data points should be
// dropped and reported via PartialSuccess rather than failing the whole
// batch.
func validateNumberDataPoint(metric *metricspb.Metric, dp *metricspb.NumberDataPoint) (reason string, ok bool) {
	if metric.GetName() == "" {
		return "empty metric name", false
	}
	if dp.GetTimeUnixNano() == 0 {
		return "missing TimeUnixNano", false
	}
	return "", true
}

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}
