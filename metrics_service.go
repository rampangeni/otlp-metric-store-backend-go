package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	rejectedDataPointsCounter metric.Int64Counter
	metadataInsertedCounter   metric.Int64Counter
)

func init() {
	var err error
	rejectedDataPointsCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.rejected_data_points",
		metric.WithDescription("The number of data points rejected due to failing validation"),
		metric.WithUnit("{datapoint}"))
	if err != nil {
		panic(err)
	}
	metadataInsertedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.metadata_inserted",
		metric.WithDescription("The number of distinct metric metadata rows inserted into the lookup table"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
}

type dash0MetricsServiceServer struct {
	addr          string
	store         MetricsStore
	identityCache *MetricIdentityCache

	colmetricspb.UnimplementedMetricsServiceServer
}

// newServer builds the gRPC metrics service. identityCache may be nil, in
// which case metadata dedup falls back to per-batch-only (every batch will
// attempt to (re-)insert metadata for every series it contains); this is
// always correct, only less efficient, since otel_metric_metadata is a
// ReplacingMergeTree keyed by MetricID. Passing nil is primarily useful for
// tests that don't care about cross-request dedup behavior.
func newServer(addr string, store MetricsStore, identityCache *MetricIdentityCache) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store, identityCache: identityCache}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	extracted := ExtractMetrics(request.GetResourceMetrics())

	if extracted.Rejected > 0 {
		rejectedDataPointsCounter.Add(ctx, int64(extracted.Rejected))
		slog.WarnContext(ctx, "rejected invalid data points",
			slog.Int("rejected", extracted.Rejected),
			slog.Any("reasons", extracted.RejectionReasons))
	}

	if m.store != nil {
		metadataToInsert := extracted.Metadata
		if m.identityCache != nil {
			metadataToInsert = metadataToInsert[:0]
			for _, row := range extracted.Metadata {
				if alreadySeen := m.identityCache.MarkSeen(ctx, row.MetricID); !alreadySeen {
					metadataToInsert = append(metadataToInsert, row)
				}
			}
		}

		if len(metadataToInsert) > 0 {
			if err := m.store.InsertMetadata(ctx, metadataToInsert); err != nil {
				return nil, storeError(ctx, "inserting metric metadata", err)
			}
			metadataInsertedCounter.Add(ctx, int64(len(metadataToInsert)))
		}
		if len(extracted.GaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, extracted.GaugeRows); err != nil {
				return nil, storeError(ctx, "inserting gauge data points", err)
			}
		}
		if len(extracted.SumRows) > 0 {
			if err := m.store.InsertSum(ctx, extracted.SumRows); err != nil {
				return nil, storeError(ctx, "inserting sum data points", err)
			}
		}
	}

	response := &colmetricspb.ExportMetricsServiceResponse{}
	if extracted.Rejected > 0 {
		response.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: int64(extracted.Rejected),
			ErrorMessage:       fmt.Sprintf("rejected %d data point(s) that failed validation: %v", extracted.Rejected, extracted.RejectionReasons),
		}
	}
	return response, nil
}

// storeError logs a ClickHouse-facing failure with enough context to debug it
// and translates it into a gRPC status the client can act on (retry, alert,
// etc.) instead of leaking a raw driver error.
func storeError(ctx context.Context, action string, err error) error {
	slog.ErrorContext(ctx, "clickhouse operation failed", slog.String("action", action), slog.Any("error", err))
	return status.Errorf(codes.Internal, "%s: %v", action, err)
}
