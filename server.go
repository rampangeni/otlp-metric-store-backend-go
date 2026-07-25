package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")

	clickhouseAddr           = flag.String("clickhouseAddr", "localhost:9000", "The ClickHouse native protocol address")
	clickhouseDatabase       = flag.String("clickhouseDatabase", "default", "The ClickHouse database to use")
	clickhouseUsername       = flag.String("clickhouseUsername", "default", "The ClickHouse username")
	clickhousePassword       = flag.String("clickhousePassword", "", "The ClickHouse password")
	clickhouseConnectRetries = flag.Int("clickhouseConnectRetries", 10, "Number of times to retry connecting to ClickHouse at startup before giving up")

	metricIdentityCacheSize = flag.Int("metricIdentityCacheSize", 100_000, "Max number of distinct metric series to remember locally, to avoid re-inserting unchanged metadata on every batch")
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	meter                  = otel.Meter(name)
	logger                 = otelslog.NewLogger(name)
	metricsReceivedCounter metric.Int64Counter
)

func init() {
	var err error
	metricsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)
	logger.Info("Starting application")

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := connectClickHouseWithRetry(ctx)
	if err != nil {
		return fmt.Errorf("connecting to clickhouse: %w", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing clickhouse connection: %w", closeErr))
		}
	}()

	slog.Debug("Creating ClickHouse tables if they do not exist")
	if err := store.CreateTables(ctx); err != nil {
		return fmt.Errorf("creating clickhouse tables: %w", err)
	}

	identityCache := NewMetricIdentityCache(*metricIdentityCacheSize)

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(*listenAddr, store, identityCache))

	serveErr := make(chan error, 1)
	go func() {
		slog.Debug("Starting gRPC server")
		serveErr <- grpcServer.Serve(listener)
	}()

	select {
	case err = <-serveErr:
		return err
	case <-ctx.Done():
		slog.Info("Shutdown signal received, stopping gRPC server gracefully")
		grpcServer.GracefulStop()
		return nil
	}
}

// connectClickHouseWithRetry retries the initial ClickHouse connection with a
// fixed backoff. ClickHouse and this service are commonly deployed together
// (e.g. rolled out by the same Helm chart), so on a cold start it's expected
// that this service may come up before ClickHouse is ready to accept
// connections; without a retry loop that would be a permanent crash loop
// instead of a few seconds of expected startup delay.
func connectClickHouseWithRetry(ctx context.Context) (*ClickHouseMetricsStore, error) {
	var lastErr error
	for attempt := 1; attempt <= *clickhouseConnectRetries; attempt++ {
		store, err := NewClickHouseMetricsStore(ctx, *clickhouseAddr, *clickhouseDatabase, *clickhouseUsername, *clickhousePassword)
		if err == nil {
			return store, nil
		}
		lastErr = err
		slog.WarnContext(ctx, "failed to connect to clickhouse, retrying",
			slog.Int("attempt", attempt),
			slog.Int("maxAttempts", *clickhouseConnectRetries),
			slog.Any("error", err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, lastErr
}
