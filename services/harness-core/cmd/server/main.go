package main

import (
	"context"
	"os"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
	"github.com/zhenrong-wang/harnessx/internal/platform/grpcx"
	"github.com/zhenrong-wang/harnessx/internal/platform/logging"
	"github.com/zhenrong-wang/harnessx/internal/platform/postgres"
	"github.com/zhenrong-wang/harnessx/internal/platform/signalx"
	harnessserver "github.com/zhenrong-wang/harnessx/services/harness-core/internal/server"
	"github.com/zhenrong-wang/harnessx/services/harness-core/internal/store"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("harness-core", platformconfig.String("LOG_LEVEL", "info"))
	databaseURL, err := platformconfig.Required("HARNESS_CORE_DATABASE_URL")
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	pool, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	contextConn := mustDial(ctx, logger, platformconfig.String("CONTEXT_CORE_GRPC_TARGET", "context-core:9102"))
	defer contextConn.Close()
	modelConn := mustDial(ctx, logger, platformconfig.String("MODEL_CORE_GRPC_TARGET", "model-core:9101"))
	defer modelConn.Close()
	toolConn := mustDial(ctx, logger, platformconfig.String("TOOL_CORE_GRPC_TARGET", "tool-core:9103"))
	defer toolConn.Close()
	eventConn := mustDial(ctx, logger, platformconfig.String("EVENT_CORE_GRPC_TARGET", "event-core:9104"))
	defer eventConn.Close()

	runStore := store.NewPostgres(pool)
	eventClient := harnessxv1.NewEventServiceClient(eventConn)
	engine := harnessserver.NewEngine(
		runStore,
		harnessxv1.NewContextServiceClient(contextConn),
		harnessxv1.NewModelServiceClient(modelConn),
		harnessxv1.NewToolServiceClient(toolConn),
		eventClient,
		logger,
	)
	go engine.RunOutbox(ctx)
	grpcServer, _ := grpcx.NewServer()
	runTimeout, err := platformconfig.Duration("HARNESS_CORE_RUN_TIMEOUT", 30*time.Minute)
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	harnessxv1.RegisterHarnessServiceServer(grpcServer, harnessserver.New(engine, runStore, eventClient, runTimeout))
	if err := grpcx.Serve(ctx, logger, platformconfig.String("HARNESS_CORE_GRPC_ADDR", ":9100"), grpcServer); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func mustDial(ctx context.Context, logger interface{ Error(string, ...any) }, target string) *grpc.ClientConn {
	conn, err := grpcx.Dial(ctx, target)
	if err != nil {
		logger.Error("dependency connection failed", "target", target, "error", err)
		os.Exit(1)
	}
	return conn
}
