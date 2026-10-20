package main

import (
	"context"
	"os"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
	"github.com/zhenrong-wang/harnessx/internal/platform/grpcx"
	"github.com/zhenrong-wang/harnessx/internal/platform/logging"
	"github.com/zhenrong-wang/harnessx/internal/platform/postgres"
	"github.com/zhenrong-wang/harnessx/internal/platform/signalx"
	eventserver "github.com/zhenrong-wang/harnessx/services/event-core/internal/server"
	"github.com/zhenrong-wang/harnessx/services/event-core/internal/store"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("event-core", platformconfig.String("LOG_LEVEL", "info"))
	databaseURL, err := platformconfig.Required("EVENT_CORE_DATABASE_URL")
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
	grpcServer, _ := grpcx.NewServer()
	harnessxv1.RegisterEventServiceServer(grpcServer, eventserver.New(store.NewPostgres(pool)))
	if err := grpcx.Serve(ctx, logger, platformconfig.String("EVENT_CORE_GRPC_ADDR", ":9104"), grpcServer); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
