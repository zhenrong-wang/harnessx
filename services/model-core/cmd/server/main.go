package main

import (
	"context"
	"log/slog"
	"os"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/internal/platform/config"
	"github.com/zhenrong-wang/harnessx/internal/platform/grpcx"
	"github.com/zhenrong-wang/harnessx/internal/platform/logging"
	"github.com/zhenrong-wang/harnessx/internal/platform/signalx"
	modelconfig "github.com/zhenrong-wang/harnessx/services/model-core/internal/config"
	modelserver "github.com/zhenrong-wang/harnessx/services/model-core/internal/server"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("model-core", config.String("LOG_LEVEL", "info"))
	cfg, err := modelconfig.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	service, err := modelserver.New(cfg)
	if err != nil {
		logger.Error("model registry failed", "error", err)
		os.Exit(1)
	}
	grpcServer, _ := grpcx.NewServer()
	harnessxv1.RegisterModelServiceServer(grpcServer, service)
	if err := grpcx.Serve(ctx, logger, cfg.GRPCAddress, grpcServer); err != nil {
		logger.Error("server stopped", slog.Any("error", err))
		os.Exit(1)
	}
}
