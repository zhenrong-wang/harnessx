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
	volumeconfig "github.com/zhenrong-wang/harnessx/services/volume-core/internal/config"
	"github.com/zhenrong-wang/harnessx/services/volume-core/internal/driver"
	volumeserver "github.com/zhenrong-wang/harnessx/services/volume-core/internal/server"
	"github.com/zhenrong-wang/harnessx/services/volume-core/internal/store"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("volume-core", platformconfig.String("LOG_LEVEL", "info"))
	cfg, err := volumeconfig.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	pool, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	local, err := driver.NewLocal(cfg.LocalRoot)
	if err != nil {
		logger.Error("volume backend failed", "error", err)
		os.Exit(1)
	}
	grpcServer, _ := grpcx.NewServer()
	harnessxv1.RegisterVolumeServiceServer(grpcServer, volumeserver.New(store.NewPostgres(pool), local))
	if err := grpcx.Serve(ctx, logger, cfg.GRPCAddress, grpcServer); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
