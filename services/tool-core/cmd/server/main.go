package main

import (
	"context"
	"os"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
	"github.com/zhenrong-wang/harnessx/internal/platform/grpcx"
	"github.com/zhenrong-wang/harnessx/internal/platform/logging"
	"github.com/zhenrong-wang/harnessx/internal/platform/signalx"
	toolconfig "github.com/zhenrong-wang/harnessx/services/tool-core/internal/config"
	toolserver "github.com/zhenrong-wang/harnessx/services/tool-core/internal/server"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("tool-core", platformconfig.String("LOG_LEVEL", "info"))
	cfg, err := toolconfig.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	service, err := toolserver.New(cfg, nil)
	if err != nil {
		logger.Error("tool registry failed", "error", err)
		os.Exit(1)
	}
	grpcServer, _ := grpcx.NewServer()
	harnessxv1.RegisterToolServiceServer(grpcServer, service)
	if err := grpcx.Serve(ctx, logger, cfg.GRPCAddress, grpcServer); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
