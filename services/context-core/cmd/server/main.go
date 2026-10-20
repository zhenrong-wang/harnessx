package main

import (
	"context"
	"os"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/internal/platform/config"
	"github.com/zhenrong-wang/harnessx/internal/platform/grpcx"
	"github.com/zhenrong-wang/harnessx/internal/platform/logging"
	"github.com/zhenrong-wang/harnessx/internal/platform/signalx"
	contextserver "github.com/zhenrong-wang/harnessx/services/context-core/internal/server"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("context-core", config.String("LOG_LEVEL", "info"))
	grpcServer, _ := grpcx.NewServer()
	harnessxv1.RegisterContextServiceServer(grpcServer, contextserver.New())
	if err := grpcx.Serve(ctx, logger, config.String("CONTEXT_CORE_GRPC_ADDR", ":9102"), grpcServer); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
