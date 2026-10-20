package main

import (
	"context"
	"net/http"
	"os"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	platformconfig "github.com/zhenrong-wang/harnessx/internal/platform/config"
	"github.com/zhenrong-wang/harnessx/internal/platform/grpcx"
	"github.com/zhenrong-wang/harnessx/internal/platform/logging"
	"github.com/zhenrong-wang/harnessx/internal/platform/signalx"
	gatewayconfig "github.com/zhenrong-wang/harnessx/services/api-gateway/internal/config"
	gatewayserver "github.com/zhenrong-wang/harnessx/services/api-gateway/internal/server"
)

func main() {
	ctx, stop := signalx.Context(context.Background())
	defer stop()
	logger := logging.New("api-gateway", platformconfig.String("LOG_LEVEL", "info"))
	cfg, err := gatewayconfig.Load()
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	conn, err := grpcx.Dial(ctx, cfg.HarnessTarget)
	if err != nil {
		logger.Error("harness-core connection failed", "error", err)
		os.Exit(1)
	}
	defer conn.Close()
	httpServer := &http.Server{
		Addr: cfg.HTTPAddress, Handler: gatewayserver.New(cfg, harnessxv1.NewHarnessServiceClient(conn)),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout, IdleTimeout: cfg.IdleTimeout,
	}
	done := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "address", cfg.HTTPAddress)
		done <- httpServer.ListenAndServe()
	}()
	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server stopped", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP shutdown failed", "error", err)
		}
	}
}
