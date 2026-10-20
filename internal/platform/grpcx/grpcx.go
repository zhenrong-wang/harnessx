package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func NewServer() (*grpc.Server, *health.Server) {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(16<<20),
		grpc.MaxSendMsgSize(16<<20),
	)
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	return server, healthServer
}

func Serve(ctx context.Context, logger *slog.Logger, address string, server *grpc.Server) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("gRPC server listening", "address", listener.Addr().String())
		errCh <- server.Serve(listener)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
		return nil
	}
}

func Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	if target == "" {
		return nil, fmt.Errorf("gRPC target is required")
	}
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20), grpc.MaxCallSendMsgSize(16<<20)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	return conn, nil
}
