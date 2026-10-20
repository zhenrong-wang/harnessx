package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/services/harness-core/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type eventReader interface {
	ListEvents(context.Context, *harnessxv1.ListEventsRequest, ...grpc.CallOption) (*harnessxv1.ListEventsResponse, error)
}

type Server struct {
	harnessxv1.UnimplementedHarnessServiceServer
	engine     *Engine
	store      store.Store
	events     eventReader
	mu         sync.Mutex
	active     map[string]context.CancelFunc
	runTimeout time.Duration
}

func New(engine *Engine, runStore store.Store, events eventReader, runTimeout time.Duration) *Server {
	if runTimeout <= 0 {
		runTimeout = 30 * time.Minute
	}
	return &Server{engine: engine, store: runStore, events: events, active: make(map[string]context.CancelFunc), runTimeout: runTimeout}
}

func (s *Server) CreateRun(ctx context.Context, req *harnessxv1.CreateRunRequest) (*harnessxv1.Run, error) {
	if _, err := resolvePolicy(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Runs are server-owned once accepted. A disconnected gateway/client must
	// not cancel durable work; only the run deadline or CancelRun may do that.
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.runTimeout)
	var runID string
	run, err := s.engine.Execute(runCtx, req, nil, func(id string) {
		runID = id
		s.mu.Lock()
		s.active[activeRunKey(req.GetContext().GetTenantId(), id)] = cancel
		s.mu.Unlock()
	})
	cancel()
	if runID != "" {
		s.mu.Lock()
		delete(s.active, activeRunKey(req.GetContext().GetTenantId(), runID))
		s.mu.Unlock()
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "execute run: %v", err)
	}
	return run, nil
}

func (s *Server) StreamRun(req *harnessxv1.CreateRunRequest, stream harnessxv1.HarnessService_StreamRunServer) error {
	if _, err := resolvePolicy(req); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	runCtx, cancel := context.WithTimeout(context.WithoutCancel(stream.Context()), s.runTimeout)
	defer cancel()
	var runID string
	_, err := s.engine.Execute(runCtx, req, stream.Send, func(id string) {
		runID = id
		s.mu.Lock()
		s.active[activeRunKey(req.GetContext().GetTenantId(), id)] = cancel
		s.mu.Unlock()
	})
	if runID != "" {
		s.mu.Lock()
		delete(s.active, activeRunKey(req.GetContext().GetTenantId(), runID))
		s.mu.Unlock()
	}
	if err != nil {
		return status.Errorf(codes.Internal, "execute run: %v", err)
	}
	return nil
}

func (s *Server) GetRun(ctx context.Context, req *harnessxv1.GetRunRequest) (*harnessxv1.Run, error) {
	if err := validateRunRequest(req.GetContext(), req.GetRunId()); err != nil {
		return nil, err
	}
	run, err := s.store.GetRun(ctx, req.GetContext().GetTenantId(), req.GetRunId())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "run not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get run: %v", err)
	}
	return run, nil
}

func (s *Server) CancelRun(ctx context.Context, req *harnessxv1.CancelRunRequest) (*harnessxv1.CancelRunResponse, error) {
	if err := validateRunRequest(req.GetContext(), req.GetRunId()); err != nil {
		return nil, err
	}
	key := activeRunKey(req.GetContext().GetTenantId(), req.GetRunId())
	s.mu.Lock()
	cancel, active := s.active[key]
	s.mu.Unlock()
	if active {
		cancel()
	}
	run, err := s.store.GetRun(ctx, req.GetContext().GetTenantId(), req.GetRunId())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "run not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get run: %v", err)
	}
	return &harnessxv1.CancelRunResponse{Run: run, Interrupted: active}, nil
}

func (s *Server) ListRunEvents(ctx context.Context, req *harnessxv1.ListRunEventsRequest) (*harnessxv1.ListRunEventsResponse, error) {
	if err := validateRunRequest(req.GetContext(), req.GetRunId()); err != nil {
		return nil, err
	}
	response, err := s.events.ListEvents(ctx, &harnessxv1.ListEventsRequest{
		Context: req.GetContext(), RunId: req.GetRunId(), AfterSequence: req.GetAfterSequence(), Limit: req.GetLimit(),
	})
	if err != nil {
		return nil, err
	}
	return &harnessxv1.ListRunEventsResponse{Events: response.GetEvents(), HasMore: response.GetHasMore()}, nil
}

func validateRunRequest(ctx *harnessxv1.RequestContext, runID string) error {
	if ctx == nil || strings.TrimSpace(ctx.GetTenantId()) == "" {
		return status.Error(codes.InvalidArgument, "request context with tenant_id is required")
	}
	if strings.TrimSpace(runID) == "" {
		return status.Error(codes.InvalidArgument, "run_id is required")
	}
	return nil
}

func activeRunKey(tenantID, runID string) string { return tenantID + "\x00" + runID }
