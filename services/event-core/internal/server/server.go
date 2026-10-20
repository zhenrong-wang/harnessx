package server

import (
	"context"
	"strings"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/services/event-core/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	harnessxv1.UnimplementedEventServiceServer
	store store.Store
}

func New(eventStore store.Store) *Server { return &Server{store: eventStore} }

func (s *Server) AppendEvent(ctx context.Context, req *harnessxv1.AppendEventRequest) (*harnessxv1.AppendEventResponse, error) {
	if err := validate(req.GetContext(), req.GetEvent().GetRunId()); err != nil {
		return nil, err
	}
	event := req.GetEvent()
	if event == nil || strings.TrimSpace(event.GetId()) == "" || strings.TrimSpace(event.GetType()) == "" {
		return nil, status.Error(codes.InvalidArgument, "event id and type are required")
	}
	if event.GetTenantId() != req.GetContext().GetTenantId() {
		return nil, status.Error(codes.PermissionDenied, "event tenant does not match request tenant")
	}
	stored, err := s.store.Append(ctx, event)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append event: %v", err)
	}
	return &harnessxv1.AppendEventResponse{Event: stored}, nil
}

func (s *Server) ListEvents(ctx context.Context, req *harnessxv1.ListEventsRequest) (*harnessxv1.ListEventsResponse, error) {
	if err := validate(req.GetContext(), req.GetRunId()); err != nil {
		return nil, err
	}
	limit := req.GetLimit()
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		return nil, status.Error(codes.InvalidArgument, "limit cannot exceed 1000")
	}
	events, hasMore, err := s.store.List(ctx, req.GetContext().GetTenantId(), req.GetRunId(), req.GetAfterSequence(), limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list events: %v", err)
	}
	return &harnessxv1.ListEventsResponse{Events: events, HasMore: hasMore}, nil
}

func (s *Server) SubscribeEvents(req *harnessxv1.SubscribeEventsRequest, stream harnessxv1.EventService_SubscribeEventsServer) error {
	if err := validate(req.GetContext(), req.GetRunId()); err != nil {
		return err
	}
	after := req.GetAfterSequence()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, _, err := s.store.List(stream.Context(), req.GetContext().GetTenantId(), req.GetRunId(), after, 500)
		if err != nil {
			return status.Errorf(codes.Internal, "subscribe events: %v", err)
		}
		for _, event := range events {
			if err := stream.Send(event); err != nil {
				return err
			}
			after = event.GetSequence()
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

func validate(ctx *harnessxv1.RequestContext, runID string) error {
	if ctx == nil || strings.TrimSpace(ctx.GetTenantId()) == "" {
		return status.Error(codes.InvalidArgument, "request context with tenant_id is required")
	}
	if strings.TrimSpace(runID) == "" {
		return status.Error(codes.InvalidArgument, "run_id is required")
	}
	return nil
}
