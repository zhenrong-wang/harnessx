package server

import (
	"context"
	"log/slog"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/services/harness-core/internal/store"
	"google.golang.org/grpc"
)

type eventAppender interface {
	AppendEvent(context.Context, *harnessxv1.AppendEventRequest, ...grpc.CallOption) (*harnessxv1.AppendEventResponse, error)
}

type publisher struct {
	store  store.Store
	client eventAppender
	logger *slog.Logger
}

func (p *publisher) flush(ctx context.Context, event *harnessxv1.Event) *harnessxv1.Event {
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := p.client.AppendEvent(callCtx, &harnessxv1.AppendEventRequest{
		Context: &harnessxv1.RequestContext{TenantId: event.GetTenantId()},
		Event:   event,
	})
	if err != nil {
		markCtx, markCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer markCancel()
		_ = p.store.MarkEventFailed(markCtx, event.GetId(), err)
		p.logger.Warn("event delivery deferred", "event_id", event.GetId(), "run_id", event.GetRunId(), "error", err)
		return event
	}
	markCtx, markCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer markCancel()
	if err := p.store.MarkEventDelivered(markCtx, event.GetId()); err != nil {
		p.logger.Warn("event delivery acknowledgement failed", "event_id", event.GetId(), "error", err)
	}
	return response.GetEvent()
}

func (p *publisher) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events, err := p.store.PendingEvents(ctx, 100)
			if err != nil {
				p.logger.Warn("event outbox query failed", "error", err)
				continue
			}
			for _, pending := range events {
				p.flush(ctx, pending.Event)
			}
		}
	}
}
