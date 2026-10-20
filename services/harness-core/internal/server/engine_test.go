package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/services/harness-core/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type memoryStore struct {
	run       *harnessxv1.Run
	events    []*harnessxv1.Event
	steps     []string
	delivered map[string]bool
}

func (s *memoryStore) CreateRun(_ context.Context, run *harnessxv1.Run, _, _ string, event *harnessxv1.Event) (*harnessxv1.Run, bool, error) {
	s.run = run
	s.events = append(s.events, event)
	return run, true, nil
}
func (s *memoryStore) UpdateRun(_ context.Context, run *harnessxv1.Run, event *harnessxv1.Event) error {
	s.run = run
	s.events = append(s.events, event)
	return nil
}
func (s *memoryStore) RecordStep(_ context.Context, _, _, _ string, _ int, kind, state string, _, _, _ []byte, _ bool) error {
	s.steps = append(s.steps, kind+":"+state)
	return nil
}
func (s *memoryStore) EnqueueEvent(_ context.Context, event *harnessxv1.Event) error {
	s.events = append(s.events, event)
	return nil
}
func (s *memoryStore) GetRun(context.Context, string, string) (*harnessxv1.Run, error) {
	return s.run, nil
}
func (s *memoryStore) PendingEvents(context.Context, int) ([]store.OutboxEvent, error) {
	return nil, nil
}
func (s *memoryStore) MarkEventDelivered(_ context.Context, id string) error {
	if s.delivered == nil {
		s.delivered = map[string]bool{}
	}
	s.delivered[id] = true
	return nil
}
func (s *memoryStore) MarkEventFailed(context.Context, string, error) error { return nil }

type fakeContexts struct{}

func (fakeContexts) BuildContext(_ context.Context, req *harnessxv1.BuildContextRequest, _ ...grpc.CallOption) (*harnessxv1.BuildContextResponse, error) {
	messages := append([]*harnessxv1.Message(nil), req.GetHistory()...)
	if req.GetInput() != "" {
		messages = append(messages, &harnessxv1.Message{Role: "user", Content: []*harnessxv1.ContentPart{{Type: "text", Text: req.GetInput()}}})
	}
	for _, result := range req.GetObservations() {
		messages = append(messages, &harnessxv1.Message{Role: "tool", ToolCallId: result.GetCallId(), Content: []*harnessxv1.ContentPart{{Type: "text", Text: string(result.GetContent())}}})
	}
	return &harnessxv1.BuildContextResponse{Messages: messages}, nil
}

type fakeModels struct {
	responses []*harnessxv1.GenerateResponse
}

func (m *fakeModels) Generate(context.Context, *harnessxv1.GenerateRequest, ...grpc.CallOption) (*harnessxv1.GenerateResponse, error) {
	if len(m.responses) == 0 {
		return nil, errors.New("no response")
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	return response, nil
}
func (*fakeModels) GetCapabilities(context.Context, *harnessxv1.GetModelCapabilitiesRequest, ...grpc.CallOption) (*harnessxv1.ModelCapabilities, error) {
	return &harnessxv1.ModelCapabilities{MaxContextTokens: 128000}, nil
}

type fakeTools struct{ calls int }

func (t *fakeTools) ExecuteTools(_ context.Context, req *harnessxv1.ExecuteToolsRequest, _ ...grpc.CallOption) (*harnessxv1.ExecuteToolsResponse, error) {
	t.calls++
	return &harnessxv1.ExecuteToolsResponse{Results: []*harnessxv1.ToolResult{{CallId: req.GetCalls()[0].GetId(), Name: req.GetCalls()[0].GetName(), Status: "completed", Content: []byte(`{"temperature":20}`), MimeType: "application/json"}}}, nil
}

type fakeEvents struct{ sequence int64 }

func (e *fakeEvents) AppendEvent(_ context.Context, req *harnessxv1.AppendEventRequest, _ ...grpc.CallOption) (*harnessxv1.AppendEventResponse, error) {
	e.sequence++
	event := proto.Clone(req.GetEvent()).(*harnessxv1.Event)
	event.Sequence = e.sequence
	return &harnessxv1.AppendEventResponse{Event: event}, nil
}

func TestEngineCompletesToolLoopDurably(t *testing.T) {
	runStore := &memoryStore{}
	models := &fakeModels{responses: []*harnessxv1.GenerateResponse{
		{Model: "provider/model", Message: &harnessxv1.Message{Role: "assistant", ToolCalls: []*harnessxv1.ToolCall{{Id: "call-1", Name: "weather", ArgumentsJson: []byte(`{"city":"Paris"}`)}}}, Usage: &harnessxv1.Usage{TotalTokens: 10}},
		{Model: "provider/model", Message: &harnessxv1.Message{Role: "assistant", Content: []*harnessxv1.ContentPart{{Type: "text", Text: "20 C"}}}, Usage: &harnessxv1.Usage{TotalTokens: 5}},
	}}
	tools := &fakeTools{}
	engine := NewEngine(runStore, fakeContexts{}, models, tools, &fakeEvents{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var emitted []string
	run, err := engine.Execute(context.Background(), &harnessxv1.CreateRunRequest{
		Context: &harnessxv1.RequestContext{TenantId: "tenant"}, Input: "weather?", Models: []string{"provider/model"},
		Tools: []*harnessxv1.ToolDefinition{{Name: "weather", InputSchemaJson: []byte(`{"type":"object"}`)}},
	}, func(event *harnessxv1.Event) error { emitted = append(emitted, event.GetType()); return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if run.GetStatus() != harnessxv1.RunStatus_RUN_STATUS_COMPLETED || run.GetUsage().GetTotalTokens() != 15 || tools.calls != 1 {
		t.Fatalf("run=%#v tool calls=%d", run, tools.calls)
	}
	if len(runStore.steps) != 6 { // model start/end, tool start/end, model start/end
		t.Fatalf("steps=%v", runStore.steps)
	}
	if emitted[len(emitted)-1] != "run.completed" {
		t.Fatalf("events=%v", emitted)
	}
}
