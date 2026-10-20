package server

import (
	"context"
	"errors"
	"testing"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/services/model-core/internal/adapter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeAdapter struct {
	response *adapter.Response
	err      error
	calls    int
}

func (f *fakeAdapter) Generate(context.Context, adapter.Request) (*adapter.Response, error) {
	f.calls++
	return f.response, f.err
}

func (f *fakeAdapter) Stream(context.Context, adapter.Request, func(adapter.StreamEvent) error) error {
	return errors.New("not implemented")
}

func TestGenerateFallsBackOnlyForRetryableErrors(t *testing.T) {
	first := &fakeAdapter{err: &adapter.Error{Reason: "busy", Message: "busy", Retryable: true}}
	second := &fakeAdapter{response: &adapter.Response{Message: &harnessxv1.Message{Role: "assistant"}, Usage: &harnessxv1.Usage{TotalTokens: 3}}}
	service := newWithRoutes(map[string]route{
		"a/model-a": testRoute("a", "model-a", first),
		"b/model-b": testRoute("b", "model-b", second),
	})
	response, err := service.Generate(context.Background(), request("a/model-a", "b/model-b"))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetModel() != "b/model-b" || len(response.GetAttempts()) != 2 || first.calls != 1 || second.calls != 1 {
		t.Fatalf("unexpected fallback result: %#v", response)
	}
}

func TestGenerateDoesNotFallbackForPermanentErrors(t *testing.T) {
	first := &fakeAdapter{err: &adapter.Error{Reason: "invalid", Message: "invalid", Retryable: false, Status: 400}}
	second := &fakeAdapter{response: &adapter.Response{Message: &harnessxv1.Message{Role: "assistant"}}}
	service := newWithRoutes(map[string]route{
		"a/model-a": testRoute("a", "model-a", first),
		"b/model-b": testRoute("b", "model-b", second),
	})
	_, err := service.Generate(context.Background(), request("a/model-a", "b/model-b"))
	if status.Code(err) != codes.InvalidArgument || second.calls != 0 {
		t.Fatalf("error = %v, second calls = %d", err, second.calls)
	}
}

func TestBareModelMustBeUnambiguous(t *testing.T) {
	service := newWithRoutes(map[string]route{
		"a/shared": testRoute("a", "shared", &fakeAdapter{}),
		"b/shared": testRoute("b", "shared", &fakeAdapter{}),
	})
	_, err := service.GetCapabilities(context.Background(), &harnessxv1.GetModelCapabilitiesRequest{Model: "shared"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error = %v", err)
	}
}

func request(models ...string) *harnessxv1.GenerateRequest {
	return &harnessxv1.GenerateRequest{
		Context:  &harnessxv1.RequestContext{TenantId: "tenant"},
		Models:   models,
		Messages: []*harnessxv1.Message{{Role: "user"}},
	}
}

func testRoute(provider, model string, target adapter.Adapter) route {
	return route{
		provider:      provider,
		providerModel: model,
		adapter:       target,
		descriptor: &harnessxv1.ModelDescriptor{
			Id:           provider + "/" + model,
			Capabilities: &harnessxv1.ModelCapabilities{Streaming: true},
		},
	}
}
