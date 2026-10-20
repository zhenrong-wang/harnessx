package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	gatewayconfig "github.com/zhenrong-wang/harnessx/services/api-gateway/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeHarness struct{ tenant string }

func (*fakeHarness) CreateRun(context.Context, *harnessxv1.CreateRunRequest, ...grpc.CallOption) (*harnessxv1.Run, error) {
	return &harnessxv1.Run{}, nil
}
func (*fakeHarness) StreamRun(context.Context, *harnessxv1.CreateRunRequest, ...grpc.CallOption) (harnessxv1.HarnessService_StreamRunClient, error) {
	return nil, nil
}
func (f *fakeHarness) GetRun(_ context.Context, req *harnessxv1.GetRunRequest, _ ...grpc.CallOption) (*harnessxv1.Run, error) {
	f.tenant = req.GetContext().GetTenantId()
	return &harnessxv1.Run{Id: req.GetRunId(), TenantId: f.tenant}, nil
}
func (*fakeHarness) CancelRun(context.Context, *harnessxv1.CancelRunRequest, ...grpc.CallOption) (*harnessxv1.CancelRunResponse, error) {
	return &harnessxv1.CancelRunResponse{}, nil
}
func (*fakeHarness) ListRunEvents(_ context.Context, req *harnessxv1.ListRunEventsRequest, _ ...grpc.CallOption) (*harnessxv1.ListRunEventsResponse, error) {
	return &harnessxv1.ListRunEventsResponse{Events: []*harnessxv1.Event{{
		Id: "event-1", TenantId: req.GetContext().GetTenantId(), RunId: req.GetRunId(), Sequence: 1,
		Type: "run.created", PayloadJson: []byte(`{"status":"queued"}`), OccurredAt: timestamppb.Now(),
	}}}, nil
}

func TestBearerKeyBindsTenantAndIgnoresSpoofedHeader(t *testing.T) {
	client := &fakeHarness{}
	handler := New(gatewayconfig.Config{APIKeys: map[string]string{"secret": "tenant-a"}}, client)
	request := httptest.NewRequest(http.MethodGet, "/v1/runs/run-1", nil)
	request.Header.Set("Authorization", "Bearer secret")
	request.Header.Set("X-HarnessX-Tenant-ID", "tenant-b")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || client.tenant != "tenant-a" {
		t.Fatalf("status=%d tenant=%q body=%s", response.Code, client.tenant, response.Body.String())
	}
}

func TestMissingKeyIsRejected(t *testing.T) {
	handler := New(gatewayconfig.Config{APIKeys: map[string]string{"secret": "tenant-a"}}, &fakeHarness{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/runs/run-1", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestEventPayloadIsPublicJSONNotBase64(t *testing.T) {
	handler := New(gatewayconfig.Config{AllowInsecure: true}, &fakeHarness{})
	request := httptest.NewRequest(http.MethodGet, "/v1/runs/run-1/events", nil)
	request.Header.Set("X-HarnessX-Tenant-ID", "local")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Events []struct {
			Payload map[string]any `json:"payload"`
		} `json:"events"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Events[0].Payload["status"] != "queued" {
		t.Fatalf("body=%s", response.Body.String())
	}
}
