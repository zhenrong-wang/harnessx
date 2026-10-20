package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	toolconfig "github.com/zhenrong-wang/harnessx/services/tool-core/internal/config"
)

func TestExecuteToolsUsesRegisteredEndpointAndPreservesOrder(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-HarnessX-Run-ID") != "run-1" || r.Header.Get("X-API-Key") != "fixed" {
			t.Errorf("missing execution headers: %#v", r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer endpoint.Close()

	service, err := New(toolconfig.Config{
		DefaultTimeout:   time.Second,
		MaxResponseBytes: 1 << 20,
		Tools: []toolconfig.Tool{{
			Name: "first", Description: "first tool", Endpoint: endpoint.URL,
			InputSchema: []byte(`{"type":"object"}`), Headers: map[string]string{"X-API-Key": "fixed"},
		}},
	}, endpoint.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteTools(context.Background(), &harnessxv1.ExecuteToolsRequest{
		Context: &harnessxv1.RequestContext{TenantId: "tenant-1"}, RunId: "run-1", StepId: "step-1",
		AllowedTools: []*harnessxv1.ToolDefinition{{Name: "first"}},
		Calls: []*harnessxv1.ToolCall{
			{Id: "call-1", Name: "first", ArgumentsJson: []byte(`{"x":1}`)},
			{Id: "call-2", Name: "not-allowed", ArgumentsJson: []byte(`{}`)},
		},
		MaxParallelism: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetResults()[0].GetStatus() != "completed" || string(response.GetResults()[0].GetContent()) != `{"ok":true}` {
		t.Fatalf("first result = %#v", response.GetResults()[0])
	}
	if response.GetResults()[1].GetError().GetReason() != "tool_not_allowed" {
		t.Fatalf("second result = %#v", response.GetResults()[1])
	}
}

func TestExecuteToolsRejectsInvalidArgumentsBeforeHTTP(t *testing.T) {
	calls := 0
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer endpoint.Close()
	service, err := New(toolconfig.Config{
		DefaultTimeout: time.Second, MaxResponseBytes: 1024,
		Tools: []toolconfig.Tool{{Name: "tool", Endpoint: endpoint.URL, InputSchema: []byte(`{"type":"object"}`)}},
	}, endpoint.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteTools(context.Background(), &harnessxv1.ExecuteToolsRequest{
		Context: &harnessxv1.RequestContext{TenantId: "tenant"}, RunId: "run", StepId: "step",
		AllowedTools: []*harnessxv1.ToolDefinition{{Name: "tool"}},
		Calls:        []*harnessxv1.ToolCall{{Id: "call", Name: "tool", ArgumentsJson: []byte(`{`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || response.GetResults()[0].GetError().GetReason() != "invalid_arguments" {
		t.Fatalf("calls=%d result=%#v", calls, response.GetResults()[0])
	}
}
