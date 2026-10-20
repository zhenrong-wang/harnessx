package adapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
)

func TestOpenAICompatibleGenerate(t *testing.T) {
	var captured map[string]any
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("authorization = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"upstream-name","choices":[{"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`))
	}))
	defer provider.Close()

	target, err := NewOpenAICompatible(provider.URL, "secret", nil, &http.Client{Timeout: time.Second}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	response, err := target.Generate(context.Background(), Request{
		Model:           "example-model",
		Messages:        []*harnessxv1.Message{{Role: "user", Content: []*harnessxv1.ContentPart{{Type: "text", Text: "hello"}}}},
		Tools:           []*harnessxv1.ToolDefinition{{Name: "weather", InputSchemaJson: []byte(`{"type":"object"}`)}},
		MaxOutputTokens: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	if captured["model"] != "example-model" || captured["stream"] != false {
		t.Fatalf("unexpected request: %#v", captured)
	}
	if response.Message.GetContent()[0].GetText() != "done" || response.Usage.GetTotalTokens() != 7 {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestOpenAICompatibleStreamAssemblesToolCall(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"content":"checking "}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"wea","arguments":"{\"city\":"}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"ther","arguments":"\"Paris\"}"}}]},"finish_reason":"tool_calls"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}`,
			`data: [DONE]`,
			"",
		}, "\n\n")))
	}))
	defer provider.Close()

	target, err := NewOpenAICompatible(provider.URL, "", nil, provider.Client(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var completed *Response
	err = target.Stream(context.Background(), Request{Model: "m", Messages: []*harnessxv1.Message{{Role: "user"}}}, func(event StreamEvent) error {
		if event.Completed != nil {
			completed = event.Completed
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed == nil || completed.Message.GetToolCalls()[0].GetName() != "weather" {
		t.Fatalf("unexpected completed event: %#v", completed)
	}
	if got := string(completed.Message.GetToolCalls()[0].GetArgumentsJson()); got != `{"city":"Paris"}` {
		t.Fatalf("arguments = %s", got)
	}
	if completed.Usage.GetTotalTokens() != 12 {
		t.Fatalf("usage = %#v", completed.Usage)
	}
}

func TestProviderHTTPErrorRetryability(t *testing.T) {
	for _, test := range []struct {
		status    int
		retryable bool
	}{{http.StatusBadRequest, false}, {http.StatusTooManyRequests, true}, {http.StatusServiceUnavailable, true}} {
		err, ok := providerHTTPError(test.status, []byte(`{"error":{"message":"failed"}}`)).(*Error)
		if !ok || err.Retryable != test.retryable || err.Message != "failed" {
			t.Fatalf("status %d: %#v", test.status, err)
		}
	}
}
