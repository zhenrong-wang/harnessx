package server

import (
	"context"
	"testing"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildContextOrdersSystemHistoryInputAndObservations(t *testing.T) {
	resp, err := New().BuildContext(context.Background(), &harnessxv1.BuildContextRequest{
		Context:      &harnessxv1.RequestContext{TenantId: "tenant-a"},
		Instructions: "be concise",
		History:      []*harnessxv1.Message{{Role: "assistant", Content: []*harnessxv1.ContentPart{{Type: "text", Text: "prior"}}}},
		Input:        "hello",
		Observations: []*harnessxv1.ToolResult{{CallId: "call-1", Name: "lookup", Content: []byte("result"), MimeType: "text/plain"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(resp.GetMessages()); got != 4 {
		t.Fatalf("messages = %d, want 4", got)
	}
	roles := []string{"system", "assistant", "user", "tool"}
	for i, role := range roles {
		if resp.GetMessages()[i].GetRole() != role {
			t.Fatalf("message %d role = %q, want %q", i, resp.GetMessages()[i].GetRole(), role)
		}
	}
}

func TestBuildContextRejectsOverBudgetContext(t *testing.T) {
	_, err := New().BuildContext(context.Background(), &harnessxv1.BuildContextRequest{
		Context:              &harnessxv1.RequestContext{TenantId: "tenant-a"},
		Input:                "this input cannot fit",
		MaxContextTokens:     2,
		ReservedOutputTokens: 1,
	})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %s, want %s", status.Code(err), codes.ResourceExhausted)
	}
}
