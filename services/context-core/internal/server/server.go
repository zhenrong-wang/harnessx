package server

import (
	"context"
	"encoding/json"
	"strings"
	"unicode/utf8"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	harnessxv1.UnimplementedContextServiceServer
}

func New() *Server { return &Server{} }

func (s *Server) BuildContext(_ context.Context, req *harnessxv1.BuildContextRequest) (*harnessxv1.BuildContextResponse, error) {
	if req == nil || req.GetContext() == nil {
		return nil, status.Error(codes.InvalidArgument, "request context is required")
	}
	if strings.TrimSpace(req.GetInput()) == "" && len(req.GetHistory()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "input or history is required")
	}

	messages := make([]*harnessxv1.Message, 0, len(req.GetHistory())+len(req.GetObservations())+2)
	if instructions := strings.TrimSpace(req.GetInstructions()); instructions != "" {
		messages = append(messages, textMessage("system", instructions))
	}
	messages = append(messages, cloneMessages(req.GetHistory())...)
	if input := strings.TrimSpace(req.GetInput()); input != "" {
		messages = append(messages, textMessage("user", input))
	}
	for _, observation := range req.GetObservations() {
		if observation == nil {
			continue
		}
		messages = append(messages, &harnessxv1.Message{
			Role:       "tool",
			Name:       observation.GetName(),
			ToolCallId: observation.GetCallId(),
			Content: []*harnessxv1.ContentPart{{
				Type:     "text",
				Text:     string(observation.GetContent()),
				MimeType: observation.GetMimeType(),
			}},
		})
	}

	estimated := estimateTokens(messages)
	available := req.GetMaxContextTokens() - req.GetReservedOutputTokens()
	if available > 0 && estimated > available {
		return nil, status.Errorf(codes.ResourceExhausted,
			"context requires approximately %d tokens but only %d are available; no context reducer is configured",
			estimated, available,
		)
	}
	audit, _ := json.Marshal(map[string]any{
		"estimator":        "utf8_runes_div_4",
		"estimated_tokens": estimated,
		"message_count":    len(messages),
		"compacted":        false,
	})
	return &harnessxv1.BuildContextResponse{
		Messages:        messages,
		EstimatedTokens: estimated,
		Compacted:       false,
		AuditJson:       audit,
	}, nil
}

func textMessage(role, text string) *harnessxv1.Message {
	return &harnessxv1.Message{Role: role, Content: []*harnessxv1.ContentPart{{Type: "text", Text: text, MimeType: "text/plain"}}}
}

func cloneMessages(messages []*harnessxv1.Message) []*harnessxv1.Message {
	out := make([]*harnessxv1.Message, 0, len(messages))
	for _, message := range messages {
		if message != nil {
			out = append(out, message)
		}
	}
	return out
}

func estimateTokens(messages []*harnessxv1.Message) int64 {
	var runes int
	for _, message := range messages {
		for _, part := range message.GetContent() {
			runes += utf8.RuneCountInString(part.GetText())
		}
	}
	return int64((runes + 3) / 4)
}
