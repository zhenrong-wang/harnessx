package adapter

import (
	"context"
	"fmt"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
)

type Request struct {
	Model              string
	Messages           []*harnessxv1.Message
	Tools              []*harnessxv1.ToolDefinition
	ToolChoice         string
	MaxOutputTokens    int32
	ResponseSchemaJSON []byte
}

type Response struct {
	Message         *harnessxv1.Message
	FinishReason    string
	Usage           *harnessxv1.Usage
	ProviderRawJSON []byte
}

type StreamEvent struct {
	Type          string
	TextDelta     string
	ToolCallDelta *harnessxv1.ToolCall
	Completed     *Response
}

type Adapter interface {
	Generate(context.Context, Request) (*Response, error)
	Stream(context.Context, Request, func(StreamEvent) error) error
}

type Error struct {
	Reason    string
	Message   string
	Retryable bool
	Status    int
}

func (e *Error) Error() string {
	if e == nil {
		return "model adapter error"
	}
	if e.Status > 0 {
		return fmt.Sprintf("%s (status %d)", e.Message, e.Status)
	}
	return e.Message
}
