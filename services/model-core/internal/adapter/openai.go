package adapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
)

type OpenAICompatible struct {
	baseURL          string
	apiKey           string
	headers          map[string]string
	client           *http.Client
	maxResponseBytes int64
}

func NewOpenAICompatible(baseURL, apiKey string, headers map[string]string, client *http.Client, maxResponseBytes int64) (*OpenAICompatible, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid OpenAI-compatible base URL %q", baseURL)
	}
	if client == nil {
		return nil, errors.New("HTTP client is required")
	}
	if maxResponseBytes <= 0 {
		maxResponseBytes = 16 << 20
	}
	return &OpenAICompatible{baseURL: parsed.String(), apiKey: apiKey, headers: headers, client: client, maxResponseBytes: maxResponseBytes}, nil
}

func (a *OpenAICompatible) Generate(ctx context.Context, req Request) (*Response, error) {
	body, err := a.requestBody(req, false)
	if err != nil {
		return nil, err
	}
	httpResp, err := a.do(ctx, body)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, a.maxResponseBytes+1))
	if err != nil {
		return nil, &Error{Reason: "provider_read_failed", Message: err.Error(), Retryable: true}
	}
	if int64(len(raw)) > a.maxResponseBytes {
		return nil, &Error{Reason: "provider_response_too_large", Message: "provider response exceeded configured limit"}
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, providerHTTPError(httpResp.StatusCode, raw)
	}
	return decodeOpenAIResponse(raw)
}

func (a *OpenAICompatible) Stream(ctx context.Context, req Request, emit func(StreamEvent) error) error {
	body, err := a.requestBody(req, true)
	if err != nil {
		return err
	}
	httpResp, err := a.do(ctx, body)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(httpResp.Body, a.maxResponseBytes))
		return providerHTTPError(httpResp.StatusCode, raw)
	}

	acc := streamAccumulator{toolCalls: map[int]*openAIToolCall{}}
	scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, a.maxResponseBytes+1))
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			break
		}
		if payload == "" {
			continue
		}
		if err := acc.consume([]byte(payload), emit); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return &Error{Reason: "provider_stream_failed", Message: err.Error(), Retryable: true}
	}
	return emit(StreamEvent{Type: "completed", Completed: acc.response()})
}

func (a *OpenAICompatible) do(ctx context.Context, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+a.apiKey)
	}
	for key, value := range a.headers {
		req.Header.Set(key, value)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, &Error{Reason: "provider_unavailable", Message: err.Error(), Retryable: true}
	}
	return resp, nil
}

func (a *OpenAICompatible) requestBody(req Request, stream bool) ([]byte, error) {
	messages, err := encodeMessages(req.Messages)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"model": req.Model, "messages": messages, "stream": stream}
	if stream {
		body["stream_options"] = map[string]any{"include_usage": true}
	}
	if req.MaxOutputTokens > 0 {
		body["max_tokens"] = req.MaxOutputTokens
	}
	if len(req.Tools) > 0 {
		tools, err := encodeTools(req.Tools)
		if err != nil {
			return nil, err
		}
		body["tools"] = tools
		if req.ToolChoice != "" {
			body["tool_choice"] = req.ToolChoice
		}
	}
	if len(bytes.TrimSpace(req.ResponseSchemaJSON)) > 0 {
		var schema any
		if err := json.Unmarshal(req.ResponseSchemaJSON, &schema); err != nil {
			return nil, fmt.Errorf("response schema: %w", err)
		}
		body["response_format"] = map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "harnessx_response", "strict": true, "schema": schema}}
	}
	return json.Marshal(body)
}

func encodeMessages(input []*harnessxv1.Message) ([]map[string]any, error) {
	messages := make([]map[string]any, 0, len(input))
	for _, message := range input {
		if message == nil {
			continue
		}
		item := map[string]any{"role": message.GetRole()}
		content, err := encodeContent(message)
		if err != nil {
			return nil, err
		}
		if content != nil {
			item["content"] = content
		}
		if message.GetName() != "" {
			item["name"] = message.GetName()
		}
		if message.GetToolCallId() != "" {
			item["tool_call_id"] = message.GetToolCallId()
		}
		if len(message.GetToolCalls()) > 0 {
			item["tool_calls"] = encodeToolCalls(message.GetToolCalls())
		}
		messages = append(messages, item)
	}
	return messages, nil
}

func encodeContent(message *harnessxv1.Message) (any, error) {
	if len(message.GetContent()) == 0 {
		return nil, nil
	}
	textOnly := true
	var textParts []string
	parts := make([]map[string]any, 0, len(message.GetContent()))
	for _, part := range message.GetContent() {
		switch part.GetType() {
		case "text", "output_text", "input_text":
			textParts = append(textParts, part.GetText())
			parts = append(parts, map[string]any{"type": "text", "text": part.GetText()})
		case "image", "image_url", "input_image":
			textOnly = false
			uri := strings.TrimSpace(part.GetUri())
			if uri == "" && len(part.GetData()) > 0 && strings.HasPrefix(part.GetMimeType(), "image/") {
				uri = "data:" + part.GetMimeType() + ";base64," + base64.StdEncoding.EncodeToString(part.GetData())
			}
			if uri == "" {
				return nil, fmt.Errorf("image content requires uri or image data with a MIME type")
			}
			parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": uri}})
		default:
			return nil, fmt.Errorf("content type %q is not supported by the OpenAI-compatible adapter", part.GetType())
		}
	}
	if textOnly {
		return strings.Join(textParts, "\n"), nil
	}
	return parts, nil
}

func encodeTools(tools []*harnessxv1.ToolDefinition) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		if tool == nil {
			continue
		}
		var parameters any = map[string]any{"type": "object"}
		if len(tool.GetInputSchemaJson()) > 0 {
			if err := json.Unmarshal(tool.GetInputSchemaJson(), &parameters); err != nil {
				return nil, fmt.Errorf("tool %q input schema: %w", tool.GetName(), err)
			}
		}
		out = append(out, map[string]any{"type": "function", "function": map[string]any{
			"name": tool.GetName(), "description": tool.GetDescription(), "parameters": parameters,
		}})
	}
	return out, nil
}

func encodeToolCalls(calls []*harnessxv1.ToolCall) []map[string]any {
	out := make([]map[string]any, 0, len(calls))
	for _, call := range calls {
		if call == nil {
			continue
		}
		out = append(out, map[string]any{"id": call.GetId(), "type": "function", "function": map[string]any{
			"name": call.GetName(), "arguments": string(call.GetArgumentsJson()),
		}})
	}
	return out
}

type openAIResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message      openAIMessage `json:"message"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage openAIUsage `json:"usage"`
}

type openAIMessage struct {
	Role      string           `json:"role"`
	Content   string           `json:"content"`
	ToolCalls []openAIToolCall `json:"tool_calls"`
}

type openAIToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openAIUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

func decodeOpenAIResponse(raw []byte) (*Response, error) {
	var decoded openAIResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, &Error{Reason: "provider_response_invalid", Message: err.Error()}
	}
	if len(decoded.Choices) == 0 {
		return nil, &Error{Reason: "provider_response_invalid", Message: "provider returned no choices"}
	}
	choice := decoded.Choices[0]
	return &Response{
		Message:         decodeMessage(choice.Message),
		FinishReason:    choice.FinishReason,
		Usage:           decodeUsage(decoded.Usage),
		ProviderRawJSON: append([]byte(nil), raw...),
	}, nil
}

func decodeMessage(message openAIMessage) *harnessxv1.Message {
	role := message.Role
	if role == "" {
		role = "assistant"
	}
	out := &harnessxv1.Message{Role: role}
	if message.Content != "" {
		out.Content = []*harnessxv1.ContentPart{{Type: "text", Text: message.Content, MimeType: "text/plain"}}
	}
	for _, call := range message.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, &harnessxv1.ToolCall{Id: call.ID, Name: call.Function.Name, ArgumentsJson: []byte(call.Function.Arguments)})
	}
	return out
}

func decodeUsage(usage openAIUsage) *harnessxv1.Usage {
	return &harnessxv1.Usage{InputTokens: usage.PromptTokens, OutputTokens: usage.CompletionTokens, TotalTokens: usage.TotalTokens}
}

func providerHTTPError(statusCode int, raw []byte) error {
	message := strings.TrimSpace(string(raw))
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &envelope) == nil && strings.TrimSpace(envelope.Error.Message) != "" {
		message = envelope.Error.Message
	}
	if len(message) > 1000 {
		message = message[:1000]
	}
	return &Error{Reason: "provider_http_error", Message: message, Status: statusCode, Retryable: statusCode == 408 || statusCode == 429 || statusCode >= 500}
}

type streamAccumulator struct {
	content      strings.Builder
	toolCalls    map[int]*openAIToolCall
	finishReason string
	usage        openAIUsage
}

func (a *streamAccumulator) consume(raw []byte, emit func(StreamEvent) error) error {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content   string           `json:"content"`
				ToolCalls []openAIToolCall `json:"tool_calls"`
			} `json:"delta"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage openAIUsage `json:"usage"`
	}
	if err := json.Unmarshal(raw, &chunk); err != nil {
		return &Error{Reason: "provider_stream_invalid", Message: err.Error()}
	}
	a.usage = mergeUsage(a.usage, chunk.Usage)
	for _, choice := range chunk.Choices {
		if choice.Delta.Content != "" {
			a.content.WriteString(choice.Delta.Content)
			if err := emit(StreamEvent{Type: "text_delta", TextDelta: choice.Delta.Content}); err != nil {
				return err
			}
		}
		if choice.FinishReason != "" {
			a.finishReason = choice.FinishReason
		}
		for _, delta := range choice.Delta.ToolCalls {
			current := a.toolCalls[delta.Index]
			if current == nil {
				current = &openAIToolCall{Index: delta.Index}
				a.toolCalls[delta.Index] = current
			}
			if delta.ID != "" {
				current.ID = delta.ID
			}
			if delta.Function.Name != "" {
				current.Function.Name += delta.Function.Name
			}
			if delta.Function.Arguments != "" {
				current.Function.Arguments += delta.Function.Arguments
			}
			if err := emit(StreamEvent{Type: "tool_call_delta", ToolCallDelta: &harnessxv1.ToolCall{Id: delta.ID, Name: delta.Function.Name, ArgumentsJson: []byte(delta.Function.Arguments)}}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *streamAccumulator) response() *Response {
	message := &harnessxv1.Message{Role: "assistant"}
	if a.content.Len() > 0 {
		message.Content = []*harnessxv1.ContentPart{{Type: "text", Text: a.content.String(), MimeType: "text/plain"}}
	}
	for index := 0; index < len(a.toolCalls); index++ {
		if call := a.toolCalls[index]; call != nil {
			message.ToolCalls = append(message.ToolCalls, &harnessxv1.ToolCall{Id: call.ID, Name: call.Function.Name, ArgumentsJson: []byte(call.Function.Arguments)})
		}
	}
	return &Response{Message: message, FinishReason: a.finishReason, Usage: decodeUsage(a.usage)}
}

func mergeUsage(current, next openAIUsage) openAIUsage {
	if next.PromptTokens != 0 {
		current.PromptTokens = next.PromptTokens
	}
	if next.CompletionTokens != 0 {
		current.CompletionTokens = next.CompletionTokens
	}
	if next.TotalTokens != 0 {
		current.TotalTokens = next.TotalTokens
	}
	return current
}
