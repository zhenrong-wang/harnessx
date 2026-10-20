package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	toolconfig "github.com/zhenrong-wang/harnessx/services/tool-core/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type registeredTool struct {
	definition       *harnessxv1.ToolDefinition
	endpoint         string
	headers          map[string]string
	timeout          time.Duration
	maxResponseBytes int64
}

type Server struct {
	harnessxv1.UnimplementedToolServiceServer
	client *http.Client
	tools  map[string]registeredTool
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

func New(cfg toolconfig.Config, client *http.Client) (*Server, error) {
	if client == nil {
		client = &http.Client{}
	}
	tools := make(map[string]registeredTool, len(cfg.Tools))
	for _, configured := range cfg.Tools {
		timeout := cfg.DefaultTimeout
		if configured.Timeout != "" {
			parsed, err := time.ParseDuration(configured.Timeout)
			if err != nil {
				return nil, fmt.Errorf("tool %q: %w", configured.Name, err)
			}
			timeout = parsed
		}
		limit := configured.MaxResponseBytes
		if limit <= 0 {
			limit = cfg.MaxResponseBytes
		}
		headers := make(map[string]string, len(configured.Headers)+len(configured.HeadersFromEnv))
		for name, value := range configured.Headers {
			headers[name] = value
		}
		for name, envName := range configured.HeadersFromEnv {
			value, present := os.LookupEnv(envName)
			if !present {
				return nil, fmt.Errorf("tool %q: environment variable %s is required for header %s", configured.Name, envName, name)
			}
			headers[name] = value
		}
		tools[configured.Name] = registeredTool{
			definition: &harnessxv1.ToolDefinition{
				Name:             configured.Name,
				Description:      configured.Description,
				InputSchemaJson:  configured.InputSchema,
				OutputSchemaJson: configured.OutputSchema,
			},
			endpoint:         configured.Endpoint,
			headers:          headers,
			timeout:          timeout,
			maxResponseBytes: limit,
		}
	}
	return &Server{client: client, tools: tools, active: make(map[string]context.CancelFunc)}, nil
}

func (s *Server) ListTools(_ context.Context, req *harnessxv1.ListToolsRequest) (*harnessxv1.ListToolsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	wanted := make(map[string]struct{}, len(req.GetNames()))
	for _, name := range req.GetNames() {
		wanted[name] = struct{}{}
	}
	tools := make([]*harnessxv1.ToolDefinition, 0, len(s.tools))
	for name, tool := range s.tools {
		if len(wanted) > 0 {
			if _, ok := wanted[name]; !ok {
				continue
			}
		}
		tools = append(tools, tool.definition)
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].GetName() < tools[j].GetName() })
	return &harnessxv1.ListToolsResponse{Tools: tools}, nil
}

func (s *Server) ExecuteTools(ctx context.Context, req *harnessxv1.ExecuteToolsRequest) (*harnessxv1.ExecuteToolsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetRunId()) == "" || strings.TrimSpace(req.GetStepId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id and step_id are required")
	}
	if len(req.GetCalls()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one tool call is required")
	}
	allowed := make(map[string]struct{}, len(req.GetAllowedTools()))
	for _, tool := range req.GetAllowedTools() {
		allowed[tool.GetName()] = struct{}{}
	}
	parallelism := int(req.GetMaxParallelism())
	if parallelism <= 0 {
		parallelism = 4
	}
	if parallelism > 32 {
		parallelism = 32
	}
	results := make([]*harnessxv1.ToolResult, len(req.GetCalls()))
	semaphore := make(chan struct{}, parallelism)
	var group sync.WaitGroup
	for index, call := range req.GetCalls() {
		index, call := index, call
		group.Add(1)
		go func() {
			defer group.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				results[index] = failedResult(call, "cancelled", ctx.Err().Error(), false)
				return
			}
			results[index] = s.execute(ctx, req, call, allowed)
		}()
	}
	group.Wait()
	return &harnessxv1.ExecuteToolsResponse{Results: results}, nil
}

func (s *Server) CancelToolCalls(_ context.Context, req *harnessxv1.CancelToolCallsRequest) (*harnessxv1.CancelToolCallsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	cancelled := make([]string, 0, len(req.GetCallIds()))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, callID := range req.GetCallIds() {
		if cancel, ok := s.active[activeKey(req.GetContext().GetTenantId(), req.GetRunId(), callID)]; ok {
			cancel()
			cancelled = append(cancelled, callID)
		}
	}
	return &harnessxv1.CancelToolCallsResponse{CancelledCallIds: cancelled}, nil
}

func (s *Server) execute(parent context.Context, req *harnessxv1.ExecuteToolsRequest, call *harnessxv1.ToolCall, allowed map[string]struct{}) *harnessxv1.ToolResult {
	if call == nil || strings.TrimSpace(call.GetId()) == "" || strings.TrimSpace(call.GetName()) == "" {
		return failedResult(call, "invalid_tool_call", "tool call id and name are required", false)
	}
	if _, ok := allowed[call.GetName()]; !ok {
		return failedResult(call, "tool_not_allowed", "tool was not allowed by the run policy", false)
	}
	tool, ok := s.tools[call.GetName()]
	if !ok {
		return failedResult(call, "tool_not_registered", "tool is not registered", false)
	}
	if !json.Valid(call.GetArgumentsJson()) {
		return failedResult(call, "invalid_arguments", "tool arguments must be valid JSON", false)
	}
	ctx, cancel := context.WithTimeout(parent, tool.timeout)
	key := activeKey(req.GetContext().GetTenantId(), req.GetRunId(), call.GetId())
	s.mu.Lock()
	s.active[key] = cancel
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.active, key)
		s.mu.Unlock()
	}()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, tool.endpoint, bytes.NewReader(call.GetArgumentsJson()))
	if err != nil {
		return failedResult(call, "tool_request_invalid", err.Error(), false)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("X-HarnessX-Run-ID", req.GetRunId())
	httpReq.Header.Set("X-HarnessX-Step-ID", req.GetStepId())
	httpReq.Header.Set("X-HarnessX-Tool-Call-ID", call.GetId())
	for name, value := range tool.headers {
		httpReq.Header.Set(name, value)
	}
	response, err := s.client.Do(httpReq)
	if err != nil {
		reason := "tool_unavailable"
		if ctx.Err() != nil {
			reason = "tool_cancelled"
		}
		return failedResult(call, reason, err.Error(), true)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, tool.maxResponseBytes+1))
	if err != nil {
		return failedResult(call, "tool_response_read_failed", err.Error(), true)
	}
	if int64(len(body)) > tool.maxResponseBytes {
		return failedResult(call, "tool_response_too_large", "tool response exceeded configured limit", false)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message := strings.TrimSpace(string(body))
		if len(message) > 1000 {
			message = message[:1000]
		}
		return failedResult(call, "tool_http_error", fmt.Sprintf("tool returned HTTP %d: %s", response.StatusCode, message), response.StatusCode == 408 || response.StatusCode == 429 || response.StatusCode >= 500)
	}
	mimeType := response.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	return &harnessxv1.ToolResult{CallId: call.GetId(), Name: call.GetName(), Status: "completed", Content: body, MimeType: mimeType}
}

func failedResult(call *harnessxv1.ToolCall, reason, message string, retryable bool) *harnessxv1.ToolResult {
	result := &harnessxv1.ToolResult{Status: "failed", Error: &harnessxv1.ErrorInfo{Reason: reason, Message: message, Retryable: retryable}}
	if call != nil {
		result.CallId = call.GetId()
		result.Name = call.GetName()
	}
	return result
}

func validateContext(ctx *harnessxv1.RequestContext) error {
	if ctx == nil || strings.TrimSpace(ctx.GetTenantId()) == "" {
		return status.Error(codes.InvalidArgument, "request context with tenant_id is required")
	}
	return nil
}

func activeKey(tenant, run, call string) string { return tenant + "\x00" + run + "\x00" + call }
