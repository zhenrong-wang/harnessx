package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/internal/platform/id"
	gatewayconfig "github.com/zhenrong-wang/harnessx/services/api-gateway/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type harnessClient interface {
	CreateRun(context.Context, *harnessxv1.CreateRunRequest, ...grpc.CallOption) (*harnessxv1.Run, error)
	StreamRun(context.Context, *harnessxv1.CreateRunRequest, ...grpc.CallOption) (harnessxv1.HarnessService_StreamRunClient, error)
	GetRun(context.Context, *harnessxv1.GetRunRequest, ...grpc.CallOption) (*harnessxv1.Run, error)
	CancelRun(context.Context, *harnessxv1.CancelRunRequest, ...grpc.CallOption) (*harnessxv1.CancelRunResponse, error)
	ListRunEvents(context.Context, *harnessxv1.ListRunEventsRequest, ...grpc.CallOption) (*harnessxv1.ListRunEventsResponse, error)
}

type Server struct {
	client        harnessClient
	apiKeys       map[string]string
	allowInsecure bool
}

type createRunBody struct {
	Input           string            `json:"input"`
	Instructions    string            `json:"instructions,omitempty"`
	Models          []string          `json:"models"`
	Tools           []toolBody        `json:"tools,omitempty"`
	MaxSteps        int32             `json:"max_steps,omitempty"`
	MaxOutputTokens int32             `json:"max_output_tokens,omitempty"`
	VolumeID        string            `json:"volume_id,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Stream          bool              `json:"stream,omitempty"`
}

type toolBody struct {
	Name         string            `json:"name"`
	Description  string            `json:"description,omitempty"`
	InputSchema  json.RawMessage   `json:"input_schema"`
	OutputSchema json.RawMessage   `json:"output_schema,omitempty"`
	Annotations  map[string]string `json:"annotations,omitempty"`
}

func New(cfg gatewayconfig.Config, client harnessClient) http.Handler {
	s := &Server{client: client, apiKeys: cfg.APIKeys, allowInsecure: cfg.AllowInsecure}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /readyz", s.health)
	mux.HandleFunc("POST /v1/runs", s.createRun)
	mux.HandleFunc("GET /v1/runs/{run_id}", s.getRun)
	mux.HandleFunc("POST /v1/runs/{run_id}/cancel", s.cancelRun)
	mux.HandleFunc("GET /v1/runs/{run_id}/events", s.listEvents)
	return recoverMiddleware(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	requestContext, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	var body createRunBody
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	request := &harnessxv1.CreateRunRequest{
		Context: requestContext, Input: body.Input, Instructions: body.Instructions, Models: body.Models,
		MaxSteps: body.MaxSteps, MaxOutputTokens: body.MaxOutputTokens, VolumeId: body.VolumeID, Metadata: body.Metadata,
	}
	for _, tool := range body.Tools {
		if !json.Valid(tool.InputSchema) || (len(tool.OutputSchema) > 0 && !json.Valid(tool.OutputSchema)) {
			writeError(w, http.StatusBadRequest, "invalid_tool_schema", "tool schemas must be valid JSON")
			return
		}
		request.Tools = append(request.Tools, &harnessxv1.ToolDefinition{
			Name: tool.Name, Description: tool.Description, InputSchemaJson: tool.InputSchema,
			OutputSchemaJson: tool.OutputSchema, Annotations: tool.Annotations,
		})
	}
	if body.Stream || strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		s.streamRun(w, r, request)
		return
	}
	run, err := s.client.CreateRun(r.Context(), request)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProtoJSON(w, http.StatusOK, run)
}

func (s *Server) streamRun(w http.ResponseWriter, r *http.Request, request *harnessxv1.CreateRunRequest) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "HTTP streaming is unavailable")
		return
	}
	stream, err := s.client.StreamRun(r.Context(), request)
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", mustJSON(map[string]string{"message": err.Error()}))
			flusher.Flush()
			return
		}
		payload, err := json.Marshal(publicEvent(event))
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.GetSequence(), event.GetType(), payload)
		flusher.Flush()
	}
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	ctx, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	run, err := s.client.GetRun(r.Context(), &harnessxv1.GetRunRequest{Context: ctx, RunId: r.PathValue("run_id")})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProtoJSON(w, http.StatusOK, run)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	ctx, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	response, err := s.client.CancelRun(r.Context(), &harnessxv1.CancelRunRequest{Context: ctx, RunId: r.PathValue("run_id")})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	writeProtoJSON(w, http.StatusOK, response)
}

type eventView struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id"`
	RunID      string            `json:"run_id"`
	StepID     string            `json:"step_id,omitempty"`
	Sequence   int64             `json:"sequence"`
	Type       string            `json:"type"`
	Payload    json.RawMessage   `json:"payload"`
	OccurredAt string            `json:"occurred_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

func publicEvent(event *harnessxv1.Event) eventView {
	payload := json.RawMessage(event.GetPayloadJson())
	if len(payload) == 0 || !json.Valid(payload) {
		payload = json.RawMessage(`{}`)
	}
	occurredAt := ""
	if event.GetOccurredAt() != nil {
		occurredAt = event.GetOccurredAt().AsTime().UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	return eventView{
		ID: event.GetId(), TenantID: event.GetTenantId(), RunID: event.GetRunId(), StepID: event.GetStepId(),
		Sequence: event.GetSequence(), Type: event.GetType(), Payload: payload, OccurredAt: occurredAt, Metadata: event.GetMetadata(),
	}
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	ctx, ok := s.authenticate(w, r)
	if !ok {
		return
	}
	after, err := strconv.ParseInt(defaultString(r.URL.Query().Get("after_sequence"), "0"), 10, 64)
	if err != nil || after < 0 {
		writeError(w, http.StatusBadRequest, "invalid_after_sequence", "after_sequence must be a non-negative integer")
		return
	}
	limit, err := strconv.ParseInt(defaultString(r.URL.Query().Get("limit"), "100"), 10, 32)
	if err != nil || limit < 1 || limit > 1000 {
		writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 1000")
		return
	}
	response, err := s.client.ListRunEvents(r.Context(), &harnessxv1.ListRunEventsRequest{
		Context: ctx, RunId: r.PathValue("run_id"), AfterSequence: after, Limit: int32(limit),
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}
	views := make([]eventView, 0, len(response.GetEvents()))
	for _, event := range response.GetEvents() {
		views = append(views, publicEvent(event))
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": views, "has_more": response.GetHasMore()})
}

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (*harnessxv1.RequestContext, bool) {
	tenantID := ""
	if s.allowInsecure && len(s.apiKeys) == 0 {
		tenantID = strings.TrimSpace(r.Header.Get("X-HarnessX-Tenant-ID"))
	} else {
		token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		for candidate, tenant := range s.apiKeys {
			if subtle.ConstantTimeCompare([]byte(token), []byte(candidate)) == 1 {
				tenantID = tenant
				break
			}
		}
	}
	if tenantID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized", "a valid tenant API key is required")
		return nil, false
	}
	requestID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID, _ = id.New("req_")
	}
	return &harnessxv1.RequestContext{
		RequestId: requestID, TenantId: tenantID, ActorId: strings.TrimSpace(r.Header.Get("X-HarnessX-Actor-ID")),
		TraceId: strings.TrimSpace(r.Header.Get("Traceparent")), IdempotencyKey: strings.TrimSpace(r.Header.Get("Idempotency-Key")),
	}, true
}

func writeGRPCError(w http.ResponseWriter, err error) {
	code := status.Code(err)
	httpStatus := http.StatusInternalServerError
	switch code {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.ResourceExhausted:
		httpStatus = http.StatusBadRequest
	case codes.Unauthenticated:
		httpStatus = http.StatusUnauthorized
	case codes.PermissionDenied:
		httpStatus = http.StatusForbidden
	case codes.NotFound:
		httpStatus = http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		httpStatus = http.StatusConflict
	case codes.Unavailable:
		httpStatus = http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		httpStatus = http.StatusGatewayTimeout
	}
	writeError(w, httpStatus, strings.ToLower(code.String()), status.Convert(err).Message())
}

func writeProtoJSON(w http.ResponseWriter, statusCode int, message proto.Message) {
	payload, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encoding_failed", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = w.Write(payload)
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, statusCode int, reason, message string) {
	writeJSON(w, statusCode, map[string]any{"error": map[string]any{"reason": reason, "message": message}})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeError(w, http.StatusInternalServerError, "internal_error", "request failed")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func mustJSON(value any) string { raw, _ := json.Marshal(value); return string(raw) }
func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
