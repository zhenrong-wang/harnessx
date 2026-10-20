package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/services/model-core/internal/adapter"
	modelconfig "github.com/zhenrong-wang/harnessx/services/model-core/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type route struct {
	provider      string
	providerModel string
	adapter       adapter.Adapter
	descriptor    *harnessxv1.ModelDescriptor
}

type Server struct {
	harnessxv1.UnimplementedModelServiceServer
	routes map[string]route
	bare   map[string][]route
}

func New(cfg modelconfig.Config) (*Server, error) {
	routes := make(map[string]route)
	bare := make(map[string][]route)
	for _, provider := range cfg.Providers {
		timeout := 2 * time.Minute
		if provider.Timeout != "" {
			parsed, err := time.ParseDuration(provider.Timeout)
			if err != nil {
				return nil, fmt.Errorf("provider %q timeout: %w", provider.Name, err)
			}
			timeout = parsed
		}
		target, err := adapter.NewOpenAICompatible(
			provider.BaseURL,
			os.Getenv(provider.APIKeyEnv),
			provider.Headers,
			&http.Client{Timeout: timeout},
			provider.MaxResponseBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("provider %q: %w", provider.Name, err)
		}
		for _, model := range provider.Models {
			canonical := provider.Name + "/" + model.ID
			r := route{
				provider:      provider.Name,
				providerModel: model.ID,
				adapter:       target,
				descriptor: &harnessxv1.ModelDescriptor{
					Id:            canonical,
					Provider:      provider.Name,
					ProviderModel: model.ID,
					Capabilities: &harnessxv1.ModelCapabilities{
						Streaming:           model.Streaming,
						ToolCalling:         model.ToolCalling,
						ParallelToolCalling: model.ParallelToolCalling,
						StructuredOutput:    model.StructuredOutput,
						Vision:              model.Vision,
						MaxContextTokens:    model.MaxContextTokens,
						MaxOutputTokens:     model.MaxOutputTokens,
					},
				},
			}
			routes[canonical] = r
			bare[model.ID] = append(bare[model.ID], r)
		}
	}
	return &Server{routes: routes, bare: bare}, nil
}

// NewWithRoutes is intentionally package-private to production callers and is
// used by tests to verify routing without an HTTP provider.
func newWithRoutes(routes map[string]route) *Server {
	bare := make(map[string][]route)
	for _, candidate := range routes {
		bare[candidate.providerModel] = append(bare[candidate.providerModel], candidate)
	}
	return &Server{routes: routes, bare: bare}
}

func (s *Server) Generate(ctx context.Context, req *harnessxv1.GenerateRequest) (*harnessxv1.GenerateResponse, error) {
	if err := validateGenerate(req); err != nil {
		return nil, err
	}
	attempts := make([]*harnessxv1.ModelAttempt, 0, len(req.GetModels()))
	for _, requested := range req.GetModels() {
		r, err := s.resolve(requested)
		if err != nil {
			return nil, err
		}
		started := time.Now()
		response, callErr := r.adapter.Generate(ctx, adapterRequest(req, r.providerModel))
		attempt := modelAttempt(r, time.Since(started), response, callErr)
		attempts = append(attempts, attempt)
		if callErr == nil {
			return responseProto(r, response, attempts), nil
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		var adapterErr *adapter.Error
		if !errors.As(callErr, &adapterErr) || !adapterErr.Retryable {
			return nil, grpcAdapterError(callErr)
		}
	}
	last := attempts[len(attempts)-1].GetError()
	return nil, status.Errorf(codes.Unavailable, "all configured model attempts failed: %s", last.GetMessage())
}

func (s *Server) StreamGenerate(req *harnessxv1.GenerateRequest, stream harnessxv1.ModelService_StreamGenerateServer) error {
	if err := validateGenerate(req); err != nil {
		return err
	}
	var sequence int64
	for _, requested := range req.GetModels() {
		r, err := s.resolve(requested)
		if err != nil {
			return err
		}
		if !r.descriptor.GetCapabilities().GetStreaming() {
			continue
		}
		emitted := false
		callErr := r.adapter.Stream(stream.Context(), adapterRequest(req, r.providerModel), func(event adapter.StreamEvent) error {
			sequence++
			emitted = true
			out := &harnessxv1.ModelStreamEvent{
				Sequence:      sequence,
				Type:          event.Type,
				Provider:      r.provider,
				Model:         r.provider + "/" + r.providerModel,
				TextDelta:     event.TextDelta,
				ToolCallDelta: event.ToolCallDelta,
			}
			if event.Completed != nil {
				out.Completed = responseProto(r, event.Completed, nil)
			}
			return stream.Send(out)
		})
		if callErr == nil {
			return nil
		}
		if stream.Context().Err() != nil {
			return status.FromContextError(stream.Context().Err()).Err()
		}
		var adapterErr *adapter.Error
		if emitted || !errors.As(callErr, &adapterErr) || !adapterErr.Retryable {
			return grpcAdapterError(callErr)
		}
	}
	return status.Error(codes.Unavailable, "no streaming-capable model completed the request")
}

func (s *Server) ListModels(context.Context, *harnessxv1.ListModelsRequest) (*harnessxv1.ListModelsResponse, error) {
	models := make([]*harnessxv1.ModelDescriptor, 0, len(s.routes))
	for _, r := range s.routes {
		models = append(models, r.descriptor)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].GetId() < models[j].GetId() })
	return &harnessxv1.ListModelsResponse{Models: models}, nil
}

func (s *Server) GetCapabilities(_ context.Context, req *harnessxv1.GetModelCapabilitiesRequest) (*harnessxv1.ModelCapabilities, error) {
	if req == nil || strings.TrimSpace(req.GetModel()) == "" {
		return nil, status.Error(codes.InvalidArgument, "model is required")
	}
	r, err := s.resolve(req.GetModel())
	if err != nil {
		return nil, err
	}
	return r.descriptor.GetCapabilities(), nil
}

func (s *Server) resolve(name string) (route, error) {
	name = strings.TrimSpace(name)
	if r, ok := s.routes[name]; ok {
		return r, nil
	}
	candidates := s.bare[name]
	switch len(candidates) {
	case 0:
		return route{}, status.Errorf(codes.NotFound, "model %q is not configured", name)
	case 1:
		return candidates[0], nil
	default:
		return route{}, status.Errorf(codes.InvalidArgument, "model %q is ambiguous; use provider/model", name)
	}
}

func validateGenerate(req *harnessxv1.GenerateRequest) error {
	if req == nil || req.GetContext() == nil || strings.TrimSpace(req.GetContext().GetTenantId()) == "" {
		return status.Error(codes.InvalidArgument, "request context with tenant_id is required")
	}
	if len(req.GetModels()) == 0 {
		return status.Error(codes.InvalidArgument, "at least one model is required")
	}
	if len(req.GetMessages()) == 0 {
		return status.Error(codes.InvalidArgument, "at least one message is required")
	}
	return nil
}

func adapterRequest(req *harnessxv1.GenerateRequest, model string) adapter.Request {
	return adapter.Request{
		Model:              model,
		Messages:           req.GetMessages(),
		Tools:              req.GetTools(),
		ToolChoice:         req.GetToolChoice(),
		MaxOutputTokens:    req.GetMaxOutputTokens(),
		ResponseSchemaJSON: req.GetResponseSchemaJson(),
	}
}

func responseProto(r route, response *adapter.Response, attempts []*harnessxv1.ModelAttempt) *harnessxv1.GenerateResponse {
	return &harnessxv1.GenerateResponse{
		Provider:             r.provider,
		Model:                r.provider + "/" + r.providerModel,
		Message:              response.Message,
		FinishReason:         response.FinishReason,
		Usage:                response.Usage,
		ProviderResponseJson: response.ProviderRawJSON,
		Attempts:             attempts,
	}
}

func modelAttempt(r route, duration time.Duration, response *adapter.Response, err error) *harnessxv1.ModelAttempt {
	attempt := &harnessxv1.ModelAttempt{Provider: r.provider, Model: r.provider + "/" + r.providerModel, DurationMs: duration.Milliseconds()}
	if err == nil {
		attempt.Status = "completed"
		attempt.Usage = response.Usage
		return attempt
	}
	attempt.Status = "failed"
	attempt.Error = errorInfo(err)
	return attempt
}

func errorInfo(err error) *harnessxv1.ErrorInfo {
	var adapterErr *adapter.Error
	if errors.As(err, &adapterErr) {
		return &harnessxv1.ErrorInfo{Reason: adapterErr.Reason, Message: adapterErr.Message, Retryable: adapterErr.Retryable}
	}
	return &harnessxv1.ErrorInfo{Reason: "model_adapter_error", Message: err.Error()}
}

func grpcAdapterError(err error) error {
	var adapterErr *adapter.Error
	if !errors.As(err, &adapterErr) {
		return status.Error(codes.Internal, err.Error())
	}
	code := codes.FailedPrecondition
	if adapterErr.Retryable {
		code = codes.Unavailable
	} else if adapterErr.Status == http.StatusUnauthorized || adapterErr.Status == http.StatusForbidden {
		code = codes.PermissionDenied
	} else if adapterErr.Status == http.StatusBadRequest || adapterErr.Status == http.StatusUnprocessableEntity {
		code = codes.InvalidArgument
	}
	return status.Error(code, adapterErr.Message)
}
