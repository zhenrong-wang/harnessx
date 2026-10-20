package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/internal/platform/id"
	"github.com/zhenrong-wang/harnessx/services/harness-core/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type contextBuilder interface {
	BuildContext(context.Context, *harnessxv1.BuildContextRequest, ...grpc.CallOption) (*harnessxv1.BuildContextResponse, error)
}

type modelClient interface {
	Generate(context.Context, *harnessxv1.GenerateRequest, ...grpc.CallOption) (*harnessxv1.GenerateResponse, error)
	GetCapabilities(context.Context, *harnessxv1.GetModelCapabilitiesRequest, ...grpc.CallOption) (*harnessxv1.ModelCapabilities, error)
}

type toolExecutor interface {
	ExecuteTools(context.Context, *harnessxv1.ExecuteToolsRequest, ...grpc.CallOption) (*harnessxv1.ExecuteToolsResponse, error)
}

type Engine struct {
	store     store.Store
	context   contextBuilder
	model     modelClient
	tools     toolExecutor
	publisher *publisher
	logger    *slog.Logger
}

type emitFunc func(*harnessxv1.Event) error

func NewEngine(runStore store.Store, contexts contextBuilder, models modelClient, tools toolExecutor, events eventAppender, logger *slog.Logger) *Engine {
	return &Engine{store: runStore, context: contexts, model: models, tools: tools, publisher: &publisher{store: runStore, client: events, logger: logger}, logger: logger}
}

func (e *Engine) RunOutbox(ctx context.Context) { e.publisher.run(ctx) }

func (e *Engine) Execute(ctx context.Context, req *harnessxv1.CreateRunRequest, emit emitFunc, started func(string)) (*harnessxv1.Run, error) {
	policy, err := resolvePolicy(req)
	if err != nil {
		return nil, err
	}
	runID, err := id.New("run_")
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	run := &harnessxv1.Run{
		Id: runID, TenantId: req.GetContext().GetTenantId(), Status: harnessxv1.RunStatus_RUN_STATUS_QUEUED,
		CreatedAt: timestamppb.New(now), Metadata: req.GetMetadata(), PolicySnapshotJson: policy.raw,
	}
	createdEvent, err := newEvent(run, "", "run.created", map[string]any{"status": "queued"})
	if err != nil {
		return nil, err
	}
	stored, created, err := e.store.CreateRun(ctx, run, policy.raw, req.GetContext().GetIdempotencyKey(), createdEvent)
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}
	if !created {
		return stored, nil
	}
	if started != nil {
		started(run.GetId())
	}
	e.deliver(ctx, createdEvent, emit)

	run.Status = harnessxv1.RunStatus_RUN_STATUS_IN_PROGRESS
	inProgress, _ := newEvent(run, "", "run.in_progress", map[string]any{"status": "in_progress"})
	if err := e.store.UpdateRun(ctx, run, inProgress); err != nil {
		return nil, fmt.Errorf("start run: %w", err)
	}
	e.deliver(ctx, inProgress, emit)

	contextLimit := e.contextLimit(ctx, req)
	var history []*harnessxv1.Message
	var observations []*harnessxv1.ToolResult
	var totalUsage harnessxv1.Usage
	for stepNumber := 1; stepNumber <= policy.maxSteps; stepNumber++ {
		if err := ctx.Err(); err != nil {
			return e.finish(ctx, run, harnessxv1.RunStatus_RUN_STATUS_CANCELLED, "cancelled", err.Error(), &totalUsage, emit)
		}
		stepID, err := id.New("step_")
		if err != nil {
			return nil, err
		}
		built, err := e.context.BuildContext(ctx, &harnessxv1.BuildContextRequest{
			Context: req.GetContext(), RunId: run.GetId(), StepId: stepID,
			Input: firstStep(stepNumber, req.GetInput()), Instructions: firstStep(stepNumber, req.GetInstructions()),
			History: history, Observations: observations, Tools: req.GetTools(),
			MaxContextTokens: contextLimit, ReservedOutputTokens: int64(policy.maxOutputTokens),
		})
		if err != nil {
			return e.finishError(ctx, run, "context_failed", err, &totalUsage, emit)
		}
		inputJSON, _ := protojson.Marshal(&harnessxv1.MessageList{Messages: built.GetMessages()})
		_ = e.store.RecordStep(ctx, run.GetTenantId(), run.GetId(), stepID, stepNumber*2-1, "model", "in_progress", inputJSON, nil, nil, false)
		generated, err := e.model.Generate(ctx, &harnessxv1.GenerateRequest{
			Context: req.GetContext(), RunId: run.GetId(), StepId: stepID, Models: req.GetModels(),
			Messages: built.GetMessages(), Tools: req.GetTools(), ToolChoice: "auto", MaxOutputTokens: int32(policy.maxOutputTokens),
		})
		if err != nil {
			errorJSON, _ := json.Marshal(errorPayload(err))
			_ = e.store.RecordStep(ctx, run.GetTenantId(), run.GetId(), stepID, stepNumber*2-1, "model", "failed", inputJSON, nil, errorJSON, true)
			return e.finishError(ctx, run, "model_failed", err, &totalUsage, emit)
		}
		addUsage(&totalUsage, generated.GetUsage())
		generatedJSON, _ := protojson.Marshal(generated)
		_ = e.store.RecordStep(ctx, run.GetTenantId(), run.GetId(), stepID, stepNumber*2-1, "model", "completed", inputJSON, generatedJSON, nil, true)
		run.Model = generated.GetModel()
		modelEvent, _ := newEvent(run, stepID, "model.completed", map[string]any{"model": generated.GetModel(), "finish_reason": generated.GetFinishReason()})
		_ = e.store.EnqueueEvent(ctx, modelEvent)
		e.deliver(ctx, modelEvent, emit)

		message := generated.GetMessage()
		if message == nil {
			return e.finish(ctx, run, harnessxv1.RunStatus_RUN_STATUS_FAILED, "model_response_invalid", "model returned no message", &totalUsage, emit)
		}
		history = append(built.GetMessages(), message)
		if len(message.GetToolCalls()) == 0 {
			run.Output = []*harnessxv1.Message{message}
			return e.finish(ctx, run, harnessxv1.RunStatus_RUN_STATUS_COMPLETED, "", "", &totalUsage, emit)
		}
		if len(req.GetTools()) == 0 || e.tools == nil {
			return e.finish(ctx, run, harnessxv1.RunStatus_RUN_STATUS_FAILED, "tools_unavailable", "model requested tools but no tool runtime is available", &totalUsage, emit)
		}
		toolStepID, _ := id.New("step_")
		callsJSON, _ := protojson.Marshal(&harnessxv1.ToolCallList{Calls: message.GetToolCalls()})
		_ = e.store.RecordStep(ctx, run.GetTenantId(), run.GetId(), toolStepID, stepNumber*2, "tool", "in_progress", callsJSON, nil, nil, false)
		executed, err := e.tools.ExecuteTools(ctx, &harnessxv1.ExecuteToolsRequest{
			Context: req.GetContext(), RunId: run.GetId(), StepId: toolStepID, Calls: message.GetToolCalls(),
			AllowedTools: req.GetTools(), VolumeId: req.GetVolumeId(), MaxParallelism: 4,
		})
		if err != nil {
			errorJSON, _ := json.Marshal(errorPayload(err))
			_ = e.store.RecordStep(ctx, run.GetTenantId(), run.GetId(), toolStepID, stepNumber*2, "tool", "failed", callsJSON, nil, errorJSON, true)
			return e.finishError(ctx, run, "tool_execution_failed", err, &totalUsage, emit)
		}
		resultsJSON, _ := protojson.Marshal(&harnessxv1.ToolResultList{Results: executed.GetResults()})
		_ = e.store.RecordStep(ctx, run.GetTenantId(), run.GetId(), toolStepID, stepNumber*2, "tool", "completed", callsJSON, resultsJSON, nil, true)
		observations = executed.GetResults()
		toolEvent, _ := newEvent(run, toolStepID, "tools.completed", map[string]any{"count": len(observations)})
		_ = e.store.EnqueueEvent(ctx, toolEvent)
		e.deliver(ctx, toolEvent, emit)
	}
	return e.finish(ctx, run, harnessxv1.RunStatus_RUN_STATUS_LIMIT_REACHED, "max_steps_reached", "run reached its configured step limit", &totalUsage, emit)
}

func (e *Engine) contextLimit(ctx context.Context, req *harnessxv1.CreateRunRequest) int64 {
	var smallest int64
	for _, model := range req.GetModels() {
		capabilities, err := e.model.GetCapabilities(ctx, &harnessxv1.GetModelCapabilitiesRequest{Context: req.GetContext(), Model: model})
		if err != nil || capabilities.GetMaxContextTokens() <= 0 {
			continue
		}
		if smallest == 0 || capabilities.GetMaxContextTokens() < smallest {
			smallest = capabilities.GetMaxContextTokens()
		}
	}
	return smallest
}

func (e *Engine) finishError(ctx context.Context, run *harnessxv1.Run, reason string, cause error, usage *harnessxv1.Usage, emit emitFunc) (*harnessxv1.Run, error) {
	statusCode, _ := status.FromError(cause)
	message := cause.Error()
	if statusCode != nil {
		message = statusCode.Message()
	}
	return e.finish(ctx, run, harnessxv1.RunStatus_RUN_STATUS_FAILED, reason, message, usage, emit)
}

func (e *Engine) finish(ctx context.Context, run *harnessxv1.Run, state harnessxv1.RunStatus, reason, message string, usage *harnessxv1.Usage, emit emitFunc) (*harnessxv1.Run, error) {
	run.Status = state
	run.Usage = usage
	run.CompletedAt = timestamppb.Now()
	if reason != "" {
		run.Error = &harnessxv1.ErrorInfo{Reason: reason, Message: message}
	}
	typeName := "run." + strings.ToLower(strings.TrimPrefix(state.String(), "RUN_STATUS_"))
	event, _ := newEvent(run, "", typeName, map[string]any{"status": state.String(), "reason": reason})
	if err := e.store.UpdateRun(ctx, run, event); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if persistErr := e.store.UpdateRun(persistCtx, run, event); persistErr != nil {
				return nil, fmt.Errorf("persist terminal run: %w", persistErr)
			}
		} else {
			return nil, fmt.Errorf("persist terminal run: %w", err)
		}
	}
	e.deliver(context.WithoutCancel(ctx), event, emit)
	return run, nil
}

func (e *Engine) deliver(ctx context.Context, event *harnessxv1.Event, emit emitFunc) {
	delivered := e.publisher.flush(ctx, event)
	if emit != nil {
		if err := emit(delivered); err != nil {
			e.logger.Debug("event stream send failed", "event_id", event.GetId(), "error", err)
		}
	}
}

type policy struct {
	maxSteps        int
	maxOutputTokens int
	raw             string
}

func resolvePolicy(req *harnessxv1.CreateRunRequest) (policy, error) {
	if req == nil || req.GetContext() == nil || strings.TrimSpace(req.GetContext().GetTenantId()) == "" {
		return policy{}, fmt.Errorf("request context with tenant_id is required")
	}
	if strings.TrimSpace(req.GetInput()) == "" || len(req.GetModels()) == 0 {
		return policy{}, fmt.Errorf("input and at least one model are required")
	}
	maxSteps := int(req.GetMaxSteps())
	if maxSteps == 0 {
		maxSteps = 8
	}
	if maxSteps < 1 || maxSteps > 64 {
		return policy{}, fmt.Errorf("max_steps must be between 1 and 64")
	}
	maxOutput := int(req.GetMaxOutputTokens())
	if maxOutput == 0 {
		maxOutput = 4096
	}
	if maxOutput < 1 || maxOutput > 131072 {
		return policy{}, fmt.Errorf("max_output_tokens must be between 1 and 131072")
	}
	raw, _ := json.Marshal(map[string]any{"max_steps": maxSteps, "max_output_tokens": maxOutput, "allowed_tools": toolNames(req.GetTools())})
	return policy{maxSteps: maxSteps, maxOutputTokens: maxOutput, raw: string(raw)}, nil
}

func newEvent(run *harnessxv1.Run, stepID, eventType string, payload any) (*harnessxv1.Event, error) {
	eventID, err := id.New("evt_")
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &harnessxv1.Event{Id: eventID, TenantId: run.GetTenantId(), RunId: run.GetId(), StepId: stepID, Type: eventType, PayloadJson: raw, OccurredAt: timestamppb.Now()}, nil
}

func firstStep[T any](step int, value T) T {
	if step == 1 {
		return value
	}
	var zero T
	return zero
}

func addUsage(total *harnessxv1.Usage, usage *harnessxv1.Usage) {
	if usage == nil {
		return
	}
	total.InputTokens += usage.GetInputTokens()
	total.OutputTokens += usage.GetOutputTokens()
	total.TotalTokens += usage.GetTotalTokens()
}

func toolNames(tools []*harnessxv1.ToolDefinition) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.GetName())
	}
	return names
}

func errorPayload(err error) map[string]any { return map[string]any{"message": err.Error()} }
