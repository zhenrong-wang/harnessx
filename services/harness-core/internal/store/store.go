package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OutboxEvent struct {
	Event    *harnessxv1.Event
	Attempts int
}

type Store interface {
	CreateRun(context.Context, *harnessxv1.Run, string, string, *harnessxv1.Event) (*harnessxv1.Run, bool, error)
	UpdateRun(context.Context, *harnessxv1.Run, *harnessxv1.Event) error
	RecordStep(context.Context, string, string, string, int, string, string, []byte, []byte, []byte, bool) error
	EnqueueEvent(context.Context, *harnessxv1.Event) error
	GetRun(context.Context, string, string) (*harnessxv1.Run, error)
	PendingEvents(context.Context, int) ([]OutboxEvent, error)
	MarkEventDelivered(context.Context, string) error
	MarkEventFailed(context.Context, string, error) error
}

func (s *Postgres) RecordStep(ctx context.Context, tenantID, runID, stepID string, sequence int, kind, status string, input, output, errorJSON []byte, completed bool) error {
	var completedAt any
	if completed {
		completedAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO harness_core.steps
  (id, tenant_id, run_id, sequence, kind, status, input_json, output_json, error_json, completed_at)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
  ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, output_json=EXCLUDED.output_json,
    error_json=EXCLUDED.error_json, completed_at=EXCLUDED.completed_at`,
		stepID, tenantID, runID, sequence, kind, status, nullJSON(input), nullJSON(output), nullJSON(errorJSON), completedAt)
	if err != nil {
		return fmt.Errorf("record step: %w", err)
	}
	return nil
}

func (s *Postgres) EnqueueEvent(ctx context.Context, event *harnessxv1.Event) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := insertOutbox(ctx, tx, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (s *Postgres) CreateRun(ctx context.Context, run *harnessxv1.Run, policyJSON, idempotencyKey string, event *harnessxv1.Event) (*harnessxv1.Run, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, fmt.Errorf("begin create run: %w", err)
	}
	defer tx.Rollback(ctx)
	metadata, err := json.Marshal(run.GetMetadata())
	if err != nil {
		return nil, false, fmt.Errorf("encode run metadata: %w", err)
	}
	command, err := tx.Exec(ctx, `INSERT INTO harness_core.runs
  (id, tenant_id, status, policy_snapshot, metadata_json, idempotency_key, created_at)
  VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),$7)
  ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> '' DO NOTHING`,
		run.GetId(), run.GetTenantId(), int32(run.GetStatus()), []byte(policyJSON), metadata, idempotencyKey, run.GetCreatedAt().AsTime())
	if err != nil {
		return nil, false, fmt.Errorf("insert run: %w", err)
	}
	if command.RowsAffected() == 0 {
		existing, err := getRun(ctx, tx, run.GetTenantId(), idempotencyKey, true)
		if err != nil {
			return nil, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, err
		}
		return existing, false, nil
	}
	if err := insertOutbox(ctx, tx, event); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit create run: %w", err)
	}
	return run, true, nil
}

func (s *Postgres) UpdateRun(ctx context.Context, run *harnessxv1.Run, event *harnessxv1.Event) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin update run: %w", err)
	}
	defer tx.Rollback(ctx)
	output, err := protojson.Marshal(&harnessxv1.MessageList{Messages: run.GetOutput()})
	if err != nil {
		return fmt.Errorf("encode run output: %w", err)
	}
	usage, err := marshalProto(run.GetUsage())
	if err != nil {
		return err
	}
	errorJSON, err := marshalProto(run.GetError())
	if err != nil {
		return err
	}
	var completedAt any
	if run.GetCompletedAt() != nil {
		completedAt = run.GetCompletedAt().AsTime()
	}
	command, err := tx.Exec(ctx, `UPDATE harness_core.runs SET status=$1, model=$2, output_json=$3,
usage_json=$4, error_json=$5, completed_at=$6 WHERE id=$7 AND tenant_id=$8`,
		int32(run.GetStatus()), run.GetModel(), output, usage, errorJSON, completedAt, run.GetId(), run.GetTenantId())
	if err != nil {
		return fmt.Errorf("update run: %w", err)
	}
	if command.RowsAffected() != 1 {
		return pgx.ErrNoRows
	}
	if err := insertOutbox(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit update run: %w", err)
	}
	return nil
}

func (s *Postgres) GetRun(ctx context.Context, tenantID, runID string) (*harnessxv1.Run, error) {
	return getRun(ctx, s.pool, tenantID, runID, false)
}

func (s *Postgres) PendingEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, tenant_id, run_id, step_id, type, payload_json, metadata_json, occurred_at, attempts
FROM harness_core.event_outbox WHERE delivered_at IS NULL AND next_attempt_at <= now()
ORDER BY next_attempt_at, occurred_at, id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("query event outbox: %w", err)
	}
	defer rows.Close()
	var events []OutboxEvent
	for rows.Next() {
		var event harnessxv1.Event
		var metadata []byte
		var occurredAt time.Time
		var attempts int
		if err := rows.Scan(&event.Id, &event.TenantId, &event.RunId, &event.StepId, &event.Type, &event.PayloadJson, &metadata, &occurredAt, &attempts); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(metadata, &event.Metadata); err != nil {
			return nil, fmt.Errorf("decode outbox metadata: %w", err)
		}
		event.OccurredAt = timestamppb.New(occurredAt)
		events = append(events, OutboxEvent{Event: &event, Attempts: attempts})
	}
	return events, rows.Err()
}

func (s *Postgres) MarkEventDelivered(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE harness_core.event_outbox SET delivered_at=now(), attempts=attempts+1, last_error=NULL WHERE id=$1`, id)
	return err
}

func (s *Postgres) MarkEventFailed(ctx context.Context, id string, cause error) error {
	message := cause.Error()
	if len(message) > 2000 {
		message = message[:2000]
	}
	_, err := s.pool.Exec(ctx, `UPDATE harness_core.event_outbox SET attempts=attempts+1, last_error=$2,
next_attempt_at=now()+make_interval(secs => LEAST(60, power(2, LEAST(attempts, 6))::integer)) WHERE id=$1`, id, message)
	return err
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func getRun(ctx context.Context, q queryer, tenantID, value string, byIdempotency bool) (*harnessxv1.Run, error) {
	where := "id=$2"
	if byIdempotency {
		where = "idempotency_key=$2"
	}
	row := q.QueryRow(ctx, `SELECT id, tenant_id, status, model, output_json, usage_json, error_json,
policy_snapshot, metadata_json, created_at, completed_at FROM harness_core.runs WHERE tenant_id=$1 AND `+where, tenantID, value)
	var run harnessxv1.Run
	var status int32
	var output, usage, errorJSON, metadata []byte
	var policy string
	var createdAt time.Time
	var completedAt *time.Time
	if err := row.Scan(&run.Id, &run.TenantId, &status, &run.Model, &output, &usage, &errorJSON, &policy, &metadata, &createdAt, &completedAt); err != nil {
		return nil, err
	}
	run.Status = harnessxv1.RunStatus(status)
	var messageList harnessxv1.MessageList
	if len(output) > 0 {
		if err := protojson.Unmarshal(output, &messageList); err != nil {
			return nil, fmt.Errorf("decode run output: %w", err)
		}
		run.Output = messageList.GetMessages()
	}
	if len(usage) > 0 {
		run.Usage = &harnessxv1.Usage{}
		if err := protojson.Unmarshal(usage, run.Usage); err != nil {
			return nil, fmt.Errorf("decode run usage: %w", err)
		}
	}
	if len(errorJSON) > 0 {
		run.Error = &harnessxv1.ErrorInfo{}
		if err := protojson.Unmarshal(errorJSON, run.Error); err != nil {
			return nil, fmt.Errorf("decode run error: %w", err)
		}
	}
	if err := json.Unmarshal(metadata, &run.Metadata); err != nil {
		return nil, fmt.Errorf("decode run metadata: %w", err)
	}
	run.PolicySnapshotJson = policy
	run.CreatedAt = timestamppb.New(createdAt)
	if completedAt != nil {
		run.CompletedAt = timestamppb.New(*completedAt)
	}
	return &run, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, event *harnessxv1.Event) error {
	if event == nil {
		return errors.New("transition event is required")
	}
	payload := event.GetPayloadJson()
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	metadata, err := json.Marshal(event.GetMetadata())
	if err != nil {
		return fmt.Errorf("encode event metadata: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO harness_core.event_outbox
  (id, tenant_id, run_id, step_id, type, payload_json, metadata_json, occurred_at)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING`,
		event.GetId(), event.GetTenantId(), event.GetRunId(), event.GetStepId(), event.GetType(), payload, metadata, event.GetOccurredAt().AsTime())
	if err != nil {
		return fmt.Errorf("insert event outbox: %w", err)
	}
	return nil
}

func marshalProto(message proto.Message) ([]byte, error) {
	if message == nil {
		return nil, nil
	}
	// The concrete values supplied here are generated protobuf messages. Keeping
	// this switch explicit prevents accepting arbitrary JSON-bearing objects.
	switch value := message.(type) {
	case *harnessxv1.Usage:
		return protojson.Marshal(value)
	case *harnessxv1.ErrorInfo:
		return protojson.Marshal(value)
	default:
		return nil, fmt.Errorf("unsupported protobuf value %T", message)
	}
}

func nullJSON(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}
