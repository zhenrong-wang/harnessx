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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Store interface {
	Append(context.Context, *harnessxv1.Event) (*harnessxv1.Event, error)
	List(context.Context, string, string, int64, int32) ([]*harnessxv1.Event, bool, error)
}

type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (s *Postgres) Append(ctx context.Context, event *harnessxv1.Event) (*harnessxv1.Event, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin append event: %w", err)
	}
	defer tx.Rollback(ctx)
	// JSON encoding gives the advisory-lock key an unambiguous, valid UTF-8
	// representation without relying on a delimiter that tenant IDs may contain.
	lockKeyJSON, _ := json.Marshal([]string{event.GetTenantId(), event.GetRunId()})
	lockKey := string(lockKeyJSON)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return nil, fmt.Errorf("lock event stream: %w", err)
	}
	var existing *harnessxv1.Event
	existing, err = getByID(ctx, tx, event.GetId())
	if err == nil {
		if existing.GetTenantId() != event.GetTenantId() || existing.GetRunId() != event.GetRunId() {
			return nil, fmt.Errorf("event id %q already belongs to another stream", event.GetId())
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM event_core.events WHERE tenant_id=$1 AND run_id=$2`, event.GetTenantId(), event.GetRunId()).Scan(&sequence); err != nil {
		return nil, fmt.Errorf("allocate event sequence: %w", err)
	}
	occurredAt := time.Now().UTC()
	if event.GetOccurredAt() != nil {
		occurredAt = event.GetOccurredAt().AsTime().UTC()
	}
	metadata, err := json.Marshal(event.GetMetadata())
	if err != nil {
		return nil, fmt.Errorf("encode event metadata: %w", err)
	}
	payload := event.GetPayloadJson()
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("event payload is not valid JSON")
	}
	_, err = tx.Exec(ctx, `INSERT INTO event_core.events
  (id, tenant_id, run_id, step_id, sequence, type, payload_json, metadata_json, occurred_at)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		event.GetId(), event.GetTenantId(), event.GetRunId(), event.GetStepId(), sequence,
		event.GetType(), payload, metadata, occurredAt)
	if err != nil {
		return nil, fmt.Errorf("insert event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit event: %w", err)
	}
	clone := proto.Clone(event).(*harnessxv1.Event)
	clone.Sequence = sequence
	clone.OccurredAt = timestamppb.New(occurredAt)
	return clone, nil
}

func (s *Postgres) List(ctx context.Context, tenantID, runID string, after int64, limit int32) ([]*harnessxv1.Event, bool, error) {
	queryLimit := int(limit) + 1
	rows, err := s.pool.Query(ctx, `SELECT id, tenant_id, run_id, step_id, sequence, type, payload_json, metadata_json, occurred_at
FROM event_core.events WHERE tenant_id=$1 AND run_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4`, tenantID, runID, after, queryLimit)
	if err != nil {
		return nil, false, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	events := make([]*harnessxv1.Event, 0, queryLimit)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, false, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate events: %w", err)
	}
	hasMore := len(events) > int(limit)
	if hasMore {
		events = events[:limit]
	}
	return events, hasMore, nil
}

type rowScanner interface{ Scan(...any) error }

func getByID(ctx context.Context, tx pgx.Tx, id string) (*harnessxv1.Event, error) {
	return scanEvent(tx.QueryRow(ctx, `SELECT id, tenant_id, run_id, step_id, sequence, type, payload_json, metadata_json, occurred_at FROM event_core.events WHERE id=$1`, id))
}

func scanEvent(row rowScanner) (*harnessxv1.Event, error) {
	var event harnessxv1.Event
	var metadataJSON []byte
	var occurredAt time.Time
	if err := row.Scan(&event.Id, &event.TenantId, &event.RunId, &event.StepId, &event.Sequence, &event.Type, &event.PayloadJson, &metadataJSON, &occurredAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(metadataJSON, &event.Metadata); err != nil {
		return nil, fmt.Errorf("decode event metadata: %w", err)
	}
	event.OccurredAt = timestamppb.New(occurredAt)
	return &event, nil
}
