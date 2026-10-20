package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Postgres struct{ pool *pgxpool.Pool }

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (s *Postgres) Create(ctx context.Context, volume *harnessxv1.Volume) error {
	metadata, _ := json.Marshal(volume.GetMetadata())
	_, err := s.pool.Exec(ctx, `INSERT INTO volume_core.volumes(id,tenant_id,name,backend,metadata_json,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$6)`,
		volume.GetId(), volume.GetTenantId(), volume.GetName(), volume.GetBackend(), metadata, volume.GetCreatedAt().AsTime())
	return err
}

func (s *Postgres) Delete(ctx context.Context, tenantID, volumeID string) error {
	command, err := s.pool.Exec(ctx, `DELETE FROM volume_core.volumes WHERE tenant_id=$1 AND id=$2`, tenantID, volumeID)
	if err == nil && command.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

func (s *Postgres) Get(ctx context.Context, tenantID, volumeID string) (*harnessxv1.Volume, error) {
	return scanVolume(s.pool.QueryRow(ctx, `SELECT v.id,v.tenant_id,v.name,v.backend,v.metadata_json,v.created_at,v.updated_at,
COALESCE(SUM(o.size_bytes) FILTER (WHERE NOT o.directory),0),COUNT(o.path) FILTER (WHERE NOT o.directory)
FROM volume_core.volumes v LEFT JOIN volume_core.objects o ON o.volume_id=v.id
WHERE v.tenant_id=$1 AND v.id=$2 GROUP BY v.id`, tenantID, volumeID))
}

func (s *Postgres) List(ctx context.Context, tenantID, token string, limit int32) ([]*harnessxv1.Volume, string, error) {
	after := ""
	if token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return nil, "", fmt.Errorf("invalid page token")
		}
		after = string(raw)
	}
	rows, err := s.pool.Query(ctx, `SELECT v.id,v.tenant_id,v.name,v.backend,v.metadata_json,v.created_at,v.updated_at,
COALESCE(SUM(o.size_bytes) FILTER (WHERE NOT o.directory),0),COUNT(o.path) FILTER (WHERE NOT o.directory)
FROM volume_core.volumes v LEFT JOIN volume_core.objects o ON o.volume_id=v.id
WHERE v.tenant_id=$1 AND v.id>$2 GROUP BY v.id ORDER BY v.id LIMIT $3`, tenantID, after, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	volumes := make([]*harnessxv1.Volume, 0, limit+1)
	for rows.Next() {
		volume, err := scanVolume(rows)
		if err != nil {
			return nil, "", err
		}
		volumes = append(volumes, volume)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(volumes) > int(limit) {
		volumes = volumes[:limit]
		next = base64.RawURLEncoding.EncodeToString([]byte(volumes[len(volumes)-1].GetId()))
	}
	return volumes, next, nil
}

func (s *Postgres) UpsertObject(ctx context.Context, tenantID, volumeID, path string, directory bool, size int64, digest, mime string, modified time.Time) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO volume_core.objects(volume_id,tenant_id,path,directory,size_bytes,digest,mime_type,modified_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(volume_id,path) DO UPDATE SET
directory=EXCLUDED.directory,size_bytes=EXCLUDED.size_bytes,digest=EXCLUDED.digest,mime_type=EXCLUDED.mime_type,modified_at=EXCLUDED.modified_at`,
		volumeID, tenantID, path, directory, size, digest, mime, modified)
	return err
}

func (s *Postgres) DeleteObject(ctx context.Context, tenantID, volumeID, path string, recursive bool) error {
	if recursive {
		_, err := s.pool.Exec(ctx, `DELETE FROM volume_core.objects WHERE tenant_id=$1 AND volume_id=$2 AND (path=$3 OR path LIKE $3 || '/%')`, tenantID, volumeID, path)
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM volume_core.objects WHERE tenant_id=$1 AND volume_id=$2 AND path=$3`, tenantID, volumeID, path)
	return err
}

func (s *Postgres) MimeType(ctx context.Context, tenantID, volumeID, path string) (string, error) {
	var mime string
	err := s.pool.QueryRow(ctx, `SELECT mime_type FROM volume_core.objects WHERE tenant_id=$1 AND volume_id=$2 AND path=$3`, tenantID, volumeID, path).Scan(&mime)
	return mime, err
}

type scanner interface{ Scan(...any) error }

func scanVolume(row scanner) (*harnessxv1.Volume, error) {
	var volume harnessxv1.Volume
	var metadata []byte
	var createdAt, updatedAt time.Time
	if err := row.Scan(&volume.Id, &volume.TenantId, &volume.Name, &volume.Backend, &metadata, &createdAt, &updatedAt, &volume.BytesUsed, &volume.ObjectCount); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(metadata, &volume.Metadata); err != nil {
		return nil, err
	}
	volume.CreatedAt = timestamppb.New(createdAt)
	volume.UpdatedAt = timestamppb.New(updatedAt)
	return &volume, nil
}
