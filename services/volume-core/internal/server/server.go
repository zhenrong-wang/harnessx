package server

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	harnessxv1 "github.com/zhenrong-wang/harnessx/gen/go/harnessx/v1"
	"github.com/zhenrong-wang/harnessx/internal/platform/id"
	"github.com/zhenrong-wang/harnessx/services/volume-core/internal/driver"
	"github.com/zhenrong-wang/harnessx/services/volume-core/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	harnessxv1.UnimplementedVolumeServiceServer
	store  *store.Postgres
	driver *driver.Local
}

func New(metadata *store.Postgres, local *driver.Local) *Server {
	return &Server{store: metadata, driver: local}
}

func (s *Server) CreateVolume(ctx context.Context, req *harnessxv1.CreateVolumeRequest) (*harnessxv1.Volume, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	backend := defaultString(req.GetBackend(), "local")
	if backend != "local" {
		return nil, status.Errorf(codes.InvalidArgument, "backend %q is not configured", backend)
	}
	volumeID, err := id.New("vol_")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	volume := &harnessxv1.Volume{Id: volumeID, TenantId: req.GetContext().GetTenantId(), Name: req.GetName(), Backend: backend, Metadata: req.GetMetadata(), CreatedAt: timestamppb.Now(), UpdatedAt: timestamppb.Now()}
	if err := s.driver.CreateVolume(volumeID); err != nil {
		return nil, status.Errorf(codes.Internal, "create volume storage: %v", err)
	}
	if err := s.store.Create(ctx, volume); err != nil {
		_ = s.driver.DeleteVolume(volumeID)
		return nil, status.Errorf(codes.AlreadyExists, "create volume metadata: %v", err)
	}
	return volume, nil
}

func (s *Server) GetVolume(ctx context.Context, req *harnessxv1.GetVolumeRequest) (*harnessxv1.Volume, error) {
	if err := validateVolumeRequest(req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	volume, err := s.store.Get(ctx, req.GetContext().GetTenantId(), req.GetVolumeId())
	return volume, storeError(err)
}

func (s *Server) ListVolumes(ctx context.Context, req *harnessxv1.ListVolumesRequest) (*harnessxv1.ListVolumesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	limit := req.GetLimit()
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		return nil, status.Error(codes.InvalidArgument, "limit cannot exceed 500")
	}
	volumes, token, err := s.store.List(ctx, req.GetContext().GetTenantId(), req.GetPageToken(), limit)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "list volumes: %v", err)
	}
	return &harnessxv1.ListVolumesResponse{Volumes: volumes, NextPageToken: token}, nil
}

func (s *Server) DeleteVolume(ctx context.Context, req *harnessxv1.DeleteVolumeRequest) (*harnessxv1.DeleteVolumeResponse, error) {
	if err := validateVolumeRequest(req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	if _, err := s.store.Get(ctx, req.GetContext().GetTenantId(), req.GetVolumeId()); err != nil {
		return nil, storeError(err)
	}
	if err := s.store.Delete(ctx, req.GetContext().GetTenantId(), req.GetVolumeId()); err != nil {
		return nil, storeError(err)
	}
	// Delete metadata first: if filesystem cleanup fails the remaining bytes are
	// unreachable orphaned data, rather than a live volume whose data vanished.
	if err := s.driver.DeleteVolume(req.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.Internal, "volume metadata was deleted but storage cleanup failed: %v", err)
	}
	return &harnessxv1.DeleteVolumeResponse{}, nil
}

func (s *Server) ListEntries(ctx context.Context, req *harnessxv1.ListEntriesRequest) (*harnessxv1.ListEntriesResponse, error) {
	if err := s.authorizeVolume(ctx, req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	entries, err := s.driver.List(req.GetVolumeId(), req.GetPath())
	if err != nil {
		return nil, pathError(err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	start := 0
	if req.GetPageToken() != "" {
		for start < len(entries) && entries[start].Path <= req.GetPageToken() {
			start++
		}
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 500
	}
	end := start + limit
	next := ""
	if end < len(entries) {
		next = entries[end-1].Path
	} else {
		end = len(entries)
	}
	response := &harnessxv1.ListEntriesResponse{NextPageToken: next}
	for _, entry := range entries[start:end] {
		response.Entries = append(response.Entries, &harnessxv1.VolumeEntry{Path: entry.Path, Directory: entry.Directory, SizeBytes: entry.Size, ModifiedAt: timestamppb.New(time.Unix(0, entry.Modified))})
	}
	return response, nil
}

func (s *Server) ReadFile(ctx context.Context, req *harnessxv1.ReadFileRequest) (*harnessxv1.ReadFileResponse, error) {
	if err := s.authorizeVolume(ctx, req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	content, total, digest, err := s.driver.Read(req.GetVolumeId(), req.GetPath(), req.GetOffset(), req.GetLimit())
	if err != nil {
		return nil, pathError(err)
	}
	mime, err := s.store.MimeType(ctx, req.GetContext().GetTenantId(), req.GetVolumeId(), req.GetPath())
	if err != nil {
		mime = http.DetectContentType(content)
	}
	return &harnessxv1.ReadFileResponse{Content: content, MimeType: mime, TotalSize: total, Digest: digest}, nil
}

func (s *Server) WriteFile(ctx context.Context, req *harnessxv1.WriteFileRequest) (*harnessxv1.WriteFileResponse, error) {
	if err := s.authorizeVolume(ctx, req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetPath()) == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	digest, err := s.driver.Write(req.GetVolumeId(), req.GetPath(), req.GetContent(), req.GetExpectedDigest())
	if errors.Is(err, driver.ErrDigestMismatch) {
		return nil, status.Error(codes.Aborted, "expected_digest does not match the current file")
	}
	if err != nil {
		return nil, pathError(err)
	}
	mime := req.GetMimeType()
	if mime == "" {
		mime = http.DetectContentType(req.GetContent())
	}
	if err := s.store.UpsertObject(ctx, req.GetContext().GetTenantId(), req.GetVolumeId(), req.GetPath(), false, int64(len(req.GetContent())), digest, mime, time.Now().UTC()); err != nil {
		return nil, status.Errorf(codes.Internal, "record file metadata: %v", err)
	}
	return &harnessxv1.WriteFileResponse{Artifact: &harnessxv1.ArtifactRef{VolumeId: req.GetVolumeId(), Path: req.GetPath(), MediaType: mime, SizeBytes: int64(len(req.GetContent())), Digest: digest}}, nil
}

func (s *Server) DeletePath(ctx context.Context, req *harnessxv1.DeletePathRequest) (*harnessxv1.DeletePathResponse, error) {
	if err := s.authorizeVolume(ctx, req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	if err := s.driver.Delete(req.GetVolumeId(), req.GetPath(), req.GetRecursive()); err != nil {
		return nil, pathError(err)
	}
	if err := s.store.DeleteObject(ctx, req.GetContext().GetTenantId(), req.GetVolumeId(), req.GetPath(), req.GetRecursive()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete path metadata: %v", err)
	}
	return &harnessxv1.DeletePathResponse{}, nil
}

func (s *Server) CreateDirectory(ctx context.Context, req *harnessxv1.CreateDirectoryRequest) (*harnessxv1.CreateDirectoryResponse, error) {
	if err := s.authorizeVolume(ctx, req.GetContext(), req.GetVolumeId()); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetPath()) == "" {
		return nil, status.Error(codes.InvalidArgument, "path is required")
	}
	if err := s.driver.CreateDirectory(req.GetVolumeId(), req.GetPath()); err != nil {
		return nil, pathError(err)
	}
	if err := s.store.UpsertObject(ctx, req.GetContext().GetTenantId(), req.GetVolumeId(), req.GetPath(), true, 0, "", "inode/directory", time.Now().UTC()); err != nil {
		return nil, status.Errorf(codes.Internal, "record directory metadata: %v", err)
	}
	return &harnessxv1.CreateDirectoryResponse{}, nil
}

func (s *Server) MintMount(context.Context, *harnessxv1.MintMountRequest) (*harnessxv1.MountGrant, error) {
	return nil, status.Error(codes.Unimplemented, "the local backend does not issue remote mount grants")
}

func (s *Server) authorizeVolume(ctx context.Context, requestContext *harnessxv1.RequestContext, volumeID string) error {
	if err := validateVolumeRequest(requestContext, volumeID); err != nil {
		return err
	}
	_, err := s.store.Get(ctx, requestContext.GetTenantId(), volumeID)
	return storeError(err)
}

func validateContext(ctx *harnessxv1.RequestContext) error {
	if ctx == nil || strings.TrimSpace(ctx.GetTenantId()) == "" {
		return status.Error(codes.InvalidArgument, "request context with tenant_id is required")
	}
	return nil
}

func validateVolumeRequest(ctx *harnessxv1.RequestContext, volumeID string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(volumeID) == "" {
		return status.Error(codes.InvalidArgument, "volume_id is required")
	}
	return nil
}

func storeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "volume not found")
	}
	return status.Errorf(codes.Internal, "volume metadata: %v", err)
}

func pathError(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Errorf(codes.InvalidArgument, "volume path: %v", err)
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
