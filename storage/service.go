package storage

import (
	"context"

	gcs "cloud.google.com/go/storage"
)

// StorageService is Storage Util Service
type StorageService struct {
	StorageClient *gcs.Client
}

// NewStorageService is StorageServiceを生成する
func NewStorageService(ctx context.Context, gcs *gcs.Client) (*StorageService, error) {
	return &StorageService{
		StorageClient: gcs,
	}, nil
}
