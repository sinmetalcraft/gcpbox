package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/api/iam/v1"

	. "github.com/sinmetal/gcpbox/storage"
)

func TestStorageService_SignedURL(t *testing.T) {
	ctx := context.Background()

	s := newStorageSignedURLService(t)

	_, err := s.CreatePutObjectURL(ctx, "sinmetal-ci-storage", "hoge", time.Now().Add(600*time.Second))
	if err != nil {
		t.Fatal(err)
	}
}

func newStorageSignedURLService(t *testing.T) *StorageSignedURLService {
	ctx := context.Background()

	iamService, err := iam.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const projectID = "sinmetal-ci"
	saID := fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, "storage@sinmetal-ci.iam.gserviceaccount.com")
	s, err := NewStorageSignedURLService(ctx, "storage", saID, iamService)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
