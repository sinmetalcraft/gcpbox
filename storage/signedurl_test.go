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

	s := newStorageService(t)

	_, err := s.PutObjectSignedURL(ctx, "sinmetal-ci-storage", "hoge", time.Now().Add(600*time.Second))
	if err != nil {
		t.Fatal(err)
	}
}

func newStorageService(t *testing.T) *StorageService {
	ctx := context.Background()

	iamService, err := iam.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const projectID = "sinmetal-ci"
	saID := fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, "storage@sinmetal-ci.iam.gserviceaccount.com")
	return NewStorageService(ctx, "storage", saID, iamService)
}
