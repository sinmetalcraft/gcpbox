package storage

import (
	"context"
	"encoding/base64"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iam/v1"
)

type StorageService struct {
	serviceAccountName string
	serviceAccountID   string
	iamService         *iam.Service
}

// NewStorageService is StorageServiceを生成する
//
// 利用するServiceAccountの roles/iam.serviceAccountTokenCreator https://cloud.google.com/iam/docs/service-accounts?hl=en#the_service_account_token_creator_role を持っている必要がある
// serviceAccountName is SignedURLを発行するServiceAccountの @ より前の値。ex. hoge@projectid.iam.gserviceaccount.com の場合は "hoge"
// serviceAccountID is serviceAccountNameに指定したものと同じServiceAccountのID。format "projects/%s/serviceAccounts/%s"。
// iamService is iamService
func NewStorageService(ctx context.Context, serviceAccountName string, serviceAccountID string, iamService *iam.Service) *StorageService {
	return &StorageService{
		serviceAccountName: serviceAccountName,
		serviceAccountID:   serviceAccountID,
		iamService:         iamService,
	}
}

// PutObjectSignedURL is ObjectをPutするSignedURLを発行する
func (s *StorageService) PutObjectSignedURL(ctx context.Context, bucket string, object string, expires time.Time) (string, error) {
	url, err := storage.SignedURL(bucket, object, &storage.SignedURLOptions{
		GoogleAccessID: s.serviceAccountName,
		Method:         "PUT",
		Expires:        expires,
		// To avoid management for private key, use SignBytes instead of PrivateKey.
		// In this example, we are using the `iam.serviceAccounts.signBlob` API for signing bytes.
		// If you hope to avoid API call for signing bytes every time,
		// you can use self hosted private key and pass it in Privatekey.
		SignBytes: func(b []byte) ([]byte, error) {
			resp, err := s.iamService.Projects.ServiceAccounts.SignBlob(
				s.serviceAccountID,
				&iam.SignBlobRequest{BytesToSign: base64.StdEncoding.EncodeToString(b)},
			).Context(ctx).Do()
			if err != nil {
				return nil, err
			}
			return base64.StdEncoding.DecodeString(resp.Signature)
		},
	})
	if err != nil {
		return "", errors.WithMessagef(err, "failed PutObjectSignedURL: saName=%s,saID=%s,bucket=%s,object=%s", s.serviceAccountName, s.serviceAccountID, bucket, object)
	}
	return url, nil
}
