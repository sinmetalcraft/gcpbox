package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	"cloud.google.com/go/storage"
	"golang.org/x/xerrors"
	"google.golang.org/api/iam/v1"
	credentialspb "google.golang.org/genproto/googleapis/iam/credentials/v1"
)

// StorageSignedURLService is Storage Signed URL Util Service
type StorageSignedURLService struct {
	ServiceAccountName   string
	ServiceAccountID     string
	IAMService           *iam.Service
	IAMCredentialsClient *credentials.IamCredentialsClient
}

// NewStorageSignedURLService is StorageServiceを生成する
//
// 利用するServiceAccountの roles/iam.serviceAccountTokenCreator https://cloud.google.com/iam/docs/service-accounts?hl=en#the_service_account_token_creator_role を持っている必要がある
// serviceAccountName is SignedURLを発行するServiceAccountEmail
// serviceAccountID is serviceAccountNameに指定したものと同じServiceAccountのID。format "projects/%s/serviceAccounts/%s"。
// iamService is iamService
// iamCredentialsClient is iamCredentialsClient
func NewStorageSignedURLService(ctx context.Context, serviceAccountName string, serviceAccountID string, iamService *iam.Service, iamCredentialsClient *credentials.IamCredentialsClient) (*StorageSignedURLService, error) {
	return &StorageSignedURLService{
		ServiceAccountName:   serviceAccountName,
		ServiceAccountID:     serviceAccountID,
		IAMService:           iamService,
		IAMCredentialsClient: iamCredentialsClient,
	}, nil
}

// CreateSignedURLForPutObject is ObjectをPutするSignedURLを発行する
//
// contentLength is optional
// contentLengthを指定すると、HeaderにContent-Lengthが追加される
func (s *StorageSignedURLService) CreatePutObjectURL(ctx context.Context, bucket string, object string, contentType string, expires time.Time) (string, error) {
	u, err := s.CreateSignedURL(ctx, bucket, object, http.MethodPut, contentType, []string{}, url.Values{}, expires)
	if err != nil {
		return "", xerrors.Errorf("failed CreatePutObjectURL: %w", err)
	}
	return u, nil
}

// CreateDownloadSignedURLParam is param for create download signed url
type CreateDownloadSignedURLParam struct {
	// DownloadFileName is Download時のFileName
	// 指定しない場合はCloud Storageの最後の "/" 以降の Object Name
	// optional
	DownloadFileName string `json:"downloadFileName"`
	// Attachment is ファイルダウンロードを強制する
	Attachment bool `json:"isAttachment"`
	// Download時のContentTypeを指定する
	// 指定しない場合はCloud StorageのObjectのContent Type
	// optional
	DownloadContentType string `json:"downloadContentType"`
}

// CreateDownloadURL
func (s *StorageSignedURLService) CreateDownloadURL(ctx context.Context, bucket string, object string, expires time.Time, param *CreateDownloadSignedURLParam) (string, error) {
	qp := url.Values{}
	if param != nil {
		var fileName string
		var cd string
		if len(param.DownloadFileName) > 0 {
			fileName = param.DownloadFileName
		}

		if param.Attachment {
			cd = fmt.Sprintf(`attachment;filename*=UTF-8''%s`, url.PathEscape(fileName))
			qp.Set("response-content-disposition", cd)
		}

		if len(param.DownloadContentType) > 0 {
			qp.Set("response-content-type", param.DownloadContentType)
		}
	}
	u, err := s.CreateSignedURL(ctx, bucket, object, http.MethodGet, "", []string{}, qp, expires)
	if err != nil {
		return "", xerrors.Errorf("failed CreateDownloadURL: %w", err)
	}
	return u, nil
}

// CreateSignedURL is 便利そうなレイヤーを挟まず、まるっと全部指定してSigned URLを生成する
func (s *StorageSignedURLService) CreateSignedURL(ctx context.Context, bucket string, object string, method string, contentType string, headers []string, queryParameters url.Values, expires time.Time) (string, error) {
	opt := &storage.SignedURLOptions{
		GoogleAccessID:  s.ServiceAccountName,
		Method:          method,
		Expires:         expires,
		ContentType:     contentType,
		Headers:         headers,
		QueryParameters: queryParameters,
		Scheme:          storage.SigningSchemeV4,
		SignBytes: func(b []byte) ([]byte, error) {
			req := &credentialspb.SignBlobRequest{
				Name:    fmt.Sprintf("projects/-/serviceAccounts/%s", s.ServiceAccountName),
				Payload: b,
			}
			resp, err := s.IAMCredentialsClient.SignBlob(ctx, req)
			if err != nil {
				return nil, err
			}
			return resp.SignedBlob, nil
		},
	}
	u, err := storage.SignedURL(bucket, object, opt)
	if err != nil {
		return "", xerrors.Errorf("failed createSignedURL: saName=%s,saID=%s,bucket=%s,object=%s : %w", s.ServiceAccountName, s.ServiceAccountID, bucket, object, err)
	}
	return u, nil
}
