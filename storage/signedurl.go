package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	credentials "cloud.google.com/go/iam/credentials/apiv1"
	"cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iam/v1"
)

// StorageSignedURLService is Storage Signed URL Util Service
type StorageSignedURLService struct {
	ServiceAccountEmail  string
	IAMService           *iam.Service
	IAMCredentialsClient *credentials.IamCredentialsClient
}

// NewStorageSignedURLService is StorageServiceを生成する
//
// 利用するServiceAccountの roles/iam.serviceAccountTokenCreator https://cloud.google.com/iam/docs/service-accounts?hl=en#the_service_account_token_creator_role を持っている必要がある
// serviceAccountEmail is SignedURLを発行するServiceAccountEmail
// iamService is iamService
// iamCredentialsClient is iamCredentialsClient
func NewStorageSignedURLService(ctx context.Context, serviceAccountEmail string, iamService *iam.Service, iamCredentialsClient *credentials.IamCredentialsClient) (*StorageSignedURLService, error) {
	// だいたい `@` より前の値だけを入れてしまうケースが多いので、とりあえず `@` が入ってるかだけチェックしている
	if !strings.Contains(serviceAccountEmail, "@") {
		return nil, NewErrInvalidFormat("invalid ServiceAccountEmail.", map[string]interface{}{"serviceAccountEmail": serviceAccountEmail}, nil)
	}
	return &StorageSignedURLService{
		ServiceAccountEmail:  serviceAccountEmail,
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
		return "", fmt.Errorf("failed CreatePutObjectURL: %w", err)
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
		return "", fmt.Errorf("failed CreateDownloadURL: %w", err)
	}
	return u, nil
}

// CreateSignedURL is 便利そうなレイヤーを挟まず、まるっと全部指定してSigned URLを生成する
func (s *StorageSignedURLService) CreateSignedURL(ctx context.Context, bucket string, object string, method string, contentType string, headers []string, queryParameters url.Values, expires time.Time) (string, error) {
	opt := &storage.SignedURLOptions{
		GoogleAccessID:  s.ServiceAccountEmail,
		Method:          method,
		Expires:         expires,
		ContentType:     contentType,
		Headers:         headers,
		QueryParameters: queryParameters,
		Scheme:          storage.SigningSchemeV4,
		SignBytes: func(b []byte) ([]byte, error) {
			req := &credentialspb.SignBlobRequest{
				Name:    fmt.Sprintf("projects/-/serviceAccounts/%s", s.ServiceAccountEmail),
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
		return "", fmt.Errorf("failed createSignedURL: sa=%s,bucket=%s,object=%s : %w", s.ServiceAccountEmail, bucket, object, err)
	}
	return u, nil
}
