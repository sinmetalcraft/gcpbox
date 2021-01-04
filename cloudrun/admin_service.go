package cloudrun

import (
	"context"
	"fmt"

	"golang.org/x/xerrors"
	"google.golang.org/api/option"
	"google.golang.org/api/run/v1"
)

// Service is Cloud Run Admin API Utility
//
// managed版とAnthos版が合体して作られていて、構造が複雑なので、sinmetalがよくやる操作だけを集めたUtility
// managed版のみ扱うようになっている
// https://cloud.google.com/run/docs/reference/rest
type AdminService struct {
	Client *run.APIService
}

// PrimitiveAPIContainer is google-api-go-client の Cloud Run Admin API Service の入れ物
// Cloud Run Admin API Service は endpoint を指定しないとほとんど 400 Bad Request が返ってくるという存在なので、 Region を指定した Endpoint を設定して欲しいという気持ちが溢れている
type PrimitiveAPIContainer struct {
	api *run.APIService
}

// PrimitiveAPIContainer is google-api-go-client の Cloud Run Admin API Service を作成する
// Cloud Run Admin API は Endpoint で Region を指定する必要があり、指定しない場合、だいたい 400 Bad Request しか返ってこない
// https://cloud.google.com/run/docs/reference/rest#service-endpoint
// region ex. asia-northeast1
func NewPrimitiveAPIContainer(ctx context.Context, region string) (*PrimitiveAPIContainer, error) {
	api, err := run.NewService(ctx, option.WithEndpoint(fmt.Sprintf("https://%s-run.googleapis.com", region)))
	if err != nil {
		return nil, err
	}
	return &PrimitiveAPIContainer{api: api}, nil
}

// NewSimplePrimitiveAPIContainer is google-api-go-client の Cloud Run Admin API Service を内包した入れ物を作る
// NewAdminService() の引数を作るためのもの。
// 基本的には NewPrimitiveAPIContainer を使えばよいが、 Option を指定したい時や、mock を使いたい時とかに利用する
func NewSimplePrimitiveAPIContainer(ctx context.Context, service *run.APIService) *PrimitiveAPIContainer {
	return &PrimitiveAPIContainer{api: service}
}

// NewAdminService is return AdminService
func NewAdminService(ctx context.Context, api *PrimitiveAPIContainer) (*AdminService, error) {
	return &AdminService{
		Client: api.api,
	}, nil
}

// RunService is Cloud Run Service の入れ物
// https://cloud.google.com/run/docs/reference/rest/v1/namespaces.services#Service から使いそうなやつだけ取ってきている
// ここの URL が欲しくて、この package が生まれたと言っても過言ではない
type RunService struct {
	Namespace          string // managed Cloud Run の場合、ProjectNumber が入ってる
	Name               string // Service Name
	URL                string // Cloud Run Service の Endpoint
	ServiceAccountName string
}

// ListRunService is get Cloud Run Service List
func (s *AdminService) ListRunService(ctx context.Context, projectID string) ([]*RunService, error) {
	var results []*RunService
	var pageToken string
	for {
		call := s.Client.Namespaces.Services.List(fmt.Sprintf("namespaces/%s", projectID)).Limit(100).Context(ctx)
		if pageToken != "" {
			call.Continue(pageToken)
		}
		res, err := call.Do()
		if err != nil {
			return nil, xerrors.Errorf("failed AdminService.ListRunService. projectID:%s : %w", projectID, err)
		}
		for _, item := range res.Items {
			results = append(results, &RunService{
				Namespace:          item.Metadata.Namespace,
				Name:               item.Metadata.Name,
				URL:                item.Status.Url,
				ServiceAccountName: item.Spec.Template.Spec.ServiceAccountName,
			})
		}
		if res.Metadata.Continue == "" {
			break
		}
		pageToken = res.Metadata.Continue
	}
	return results, nil
}

// GetRunService is 指定した Cloud Run Service を取得する
func (s *AdminService) GetRunService(ctx context.Context, projectID string, name string) (*RunService, error) {
	item, err := s.Client.Namespaces.Services.Get(fmt.Sprintf("namespaces/%s/services/%s", projectID, name)).Context(ctx).Do()
	if err != nil {
		return nil, xerrors.Errorf("failed AdminService.GetRunService. projectID:%s,name=%s : %w", projectID, name, err)
	}
	return &RunService{
		Namespace:          item.Metadata.Namespace,
		Name:               item.Metadata.Name,
		URL:                item.Status.Url,
		ServiceAccountName: item.Spec.Template.Spec.ServiceAccountName,
	}, nil
}
