package serviceusage

import (
	"context"
	"fmt"
	"strings"
	"time"

	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/serviceusage/v1"
)

const (
	// StateEnabled is Stateに設定するEnableの文字列
	StateEnabled = "ENABLED"

	// StateDisabled is Stateに設定するDisableの文字列
	StateDisabled = "DISABLED"
)

// ServiceUsageService is ServiceUsage Service
// https://cloud.google.com/service-usage/docs/overview
type ServiceUsageService struct {
	client      *serviceusage.Service
	crmv1Client *crmv1.Service
}

// NewService is return ServiceUsageService
func NewService(ctx context.Context, client *serviceusage.Service, crmv1Client *crmv1.Service) (*ServiceUsageService, error) {
	return &ServiceUsageService{
		client:      client,
		crmv1Client: crmv1Client,
	}, nil
}

// Service is ServiceUsage service model
// https://cloud.google.com/service-usage/docs/reference/rest/v1/Service の内容を格納するためのもの
// config は現状では使ってないので、入れていない
type Service struct {
	Name    string // ex. projects/{$PROJECT_NUMBER}/services/bigquery.googleapis.com
	APIName string // ex. bigquery.googleapis.com
	Parent  string // ex. projects/{$PROJECT_NUMBER}
	State   string // ENABLED or DISABLED
}

// ListAll is 指定した ProjectID の ServiceUsage を全件取得する
func (s *ServiceUsageService) ListAll(ctx context.Context, projectID string) ([]*Service, error) {
	return s.list(ctx, projectID, "")
}

// ListByStateEnabled is 指定した ProjectID の Enable になっている ServiceUsage を取得する
func (s *ServiceUsageService) ListByStateEnabled(ctx context.Context, projectID string) ([]*Service, error) {
	return s.list(ctx, projectID, StateEnabled)
}

// ListByStateDisabled is 指定した ProjectID の Disable になっている ServiceUsage を取得する
func (s *ServiceUsageService) ListByStateDisabled(ctx context.Context, projectID string) ([]*Service, error) {
	return s.list(ctx, projectID, StateDisabled)
}

// ServiceDiff is ServiceのDiffがある時に返す struct
type ServiceDiff struct {
	Base         *Service
	Target       *Service
	ExistsTarget bool // Targetが存在する場合はtrue. 基本的にはtrueのはずだが、αのものとかでもしかしたら、baseにはあるけど、targetにはないみたいなのがあるかもしれないので、存在している
}

// ListByDiff is base と target のServiceUsageを比較して、Stateの差がある場合、target の ServiceUsage を返す
func (s *ServiceUsageService) ListByDiff(ctx context.Context, baseProjectID string, targetProjectID string) ([]*ServiceDiff, error) {
	baseList, err := s.ListAll(ctx, baseProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed List ServiceUsage. projectID:%s : %w", baseProjectID, err)
	}

	target, err := s.ListAll(ctx, targetProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed List ServiceUsage. projectID:%s : %w", targetProjectID, err)
	}
	targetMap := map[string]*Service{}
	for _, v := range target {
		targetMap[v.APIName] = v
	}

	var result []*ServiceDiff
	for _, base := range baseList {
		target, ok := targetMap[base.APIName]
		if !ok {
			result = append(result, &ServiceDiff{
				Base:         base,
				Target:       target,
				ExistsTarget: false,
			})
			continue
		}
		if base.State != target.State {
			result = append(result, &ServiceDiff{
				Base:         base,
				Target:       target,
				ExistsTarget: true,
			})
		}
	}
	return result, nil
}

func (s *ServiceUsageService) list(ctx context.Context, projectID string, state string) ([]*Service, error) {
	p, err := s.getProject(ctx, projectID)
	if err != nil {
		return nil, err
	}

	var results []*Service
	var nextPageToken string
	for {
		call := s.client.Services.List(fmt.Sprintf("projects/%d", p.ProjectNumber)).Context(ctx)
		if state != "" {
			call.Filter(fmt.Sprintf("state:%s", state))
		}
		if nextPageToken != "" {
			call.PageToken(nextPageToken)
			time.Sleep(1 * time.Second) // requests per minute 60 に引っかからないように少し待つ
		}
		resp, err := call.Do()
		if err != nil {
			return nil, err
		}

		for _, v := range resp.Services {
			nl := strings.Split(v.Name, "/")
			results = append(results, &Service{
				Name:    v.Name,
				APIName: nl[len(nl)-1],
				Parent:  v.Parent,
				State:   v.State,
			})
		}
		nextPageToken = resp.NextPageToken
		if resp.NextPageToken == "" {
			break
		}
	}

	return results, nil
}

// SetState is APIをEnable/Disable にする
// name : projects/{$PROJECT_NUMBER}/services/bigquery.googleapis.com 形式の文字列
// state : StateEnabled or StateDisabled
func (s *ServiceUsageService) SetState(ctx context.Context, name string, state string) error {
	var ope *serviceusage.Operation
	var err error

	switch state {
	case StateEnabled:
		ope, err = s.client.Services.Enable(name, &serviceusage.EnableServiceRequest{}).Context(ctx).Do()
	case StateDisabled:
		ope, err = s.client.Services.Disable(name, &serviceusage.DisableServiceRequest{}).Context(ctx).Do()
	default:
		return NewUnsupportedState(state)
	}
	if err != nil {
		return fmt.Errorf("failed SetState. name:%s,state:%s : %w", name, state, err)
	}
	opeName := ope.Name
	for {
		if ope.Done {
			break
		}
		ope, err = s.client.Operations.Get(opeName).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("failed GetOperation. opeName:%s,name:%s,state:%s : %w", opeName, name, state, err)
		}
	}
	if ope.Error != nil {
		return fmt.Errorf("failed SetState. operation error. name:%s,state:%s : %v", name, state, ope.Error)
	}
	return nil
}

func (s *ServiceUsageService) getProject(ctx context.Context, projectID string) (*crmv1.Project, error) {
	p, err := s.crmv1Client.Projects.Get(projectID).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("failed get project. projectID:%s : %w", projectID, err)
	}
	return p, nil
}
