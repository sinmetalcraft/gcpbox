package asset

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/api/cloudasset/v1"
)

type Scope interface {
	Scope() string
}

type OrganizationScope struct {
	Number string
}

func (s *OrganizationScope) Scope() string {
	return fmt.Sprintf("organizations/%s", s.Number)
}

func (s *OrganizationScope) String() string {
	return s.Scope()
}

type FolderScope struct {
	IDOrNumber string
}

func (s *FolderScope) Scope() string {
	return fmt.Sprintf("folders/%s", s.IDOrNumber)
}

func (s *FolderScope) String() string {
	return s.Scope()
}

type ProjectScope struct {
	IDOrNumber string
}

func (s *ProjectScope) Scope() string {
	return fmt.Sprintf("projects/%s", s.IDOrNumber)
}

func (s *ProjectScope) String() string {
	return s.Scope()
}

type Service struct {
	AssetService *cloudasset.Service
}

func NewService(ctx context.Context, assetService *cloudasset.Service) (*Service, error) {
	return &Service{
		AssetService: assetService,
	}, nil
}

type Project struct {
	ProjectID              string    `json:"projectID"`
	ProjectNumber          string    `json:"projectNumber"`
	DisplayName            string    `json:"displayName"`
	State                  string    `json:"state"`
	OrganizationNumber     string    `json:"organizationNumber"`
	ParentFullResourceName string    `json:"parentFullResourceName"`
	CreateTime             time.Time `json:"createTime"`
}

const (
	OrderByCreateTime                 = "createTime"
	OrderByCreateTimeDesc             = "createTime DESC"
	OrderByProjectID                  = "name"
	OrderByProjectIDDesc              = "name DESC"
	OrderByParentFullResourceName     = "parentFullResourceName"
	OrderByParentFullResourceNameDesc = "parentFullResourceName DESC"
)

func (s *Service) ListProject(ctx context.Context, scope Scope, query string, orderBy string) (rets []*Project, err error) {
	const assetTypes = "cloudresourcemanager.googleapis.com/Project"

	call := s.AssetService.V1.SearchAllResources(scope.Scope()).AssetTypes(assetTypes).Context(ctx)
	if query != "" {
		call = call.Query(query)
	}
	if orderBy != "" {
		call = call.OrderBy(orderBy)
	}
	resp, err := call.Do()
	if err != nil {
		return nil, err
	}
	for _, v := range resp.Results {
		attributes := map[string]interface{}{}
		if err := json.Unmarshal(v.AdditionalAttributes, &attributes); err != nil {
			return nil, fmt.Errorf("failed parse AdditionalAttributes %s: %w", v.AdditionalAttributes, err)
		}
		projectNumber := strings.ReplaceAll(v.Project, "projects/", "")
		orgNumber := strings.ReplaceAll(v.Organization, "organizations/", "")
		createTime, err := time.Parse(time.RFC3339, v.CreateTime)
		if err != nil {
			return nil, fmt.Errorf("failed parse CreateTime %s: %w", v.CreateTime, err)
		}
		rets = append(rets, &Project{
			ProjectID:              attributes["projectId"].(string),
			ProjectNumber:          projectNumber,
			DisplayName:            v.DisplayName,
			State:                  v.State,
			OrganizationNumber:     orgNumber,
			ParentFullResourceName: v.ParentFullResourceName,
			CreateTime:             createTime,
		})
	}
	return rets, nil
}
