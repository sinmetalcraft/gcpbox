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

type FolderScope struct {
	IDOrNumber string
}

func (s *FolderScope) Scope() string {
	return fmt.Sprintf("folders/%s", s.IDOrNumber)
}

type ProjectScope struct {
	IDOrNumber string
}

func (s *ProjectScope) Scope() string {
	return fmt.Sprintf("projects/%s", s.IDOrNumber)
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

/*
	{
	  "name": "//cloudresourcemanager.googleapis.com/projects/sinmetalcraft-monitoring-all1",
	  "assetType": "cloudresourcemanager.googleapis.com/Project",
	  "project": "projects/336622473699",
	  "displayName": "sinmetalcraft-monitoring-all1",
	  "location": "global",
	  "additionalAttributes": {
	    "projectId": "sinmetalcraft-monitoring-all1"
	  },
	  "createTime": "2023-01-19T08:30:31Z",
	  "state": "ACTIVE",
	  "organization": "organizations/190932998497",
	  "parentFullResourceName": "//cloudresourcemanager.googleapis.com/organizations/190932998497",
	  "parentAssetType": "cloudresourcemanager.googleapis.com/Organization"
	},
*/
func (s *Service) ListProject(ctx context.Context, scope Scope, orderBy string) (rets []*Project, err error) {
	const assetTypes = "cloudresourcemanager.googleapis.com/Project"
	resp, err := s.AssetService.V1.SearchAllResources(scope.Scope()).AssetTypes(assetTypes).OrderBy(orderBy).Context(ctx).Do()
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
