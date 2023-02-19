package asset

import (
	"context"
	"fmt"
	"strings"
	"time"

	cloudasset "cloud.google.com/go/asset/apiv1"
	assetpb "cloud.google.com/go/asset/apiv1/assetpb"
	"google.golang.org/api/iterator"
)

type Scope interface {
	Scope() string
}

type OrganizationScope struct {
	ID string
}

func (s *OrganizationScope) Scope() string {
	return fmt.Sprintf("organizations/%s", s.ID)
}

func (s *OrganizationScope) String() string {
	return s.Scope()
}

type FolderScope struct {
	ID string
}

func (s *FolderScope) Scope() string {
	return fmt.Sprintf("folders/%s", s.ID)
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
	AssetClient *cloudasset.Client
}

func NewService(ctx context.Context, assetClient *cloudasset.Client) (*Service, error) {
	return &Service{
		AssetClient: assetClient,
	}, nil
}

type Project struct {
	ProjectID              string    `json:"projectID"`
	ProjectNumber          string    `json:"projectNumber"`
	DisplayName            string    `json:"displayName"`
	State                  string    `json:"state"`
	OrganizationID         string    `json:"organizationID"`
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
	const projectAssetType = "cloudresourcemanager.googleapis.com/Project"

	req := &assetpb.SearchAllResourcesRequest{
		Scope:      scope.Scope(),
		AssetTypes: []string{projectAssetType},
		Query:      query,
		OrderBy:    orderBy,
	}

	iter := s.AssetClient.SearchAllResources(ctx, req)
	for {
		ret, err := iter.Next()
		if err == iterator.Done {
			break
		}

		projectNumber := strings.ReplaceAll(ret.GetProject(), "projects/", "")
		orgID := strings.ReplaceAll(ret.GetOrganization(), "organizations/", "")
		createTime := ret.GetCreateTime().AsTime()
		rets = append(rets, &Project{
			ProjectID:              ret.GetAdditionalAttributes().GetFields()["projectId"].GetStringValue(),
			ProjectNumber:          projectNumber,
			DisplayName:            ret.GetDisplayName(),
			State:                  ret.GetState(),
			OrganizationID:         orgID,
			ParentFullResourceName: ret.GetParentFullResourceName(),
			CreateTime:             createTime,
		})
	}

	return rets, nil
}
