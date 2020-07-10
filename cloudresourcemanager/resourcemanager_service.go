package cloudresourcemanager

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	crmv2 "google.golang.org/api/cloudresourcemanager/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewResourceManagerService is return ResourceManagerService
func NewResourceManagerService(ctx context.Context, crmv1Service *crmv1.Service, crmv2Service *crmv2.Service) (*ResourceManagerService, error) {
	return &ResourceManagerService{
		crmv1: crmv1Service,
		crmv2: crmv2Service,
	}, nil
}

type ResourceManagerService struct {
	crmv1 *crmv1.Service
	crmv2 *crmv2.Service
}

// IamMember is GCP IAMのMember struct
type IamMember struct {
	Type  string
	Email string
}

// ExistsMemberInGCPProject is GCP Projectに指定したユーザが権限を持っているかを返す
// defaultだと何らかのroleを持っているかを返す。rolesを指定するといずれか1つ以上を持っているかを返す。
func (s *ResourceManagerService) ExistsMemberInGCPProject(ctx context.Context, projectID string, email string, roles ...string) (bool, error) {
	p, err := s.crmv1.Projects.GetIamPolicy(projectID, &crmv1.GetIamPolicyRequest{}).Context(ctx).Do()
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return false, fmt.Errorf("CloudResourceManager.Projects.GetIamPolicy response 404. projectID=%v,err=%v", projectID, err)
		}
		if status.Code(err) == codes.PermissionDenied {
			return false, fmt.Errorf("CloudResourceManager.Projects.GetIamPolicy response 403. projectID=%v,roles=%+v,err=%v", projectID, roles, err)
		}
		v, ok := err.(*googleapi.Error)
		if ok {
			if v.Code == http.StatusForbidden {
				return false, fmt.Errorf("CloudResourceManager.Projects.GetIamPolicy(Google APIs) response 403. projectID=%v,roles=%+v,err=%v", projectID, roles, err)
			}
		}

		return false, errors.WithStack(err)
	}
	roleMap := map[string]bool{}
	for _, role := range roles {
		roleMap[role] = true
	}

	for _, binding := range p.Bindings {
		if len(roleMap) > 0 {
			v := roleMap[binding.Role]
			if !v {
				continue
			}
		}
		hit, err := s.existsIamMember(binding.Members, email)
		if err != nil {
			return false, err
		}
		if hit {
			return true, nil
		}
	}

	return false, nil
}

func (s *ResourceManagerService) existsIamMember(members []string, email string) (bool, error) {
	for _, member := range members {
		iamMember, err := s.ConvertIamMember(member)
		if err != nil {
			return false, err
		}
		if iamMember.Type != "user" {
			continue
		}
		if email == iamMember.Email {
			return true, nil
		}
	}
	return false, nil
}

// ConvertIamMember is IAM RoleのAPIで取得できるMember文字列をIamMember structに変換して返す
func (s *ResourceManagerService) ConvertIamMember(member string) (*IamMember, error) {
	l := strings.Split(member, ":")
	if len(l) != 2 {
		return nil, fmt.Errorf("invalid iam member text. text:%v", member)
	}

	switch l[0] {
	case "user", "serviceAccount", "group", "domain":
		return &IamMember{l[0], l[1]}, nil
	default:
		return nil, fmt.Errorf("invalid iam member type. type:%v", l[0])
	}
}

// Folders 指定した parent の下にあるすべてのFolderを返す
// 階層構造は保持せずにフラットにすべてのFolderを返す
// parent は `folders/{folder_id}` or `organizations/{org_id}` の形式で指定する
func (s *ResourceManagerService) Folders(ctx context.Context, parent string) ([]*crmv2.Folder, error) {
	var folders []*crmv2.Folder
	var err error
	folders, err = s.folders(ctx, parent, folders)
	if err != nil {
		return nil, err
	}
	return folders, nil
}

func (s *ResourceManagerService) folders(ctx context.Context, parent string, dst []*crmv2.Folder) ([]*crmv2.Folder, error) {
	req := s.crmv2.Folders.List().Parent(parent)
	if err := req.Pages(ctx, func(page *crmv2.ListFoldersResponse) error {
		for _, folder := range page.Folders {
			l, err := s.folders(ctx, folder.Name, dst)
			if err != nil {
				return err
			}
			dst = append(dst, folder)
			dst = append(dst, l...)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return dst, nil
}

// Projects is 指定したリソース以下のProject一覧を返す
func (s *ResourceManagerService) Projects(ctx context.Context, parentID string) ([]*crmv1.Project, error) {
	req := s.crmv1.Projects.List()
	if len(parentID) > 0 {
		req.Filter(fmt.Sprintf("parent.id:%s", parentID))
	}

	var list []*crmv1.Project
	if err := req.Pages(ctx, func(page *crmv1.ListProjectsResponse) error {
		for _, project := range page.Projects {
			list = append(list, project)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	return list, nil
}
