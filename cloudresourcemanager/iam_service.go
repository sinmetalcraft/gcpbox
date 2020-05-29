package cloudresourcemanager

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewResourceManagerService is return ResourceManagerService
func NewResourceManagerService(ctx context.Context, crmService *cloudresourcemanager.Service) (*ResourceManagerService, error) {
	return &ResourceManagerService{
		crm: crmService,
	}, nil
}

type ResourceManagerService struct {
	crm *cloudresourcemanager.Service
}

// IamMember is GCP IAMのMember struct
type IamMember struct {
	Type  string
	Email string
}

// ExistsMemberInGCPProject is GCP Projectに指定したユーザが権限を持っているかを返す
// defaultだと何らかのroleを持っているかを返す。rolesを指定するといずれか1つ以上を持っているかを返す。
func (s *ResourceManagerService) ExistsMemberInGCPProject(ctx context.Context, projectID string, email string, roles ...string) (bool, error) {
	p, err := s.crm.Projects.GetIamPolicy(projectID, &cloudresourcemanager.GetIamPolicyRequest{}).Context(ctx).Do()
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
