package cloudresourcemanager

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

	"golang.org/x/xerrors"
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
	Type    string
	Email   string
	Deleted bool
	UID     string
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

		return false, xerrors.Errorf(": %w", err)
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
// 削除済みのメンバーのフォーマットは https://cloud.google.com/iam/docs/policies#handle-deleted-members
func (s *ResourceManagerService) ConvertIamMember(member string) (*IamMember, error) {
	l := strings.Split(member, ":")
	if len(l) < 1 {
		return nil, fmt.Errorf("invalid iam member format. text:%v", member)
	}

	switch l[0] {
	case "user", "serviceAccount", "group", "domain":
		if len(l) < 2 {
			return nil, fmt.Errorf("invalid iam member account. text=%v", member)
		}
		return &IamMember{l[0], l[1], false, ""}, nil
	case "deleted":
		if len(l) < 3 {
			return nil, fmt.Errorf("invalid deleted iam member format. text=%v", member)
		}

		accountTxts := strings.Split(l[2], "?")
		if len(accountTxts) != 2 {
			return nil, fmt.Errorf("invalid deleted iam member account txt format. text=%v", member)
		}

		// QueryStringのようなFormatでくっついている値がuidのみであると決め打ちしている
		uids := strings.Split(accountTxts[1], "=")
		if len(uids) != 2 {
			return nil, fmt.Errorf("invalid deleted iam member uid txt format. text=%v", member)
		}
		im, err := s.ConvertIamMember(fmt.Sprintf("%s:%s", l[1], accountTxts[0]))
		if err != nil {
			return nil, xerrors.Errorf("invalid deleted iam member text. text=%v : %w", member, err)
		}
		im.Deleted = true
		im.UID = uids[1]
		return im, nil
	default:
		return nil, fmt.Errorf("invalid iam member type. type:%v, text:%v", l[0], member)
	}
}

// Folder: A Folder in an Organization's resource hierarchy, used
// to
// organize that Organization's resources.
type Folder struct {
	// CreateTime: Output only. Timestamp when the Folder was created.
	// Assigned by the server.
	CreateTime string `json:"createTime,omitempty"`

	// DisplayName: The folder’s display name.
	// A folder’s display name must be unique amongst its siblings,
	// e.g.
	// no two folders with the same parent can share the same display
	// name.
	// The display name must start and end with a letter or digit, may
	// contain
	// letters, digits, spaces, hyphens and underscores and can be no
	// longer
	// than 30 characters. This is captured by the regular
	// expression:
	// [\p{L}\p{N}]([\p{L}\p{N}_- ]{0,28}[\p{L}\p{N}])?.
	DisplayName string `json:"displayName,omitempty"`

	// LifecycleState: Output only. The lifecycle state of the
	// folder.
	// Updates to the lifecycle_state must be performed via
	// DeleteFolder and
	// UndeleteFolder.
	//
	// Possible values:
	//   "LIFECYCLE_STATE_UNSPECIFIED" - Unspecified state.
	//   "ACTIVE" - The normal and active state.
	//   "DELETE_REQUESTED" - The folder has been marked for deletion by the
	// user.
	LifecycleState string `json:"lifecycleState,omitempty"`

	// Name: Output only. The resource name of the Folder.
	// Its format is `folders/{folder_id}`, for example: "folders/1234".
	Name string `json:"name,omitempty"`

	// Parent: Required. The Folder’s parent's resource name.
	// Updates to the folder's parent must be performed via
	// MoveFolder.
	Parent string `json:"parent,omitempty"`
}

// Project: A Project is a high-level Google Cloud Platform entity.  It
// is a
// container for ACLs, APIs, App Engine Apps, VMs, and other
// Google Cloud Platform resources.
type Project struct {
	// CreateTime: Creation time.
	//
	// Read-only.
	CreateTime string `json:"createTime,omitempty"`

	// Labels: The labels associated with this Project.
	//
	// Label keys must be between 1 and 63 characters long and must
	// conform
	// to the following regular expression:
	// \[a-z\](\[-a-z0-9\]*\[a-z0-9\])?.
	//
	// Label values must be between 0 and 63 characters long and must
	// conform
	// to the regular expression (\[a-z\](\[-a-z0-9\]*\[a-z0-9\])?)?. A
	// label
	// value can be empty.
	//
	// No more than 256 labels can be associated with a given
	// resource.
	//
	// Clients should store labels in a representation such as JSON that
	// does not
	// depend on specific characters being disallowed.
	//
	// Example: <code>"environment" : "dev"</code>
	// Read-write.
	Labels map[string]string `json:"labels,omitempty"`

	// LifecycleState: The Project lifecycle state.
	//
	// Read-only.
	//
	// Possible values:
	//   "LIFECYCLE_STATE_UNSPECIFIED" - Unspecified state.  This is only
	// used/useful for distinguishing
	// unset values.
	//   "ACTIVE" - The normal and active state.
	//   "DELETE_REQUESTED" - The project has been marked for deletion by
	// the user
	// (by invoking
	// DeleteProject)
	// or by the system (Google Cloud Platform).
	// This can generally be reversed by invoking UndeleteProject.
	//   "DELETE_IN_PROGRESS" - This lifecycle state is no longer used and
	// not returned by the API.
	LifecycleState string `json:"lifecycleState,omitempty"`

	// Name: The optional user-assigned display name of the Project.
	// When present it must be between 4 to 30 characters.
	// Allowed characters are: lowercase and uppercase letters,
	// numbers,
	// hyphen, single-quote, double-quote, space, and exclamation
	// point.
	//
	// Example: <code>My Project</code>
	// Read-write.
	Name string `json:"name,omitempty"`

	// Parent: An optional reference to a parent Resource.
	//
	// Supported parent types include "organization" and "folder". Once set,
	// the
	// parent cannot be cleared. The `parent` can be set on creation or
	// using the
	// `UpdateProject` method; the end user must have
	// the
	// `resourcemanager.projects.create` permission on the
	// parent.
	//
	// Read-write.
	Parent *ResourceID `json:"parent,omitempty"`

	// ProjectId: The unique, user-assigned ID of the Project.
	// It must be 6 to 30 lowercase letters, digits, or hyphens.
	// It must start with a letter.
	// Trailing hyphens are prohibited.
	//
	// Example: <code>tokyo-rain-123</code>
	// Read-only after creation.
	ProjectID string `json:"projectId,omitempty"`

	// ProjectNumber: The number uniquely identifying the project.
	//
	// Example: <code>415104041262</code>
	// Read-only.
	ProjectNumber int64 `json:"projectNumber,omitempty,string"`
}

// ResourceId: A container to reference an id for any resource type. A
// `resource` in Google
// Cloud Platform is a generic term for something you (a developer) may
// want to
// interact with through one of our API's. Some examples are an App
// Engine app,
// a Compute Engine instance, a Cloud SQL database, and so on.
type ResourceID struct {
	// Id: Required field for the type-specific id. This should correspond
	// to the id
	// used in the type-specific API's.
	ID string `json:"id,omitempty"`

	// Type: Required field representing the resource type this id is
	// for.
	// At present, the valid types are: "organization", "folder", and
	// "project".
	Type string `json:"type,omitempty"`
}

// Folders 指定した parent の下にあるすべてのFolderを返す
// 階層構造は保持せずにフラットにすべてのFolderを返す
// parent は `folders/{folder_id}` or `organizations/{org_id}` の形式で指定する
func (s *ResourceManagerService) Folders(ctx context.Context, parent string) ([]*Folder, error) {
	var folders []*Folder
	var err error
	folders, err = s.folders(ctx, parent, folders)
	if err != nil {
		return nil, err
	}
	return folders, nil
}

func (s *ResourceManagerService) folders(ctx context.Context, parent string, dst []*Folder) ([]*Folder, error) {
	req := s.crmv2.Folders.List().Parent(parent)
	if err := req.Pages(ctx, func(page *crmv2.ListFoldersResponse) error {
		for _, folder := range page.Folders {
			l, err := s.folders(ctx, folder.Name, dst)
			if err != nil {
				return err
			}
			dst = append(dst, &Folder{
				Name:           folder.Name,
				Parent:         folder.Parent,
				LifecycleState: folder.LifecycleState,
				CreateTime:     folder.CreateTime,
			})
			dst = append(dst, l...)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return dst, nil
}

// Projects is 指定したリソース以下のProject一覧を返す
func (s *ResourceManagerService) Projects(ctx context.Context, parentID string) ([]*Project, error) {
	req := s.crmv1.Projects.List()
	if len(parentID) > 0 {
		req.Filter(fmt.Sprintf("parent.id:%s", parentID))
	}

	var list []*Project
	if err := req.Pages(ctx, func(page *crmv1.ListProjectsResponse) error {
		for _, project := range page.Projects {
			p := &Project{
				ProjectID:      project.ProjectId,
				ProjectNumber:  project.ProjectNumber,
				Name:           project.Name,
				LifecycleState: project.LifecycleState,
				Labels:         project.Labels,
				CreateTime:     project.CreateTime,
			}
			if project.Parent != nil {
				p.Parent = &ResourceID{
					ID:   project.Parent.Id,
					Type: project.Parent.Type,
				}
			}

			list = append(list, p)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	return list, nil
}
