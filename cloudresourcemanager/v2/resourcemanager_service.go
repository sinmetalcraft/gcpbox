package cloudresourcemanager

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/xerrors"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	crmv2 "google.golang.org/api/cloudresourcemanager/v2"
	"google.golang.org/api/googleapi"
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

// Expr: Represents a textual expression in the Common Expression
// Language (CEL)
// syntax. CEL is a C-like expression language. The syntax and semantics
// of CEL
// are documented at https://github.com/google/cel-spec.
//
// Example (Comparison):
//
//     title: "Summary size limit"
//     description: "Determines if a summary is less than 100 chars"
//     expression: "document.summary.size() < 100"
//
// Example (Equality):
//
//     title: "Requestor is owner"
//     description: "Determines if requestor is the document owner"
//     expression: "document.owner ==
// request.auth.claims.email"
//
// Example (Logic):
//
//     title: "Public documents"
//     description: "Determine whether the document should be publicly
// visible"
//     expression: "document.type != 'private' && document.type !=
// 'internal'"
//
// Example (Data Manipulation):
//
//     title: "Notification string"
//     description: "Create a notification string with a timestamp."
//     expression: "'New message received at ' +
// string(document.create_time)"
//
// The exact variables and functions that may be referenced within an
// expression
// are determined by the service that evaluates it. See the
// service
// documentation for additional information.
type Expr struct {
	// Description: Optional. Description of the expression. This is a
	// longer text which
	// describes the expression, e.g. when hovered over it in a UI.
	Description string `json:"description,omitempty"`

	// Expression: Textual representation of an expression in Common
	// Expression Language
	// syntax.
	Expression string `json:"expression,omitempty"`

	// Location: Optional. String indicating the location of the expression
	// for error
	// reporting, e.g. a file name and a position in the file.
	Location string `json:"location,omitempty"`

	// Title: Optional. Title for the expression, i.e. a short string
	// describing
	// its purpose. This can be used e.g. in UIs which allow to enter
	// the
	// expression.
	Title string `json:"title,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Description") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Description") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

// Binding: Associates `members` with a `role`.
type Binding struct {
	// Condition: The condition that is associated with this binding.
	//
	// If the condition evaluates to `true`, then this binding applies to
	// the
	// current request.
	//
	// If the condition evaluates to `false`, then this binding does not
	// apply to
	// the current request. However, a different role binding might grant
	// the same
	// role to one or more of the members in this binding.
	//
	// To learn which resources support conditions in their IAM policies,
	// see
	// the
	// [IAM
	// documentation](https://cloud.google.com/iam/help/conditions/r
	// esource-policies).
	Condition *Expr `json:"condition,omitempty"`

	// Members: Specifies the identities requesting access for a Cloud
	// Platform resource.
	// `members` can have the following values:
	//
	// * `allUsers`: A special identifier that represents anyone who is
	//    on the internet; with or without a Google account.
	//
	// * `allAuthenticatedUsers`: A special identifier that represents
	// anyone
	//    who is authenticated with a Google account or a service
	// account.
	//
	// * `user:{emailid}`: An email address that represents a specific
	// Google
	//    account. For example, `alice@example.com` .
	//
	//
	// * `serviceAccount:{emailid}`: An email address that represents a
	// service
	//    account. For example,
	// `my-other-app@appspot.gserviceaccount.com`.
	//
	// * `group:{emailid}`: An email address that represents a Google
	// group.
	//    For example, `admins@example.com`.
	//
	// * `deleted:user:{emailid}?uid={uniqueid}`: An email address (plus
	// unique
	//    identifier) representing a user that has been recently deleted.
	// For
	//    example, `alice@example.com?uid=123456789012345678901`. If the
	// user is
	//    recovered, this value reverts to `user:{emailid}` and the
	// recovered user
	//    retains the role in the binding.
	//
	// * `deleted:serviceAccount:{emailid}?uid={uniqueid}`: An email address
	// (plus
	//    unique identifier) representing a service account that has been
	// recently
	//    deleted. For example,
	//
	// `my-other-app@appspot.gserviceaccount.com?uid=123456789012345678901`.
	//
	//    If the service account is undeleted, this value reverts to
	//    `serviceAccount:{emailid}` and the undeleted service account
	// retains the
	//    role in the binding.
	//
	// * `deleted:group:{emailid}?uid={uniqueid}`: An email address (plus
	// unique
	//    identifier) representing a Google group that has been recently
	//    deleted. For example,
	// `admins@example.com?uid=123456789012345678901`. If
	//    the group is recovered, this value reverts to `group:{emailid}`
	// and the
	//    recovered group retains the role in the binding.
	//
	//
	// * `domain:{domain}`: The G Suite domain (primary) that represents all
	// the
	//    users of that domain. For example, `google.com` or
	// `example.com`.
	//
	//
	Members []string `json:"members,omitempty"`

	// Role: Role that is assigned to `members`.
	// For example, `roles/viewer`, `roles/editor`, or `roles/owner`.
	Role string `json:"role,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Condition") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Condition") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
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

	// ID is FolderID
	// e.g. folder/1234 -> 1234
	ID string

	// Parent: Required. The Folder’s parent's resource name.
	// Updates to the folder's parent must be performed via
	// MoveFolder.
	Parent *ResourceID `json:"parent,omitempty"`
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

// Name is type/id 形式の文字列を返す
// e.g. organization/1234, folder/1234
func (r *ResourceID) Name() string {
	return fmt.Sprintf("%s/%s", r.Type, r.ID)
}

// ExistsMemberInGCPProject is GCP Projectに指定したユーザが権限を持っているかを返す
// defaultだと何らかのroleを持っているかを返す。rolesを指定するといずれか1つ以上を持っているかを返す。
func (s *ResourceManagerService) ExistsMemberInGCPProject(ctx context.Context, projectID string, email string, roles ...string) (bool, error) {
	exists, err := s.existsMemberInGCPProject(ctx, projectID, email, roles...)
	if err != nil {
		return false, xerrors.Errorf("failed existsMemberInGCPProject: projectID=%s, email=%s, roles=%+v : %w", projectID, email, roles, err)
	}
	return exists, nil
}

// ExistsMemberCheckResult is 上位階層のIAMをチェックした履歴
type ExistsMemberCheckResult struct {
	Resource *ResourceID
	Parent *ResourceID
	Exists bool
	Err error
}

// ExistsMemberInGCPProjectWithInherit is GCP Projectに指定したユーザが権限を持っているかを返す
// 対象のProjectの上位階層のIAMもチェックする。
// defaultだと何らかのroleを持っているかを返す。rolesを指定するといずれか1つ以上を持っているかを返す。
func (s *ResourceManagerService) ExistsMemberInGCPProjectWithInherit(ctx context.Context, projectID string, email string, roles ...string) (bool, []*ExistsMemberCheckResult, error) {
	exists, err := s.existsMemberInGCPProject(ctx, projectID, email, roles...)
	if err != nil {
		return false, nil, xerrors.Errorf("failed existsMemberInGCPProject: projectID=%s, email=%s, roles=%+v : %w", projectID, email, roles, err)
	}
	if exists {
		return true, nil, nil
	}

	// 親のIAMをチェック
	var rets []*ExistsMemberCheckResult
	for {
		project, err := s.GetProject(ctx, projectID)
		if err != nil {
			return false, nil, xerrors.Errorf("failed get project: projectID=%s, email=%s, roles=%+v : %w", projectID, email, roles, err)
		}
		if project.Parent == nil {
			return false, rets, nil
		}

		exists, err := s.existsMemberInGCPProject(ctx, project.Parent.ID, email, roles...)
		if err != nil {
			rets = append(rets, &ExistsMemberCheckResult{
				Resource: &ResourceID{
					ID : project.Parent.ID,
					Type: project.Parent.Type,
				},
				Err : err,
			})
			return false, rets, nil
		}
		rets = append(rets, &ExistsMemberCheckResult{
			Resource: &ResourceID{
				ID : project.Parent.ID,
				Type: project.Parent.Type,
			},
			// TODO Parent
			Exists: exists,
			Err : nil,
		})
		if exists {
			return true, rets, nil
		}
	}
}

func (s *ResourceManagerService) existsMemberInGCPProject(ctx context.Context, projectID string, email string, roles ...string) (bool, error) {
	p, err := s.crmv1.Projects.GetIamPolicy(projectID, &crmv1.GetIamPolicyRequest{}).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if xerrors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return false, NewErrPermissionDenied("failed Projects.GetIamPolicy", map[string]interface{}{"input_project": projectID}, err)
			}
		}

		return false, xerrors.Errorf("failed Projects.GetIamPolicy: projectID=%s, : %w", projectID, err)
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

func (s *ResourceManagerService) existsMemberInFolder(ctx context.Context, projectID string, email string, roles ...string) (bool, error) {
	resource, err := s.crmv2.Folders.GetIamPolicy(projectID, &crmv2.GetIamPolicyRequest{}).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if xerrors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return false, NewErrPermissionDenied("failed Projects.GetIamPolicy", map[string]interface{}{"input_project": projectID}, err)
			}
		}

		return false, xerrors.Errorf("failed Projects.GetIamPolicy: projectID=%s, : %w", projectID, err)
	}
	roleMap := map[string]bool{}
	for _, role := range roles {
		roleMap[role] = true
	}

	for _, binding := range resource.Bindings {
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

func (s *ResourceManagerService) existsIamMemberInBindings(email string, bindings []*Binding, roles ...string) (bool, error) {
	roleMap := map[string]bool{}
	for _, role := range roles {
		roleMap[role] = true
	}

	for _, binding := range bindings {
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

// Folders 指定した parent の下にあるすべてのFolderを返す
// 階層構造は保持せずにフラットにすべてのFolderを返す
// parent は `folders/{folder_id}` or `organizations/{org_id}` の形式で指定する
// 対象のparentの権限がない場合、 ErrPermissionDenied を返す
func (s *ResourceManagerService) GetFolders(ctx context.Context, parent *ResourceID) ([]*Folder, error) {
	var folders []*Folder
	var err error
	folders, err = s.folders(ctx, parent, folders)
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if xerrors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return nil, NewErrPermissionDenied("failed get folders", map[string]interface{}{"parent": parent}, err)
			}
		}
		return nil, xerrors.Errorf("failed get folders : %w", err)
	}
	return folders, nil
}

func (s *ResourceManagerService) folders(ctx context.Context, parent *ResourceID, dst []*Folder) ([]*Folder, error) {
	req := s.crmv2.Folders.List().Parent(parent.Name())
	if err := req.Pages(ctx, func(page *crmv2.ListFoldersResponse) error {
		for _, folder := range page.Folders {
			resourceID, err := s.ConvertResourceID(folder.Name)
			if err != nil {
				return err
			}

			l, err := s.folders(ctx, resourceID, dst)
			if err != nil {
				return err
			}
			v := &Folder{
				Name:           folder.Name,
				LifecycleState: folder.LifecycleState,
				CreateTime:     folder.CreateTime,
			}
			if folder.Parent != "" {
				parent, err := s.ConvertResourceID(folder.Parent)
				if err != nil {
					return err
				}
				v.Parent = parent
			}
			dst = append(dst, v)
			dst = append(dst, l...)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return dst, nil
}

// Projects is 指定したリソース以下のProject一覧を返す
// 権限がない (存在しない) parentID を指定しても 空のList を返す
func (s *ResourceManagerService) GetProjects(ctx context.Context, parentID string) ([]*Project, error) {
	req := s.crmv1.Projects.List().Context(ctx)
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
					Type: project.Parent.Type,
					ID:project.Parent.Id,
				}
			}
			list = append(list, p)
		}
		return nil
	}); err != nil {
		return nil, xerrors.Errorf("failed get projects : %w", err)
	}

	return list, nil
}

// GetRelatedProject is 指定したParent配下のすべてのProjectを返す
// parentType : folders or organizations
// 対象のparentの権限がない場合、 ErrPermissionDenied を返す
func (s *ResourceManagerService) GetRelatedProject(ctx context.Context, parent *ResourceID) ([]*Project, error) {
	var projects []*Project

	// 直下のProjectを取得
	{
		ps, err := s.GetProjects(ctx, parent.ID)
		if err != nil {
			return nil, xerrors.Errorf("failed get projects. parent=%s: %w", parent.ID, err)
		}

		projects = append(projects, ps...)
	}

	// 配下の全Folderを取得して、その中のProjectを全部引っ張ってくる
	folders, err := s.GetFolders(ctx, parent)
	if err != nil {
		return nil, xerrors.Errorf("failed get folders. parent=%s: %w", parent.ID, err)
	}

	for _, folder := range folders {
		fn, err := s.ConvertResourceID(folder.Name)
		if err != nil {
			return nil, xerrors.Errorf("invalid folder.Name. name=%s : %w", folder.Name, err)
		}
		ps, err := s.GetProjects(ctx, fn.ID)
		if err != nil {
			return nil, xerrors.Errorf("failed get projects. parent=%v: %w", folder.Name, err)
		}
		projects = append(projects, ps...)
	}

	return projects, nil
}

// GetProject is 指定したProjectIDのProjectを取得する
func (s *ResourceManagerService) GetProject(ctx context.Context, projectID string) (*Project, error) {
	project, err := s.crmv1.Projects.Get(projectID).Context(ctx).Do()
	if err != nil {
		return nil, xerrors.Errorf("failed get project. projectID=%s: %w", projectID, err)
	}
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
			Type: project.Parent.Type,
			ID: project.Parent.Id,
		}
	}
	return p, nil
}

// GetFolder is 指定したFolderIDのFolderを取得する
func (s *ResourceManagerService) GetFolder(ctx context.Context, folderID string) (*Folder, error) {
	folder, err := s.crmv2.Folders.Get(folderID).Context(ctx).Do()
	if err != nil {
		return nil, xerrors.Errorf("failed get folder. folderID=%s: %w", folderID, err)
	}
	ret := &Folder{
		CreateTime:     folder.CreateTime,
		DisplayName:    folder.DisplayName,
		LifecycleState: folder.LifecycleState,
		Name:           folder.Name,
	}
	id, err := s.ConvertResourceID(folder.Name)
	if err != nil {
		return nil, xerrors.Errorf("failed ConvertResourceID(). folder.Name=%s: %w", folder.Name, err)
	}
	ret.ID = id.ID

	if folder.Parent != "" {
		parent, err := s.ConvertResourceID(folder.Parent)
		if err != nil {
			return nil, xerrors.Errorf("failed ConvertResourceID(). folder.Name=%s: %w", folder.Name, err)
		}
		ret.Parent = parent
	}
	return ret, nil
}

func (s *ResourceManagerService) Organization(ctx context.Context, organizationID string) () {
	// organization, err := s.crmv2.Operations.Get(organizationID).Context()
}

// ConvertResourceID is "type/id" 形式の文字列をResourceIDに返還する
// e.g. folder/100, organization/100
func (s *ResourceManagerService) ConvertResourceID(name string) (*ResourceID, error) {
	vl := strings.Split(name, "/")
	if len(vl) < 2 {
		return nil, xerrors.Errorf("invalid resource name. name=%s", name)
	}
	return &ResourceID{
		ID : vl[1],
		Type: vl[0],
	}, nil
}