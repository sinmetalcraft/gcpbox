package cloudresourcemanager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
	crm "google.golang.org/api/cloudresourcemanager/v3"
	"google.golang.org/api/googleapi"
)

const (
	// ResourceTypeProject is projectを表すResourceType
	ResourceTypeProject = "project"

	// ResourceTypeFolder is folderを表すResourceType
	ResourceTypeFolder = "folder"

	// ResourceTypeOrganization is organizationを表すResourceType
	ResourceTypeOrganization = "organization"
)

// NewResourceManagerService is return ResourceManagerService
func NewResourceManagerService(ctx context.Context, crmService *crm.Service) (*ResourceManagerService, error) {
	return &ResourceManagerService{
		crm: crmService,
	}, nil
}

type ResourceManagerService struct {
	crm *crm.Service
}

// IamMember is GCP IAMのMember struct
type IamMember struct {
	Type    string
	Email   string
	Deleted bool
	UID     string
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
// e.g. organizations/1234, folders/1234
func (r *ResourceID) Name() string {
	return fmt.Sprintf("%ss/%s", r.Type, r.ID)
}

// NewResourceID is ResourceIDを生成する
func NewResourceID(resourceType string, id string) *ResourceID {
	switch resourceType {
	case "projects":
		return &ResourceID{
			Type: "project",
			ID:   id,
		}
	case "folders":
		return &ResourceID{
			Type: "folder",
			ID:   id,
		}
	case "organizations":
		return &ResourceID{
			Type: "organization",
			ID:   id,
		}
	default:
		return &ResourceID{
			Type: resourceType,
			ID:   id,
		}
	}
}

// ExistsMemberInGCPProject is GCP Projectに指定したユーザが権限を持っているかを返す
// defaultだと何らかのroleを持っているかを返す。rolesを指定するといずれか1つ以上を持っているかを返す。
func (s *ResourceManagerService) ExistsMemberInGCPProject(ctx context.Context, projectID string, email string, roles ...string) (exists bool, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.ExistsMemberInGCPProject")
	defer trace.EndSpan(ctx, err)

	exists, err = s.existsMemberInGCPProject(ctx, projectID, email, roles...)
	if err != nil {
		return false, fmt.Errorf("failed existsMemberInGCPProject: projectID=%s, email=%s, roles=%+v : %w", projectID, email, roles, err)
	}
	return exists, nil
}

// ExistsMemberCheckResult is 上位階層のIAMをチェックした履歴
type ExistsMemberCheckResult struct {
	Resource     *ResourceID
	Exists       bool
	TopNode      bool
	CensoredNode bool
	StepOver     bool
	Err          error
}

// ExistsMemberInGCPProjectWithInherit is GCP Projectに指定したユーザが権限を持っているかを返す
// 対象のProjectの上位階層のIAMもチェックする。
func (s *ResourceManagerService) ExistsMemberInGCPProjectWithInherit(ctx context.Context, projectID string, email string, ops ...ExistsMemberInheritOptions) (exists bool, results []*ExistsMemberCheckResult, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.ExistsMemberInGCPProjectWithInherit")
	defer trace.EndSpan(ctx, err)

	opt := existsMemberInheritOption{}
	for _, o := range ops {
		o(&opt)
	}

	exists, err = s.existsMemberInGCPProject(ctx, projectID, email, opt.roles...)
	if err != nil {
		return false, nil, fmt.Errorf("failed existsMemberInGCPProject: projectID=%s, email=%s, roles=%+v : %w", projectID, email, opt.roles, err)
	}
	if exists {
		return true, nil, nil
	}

	// 親のIAMをチェック
	var step int
	var rets []*ExistsMemberCheckResult
	project, err := s.GetProject(ctx, projectID)
	if err != nil {
		return false, rets, fmt.Errorf("failed get project: projectID=%s, email=%s, roles=%+v : %w", projectID, email, opt.roles, err)
	}
	if project.Parent == "" {
		return false, rets, nil
	}

	parent, err := ConvertResourceID(project.Parent)
	if err != nil {
		return false, rets, fmt.Errorf("failed ConvertResourceID. parent=%s, projectID=%s, email=%s, roles=%+v : %w", project.Parent, projectID, email, opt.roles, err)
	}
	for {
		step++
		exists, next, log, err := s.existsMemberInGCPProjectWithInherit(ctx, email, parent, step, opt)
		rets = append(rets, log)
		if err != nil {
			return exists, rets, err
		}

		if exists {
			return true, rets, nil
		}
		if next == nil {
			return exists, rets, nil
		}
		parent = next
	}
}

func (s *ResourceManagerService) existsMemberInGCPProjectWithInherit(ctx context.Context, email string, parent *ResourceID, step int, opt existsMemberInheritOption) (exists bool, resourceID *ResourceID, result *ExistsMemberCheckResult, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.existsMemberInGCPProjectWithInherit")
	defer trace.EndSpan(ctx, err)

	log := &ExistsMemberCheckResult{
		Resource: parent,
	}

	if s.findResource(opt.censoredNodes, parent) {
		log.CensoredNode = true
		return false, nil, log, nil
	}

	switch parent.Type {
	case "folder":
		exists, err = s.existsMemberInFolder(ctx, parent, email, opt.roles...)
	case "organization":
		exists, err = s.existsMemberInOrganization(ctx, parent, email, opt.roles...)
	default:
		log.Err = fmt.Errorf("%s is unsupported resource type", parent.Type)
		return false, nil, log, log.Err
	}
	if err != nil {
		log.Err = err
		return false, nil, log, err
	}
	log.Exists = exists
	if exists {
		return true, nil, log, nil // 見つかったので、親を遡るのをやめて終了
	}

	if opt.step != 0 && step >= opt.step {
		log.StepOver = true
		return exists, nil, log, nil
	}
	switch parent.Type {
	case "folder":
		if s.findResource(opt.topNodes, parent) {
			log.TopNode = true
			return false, nil, log, nil
		}

		folder, err := s.GetFolder(ctx, parent)
		if err != nil {
			log.Err = fmt.Errorf("failed get folder : resource=%+v, : %w", parent, err)
			return false, nil, log, log.Err
		}
		if folder.Parent == "" {
			return false, nil, log, nil
		}
		next, err := ConvertResourceID(folder.Parent)
		if err != nil {
			return false, next, nil, fmt.Errorf("failed ConvertResourceID. folder.Parent=%s : %w", folder.Parent, err)
		}
		return false, next, log, nil
	case "organization":
		// orgの親は存在しないので、終了する
		return false, nil, log, nil
	default:
		log.Err = fmt.Errorf("%s is unsupported resource type", parent.Type)
		return false, nil, log, log.Err
	}
}

func (s *ResourceManagerService) findResource(resources []*ResourceID, resource *ResourceID) bool {
	for _, r := range resources {
		if cmp.Equal(r, resource) {
			return true
		}
	}
	return false
}

func (s *ResourceManagerService) existsMemberInGCPProject(ctx context.Context, projectID string, email string, roles ...string) (exists bool, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.existsMemberInGCPProject")
	defer trace.EndSpan(ctx, err)

	resource, err := s.crm.Projects.GetIamPolicy(fmt.Sprintf("projects/%s", projectID), &crm.GetIamPolicyRequest{}).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden || errGoogleAPI.Code == http.StatusNotFound {
				return false, NewErrPermissionDenied("failed Projects.GetIamPolicy", map[string]interface{}{"input_project": projectID}, err)
			}
		}

		return false, fmt.Errorf("failed Projects.GetIamPolicy: projectID=%s, : %w", projectID, err)
	}
	return s.existsIamMemberInBindings(email, resource.Bindings, roles...)
}

func (s *ResourceManagerService) existsMemberInFolder(ctx context.Context, folder *ResourceID, email string, roles ...string) (exists bool, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.existsMemberInFolder")
	defer trace.EndSpan(ctx, err)

	resource, err := s.crm.Folders.GetIamPolicy(folder.Name(), &crm.GetIamPolicyRequest{}).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden || errGoogleAPI.Code == http.StatusNotFound {
				return false, NewErrPermissionDenied("failed Folders.GetIamPolicy", map[string]interface{}{"input_folder": folder}, err)
			}
		}

		return false, fmt.Errorf("failed Folders.GetIamPolicy: folder=%+v, : %w", folder, err)
	}
	return s.existsIamMemberInBindings(email, resource.Bindings, roles...)
}

func (s *ResourceManagerService) existsMemberInOrganization(ctx context.Context, organization *ResourceID, email string, roles ...string) (exists bool, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.existsMemberInOrganization")
	defer trace.EndSpan(ctx, err)

	resource, err := s.crm.Organizations.GetIamPolicy(organization.Name(), &crm.GetIamPolicyRequest{}).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden || errGoogleAPI.Code == http.StatusNotFound {
				return false, NewErrPermissionDenied("failed Organizations.GetIamPolicy", map[string]interface{}{"input_organization": organization}, err)
			}
		}

		return false, fmt.Errorf("failed Organizations.GetIamPolicy: organization=%+v, : %w", organization, err)
	}
	return s.existsIamMemberInBindings(email, resource.Bindings, roles...)
}

func (s *ResourceManagerService) existsIamMemberInBindings(email string, bindings []*crm.Binding, roles ...string) (bool, error) {
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
			return nil, fmt.Errorf("invalid deleted iam member text. text=%v : %w", member, err)
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
func (s *ResourceManagerService) GetFolders(ctx context.Context, parent *ResourceID) (folders []*crm.Folder, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.GetFolders")
	defer trace.EndSpan(ctx, err)

	l, err := s.folders(ctx, parent, "", []*crm.Folder{})
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return nil, NewErrPermissionDenied("failed get folders", map[string]interface{}{"parent": parent}, err)
			}
		}
		return nil, fmt.Errorf("failed get folders : %w", err)
	}
	return l, nil
}

func (s *ResourceManagerService) folders(ctx context.Context, parent *ResourceID, pageToken string, dst []*crm.Folder) (folders []*crm.Folder, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.folders")
	defer trace.EndSpan(ctx, err)

	req := s.crm.Folders.List().Parent(parent.Name()).PageSize(1000).Context(ctx)
	if pageToken != "" {
		req = req.PageToken(pageToken)
	}
	resp, err := req.Do()
	if err != nil {
		return nil, err
	}
	for _, folder := range resp.Folders {
		dst = append(dst, folder)

		resourceID, err := ConvertResourceID(folder.Name)
		if err != nil {
			return nil, err
		}

		dst, err = s.folders(ctx, resourceID, "", dst)
		if err != nil {
			return nil, err
		}
	}
	if resp.NextPageToken != "" {
		dst, err = s.folders(ctx, parent, resp.NextPageToken, dst)
		if err != nil {
			return nil, err
		}
	}

	return dst, nil
}

// Projects is 指定したリソース以下のProject一覧を返す
// 対象のparentの権限がない場合、 ErrPermissionDenied を返す
func (s *ResourceManagerService) GetProjects(ctx context.Context, parent *ResourceID) (projects []*crm.Project, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.GetProjects")
	defer trace.EndSpan(ctx, err)

	var ret []*crm.Project
	var pageToken string
	for {
		req := s.crm.Projects.List().PageSize(1000).Context(ctx)
		if parent != nil {
			req = req.Parent(parent.Name())
		}

		if pageToken != "" {
			req = req.PageToken(pageToken)
		}
		resp, err := req.Do()
		if err != nil {
			var errGoogleAPI *googleapi.Error
			if errors.As(err, &errGoogleAPI) {
				if errGoogleAPI.Code == http.StatusForbidden {
					return nil, NewErrPermissionDenied("failed get projects", map[string]interface{}{"parent": parent}, err)
				}
			}
			return nil, fmt.Errorf("failed get projects. parent=%v : %w", parent, err)
		}
		ret = append(ret, resp.Projects...)
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return ret, nil
}

// GetRelatedProject is 指定したParent配下のすべてのProjectを返す
// parentType : folders or organizations
// 対象のparentの権限がない場合、 ErrPermissionDenied を返す
func (s *ResourceManagerService) GetRelatedProject(ctx context.Context, parent *ResourceID, ops ...GetRelatedProjectOptions) (projects []*crm.Project, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.GetRelatedProject")
	defer trace.EndSpan(ctx, err)

	opt := getRelatedProjectOptions{}
	for _, o := range ops {
		o.applyGetRelatedProject(&opt)
	}

	// 直下のProjectを取得
	{
		ps, err := s.GetProjects(ctx, parent)
		if err != nil {
			return nil, fmt.Errorf("failed get projects. parent=%v: %w", parent, err)
		}
		projects = append(projects, ps...)
	}

	// 配下の全Folderを取得して、その中のProjectを全部引っ張ってくる
	folders, err := s.GetFolders(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("failed get folders. parent=%s: %w", parent.ID, err)
	}

	var apiCallCount int = 1
	for _, folder := range folders {
		if apiCallCount >= opt.apiCallCount {
			apiCallCount = 1
			time.Sleep(opt.interval)
		}

		fn, err := ConvertResourceID(folder.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid folder.Name. name=%s : %w", folder.Name, err)
		}

		// CloudResourceManagerAPIはQuotaが低いので、QuotaErrorが返ってきたら、しばらく待ってから再度実行してみる
		for count := 3; count < 6; count++ {
			ps, err := s.GetProjects(ctx, fn)
			if err != nil {
				var errGoogleAPI *googleapi.Error
				if errors.As(err, &errGoogleAPI) {
					if errGoogleAPI.Code == http.StatusTooManyRequests {
						time.Sleep(time.Duration(count*count) * time.Second)
						continue
					}
				}
				return nil, fmt.Errorf("failed get projects. parent=%v: %w", folder.Name, err)
			}
			projects = append(projects, ps...)
			apiCallCount++
			break
		}
	}

	return projects, nil
}

// GetProject is 指定したProjectIDのProjectを取得する
// projectID は "my-project-id" という値を渡されるのを期待している
func (s *ResourceManagerService) GetProject(ctx context.Context, projectID string) (project *crm.Project, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.GetProject")
	defer trace.EndSpan(ctx, err)

	project, err = s.crm.Projects.Get(fmt.Sprintf("projects/%s", projectID)).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return nil, NewErrPermissionDenied("failed get project", map[string]interface{}{"projectID": projectID}, err)
			}
		}
		return nil, fmt.Errorf("failed get project. projectID=%s: %w", projectID, err)
	}
	return project, nil
}

// GetFolder is 指定したFolderIDのFolderを取得する
func (s *ResourceManagerService) GetFolder(ctx context.Context, folder *ResourceID) (fol *crm.Folder, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.GetFolder")
	defer trace.EndSpan(ctx, err)

	fol, err = s.crm.Folders.Get(folder.Name()).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return nil, NewErrPermissionDenied("failed get folder", map[string]interface{}{"folder": folder}, err)
			}
		}
		return nil, fmt.Errorf("failed get folder. folder=%+v: %w", folder, err)
	}
	return fol, nil
}

// GetOrganization is Organizationを取得する
func (s *ResourceManagerService) GetOrganization(ctx context.Context, organization *ResourceID) (org *crm.Organization, err error) {
	ctx = trace.StartSpan(ctx, "cloudresourcemanager.v3.GetOrganization")
	defer trace.EndSpan(ctx, err)

	org, err = s.crm.Organizations.Get(organization.Name()).Context(ctx).Do()
	if err != nil {
		var errGoogleAPI *googleapi.Error
		if errors.As(err, &errGoogleAPI) {
			if errGoogleAPI.Code == http.StatusForbidden {
				return nil, NewErrPermissionDenied("failed get organization", map[string]interface{}{"organization": organization}, err)
			}
		}
		return nil, fmt.Errorf("failed get organization. organization=%+v: %w", organization, err)
	}
	return org, nil
}

// ConvertResourceID is "type/id" 形式の文字列をResourceIDに返還する
// e.g. folders/100, organizations/100
func ConvertResourceID(name string) (*ResourceID, error) {
	vl := strings.Split(name, "/")
	if len(vl) < 2 {
		return nil, fmt.Errorf("invalid resource name. name=%s", name)
	}
	return NewResourceID(vl[0], vl[1]), nil
}
