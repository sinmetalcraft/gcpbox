package cloudresourcemanager_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	adcplusts "github.com/apstndb/adcplus/tokensource"
	"github.com/google/go-cmp/cmp"
	"github.com/k0kubun/pp"
	crm "google.golang.org/api/cloudresourcemanager/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager/v3"
)

const (
	metalTileFolder    = "1050500061186"
	sinmetalcraftJPOrg = "190932998497"

	gcpboxFolderSinmetalcraftJPOrg = "484650900491"
)

func TestResourceManagerService_GetFolders(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name         string
		parent       *crmbox.ResourceID
		wantMinCount int // Folderを新たに作ったりすることがあるので、最低この数以上は取得できるという数を取っている
	}{
		{"Organizationを指定して取得", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, sinmetalcraftJPOrg), 5},
		{"Folderを指定して取得", crmbox.NewResourceID(crmbox.ResourceTypeFolder, gcpboxFolderSinmetalcraftJPOrg), 2},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.GetFolders(ctx, tt.parent)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.wantMinCount, len(got); e > g {
				t.Errorf("want min count %d but got %d", e, g)
			}
			m := map[string]bool{}
			for _, v := range got {
				_, ok := m[v.Name]
				if ok {
					t.Errorf("duplicate folder")
				}
				m[v.Name] = true
			}
		})
	}
}

func TestResourceManagerService_GetFolders_StatusFailed(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name    string
		parent  *crmbox.ResourceID
		wantErr error
	}{
		{"権限がないparent", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, "1050507061166"), crmbox.ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.GetFolders(ctx, tt.parent)
			if e, g := tt.wantErr, err; !errors.Is(g, e) {
				t.Errorf("want error %T but got %T", e, g)
			}
			var errPermissionDenied *crmbox.Error
			if errors.As(err, &errPermissionDenied) {
				if errPermissionDenied.KV["parent"] == "" {
					t.Errorf("ErrPermissionDenied.Target is empty...")
				}
			}
		})
	}
}

func TestResourceManagerService_GetProjects(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name       string
		parent     *crmbox.ResourceID
		wantExists bool
		wantErr    error
	}{
		{"正常系", &crmbox.ResourceID{Type: crmbox.ResourceTypeFolder, ID: metalTileFolder}, true, nil},
		{"権限がないparent", &crmbox.ResourceID{Type: crmbox.ResourceTypeFolder, ID: "105058807061166"}, false, crmbox.ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.GetProjects(ctx, tt.parent)
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !errors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *crmbox.Error
				if errors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["parent"] == "" {
						t.Errorf("ErrPermissionDenied.Target is empty...")
					}
				}
			} else {
				if tt.wantExists {
					if len(got) < 1 {
						t.Errorf("project list length is zero.")
					}
				} else {
					if len(got) >= 1 {
						t.Errorf("project list length is not zero.")
					}
				}
			}
		})
	}
}

func TestResourceManagerService_GetRelatedProject(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name                 string
		parent               *crmbox.ResourceID
		ops                  []crmbox.GetRelatedProjectOptions
		wantCountMin         int
		wantExcludeProjectID map[string]bool //このProjectIDは取得できてはダメ
		wantErr              error
	}{
		{"正常系 folder", crmbox.NewResourceID(crmbox.ResourceTypeFolder, metalTileFolder), []crmbox.GetRelatedProjectOptions{}, 2, map[string]bool{}, nil},
		{"正常系 organization", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, sinmetalcraftJPOrg), []crmbox.GetRelatedProjectOptions{}, 10, map[string]bool{}, nil},
		{"gcpbox/exclude-metrics-scope", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, sinmetalcraftJPOrg), []crmbox.GetRelatedProjectOptions{crmbox.WithSkipResources(&crmbox.ResourceID{ID: "277206386593", Type: crmbox.ResourceTypeFolder})}, 10, map[string]bool{"firebase-deploy-20230110": true}, nil},
		{"権限がないparent", crmbox.NewResourceID(crmbox.ResourceTypeFolder, "105058807061166"), []crmbox.GetRelatedProjectOptions{}, 0, map[string]bool{}, crmbox.ErrPermissionDenied},
		{"WithAPICallInterval", crmbox.NewResourceID(crmbox.ResourceTypeFolder, metalTileFolder), []crmbox.GetRelatedProjectOptions{crmbox.WithAPICallInterval(1, 10*time.Microsecond)}, 2, map[string]bool{}, nil},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.GetRelatedProject(ctx, tt.parent)
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !errors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *crmbox.Error
				if errors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["parent"] == "" {
						t.Errorf("ErrPermissionDenied.Target is empty...")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.wantCountMin, len(got); e > g {
				t.Errorf("want %d but got %d", e, g)
			}
			for _, project := range got {
				_, ok := tt.wantExcludeProjectID[project.ProjectId]
				if ok {
					t.Errorf("hit exclude project!? %s", project.ProjectId)
				}
			}
		})
	}
}

// TestResourceManagerService_ExistsMemberInGCPProject_ExistsMember is Memberが存在するかのテスト
func TestResourceManagerService_ExistsMemberInGCPProject(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name    string
		project string
		member  string
		want    bool
		wantErr error
	}{
		{"Projectが存在して権限を持っており、メンバーが存在している", "sinmetal-ci", "sinmetal@sinmetalcraft.jp", true, nil},
		{"Projectが存在して権限を持っており、メンバーが存在していない", "sinmetal-ci", "hoge@example.com", false, nil},
		{"Projectが存在して権限を持っていない", "gcpug-public-spanner", "hoge@example.com", false, crmbox.ErrPermissionDenied},
		{"Projectが存在していない", "adoi893lda3fd1", "hoge@example.com", false, crmbox.ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.ExistsMemberInGCPProject(ctx, tt.project, tt.member)
			if tt.wantErr == nil && tt.wantErr != err {
				t.Fatal(err)
			}

			if e, g := tt.want, got; !cmp.Equal(e, g) {
				t.Errorf("want %v but got %v", e, g)
			}
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; errors.Is(e, g) {
					return
				}

				t.Errorf("want error %T but got %T", tt.wantErr, err)
				var errPermissionDenied *crmbox.Error
				if errors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["input_project"] == "" {
						t.Errorf("ErrPermissionDenied.input_project is empty...")
					}
				}

				var errGoogleAPI *googleapi.Error
				if errors.As(err, &errGoogleAPI) {
					if errGoogleAPI.Code != http.StatusForbidden {
						t.Errorf("want StatusForbidden but got %v", errGoogleAPI.Code)
					}
				}
			}
		})
	}
}

// TestResourceManagerService_ExistsMemberInGCPProjectWithInherit is Memberが存在するかのテスト
// 上位階層までIAMをチェックする
func TestResourceManagerService_ExistsMemberInGCPProjectWithInherit(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name    string
		project string
		member  string
		opt     []crmbox.ExistsMemberInheritOptions
		want    bool
		wantErr error
	}{
		{"Projectが存在して権限を持っており、メンバーが存在している", "sinmetal-ci", "sinmetal@sinmetalcraft.jp", nil, true, nil},
		{"Projectが存在して、Projectが所属している親のFolderの権限を持っているメンバーが存在している", "gcpbox-ci", "gcpbox-iam-test-3@sinmetal-ci.iam.gserviceaccount.com", nil, true, nil},
		{"Projectが存在して、Projectが所属している祖父のFolderの権限を持っているメンバーが存在している", "gcpbox-ci", "gcpbox-iam-test-1@sinmetal-ci.iam.gserviceaccount.com", nil, true, nil},
		{"Projectが存在して、Projectが所属しているOrganizationの権限を持っているメンバーが存在している", "gcpbox-ci", "gcpbox-iam-test-2@sinmetal-ci.iam.gserviceaccount.com", nil, true, nil},
		{"Projectが存在して権限を持っているが、親のFolderの権限をAppが持っていない", "gentle-mapper-229103", "hoge@example.com", nil, false, crmbox.ErrPermissionDenied},
		{"Projectが存在して権限を持っているが、親のOrganizationの権限をAppが持っていない", "gentle-mapper-229103", "hoge@example.com", nil, false, crmbox.ErrPermissionDenied},
		{"Projectが存在して権限を持っており、メンバーが存在していない", "sinmetal-ci", "hoge@example.com", nil, false, nil},
		{"Projectが存在して権限を持っていない", "gcpug-public-spanner", "hoge@example.com", nil, false, crmbox.ErrPermissionDenied},
		{"Projectが存在していない", "adoi893lda3fd1", "hoge@example.com", nil, false, crmbox.ErrPermissionDenied},
		{"Projectが存在して、Projectが所属している祖父のFolderの権限を持っているメンバーが存在しているが、step数的に届かない", "gcpbox-ci", "gcpbox-iam-test-1@sinmetal-ci.iam.gserviceaccount.com", []crmbox.ExistsMemberInheritOptions{crmbox.WithStep(1)}, false, nil},
		{"Projectが存在して、Projectが所属しているOrganizationの権限を持っているメンバーが存在しているが、手前のfolderをtopで終わる", "gcpbox-ci", "gcpbox-iam-test-2@sinmetal-ci.iam.gserviceaccount.com", []crmbox.ExistsMemberInheritOptions{crmbox.WithTopNodes(&crmbox.ResourceID{Type: "folder", ID: "484650900491"})}, false, nil},
		{"Projectが存在して、Projectが所属しているOrganizationの権限を持っているメンバーが存在しているが、Organizationの権限チェックは打ち切る", "gcpbox-ci", "gcpbox-iam-test-2@sinmetal-ci.iam.gserviceaccount.com", []crmbox.ExistsMemberInheritOptions{crmbox.WithCensoredNodes(&crmbox.ResourceID{Type: "organization", ID: sinmetalcraftJPOrg})}, false, nil},
		{"Projectが存在して権限を持っており、メンバーが存在しているが指定されたRoleではない", "sinmetal-ci", "sinmetal@sinmetalcraft.jp", []crmbox.ExistsMemberInheritOptions{crmbox.WithRolesHaveOne("roles/Owner")}, false, nil},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, logs, err := s.ExistsMemberInGCPProjectWithInherit(ctx, tt.project, tt.member, tt.opt...)
			if tt.wantErr == nil && err != nil {
				t.Fatal(err)
			}

			if e, g := tt.want, got; e != g {
				t.Errorf("want %v but got %v", e, g)
				pp.Println(logs)
			}
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; errors.Is(g, e) {
					return
				}
				t.Errorf("want error %T, %s but got %T %s", tt.wantErr, tt.wantErr, err, err)
			}
		})
	}
}

func TestResourceManagerService_ConvertIamMember(t *testing.T) {
	s := newResourceManagerService(t)

	cases := []struct {
		name   string
		member string
		want   *crmbox.IamMember
	}{
		{"deleted-sa", "deleted:serviceAccount:my-service-account@project-id.iam.gserviceaccount.com?uid=123456789012345678901",
			&crmbox.IamMember{Type: "serviceAccount", Email: "my-service-account@project-id.iam.gserviceaccount.com", Deleted: true, UID: "123456789012345678901"}},
		{"deleted-user", "deleted:user:donald@example.com?uid=234567890123456789012",
			&crmbox.IamMember{Type: "user", Email: "donald@example.com", Deleted: true, UID: "234567890123456789012"}},
		{"user", "user:donald@example.com",
			&crmbox.IamMember{Type: "user", Email: "donald@example.com", Deleted: false, UID: ""}},
		{"service-account", "serviceAccount:my-service-account@project-id.iam.gserviceaccount.com",
			&crmbox.IamMember{Type: "serviceAccount", Email: "my-service-account@project-id.iam.gserviceaccount.com", Deleted: false, UID: ""}},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.ConvertIamMember(tt.member)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.want, got; !cmp.Equal(e, g) {
				t.Errorf("want %v but got %v", e, g)
			}
		})
	}
}

func TestResourceManagerService_GetFolder(t *testing.T) {
	s := newResourceManagerService(t)

	ctx := context.Background()

	_, err := s.GetFolder(ctx, crmbox.NewResourceID(crmbox.ResourceTypeFolder, metalTileFolder))
	if err != nil {
		t.Fatal(err)
	}
}

func TestResourceManagerService_GetOrganization(t *testing.T) {
	s := newResourceManagerService(t)

	ctx := context.Background()

	_, err := s.GetOrganization(ctx, crmbox.NewResourceID(crmbox.ResourceTypeOrganization, sinmetalcraftJPOrg))
	if err != nil {
		t.Fatal(err)
	}
}

func newResourceManagerService(t *testing.T) *crmbox.ResourceManagerService {
	ctx := context.Background()

	ts, err := adcplusts.SmartAccessTokenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	crmService, err := crm.NewService(ctx, option.WithTokenSource(ts))
	if err != nil {
		t.Fatal(err)
	}

	s, err := crmbox.NewResourceManagerService(ctx, crmService)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
