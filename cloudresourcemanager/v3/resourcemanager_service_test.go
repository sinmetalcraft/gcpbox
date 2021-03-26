package cloudresourcemanager_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/k0kubun/pp"
	"golang.org/x/xerrors"
	crm "google.golang.org/api/cloudresourcemanager/v3"
	"google.golang.org/api/googleapi"

	crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager/v3"
)

const (
	metalTileFolder    = "1050500061186"
	sinmetalcraftJPOrg = "190932998497"
	// gcpalcatrazLandOrg = "69098872916"
	// sinmetalJPOrg      = "870462276916"

	// gcpboxFolderSinmetalcraftJPOrg = "484650900491"
	// gcpboxFolderSinmetalJPOrg      = "167285374874"
)

func TestResourceManagerService_GetFolders(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name    string
		parent  *crmbox.ResourceID
		wantErr error
	}{
		{"正常系", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, sinmetalcraftJPOrg), nil},
		{"権限がないparent", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, "1050507061166"), crmbox.ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.GetFolders(ctx, tt.parent)
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !xerrors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *crmbox.Error
				if xerrors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["parent"] == "" {
						t.Errorf("ErrPermissionDenied.Target is empty...")
					}
				}
			} else {
				if len(got) < 1 {
					t.Errorf("folder list length is zero.")
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
				if e, g := tt.wantErr, err; !xerrors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *crmbox.Error
				if xerrors.As(err, &errPermissionDenied) {
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
		name         string
		parent       *crmbox.ResourceID
		wantCountMin int
		wantErr      error
	}{
		{"正常系 folder", crmbox.NewResourceID(crmbox.ResourceTypeFolder, metalTileFolder), 2, nil},
		{"正常系 organization", crmbox.NewResourceID(crmbox.ResourceTypeOrganization, sinmetalcraftJPOrg), 10, nil},
		{"権限がないparent", crmbox.NewResourceID(crmbox.ResourceTypeFolder, "105058807061166"), 0, crmbox.ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.GetRelatedProject(ctx, tt.parent)
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !xerrors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *crmbox.Error
				if xerrors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["parent"] == "" {
						t.Errorf("ErrPermissionDenied.Target is empty...")
					}
				}
			} else {
				if e, g := tt.wantCountMin, len(got); e > g {
					t.Errorf("want %d but got %d", e, g)
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
			if tt.wantErr != nil && tt.wantErr != err {
				t.Fatal(err)
			}

			if e, g := tt.want, got; !cmp.Equal(e, g) {
				t.Errorf("want %v but got %v", e, g)
			}
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !xerrors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *crmbox.Error
				if xerrors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["input_project"] == "" {
						t.Errorf("ErrPermissionDenied.input_project is empty...")
					}
				}

				var errGoogleAPI *googleapi.Error
				if xerrors.As(err, &errGoogleAPI) {
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
		{"Projectが存在して、Projectが所属しているOrganizationの権限を持っているメンバーが存在しているがOrganizationまで見に行かない", "gcpbox-ci", "gcpbox-iam-test-2@sinmetal-ci.iam.gserviceaccount.com", []crmbox.ExistsMemberInheritOptions{crmbox.WithTopNode(&crmbox.ResourceID{Type: "folder", ID: "484650900491"})}, false, nil},
		{"Projectが存在して権限を持っており、メンバーが存在しているが指定されたRoleではない", "sinmetal-ci", "sinmetal@sinmetalcraft.jp", []crmbox.ExistsMemberInheritOptions{crmbox.WithRolesHaveOne("roles/Owner")}, false, nil},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, logs, err := s.ExistsMemberInGCPProjectWithInherit(ctx, tt.project, tt.member, tt.opt...)
			if e, g := tt.want, got; !cmp.Equal(e, g) {
				pp.Println(logs)
				t.Errorf("want %v but got %v", e, g)
			}
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; xerrors.Is(g, e) {
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

	crmService, err := crm.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := crmbox.NewResourceManagerService(ctx, crmService)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
