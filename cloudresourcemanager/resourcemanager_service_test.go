package cloudresourcemanager_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/xerrors"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	crmv2 "google.golang.org/api/cloudresourcemanager/v2"
	"google.golang.org/api/googleapi"

	. "github.com/sinmetal/gcpbox/cloudresourcemanager"
)

func TestResourceManagerService_Folders(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name    string
		parent  string
		wantErr error
	}{
		{"正常系", fmt.Sprintf("organizations/%s", "190932998497"), nil},
		{"権限がないparent", fmt.Sprintf("organizations/%s", "1050507061166"), ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Folders(ctx, tt.parent)
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !xerrors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *Error
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

func TestResourceManagerService_Projects(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	cases := []struct {
		name       string
		parent     string
		wantExists bool
		wantErr    error
	}{
		{"正常系", "1050500061186", true, nil},
		{"権限がないparent", "105058807061166", false, nil},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Projects(ctx, tt.parent)
			if err != nil {
				t.Fatal(err)
			}

			if tt.wantExists {
				if len(got) < 1 {
					t.Errorf("project list length is zero.")
				}
			} else {
				if len(got) >= 1 {
					t.Errorf("project list length is not zero.")
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
		{"Projectが存在して権限を持っていない", "gcpug-public-spanner", "hoge@example.com", false, ErrPermissionDenied},
		{"Projectが存在していない", "adoi893lda3fd1", "hoge@example.com", false, ErrPermissionDenied},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.ExistsMemberInGCPProject(ctx, tt.project, tt.member)
			if e, g := tt.want, got; !cmp.Equal(e, g) {
				t.Errorf("want %v but got %v", e, g)
			}
			if tt.wantErr != nil {
				if e, g := tt.wantErr, err; !xerrors.Is(g, e) {
					t.Errorf("want error %T but got %T", e, g)
				}
				var errPermissionDenied *Error
				if xerrors.As(err, &errPermissionDenied) {
					if errPermissionDenied.KV["input_project"] == "" {
						t.Errorf("ErrPermissionDenied.input_project is empty...")
					}
				}

				var errGoogleAPI *googleapi.Error
				if !xerrors.As(err, &errGoogleAPI) {
					if errGoogleAPI.Code != http.StatusForbidden {
						t.Errorf("want StatusForbidden but got %v", errGoogleAPI.Code)
					}
				}
			}
		})
	}
}

func TestResourceManagerService_ConvertIamMember(t *testing.T) {
	s := newResourceManagerService(t)

	cases := []struct {
		name   string
		member string
		want   *IamMember
	}{
		{"deleted-sa", "deleted:serviceAccount:my-service-account@project-id.iam.gserviceaccount.com?uid=123456789012345678901",
			&IamMember{Type: "serviceAccount", Email: "my-service-account@project-id.iam.gserviceaccount.com", Deleted: true, UID: "123456789012345678901"}},
		{"deleted-user", "deleted:user:donald@example.com?uid=234567890123456789012",
			&IamMember{Type: "user", Email: "donald@example.com", Deleted: true, UID: "234567890123456789012"}},
		{"user", "user:donald@example.com",
			&IamMember{Type: "user", Email: "donald@example.com", Deleted: false, UID: ""}},
		{"service-account", "serviceAccount:my-service-account@project-id.iam.gserviceaccount.com",
			&IamMember{Type: "serviceAccount", Email: "my-service-account@project-id.iam.gserviceaccount.com", Deleted: false, UID: ""}},
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

func newResourceManagerService(t *testing.T) *ResourceManagerService {
	ctx := context.Background()

	crmv1Service, err := crmv1.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	crmv2Service, err := crmv2.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewResourceManagerService(ctx, crmv1Service, crmv2Service)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
