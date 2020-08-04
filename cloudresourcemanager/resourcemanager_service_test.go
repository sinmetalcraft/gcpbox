package cloudresourcemanager_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	crmv2 "google.golang.org/api/cloudresourcemanager/v2"

	. "github.com/sinmetal/gcpbox/cloudresourcemanager"
)

func TestResourceManagerService_Folders(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	list, err := s.Folders(ctx, fmt.Sprintf("organizations/%s", "190932998497"))
	if err != nil {
		t.Fatal(err)
	}
	if len(list) < 1 {
		t.Errorf("folder list length is zero.")
	}
}

func TestResourceManagerService_Projects(t *testing.T) {
	ctx := context.Background()

	s := newResourceManagerService(t)

	list, err := s.Projects(ctx, "1050500061186")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) < 1 {
		t.Errorf("project list length is zero.")
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
