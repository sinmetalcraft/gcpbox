package cloudresourcemanager_test

import (
	"context"
	"fmt"
	"testing"

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
