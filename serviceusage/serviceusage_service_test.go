package serviceusage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/sinmetalcraft/gcpbox/serviceusage"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	orgsus "google.golang.org/api/serviceusage/v1"
)

func TestServiceUsageService_ListAll(t *testing.T) {
	ctx := context.Background()

	sus := newTestServiceUsageService(t)

	_, err := sus.ListAll(ctx, "sinmetal-ci")
	if err != nil {
		t.Fatal(err)
	}
}

func TestServiceUsageService_ListByDiff(t *testing.T) {
	ctx := context.Background()

	sus := newTestServiceUsageService(t)

	list, err := sus.ListByDiff(ctx, "sinmetal-lab", "sinmetal-ci")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list {
		fmt.Printf("%+v\n", v)
	}
}

func TestServiceUsageService_SetState(t *testing.T) {
	ctx := context.Background()

	sus := newTestServiceUsageService(t)

	list, err := sus.ListByDiff(ctx, "sinmetal-lab", "sinmetal-ci")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range list {
		if !v.ExistsTarget {
			t.Logf("not found target %+v\n", v)
			continue
		}
		if v.Target.State == serviceusage.StateDisabled {
			// Disableにするのは、Resourceが残ってたりするとエラーになるので、やめておく
			continue
		}
		if err := sus.SetState(ctx, v.Base.Name, v.Target.State); err != nil {
			t.Fatal(err)
		}
		break // 全部実行すると時間がかかるので、1つ設定したら、おしまい
	}
}

func newTestServiceUsageService(t *testing.T) *serviceusage.ServiceUsageService {
	ctx := context.Background()

	orgService, err := orgsus.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	crmv1Client, err := crmv1.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sus, err := serviceusage.NewService(ctx, orgService, crmv1Client)
	if err != nil {
		t.Fatal(err)
	}
	return sus
}
