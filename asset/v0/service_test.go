package asset_test

import (
	"context"
	"testing"

	cloudasset "cloud.google.com/go/asset/apiv1"
	assetbox "github.com/sinmetalcraft/gcpbox/asset/v0"
)

func TestService_ListProject(t *testing.T) {
	ctx := context.Background()

	assetClient, err := cloudasset.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assetBoxService, err := assetbox.NewService(ctx, assetClient)
	if err != nil {
		t.Fatal(err)
	}

	rets, err := assetBoxService.ListProject(ctx, &assetbox.OrganizationScope{ID: "190932998497"}, "NOT folders:folders/277206386593", assetbox.OrderByCreateTimeDesc)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range rets {
		if v.ProjectID == "" {
			t.Errorf("ProjectID is empty")
		}
		if v.ProjectNumber == "" {
			t.Errorf("ProjectNumber is empty")
		}
		if v.DisplayName == "" {
			t.Errorf("DisplayName is empty")
		}
		if v.State == "" {
			t.Errorf("State is empty")
		}
		if v.OrganizationID == "" {
			t.Errorf("OrganizationID is empty")
		}
		if v.ParentFullResourceName == "" {
			t.Errorf("ParentFullResourceName is empty")
		}
		if v.CreateTime.IsZero() {
			t.Errorf("CreateTime is zero")
		}
		t.Logf("%#v", v)
	}
}
