package asset_test

import (
	"context"
	"testing"

	assetbox "github.com/sinmetalcraft/gcpbox/asset/v0"
	"google.golang.org/api/cloudasset/v1"
)

func TestService_ListProject(t *testing.T) {
	ctx := context.Background()

	assetService, err := cloudasset.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assetBoxService, err := assetbox.NewService(ctx, assetService)
	if err != nil {
		t.Fatal(err)
	}

	rets, err := assetBoxService.ListProject(ctx, &assetbox.OrganizationScope{Number: "190932998497"}, "NOT folders:folders/277206386593", assetbox.OrderByCreateTimeDesc)
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
		if v.OrganizationNumber == "" {
			t.Errorf("OrganizationNumber is empty")
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
