package group_test

import (
	"context"
	"testing"

	adts "github.com/apstndb/adcplus/tokensource"
	groupbox "github.com/sinmetalcraft/gcpbox/group/v0"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/option"
)

func TestService_HasMember(t *testing.T) {
	ctx := context.Background()

	ts, err := adts.SmartAccessTokenSource(ctx)
	if err != nil {
		t.Fatal(err)
	}

	directoryService, err := admin.NewService(ctx, option.WithTokenSource(ts), option.WithQuotaProject("sinmetal-ci"))
	if err != nil {
		t.Fatal(err)
	}

	s, err := groupbox.NewService(ctx, directoryService)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := s.HasMember("group-test-tier1@sinmetalcraft.jp", "sinmetal@sinmetalcraft.jp")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("has not member")
	}
}
