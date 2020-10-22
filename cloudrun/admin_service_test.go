package cloudrun_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/sinmetalcraft/gcpbox"
	runbox "github.com/sinmetalcraft/gcpbox/cloudrun"
	"golang.org/x/xerrors"
	"google.golang.org/api/googleapi"
)

func TestAdminService_ListRunService(t *testing.T) {
	ctx := context.Background()

	s := newTestAdminService(t)

	list, err := s.ListRunService(ctx, "sinmetal-ci")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) < 1 {
		t.Errorf("want list > 0")
	}
}

func TestAdminService_GetRunService(t *testing.T) {
	ctx := context.Background()

	s := newTestAdminService(t)

	cases := []struct {
		name           string
		runServiceName string
		wantError      error
		wantErrorCode  int
	}{
		{"hit", "gcpboxtest", nil, 0},
		{"not found", "hellworld", &googleapi.Error{}, http.StatusNotFound},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.GetRunService(ctx, "sinmetal-ci", tt.runServiceName)
			if tt.wantError == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatalf("want error %v but got nil", tt.wantError)
				}
				var errGoogleAPI *googleapi.Error
				if xerrors.As(err, &errGoogleAPI) {
					if e, g := tt.wantErrorCode, errGoogleAPI.Code; e != g {
						t.Errorf("want error code %d but got %d", e, g)
					}
				}
			}
		})
	}
}

func newTestAdminService(t *testing.T) *runbox.AdminService {
	ctx := context.Background()

	apiContainer, err := runbox.NewPrimitiveAPIContainer(ctx, gcpbox.TokyoRegion)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := runbox.NewAdminService(ctx, apiContainer)
	if err != nil {
		t.Fatal(err)
	}
	return admin
}
