package policytroubleshooter_test

import (
	"context"
	"fmt"
	"testing"

	pt "cloud.google.com/go/policytroubleshooter/apiv1"
	ptpb "cloud.google.com/go/policytroubleshooter/apiv1/policytroubleshooterpb"
	"google.golang.org/api/option"

	policytroubleshooterbox "github.com/sinmetalcraft/gcpbox/policytroubleshooter/v0"
)

func TestService_HasPermission(t *testing.T) {
	ctx := context.Background()

	s := newService(t)
	defer func() {
		if err := s.Close(ctx); err != nil {
			t.Logf("failed Service.Close %s", err)
		}
	}()

	const (
		sa1 = "policytroubleshooter-1@sinmetal-ci.iam.gserviceaccount.com"
		sa2 = "policytroubleshooter-2@sinmetal-ci.iam.gserviceaccount.com"
		sa3 = "policytroubleshooter-3@sinmetal-ci.iam.gserviceaccount.com"
	)

	const (
		bucket1 = "sinmetal-ci-policytroubleshooter-1"
		bucket2 = "sinmetal-ci-policytroubleshooter-2"
	)

	cases := []struct {
		name      string
		principal string
		bucket    string
		want      bool
	}{
		{"Project Level IAMを持っている", sa1, bucket1, true},
		{"Bucket Level IAMを持っている", sa2, bucket1, true},
		{"Bucket Level IAMを持っていない", sa2, bucket2, false},
		{"権限を持っていない", sa3, bucket1, false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.HasPermission(ctx,
				tt.principal,
				fmt.Sprintf("//storage.googleapis.com/projects/_/buckets/%s", tt.bucket),
				"storage.objects.get")
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.want, got; e != g {
				t.Errorf("want %#v but got %#v", e, g)
			}
		})
	}
}

func TestService_TroubleshootIamPolicy(t *testing.T) {
	ctx := context.Background()

	s := newService(t)
	defer func() {
		if err := s.Close(ctx); err != nil {
			t.Logf("failed Service.Close %s", err)
		}
	}()

	const (
		sa1 = "policytroubleshooter-1@sinmetal-ci.iam.gserviceaccount.com"
		sa2 = "policytroubleshooter-2@sinmetal-ci.iam.gserviceaccount.com"
		sa3 = "policytroubleshooter-3@sinmetal-ci.iam.gserviceaccount.com"
	)

	const (
		bucket1 = "sinmetal-ci-policytroubleshooter-1"
		bucket2 = "sinmetal-ci-policytroubleshooter-2"
	)

	cases := []struct {
		name      string
		principal string
		bucket    string
		want      ptpb.AccessState
	}{
		{"Project Level IAMを持っている", sa1, bucket1, ptpb.AccessState_GRANTED},
		{"Bucket Level IAMを持っている", sa2, bucket1, ptpb.AccessState_GRANTED},
		{"Bucket Level IAMを持っていない", sa2, bucket2, ptpb.AccessState_NOT_GRANTED},
		{"権限を持っていない", sa3, bucket1, ptpb.AccessState_NOT_GRANTED},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.TroubleshootIamPolicy(ctx,
				tt.principal,
				fmt.Sprintf("//storage.googleapis.com/projects/_/buckets/%s", tt.bucket),
				"storage.objects.get")
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.want, resp.GetAccess(); e != g {
				t.Errorf("want %#v but got %#v", e, g)
			}
		})
	}
}

func newService(t *testing.T) *policytroubleshooterbox.Service {
	ctx := context.Background()

	iamCheckerClient, err := pt.NewIamCheckerClient(ctx, option.WithQuotaProject("sinmetal-ci"))
	if err != nil {
		t.Fatal(err)
	}

	s, err := policytroubleshooterbox.NewService(ctx, iamCheckerClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
