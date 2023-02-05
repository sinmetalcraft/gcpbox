package metricsscope_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	metricsscopebox "github.com/sinmetalcraft/gcpbox/monitoring/metricsscope/v0"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestService_ListMetricsScopesByMonitoredProject(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newService(t)

	got, err := s.ListMetricsScopesByMonitoredProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		// 1件だけ返ってくる
		t.Errorf("want 1 but got %d", len(got))
	}
	for _, v := range got {
		t.Logf("%s\n", v.Name)
		// locations/global/metricsScopes/ID
		for _, p := range v.MonitoredProjects {
			t.Logf("\t%s\n", p.Name)

		}
	}
}

func TestService_GetMetricsScope(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)
	scopingProjectNumber := getScopingProjectNumber(t)

	s := newService(t)

	got, err := s.GetMetricsScope(ctx, project)
	if err != nil {
		t.Fatal(err)
	}

	gotScopingProjectNumber, err := got.ScopingProjectIDOrNumber()
	if err != nil {
		t.Fatal(err)
	}
	if e, g := scopingProjectNumber, gotScopingProjectNumber; e != g {
		t.Errorf("want %s but got %s", e, g)
	}

	const sinmetalCIProjectNumber = "401580979819"

	// metrics scopeを作っている場合、いっぱい返ってくる
	for _, v := range got.MonitoredProjects {
		t.Logf("%s\n", v.Name)
		// locations/global/metricsScopes/{ScopingProjectNumber}/projects/{MonitoringProjectNumber}

		// ScopingProjectOrNumber check
		if strings.Contains(v.Name, fmt.Sprintf("metricsScopes/%s/", project)) {
			scopingProjectIDOrNumber, err := v.ScopingProjectIDOrNumber()
			if err != nil {
				t.Fatalf("failed ScopingProjectIDOrNumber(). %s. name is %s", err, v.Name)
			}
			if e, g := project, scopingProjectIDOrNumber; e != g {
				t.Errorf("want %s but got %s", e, g)
			}
		}

		// MonitoredProjectIDOrNumber check
		if strings.Contains(v.Name, fmt.Sprintf("/projects/%s", sinmetalCIProjectNumber)) {
			monitoredProjectIDOrNumber, err := v.MonitoredProjectIDOrNumber()
			if err != nil {
				t.Fatalf("failed MonitoredProjectIDOrNumber(). %s. name is %s", err, v.Name)
			}
			if e, g := sinmetalCIProjectNumber, monitoredProjectIDOrNumber; e != g {
				t.Errorf("want %s but got %s", e, g)
			}
		}
	}
}

func TestService_CreateMonitoredProject(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newService(t)

	const monitoredProject = "sinmetal"
	got, err := s.CreateMonitoredProject(ctx, project, monitoredProject)
	if status.Code(err) == codes.AlreadyExists {
		// すでに存在してい場合はスルー
		t.Logf("%s is Already Exists in metrics scope", monitoredProject)
	} else if err != nil {
		t.Fatal(err)
	} else {
		t.Logf("got MonitoredProject:%s\n", got.Name)
	}

	// すでに存在する場合はAlreadyExistsが返ってくる
	_, err = s.CreateMonitoredProject(ctx, project, monitoredProject)
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("want AlreadyExists but got %v", err)
	}

	// 次回テストの時のために削除しておく
	if err := s.DeleteMonitoredProject(ctx, project, monitoredProject); err != nil {
		t.Fatal(err)
	}
}

func newService(t *testing.T) *metricsscopebox.Service {
	ctx := context.Background()

	client, err := metricsscope.NewMetricsScopesClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := metricsscopebox.NewService(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func getScopingProjectID(t *testing.T) string {
	v := os.Getenv("GCPBOX_SCOPING_PROJECT_ID")
	if v == "" {
		t.Fatal("required GCPBOX_SCOPING_PROJECT_ID")
	}
	return v
}

func getScopingProjectNumber(t *testing.T) string {
	v := os.Getenv("GCPBOX_SCOPING_PROJECT_NUMBER")
	if v == "" {
		t.Fatal("required GCPBOX_SCOPING_PROJECT_NUMBER")
	}
	return v
}

func getOrganizationID(t *testing.T) string {
	v := os.Getenv("GCPBOX_ORGANIZATION")
	if v == "" {
		t.Fatal("required GCPBOX_ORGANIZATION")
	}
	return v
}
