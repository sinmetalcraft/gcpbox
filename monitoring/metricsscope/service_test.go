package metricsscope_test

import (
	"context"
	"os"
	"testing"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metricsscopebox "github.com/sinmetalcraft/gcpbox/monitoring/metricsscope"
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
		t.Logf("%s\n", v.GetName())
		// locations/global/metricsScopes/ID
		for _, p := range v.GetMonitoredProjects() {
			t.Logf("\t%s\n", p.GetName())

		}
	}
}

func TestService_GetMetricsScope(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newService(t)

	got, err := s.GetMetricsScope(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	// metrics scopeを作っている場合、いっぱい返ってくる
	for _, v := range got.MonitoredProjects {
		t.Logf("%s\n", v.GetName())
		// locations/global/metricsScopes/{ScopingProjectNumber}/projects/{MonitoringProjectNumber}
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
		t.Logf("got MonitoredProject:%s\n", got.GetName())
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
	v := os.Getenv("GCPBOX_SCOPING_PROJECT")
	if v == "" {
		t.Fatal("required GCPBOX_SCOPING_PROJECT")
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
