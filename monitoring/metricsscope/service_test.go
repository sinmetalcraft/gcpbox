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
	for _, v := range got {
		t.Logf("%s\n", v.GetName())
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
	for _, v := range got.MonitoredProjects {
		t.Logf("%s\n", v.GetName())
	}
}

func TestService_CreateMonitoredProject(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newService(t)

	const monitoredProject = "sinmetal-ci"
	got, err := s.CreateMonitoredProject(ctx, project, monitoredProject)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("got MonitoredProject:%s\n", got.GetName())

	// すでに存在する場合はAlreadyExistsが返ってくる
	_, err = s.CreateMonitoredProject(ctx, project, monitoredProject)
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("want AlreadyExists but got %v", err)
	}

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
