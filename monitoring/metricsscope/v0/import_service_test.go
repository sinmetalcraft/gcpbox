package metricsscope_test

import (
	"context"
	"testing"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager/v3"
	metricsscopebox "github.com/sinmetalcraft/gcpbox/monitoring/metricsscope/v0"
	"google.golang.org/api/cloudresourcemanager/v3"
)

func TestService_ImportMonitoredProjects(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newTestImportService(t)

	skipFolder := &crmbox.ResourceID{ID: "277206386593", Type: crmbox.ResourceTypeFolder}
	const excludeProjectNumber = "608153103826" // skipFolderの中のProject

	// すでに入ってると、Importしなくても、すでにある状態になってしまうので、削除する
	if err := s.MetricsScopesService.DeleteMonitoredProject(ctx, project, excludeProjectNumber); err != nil {
		t.Fatal(err)
	}

	count, err := s.ImportMonitoredProjects(ctx, project, &crmbox.ResourceID{ID: getOrganizationID(t), Type: crmbox.ResourceTypeOrganization},
		metricsscopebox.WithSkipResources(skipFolder))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Import MonitoredProject Count %d\n", count)

	ms, err := s.MetricsScopesService.GetMetricsScope(ctx, project)
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range ms.MonitoredProjects {
		monitoredProject, err := p.MonitoredProjectIDOrNumber()
		if err != nil {
			t.Fatal(err)
		}

		if monitoredProject == excludeProjectNumber {
			t.Errorf("%s is exclude project", excludeProjectNumber)
		}
	}
}

func newTestImportService(t *testing.T) *metricsscopebox.ImportService {
	ctx := context.Background()

	client, err := metricsscope.NewMetricsScopesClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	metricsScopesService, err := metricsscopebox.NewService(ctx, client)
	if err != nil {
		t.Fatal(err)
	}

	crmService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}

	resourceManagerService, err := crmbox.NewResourceManagerService(ctx, crmService)
	if err != nil {
		t.Fatal(err)
	}

	s, err := metricsscopebox.NewImportService(ctx, metricsScopesService, resourceManagerService)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
