package metricsscope_test

import (
	"context"
	"testing"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager/v3"
	"google.golang.org/api/cloudresourcemanager/v3"

	metricsscopebox "github.com/sinmetalcraft/gcpbox/monitoring/metricsscope"
)

func TestService_ImportMonitoredProjects(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newTestImportService(t)

	count, err := s.ImportMonitoredProjects(ctx, project, &crmbox.ResourceID{ID: getOrganizationID(t), Type: "organization"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Import MonitoredProject Count %d\n", count)
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
