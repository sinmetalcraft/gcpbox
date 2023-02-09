package metricsscope_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	"github.com/googleapis/gax-go/v2/apierror"
	assetbox "github.com/sinmetalcraft/gcpbox/asset/v0"
	metricsscopebox "github.com/sinmetalcraft/gcpbox/monitoring/metricsscope/v0"
	"google.golang.org/api/cloudasset/v1"
	"google.golang.org/grpc/codes"
)

func TestService_ImportMonitoredProjects(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	s := newTestImportService(t)

	skipFolder := "277206386593"
	const excludeProjectNumber = "608153103826" // skipFolderの中のProject

	// すでに入ってると、Importしなくても、すでにある状態になってしまうので、削除する
	if err := s.MetricsScopesService.DeleteMonitoredProject(ctx, project, excludeProjectNumber); err != nil {
		var aerr *apierror.APIError
		if errors.As(err, &aerr) {
			if aerr.GRPCStatus().Code() == codes.NotFound {
				// noop
			} else {
				t.Fatal(err)
			}
		} else {
			t.Fatal(err)
		}
	}

	count, err := s.ImportMonitoredProjects(ctx, project, &assetbox.OrganizationScope{Number: getOrganizationID(t)},
		fmt.Sprintf("state=ACTIVE NOT folders:folders/%s", skipFolder))
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

	assetService, err := cloudasset.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}

	assetBoxService, err := assetbox.NewService(ctx, assetService)
	if err != nil {
		t.Fatal(err)
	}

	s, err := metricsscopebox.NewImportService(ctx, metricsScopesService, assetBoxService)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
