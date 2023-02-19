package metricsscope_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	cloudasset "cloud.google.com/go/asset/apiv1"
	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	"github.com/googleapis/gax-go/v2/apierror"
	assetbox "github.com/sinmetalcraft/gcpbox/asset/v0"
	metricsscopebox "github.com/sinmetalcraft/gcpbox/monitoring/metricsscope/v0"
	"google.golang.org/grpc/codes"
)

type AllClient struct {
	MetricsScopesClient *metricsscope.MetricsScopesClient
	AssetClient         *cloudasset.Client
}

func (c *AllClient) Close() error {
	var errs []error
	err := c.MetricsScopesClient.Close()
	if err != nil {
		errs = append(errs, err)
	}

	err = c.AssetClient.Close()
	if err != nil {
		errs = append(errs, err)
	}

	// TODO 1.20で使えるjoinで合体させたい
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func TestGathererService_GatherMonitoredProjects(t *testing.T) {
	ctx := context.Background()

	project := getScopingProjectID(t)

	allClient, s := newTestImportService(t)
	defer func() {
		if err := allClient.Close(); err != nil {
			t.Logf("failed client cloes %s", err)
		}
	}()

	skipFolder := "277206386593"
	const excludeProjectNumber = "608153103826" // skipFolderの中のProject

	// すでに入ってると、衝突するので、削除する
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

	count, err := s.GatherMonitoredProjects(ctx, project, &assetbox.OrganizationScope{ID: getOrganizationID(t)},
		fmt.Sprintf("NOT folders:folders/%s", skipFolder))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Gather MonitoredProject Count %d\n", count)

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

func TestGathererService_CleanUp(t *testing.T) {
	ctx := context.Background()

	project := "sinmetalcraft-monitoring-all2"

	allClient, s := newTestImportService(t)
	defer func() {
		if err := allClient.Close(); err != nil {
			t.Logf("failed client cloes %s", err)
		}
	}()

	createCount, err := s.GatherMonitoredProjects(ctx, project, &assetbox.OrganizationScope{ID: getOrganizationID(t)}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Gather MonitoredProject Count %d\n", createCount)

	cleanUpCount, err := s.CleanUp(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if cleanUpCount < 1 {
		t.Errorf("cleanUpCount is %d", cleanUpCount)
	}
}

func newTestImportService(t *testing.T) (*AllClient, *metricsscopebox.GathererService) {
	ctx := context.Background()

	allClient := &AllClient{}

	metricsScopesClient, err := metricsscope.NewMetricsScopesClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	allClient.MetricsScopesClient = metricsScopesClient

	metricsScopesService, err := metricsscopebox.NewService(ctx, metricsScopesClient)
	if err != nil {
		t.Fatal(err)
	}

	assetClient, err := cloudasset.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	allClient.AssetClient = assetClient

	assetBoxService, err := assetbox.NewService(ctx, assetClient)
	if err != nil {
		t.Fatal(err)
	}

	s, err := metricsscopebox.NewGathererService(ctx, metricsScopesService, assetBoxService)
	if err != nil {
		t.Fatal(err)
	}
	return allClient, s
}
