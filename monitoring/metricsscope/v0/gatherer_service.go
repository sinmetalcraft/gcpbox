package metricsscope

import (
	"context"
	"fmt"

	assetbox "github.com/sinmetalcraft/gcpbox/asset/v0"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
)

type GathererService struct {
	MetricsScopesService *Service
	AssetService         *assetbox.Service
}

func NewGathererService(ctx context.Context, metricsScopesService *Service, assetService *assetbox.Service) (*GathererService, error) {
	return &GathererService{
		MetricsScopesService: metricsScopesService,
		AssetService:         assetService,
	}, nil
}

// GatherMonitoredProjects is scopingProjectのMetricsScopeにparentResourceID配下のProjectを追加する
//
// すでに存在しているProjectは無視する
// queryはCloud Asset APIのquery https://cloud.google.com/asset-inventory/docs/searching-resources?hl=ja#how_to_construct_a_query
func (s *GathererService) GatherMonitoredProjects(ctx context.Context, scopingProject string, parentScope assetbox.Scope, query string) (gatherCount int, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.GathererService.GatherMonitoredProjects")
	defer trace.EndSpan(ctx, err)

	scope, err := s.MetricsScopesService.GetMetricsScope(ctx, scopingProject)
	if err != nil {
		return 0, fmt.Errorf("failed MetricsScopesService.GetMetricsScope. scopingProject=%s,parentScope=%v : %w", scopingProject, scope, err)
	}
	existsMonitoredProjects := make(map[string]bool, len(scope.MonitoredProjects))
	for _, v := range scope.MonitoredProjects {
		monitoredProjectNumber, err := v.MonitoredProjectIDOrNumber()
		if err != nil {
			return 0, fmt.Errorf("failed MonitoredProjectIDOrNumber. resource=%s : %w", v.Name, err)
		}
		existsMonitoredProjects[monitoredProjectNumber] = true
	}

	if query == "" {
		query = "state=ACTIVE"
	} else {
		query = fmt.Sprintf("state=ACTIVE AND %s", query)
	}

	l, err := s.AssetService.ListProject(ctx, parentScope, query, assetbox.OrderByCreateTimeDesc)
	if err != nil {
		return 0, err
	}

	var createdCount int
	for _, v := range l {
		if v.ProjectNumber == scopingProject {
			continue
		}
		_, ok := existsMonitoredProjects[v.ProjectNumber]
		if ok {
			continue
		}

		ret, err := s.MetricsScopesService.CreateMonitoredProject(ctx, scopingProject, v.ProjectNumber)
		if err != nil {
			fmt.Printf("failed CreateMonitoredProject: %s(%s). %s\n", v.ProjectID, v.ProjectNumber, err)
			continue
		}
		fmt.Printf("created MonitoredProject: %s (%s)\n", ret.Name, v.ProjectID)
		createdCount++
	}
	return createdCount, nil
}

// CleanUp is 指定したscopingProjectのmetrics scopeをすべて削除して、初期状態にする
func (s *GathererService) CleanUp(ctx context.Context, scopingProject string) (cleanUpCount int, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.GathererService.CleanUp")
	defer trace.EndSpan(ctx, err)

	scope, err := s.MetricsScopesService.GetMetricsScope(ctx, scopingProject)
	if err != nil {
		return 0, fmt.Errorf("failed MetricsScopesService.GetMetricsScope. scopingProject=%s : %w", scopingProject, err)
	}

	var count int
	for _, v := range scope.MonitoredProjects {
		scopingProject, err := v.ScopingProjectIDOrNumber()
		if err != nil {
			return count, fmt.Errorf("failed ScopingProjectIDOrNumber name=%s : %w", v.Name, err)
		}

		monitoredProject, err := v.MonitoredProjectIDOrNumber()
		if err != nil {
			return count, fmt.Errorf("failed MonitoredProjectIDOrNumber name=%s : %w", v.Name, err)
		}

		if scopingProject == monitoredProject {
			// 自分は削除できないので、skip
			continue
		}

		if err := s.MetricsScopesService.DeleteMonitoredProjectByMonitoredProjectName(ctx, v.Name); err != nil {
			return count, fmt.Errorf("failed GathererService.DeleteMonitoredProjectByMonitoredProjectName. name=%s : %w", v.Name, err)
		} else {
			count++
		}
	}
	return count, err
}
