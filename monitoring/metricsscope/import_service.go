package metricsscope

import (
	"context"
	"fmt"
	"strings"

	crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager/v3"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
)

type ImportService struct {
	MetricsScopesService   *Service
	ResourceManagerService *crmbox.ResourceManagerService
}

func NewImportService(ctx context.Context, metricsScopesService *Service, resourceManagerService *crmbox.ResourceManagerService) (*ImportService, error) {
	return &ImportService{
		MetricsScopesService:   metricsScopesService,
		ResourceManagerService: resourceManagerService,
	}, nil
}

// ImportMonitoredProjects is scopingProjectのMetricsScopeにparentResourceID配下のProjectを追加する
func (s *ImportService) ImportMonitoredProjects(ctx context.Context, scopingProject string, parentResourceID *crmbox.ResourceID, ops ...ImportServiceOptions) (importCount int, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.ImportService.ImportMonitoredProjects")
	defer trace.EndSpan(ctx, err)

	opt := importServiceOptions{}
	for _, o := range ops {
		o(&opt)
	}

	scope, err := s.MetricsScopesService.GetMetricsScope(ctx, scopingProject)
	if err != nil {
		return 0, fmt.Errorf("failed MetricsScopesService.GetMetricsScope. scopingProject=%s,parentResourceID=%v : %w", scopingProject, parentResourceID, err)
	}
	existsMonitoredProjects := make(map[string]bool, len(scope.GetMonitoredProjects()))
	for _, v := range scope.GetMonitoredProjects() {
		// locations/global/metricsScopes/{ScopingProjectNumber}/projects/{MonitoredProjectNumber}
		l := strings.Split(v.GetName(), "/")
		if len(l) != 6 {
			return 0, fmt.Errorf("invalid MonitoredProjects format. %s", v.GetName())
		}
		existsMonitoredProjects[l[5]] = true
	}

	l, err := s.ResourceManagerService.GetRelatedProject(ctx, parentResourceID, crmbox.WithSkipResources(opt.skipResources...))
	if err != nil {
		return 0, fmt.Errorf("failed ResourceManagerService.GetRelatedProject. scopingProject=%s,parentResourceID=%v : %w", scopingProject, parentResourceID, err)
	}

	var createdCount int
	for _, v := range l {
		projectNumber := strings.ReplaceAll(v.Name, "projects/", "")
		if projectNumber == scopingProject {
			continue
		}
		_, ok := existsMonitoredProjects[projectNumber]
		if ok {
			continue
		}

		ret, err := s.MetricsScopesService.CreateMonitoredProject(ctx, scopingProject, projectNumber)
		if err != nil {
			fmt.Printf("failed CreateMonitoredProject: %s(%s). %s\n", v.ProjectId, projectNumber, err)
			continue
		}
		fmt.Printf("created MonitoredProject: %s (%s)\n", ret.GetName(), v.ProjectId)
		createdCount++
	}
	return createdCount, nil
}
