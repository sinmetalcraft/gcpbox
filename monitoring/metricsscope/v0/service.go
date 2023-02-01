package metricsscope

import (
	"context"
	"fmt"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	"cloud.google.com/go/monitoring/metricsscope/apiv1/metricsscopepb"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
)

// Service is Monitoring Metrics Scope Service
type Service struct {
	metricsScopeClient *metricsscope.MetricsScopesClient
}

func NewService(ctx context.Context, metricsScopeClient *metricsscope.MetricsScopesClient) (*Service, error) {
	return &Service{
		metricsScopeClient,
	}, nil
}

// ListMetricsScopesByMonitoredProject is 指定したProjectのMetricsScopesを返す
// 指定するのはPROJECT_ID or PROJECT_NUMBER
func (s *Service) ListMetricsScopesByMonitoredProject(ctx context.Context, project string) (rets []*metricsscopepb.MetricsScope, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.ListMetricsScopesByMonitoredProject")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.ListMetricsScopesByMonitoredProjectRequest{
		MonitoredResourceContainer: fmt.Sprintf("projects/%s", project),
	}
	resp, err := s.metricsScopeClient.ListMetricsScopesByMonitoredProject(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetMetricsScopes(), nil
}

// GetMetricsScope is 指定したScopingProjectのMetricsScopeを返す
// 指定するのはPROJECT_ID or PROJECT_NUMBER
func (s *Service) GetMetricsScope(ctx context.Context, project string) (ret *metricsscopepb.MetricsScope, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.GetMetricsScope")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.GetMetricsScopeRequest{
		Name: fmt.Sprintf("locations/global/metricsScopes/%s", project),
	}
	v, err := s.metricsScopeClient.GetMetricsScope(ctx, req)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// CreateMonitoredProject is scopingProjectにmonitoringProjectのmetricsを追加する
// scopingProject, monitoringProjectはPROJECT_ID or PROJECT_NUMBERを指定する
func (s *Service) CreateMonitoredProject(ctx context.Context, scopingProject string, monitoredProject string) (ret *metricsscopepb.MonitoredProject, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.CreateMonitoredProject")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.CreateMonitoredProjectRequest{
		Parent: fmt.Sprintf("locations/global/metricsScopes/%s", scopingProject),
		MonitoredProject: &metricsscopepb.MonitoredProject{
			Name: fmt.Sprintf("locations/global/metricsScopes/%s/projects/%s", scopingProject, monitoredProject),
		},
	}
	ope, err := s.metricsScopeClient.CreateMonitoredProject(ctx, req)
	if err != nil {
		return nil, err
	}
	ret, err = ope.Wait(ctx)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// DeleteMonitoredProject is 指定したMonitoredProjectをScoping Projectのmetrics scopeから削除する
// scopingProject, monitoringProjectはPROJECT_ID or PROJECT_NUMBERを指定する
func (s *Service) DeleteMonitoredProject(ctx context.Context, scopingProject string, monitoredProject string) (err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.DeleteMonitoredProject")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.DeleteMonitoredProjectRequest{
		Name: fmt.Sprintf("locations/global/metricsScopes/%s/projects/%s", scopingProject, monitoredProject),
	}
	ope, err := s.metricsScopeClient.DeleteMonitoredProject(ctx, req)
	if err != nil {
		return err
	}
	err = ope.Wait(ctx)
	if err != nil {
		return err
	}
	return nil
}

// DeleteMonitoredProjectByMonitoredProjectName is 指定したMonitoredProjectを削除する
//
//	Example:
//	  `locations/global/metricsScopes/{SCOPING_PROJECT_ID_OR_NUMBER}/projects/{MONITORED_PROJECT_ID_OR_NUMBER}`
func (s *Service) DeleteMonitoredProjectByMonitoredProjectName(ctx context.Context, monitoredProjectName string) (err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.DeleteMonitoredProjectByMonitoredProjectName")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.DeleteMonitoredProjectRequest{
		Name: monitoredProjectName,
	}
	ope, err := s.metricsScopeClient.DeleteMonitoredProject(ctx, req)
	if err != nil {
		return err
	}
	err = ope.Wait(ctx)
	if err != nil {
		return err
	}
	return nil
}
