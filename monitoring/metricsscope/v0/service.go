package metricsscope

import (
	"context"
	"fmt"
	"strings"
	"time"

	metricsscope "cloud.google.com/go/monitoring/metricsscope/apiv1"
	"cloud.google.com/go/monitoring/metricsscope/apiv1/metricsscopepb"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
)

type MetricsScope struct {
	// Immutable. The resource name of the Monitoring Metrics Scope.
	// On input, the resource name can be specified with the
	// scoping project ID or number. On output, the resource name is
	// specified with the scoping project number.
	// Example:
	// `locations/global/metricsScopes/{SCOPING_PROJECT_ID_OR_NUMBER}`
	Name string `json:"name"`

	// Output only. The time when this `Metrics Scope` was created.
	CreateTime time.Time `json:"createTime"`

	// Output only. The time when this `Metrics Scope` record was last updated.
	UpdateTime time.Time `json:"updateTime"`

	// Output only. The list of projects monitored by this `Metrics Scope`.
	MonitoredProjects []*MonitoredProject `json:"monitoredProjects"`
}

func NewMetricsScope(value *metricsscopepb.MetricsScope) *MetricsScope {
	v := &MetricsScope{}
	v.Name = value.GetName()
	v.CreateTime = value.GetCreateTime().AsTime()
	v.UpdateTime = value.GetUpdateTime().AsTime()

	var list []*MonitoredProject
	for _, v := range value.GetMonitoredProjects() {
		p := &MonitoredProject{
			Name:       v.GetName(),
			CreateTime: v.CreateTime.AsTime(),
		}
		list = append(list, p)
	}
	v.MonitoredProjects = list
	return v
}

// ScopingProjectIDOrNumber is MetricsScope.NameからProjectIDOrNumberを抜き出す
// 基本、ProjectNumberが返ってくる
func (ms *MetricsScope) ScopingProjectIDOrNumber() (string, error) {
	if ms.Name == "" {
		return "", fmt.Errorf("MetricsScopeName is empty")
	}

	l := strings.Split(ms.Name, "/")
	if len(l) != 4 {
		return "", fmt.Errorf("invalid format MetricsScopeName")
	}
	return l[3], nil
}

type MonitoredProject struct {
	// Immutable. The resource name of the `MonitoredProject`. On input, the resource name
	// includes the scoping project ID and monitored project ID. On output, it
	// contains the equivalent project numbers.
	// Example:
	// `locations/global/metricsScopes/{SCOPING_PROJECT_ID_OR_NUMBER}/projects/{MONITORED_PROJECT_ID_OR_NUMBER}`
	Name string `json:"name"`

	CreateTime time.Time `json:"createTime"`
}

func NewMonitoredProject(value *metricsscopepb.MonitoredProject) *MonitoredProject {
	v := &MonitoredProject{}
	v.Name = value.GetName()
	v.CreateTime = value.GetCreateTime().AsTime()
	return v
}

// ScopingProjectIDOrNumber is MonitoredProject.NameからScopingProjectIDOrNumberを抜き出す
// 基本、ProjectNumberが返ってくる
func (mp *MonitoredProject) ScopingProjectIDOrNumber() (string, error) {
	return mp.getFromName(3)
}

// MonitoredProjectIDOrNumber is MonitoredProject.NameからMonitoredProjectIDOrNumberを抜き出す
// 基本、ProjectNumberが返ってくる
func (mp *MonitoredProject) MonitoredProjectIDOrNumber() (string, error) {
	return mp.getFromName(5)
}

func (mp *MonitoredProject) getFromName(index int) (string, error) {
	if mp.Name == "" {
		return "", fmt.Errorf("MonitoredProjectResourceName is empty")
	}

	l := strings.Split(mp.Name, "/")
	if len(l) != 6 {
		return "", fmt.Errorf("invalid format MonitoredProjectResourceName")
	}
	return l[index], nil
}

// Service is Monitoring Metrics Scope Service
type Service struct {
	metricsScopeClient *metricsscope.MetricsScopesClient
}

func NewService(ctx context.Context, metricsScopeClient *metricsscope.MetricsScopesClient) (*Service, error) {
	return &Service{
		metricsScopeClient,
	}, nil
}

// ListMetricsScopesByMonitoredProject is 指定したProjectを追加しているMetricsScopeの一覧を返す
// 指定するのはPROJECT_ID or PROJECT_NUMBER
func (s *Service) ListMetricsScopesByMonitoredProject(ctx context.Context, project string) (rets []*MetricsScope, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.ListMetricsScopesByMonitoredProject")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.ListMetricsScopesByMonitoredProjectRequest{
		MonitoredResourceContainer: fmt.Sprintf("projects/%s", project),
	}
	resp, err := s.metricsScopeClient.ListMetricsScopesByMonitoredProject(ctx, req)
	if err != nil {
		return nil, err
	}

	for _, v := range resp.GetMetricsScopes() {
		ms := NewMetricsScope(v)
		rets = append(rets, ms)
	}
	return rets, nil
}

// GetMetricsScope is 指定したScopingProjectのMetricsScopeを返す
// 指定するのはPROJECT_ID or PROJECT_NUMBER
func (s *Service) GetMetricsScope(ctx context.Context, project string) (ret *MetricsScope, err error) {
	ctx = trace.StartSpan(ctx, "monitoring.metricsscope.Service.GetMetricsScope")
	defer trace.EndSpan(ctx, err)

	req := &metricsscopepb.GetMetricsScopeRequest{
		Name: fmt.Sprintf("locations/global/metricsScopes/%s", project),
	}
	v, err := s.metricsScopeClient.GetMetricsScope(ctx, req)
	if err != nil {
		return nil, err
	}
	return NewMetricsScope(v), nil
}

// CreateMonitoredProject is scopingProjectにmonitoringProjectのmetricsを追加する
// scopingProject, monitoringProjectはPROJECT_ID or PROJECT_NUMBERを指定する
func (s *Service) CreateMonitoredProject(ctx context.Context, scopingProject string, monitoredProject string) (ret *MonitoredProject, err error) {
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
	ms, err := ope.Wait(ctx)
	if err != nil {
		return nil, err
	}
	return NewMonitoredProject(ms), nil
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
