package cloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
)

// Service is Cloud Tasks Service
type Service struct {
	taskClient          *cloudtasks.Client
	serviceAccountEmail string
}

// NewService is return Service
func NewService(ctx context.Context, taskClient *cloudtasks.Client, serviceAccountEmail string) (*Service, error) {
	return &Service{
		taskClient:          taskClient,
		serviceAccountEmail: serviceAccountEmail,
	}, nil
}

// Queue is Cloud Tasks Queue
type Queue struct {
	ProjectID string
	Region    string
	Name      string
}

// Parent is return Cloud Tasks Parent format value
func (q *Queue) Parent() string {
	return fmt.Sprintf("projects/%s/locations/%s/queues/%s", q.ProjectID, q.Region, q.Name)
}

// CreateTask is add to task
// 一番 Primitive なやつ
func (s *Service) CreateTask(ctx context.Context, queue *Queue, taskName string, req *taskspb.HttpRequest, scheduledTime time.Time, deadline time.Duration) (*taskspb.Task, error) {
	taskReq := &taskspb.CreateTaskRequest{
		Parent: queue.Parent(),
		Task: &taskspb.Task{
			Name: taskName,
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: req,
			},
		},
	}
	if !scheduledTime.IsZero() {
		stpb, err := ptypes.TimestampProto(scheduledTime)
		if err != nil {
			return nil, NewErrInvalidArgument("invalid ScheduleTime", map[string]interface{}{"ScheduledTime": scheduledTime}, err)
		}
		taskReq.Task.ScheduleTime = stpb
	}
	if deadline != 0 {
		taskReq.Task.DispatchDeadline = ptypes.DurationProto(deadline)
	}
	return s.taskClient.CreateTask(ctx, taskReq)
}

// JsonPostTask is JsonをBodyに入れるTask
type JsonPostTask struct {
	// OIDC の Audience
	//
	// IAPに向けて投げる時は、IAPのClient IDを指定する
	// https://cloud.google.com/iap/docs/authentication-howto#authenticating_from_a_service_account
	//
	// Cloud Run.Invokerに投げる場合は RelativeUri と同じものを指定する
	Audience string

	// Task が到達する Handler の URL
	RelativeUri string

	// ScheduledTime is estimated time of arrival
	ScheduledTime time.Time

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration

	// Task Body
	// 中で JSON に変換する
	Body interface{}

	// Name is Task Name
	// optional
	// Task の重複を抑制するために指定するTaskのName
	// 未指定の場合は自動的に設定される
	Name string
}

// CreateJsonPostTask is BodyにJsonを入れるTaskを作る
func (s *Service) CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask) (string, error) {
	body, err := json.Marshal(task.Body)
	if err != nil {
		return "", xerrors.Errorf("failed json.Marshal(). body=%+v : %w", task.Body, err)
	}
	got, err := s.CreateTask(ctx, queue, task.Name, &taskspb.HttpRequest{
		Url:        task.RelativeUri,
		Headers:    map[string]string{"Content-Type": "application/json"},
		HttpMethod: taskspb.HttpMethod_POST,
		Body:       body,
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: s.serviceAccountEmail,
				Audience:            task.Audience,
			},
		},
	}, task.ScheduledTime, task.Deadline)
	if err != nil {
		return "", xerrors.Errorf("failed CreateJsonPostTask(). queue=%+v, body=%+v : %w", queue, task.Body, err)
	}
	return got.Name, nil
}

// GetTask is Get Request 用の Task
type GetTask struct {
	// OIDC の Audience
	//
	// IAPに向けて投げる時は、IAPのClient IDを指定する
	// https://cloud.google.com/iap/docs/authentication-howto#authenticating_from_a_service_account
	//
	// Cloud Run.Invokerに投げる場合は RelativeUri と同じものを指定する
	Audience string

	// Task Request の Header
	Headers map[string]string

	// Task が到達する Handler の URL
	RelativeUri string

	// ScheduledTime is estimated time of arrival
	ScheduledTime time.Time

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration

	// Name is Task Name
	// optional
	// Task の重複を抑制するために指定するTaskのName
	// 未指定の場合は自動的に設定される
	Name string
}

// CreateGetTask is Get Request 用の Task を作る
func (s *Service) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask) (string, error) {
	got, err := s.CreateTask(ctx, queue, task.Name, &taskspb.HttpRequest{
		Url:        task.RelativeUri,
		Headers:    task.Headers,
		HttpMethod: taskspb.HttpMethod_GET,
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: s.serviceAccountEmail,
				Audience:            task.Audience,
			},
		},
	}, task.ScheduledTime, task.Deadline)
	if err != nil {
		return "", xerrors.Errorf("failed CreateJsonPostTask(). queue=%+v, url=%s : %w", queue, task.RelativeUri, err)
	}
	return got.Name, nil
}
