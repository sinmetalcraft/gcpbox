package cloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"golang.org/x/xerrors"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/protobuf/types/known/durationpb"
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
func (s *Service) CreateTask(ctx context.Context, queue *Queue, req *taskspb.HttpRequest, deadline time.Duration) (*taskspb.Task, error) {
	taskReq := &taskspb.CreateTaskRequest{
		Parent: queue.Parent(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: req,
			},
		},
	}
	if deadline.Milliseconds() > 0 {
		ms := deadline.Milliseconds()
		sec := ms / 1000
		taskReq.Task.DispatchDeadline = &durationpb.Duration{Seconds: sec}
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

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration

	// Task Body
	// 中で JSON に変換する
	Body interface{}
}

// CreateJsonPostTask is BodyにJsonを入れるTaskを作る
func (s *Service) CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask) (string, error) {
	body, err := json.Marshal(task.Body)
	if err != nil {
		return "", xerrors.Errorf("failed json.Marshal(). body=%+v : %w", task.Body, err)
	}
	got, err := s.CreateTask(ctx, queue, &taskspb.HttpRequest{
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
	}, task.Deadline)
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

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration
}

// CreateGetTask is Get Request 用の Task を作る
func (s *Service) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask) (string, error) {
	got, err := s.CreateTask(ctx, queue, &taskspb.HttpRequest{
		Url:        task.RelativeUri,
		Headers:    task.Headers,
		HttpMethod: taskspb.HttpMethod_GET,
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: s.serviceAccountEmail,
				Audience:            task.Audience,
			},
		},
	}, task.Deadline)
	if err != nil {
		return "", xerrors.Errorf("failed CreateJsonPostTask(). queue=%+v, url=%s : %w", queue, task.RelativeUri, err)
	}
	return got.Name, nil
}