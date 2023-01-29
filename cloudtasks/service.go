package cloudtasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
// taskName は中で projects/{PROJECT_ID}/locations/{LOCATION}/queues/{QUEUE_ID}/tasks/{TASK_ID} 形式にしているので指定するのは {TASK_ID} の部分だけ
func (s *Service) CreateTask(ctx context.Context, queue *Queue, taskName string, req *taskspb.HttpRequest, scheduleTime time.Time, deadline time.Duration, ops ...CreateTaskOptions) (*taskspb.Task, error) {
	opt := createTaskOptions{}
	for _, o := range ops {
		o(&opt)
	}

	taskReq := &taskspb.CreateTaskRequest{
		Parent: queue.Parent(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: req,
			},
		},
	}
	if len(taskName) > 0 {
		taskReq.GetTask().Name = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", queue.ProjectID, queue.Region, queue.Name, taskName)
	}
	if !scheduleTime.IsZero() {
		taskReq.Task.ScheduleTime = timestamppb.New(scheduleTime)
	}
	if deadline != 0 {
		taskReq.Task.DispatchDeadline = durationpb.New(deadline)
	}
	task, err := s.taskClient.CreateTask(ctx, taskReq)
	if err != nil {
		sts, ok := status.FromError(err)
		if ok {
			if sts.Code() == codes.AlreadyExists {
				if opt.ignoreAlreadyExists {
					return taskReq.GetTask(), nil
				}
				return nil, NewErrAlreadyExists(fmt.Sprintf("%s is already exists.", taskReq.GetTask().Name), map[string]interface{}{"taskName": taskReq.GetTask().Name}, err)
			}
		}
		return nil, err
	}
	return task, nil
}

// Task is Response Task
type Task struct {
	// OIDC の Audience
	//
	// IAPに向けて投げる時は、IAPのClient IDを指定する
	// https://cloud.google.com/iap/docs/authentication-howto#authenticating_from_a_service_account
	//
	// Cloud Run.Invokerに投げる場合は RelativeURI と同じものを指定する
	Audience string

	// Task Request の Header
	Headers map[string]string

	// Task が到達する Handler の URL
	RelativeURI string

	// HTTP Method
	// optional 省略した場合は POST になる
	Method string

	// ScheduleTime is estimated time of arrival
	ScheduleTime time.Time

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration

	// Body is Task Body
	Body []byte

	// Name is Task Name
	// optional
	// Task の重複を抑制するために指定するTaskのName
	// 中で projects/{PROJECT_ID}/locations/{LOCATION}/queues/{QUEUE_ID}/tasks/{TASK_ID} 形式にしているので指定するのは {TASK_ID} の部分だけ
	// 未指定の場合は自動的に設定される
	Name string
}

// HttpMethodProtoToHttpMethod is HttpMethodProto から HttpMethod に変換する
func HttpMethodProtoToHttpMethod(method taskspb.HttpMethod) (string, error) {
	switch method {
	case taskspb.HttpMethod_POST:
		return http.MethodPost, nil
	case taskspb.HttpMethod_GET:
		return http.MethodGet, nil
	case taskspb.HttpMethod_PUT:
		return http.MethodPut, nil
	case taskspb.HttpMethod_DELETE:
		return http.MethodDelete, nil
	default:
		return "", fmt.Errorf("unsupported method %s", method.String())
	}
}

// JsonPostTask is JsonをBodyに入れるTask
type JsonPostTask struct {
	// OIDC の Audience
	//
	// IAPに向けて投げる時は、IAPのClient IDを指定する
	// https://cloud.google.com/iap/docs/authentication-howto#authenticating_from_a_service_account
	//
	// Cloud Run.Invokerに投げる場合は RelativeURI と同じものを指定する
	Audience string

	// Task が到達する Handler の URL
	RelativeURI string

	// Deprecated: should not be used
	// ScheduledTime is estimated time of arrival
	ScheduledTime time.Time

	// ScheduleTime is estimated time of arrival
	ScheduleTime time.Time

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration

	// Task Body
	// 中で JSON に変換する
	Body interface{}

	// Name is Task Name
	// optional
	// Task の重複を抑制するために指定するTaskのName
	// 中で projects/{PROJECT_ID}/locations/{LOCATION}/queues/{QUEUE_ID}/tasks/{TASK_ID} 形式にしているので指定するのは {TASK_ID} の部分だけ
	// 未指定の場合は自動的に設定される
	Name string
}

// ToTask is JsonPostTask convert to Task
func (jpTask *JsonPostTask) ToTask() (*Task, error) {
	var body []byte
	if jpTask.Body != nil {
		b, err := json.Marshal(jpTask.Body)
		if err != nil {
			return nil, fmt.Errorf("failed JsonPostTask convert to Task :%w", err)
		}
		body = b
	}
	return &Task{
		Audience:     jpTask.Audience,
		RelativeURI:  jpTask.RelativeURI,
		Method:       http.MethodPost,
		ScheduleTime: jpTask.ScheduleTime,
		Deadline:     jpTask.Deadline,
		Body:         body,
		Name:         jpTask.Name,
	}, nil
}

// CreateJsonPostTask is BodyにJsonを入れるTaskを作る
func (s *Service) CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask, ops ...CreateTaskOptions) (string, error) {
	// ScheduledTimeを使っている古いものへの対応
	if task.ScheduleTime.IsZero() && !task.ScheduledTime.IsZero() {
		task.ScheduleTime = task.ScheduledTime
	}

	body, err := json.Marshal(task.Body)
	if err != nil {
		return "", fmt.Errorf("failed json.Marshal(). body=%+v : %w", task.Body, err)
	}
	got, err := s.CreateTask(ctx, queue, task.Name, &taskspb.HttpRequest{
		Url:        task.RelativeURI,
		Headers:    map[string]string{"Content-Type": "application/json"},
		HttpMethod: taskspb.HttpMethod_POST,
		Body:       body,
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: s.serviceAccountEmail,
				Audience:            task.Audience,
			},
		},
	}, task.ScheduleTime, task.Deadline, ops...)
	if err != nil {
		return "", fmt.Errorf("failed CreateJsonPostTask(). queue=%+v, body=%+v : %w", queue, task.Body, err)
	}
	return got.Name, nil
}

// CreateJsonPostTaskMulti is Queue に JsonPostTask を複数作成する
func (s *Service) CreateJsonPostTaskMulti(ctx context.Context, queue *Queue, tasks []*JsonPostTask, ops ...CreateTaskOptions) ([]string, error) {
	results := make([]string, len(tasks))
	merr := MultiError{}
	wg := &sync.WaitGroup{}
	for i, task := range tasks {
		wg.Add(1)
		go func(i int, task *JsonPostTask) {
			defer wg.Done()
			tn, err := s.CreateJsonPostTask(ctx, queue, task, ops...)
			if err != nil {
				appErr := &Error{}
				if errors.As(err, &appErr) && appErr.Code == ErrAlreadyExists.Code {
					appErr.KV["index"] = i
					merr.Append(appErr)
					return
				}
				merr.Append(NewErrCreateMultiTask("failed CreateJsonPostTask", map[string]interface{}{"index": i, "taskName": task.Name, "URI": task.RelativeURI}, err))
			}
			results[i] = tn
		}(i, task)
	}
	wg.Wait()
	return results, merr.ErrorOrNil()
}

// GetTask is Get Request 用の Task
type GetTask struct {
	// OIDC の Audience
	//
	// IAPに向けて投げる時は、IAPのClient IDを指定する
	// https://cloud.google.com/iap/docs/authentication-howto#authenticating_from_a_service_account
	//
	// Cloud Run.Invokerに投げる場合は RelativeURI と同じものを指定する
	Audience string

	// Task Request の Header
	Headers map[string]string

	// Task が到達する Handler の URL
	RelativeURI string

	// Deprecated: should not be used
	// ScheduledTime is estimated time of arrival
	ScheduledTime time.Time

	// ScheduleTime is estimated time of arrival
	ScheduleTime time.Time

	// HandlerのDeadline
	// default は 10min 最長は 30min
	Deadline time.Duration

	// Name is Task Name
	// optional
	// Task の重複を抑制するために指定するTaskのName
	// 中で projects/{PROJECT_ID}/locations/{LOCATION}/queues/{QUEUE_ID}/tasks/{TASK_ID} 形式にしているので指定するのは {TASK_ID} の部分だけ
	// 未指定の場合は自動的に設定される
	Name string
}

// ToTask is GetTask convert to Task
func (gTask *GetTask) ToTask() (*Task, error) {
	return &Task{
		Audience:     gTask.Audience,
		RelativeURI:  gTask.RelativeURI,
		Headers:      gTask.Headers,
		Method:       http.MethodGet,
		ScheduleTime: gTask.ScheduleTime,
		Deadline:     gTask.Deadline,
		Body:         nil,
		Name:         gTask.Name,
	}, nil
}

// CreateGetTask is Get Request 用の Task を作る
func (s *Service) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask, ops ...CreateTaskOptions) (string, error) {
	// ScheduledTimeを使っている古いものへの対応
	if task.ScheduleTime.IsZero() && !task.ScheduledTime.IsZero() {
		task.ScheduleTime = task.ScheduledTime
	}

	got, err := s.CreateTask(ctx, queue, task.Name, &taskspb.HttpRequest{
		Url:        task.RelativeURI,
		Headers:    task.Headers,
		HttpMethod: taskspb.HttpMethod_GET,
		AuthorizationHeader: &taskspb.HttpRequest_OidcToken{
			OidcToken: &taskspb.OidcToken{
				ServiceAccountEmail: s.serviceAccountEmail,
				Audience:            task.Audience,
			},
		},
	}, task.ScheduledTime, task.Deadline, ops...)
	if err != nil {
		return "", fmt.Errorf("failed CreateJsonPostTask(). queue=%+v, url=%s : %w", queue, task.RelativeURI, err)
	}
	return got.Name, nil
}

// CreateGetTaskMulti is Queue に GetTask を作成する
func (s *Service) CreateGetTaskMulti(ctx context.Context, queue *Queue, tasks []*GetTask, ops ...CreateTaskOptions) ([]string, error) {
	results := make([]string, len(tasks))
	merr := MultiError{}
	wg := &sync.WaitGroup{}
	for i, task := range tasks {
		wg.Add(1)
		go func(i int, task *GetTask) {
			defer wg.Done()
			tn, err := s.CreateGetTask(ctx, queue, task, ops...)
			if err != nil {
				appErr := &Error{}
				if errors.As(err, &appErr) && appErr.Code == ErrAlreadyExists.Code {
					appErr.KV["index"] = i
					merr.Append(appErr)
					return
				}
				merr.Append(NewErrCreateMultiTask("failed CreateGetTask", map[string]interface{}{"index": i, "taskName": task.Name, "URI": task.RelativeURI}, err))
			}
			results[i] = tn
		}(i, task)
	}
	wg.Wait()
	return results, merr.ErrorOrNil()
}
