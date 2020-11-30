package appengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/xerrors"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service is App Engine Task Service
type Service interface {
	CreateTask(ctx context.Context, queue *Queue, task *Task, ops ...CreateTaskOptions) (string, error)
	CreateTaskMulti(ctx context.Context, queue *Queue, tasks []*Task, ops ...CreateTaskOptions) ([]string, error)
	CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask, ops ...CreateTaskOptions) (string, error)
	CreateJsonPostTaskMulti(ctx context.Context, queue *Queue, tasks []*JsonPostTask, ops ...CreateTaskOptions) ([]string, error)
	CreateGetTask(ctx context.Context, queue *Queue, task *GetTask, ops ...CreateTaskOptions) (string, error)
	CreateGetTaskMulti(ctx context.Context, queue *Queue, tasks []*GetTask, ops ...CreateTaskOptions) ([]string, error)
}

type serviceImple struct {
	taskClient *cloudtasks.Client
}

// NewService is return serviceImple
func NewService(ctx context.Context, taskClient *cloudtasks.Client) (Service, error) {
	return &serviceImple{
		taskClient: taskClient,
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

// Routing is Push 先の App EngineのServiceとVersionを指定するのに使う
type Routing struct {
	Service string
	Version string
}

// AppEngineRoutingProtoToRouting is AppEngineRouting から Routing に変換する
func AppEngineRoutingProtoToRouting(routing *taskspb.AppEngineRouting) (*Routing, error) {
	if routing == nil {
		return nil, fmt.Errorf("routing is required")
	}
	return &Routing{
		Service: routing.GetService(),
		Version: routing.GetVersion(),
	}, nil
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
		return "", xerrors.Errorf("unsupported method %s", method.String())
	}
}

// Task is Task
type Task struct {
	// Task を Unique にしたい場合に設定する ID
	//
	// optional
	// 中で projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID 形式に設定するので、TASK_ID の部分を設定する
	Name string

	// Task を到達させる App Engine の Service/Version
	// 設定しない場合は Queue の設定に従う
	// optional
	Routing *Routing

	// 任意の HTTP Request Header
	// optional
	Headers map[string]string

	// HTTP Method
	// optional 省略した場合は POST になる
	Method string

	// Task を到達させる path
	// "/" で始まる必要がある
	RelativeURI string

	// Http Request Body
	Body []byte

	// Task を実行する時刻
	// optional 省略した場合は即時実行
	ScheduleTime time.Time

	// Worker で Task を実行する Deadline
	// optional 省略した場合は App Engine の Instance class に従う (frontend 10min, backend 24h)
	DispatchDeadline time.Duration
}

// ToCreateTaskRequestProto is CreateTaskRequest に変換する
func (task *Task) ToCreateTaskRequestProto(queue *Queue) (*taskspb.CreateTaskRequest, error) {
	if queue == nil {
		return nil, NewErrInvalidArgument("queue is required", map[string]interface{}{}, nil)
	}

	var method taskspb.HttpMethod
	switch task.Method {
	case http.MethodPost:
		method = taskspb.HttpMethod_POST
	case http.MethodPut:
		method = taskspb.HttpMethod_PUT
	case http.MethodGet:
		method = taskspb.HttpMethod_GET
	case http.MethodDelete:
		method = taskspb.HttpMethod_DELETE
	default:
		return nil, xerrors.Errorf("unsupported HttpMethod : %v", task.Method)
	}

	appEngineRequest := &taskspb.AppEngineHttpRequest{
		HttpMethod:  method,
		RelativeUri: task.RelativeURI,
		Headers:     task.Headers,
		Body:        task.Body,
	}
	if task.Routing != nil {
		appEngineRequest.AppEngineRouting = &taskspb.AppEngineRouting{
			Service: task.Routing.Service,
			Version: task.Routing.Version,
		}
	}

	pbTask := &taskspb.Task{
		MessageType: &taskspb.Task_AppEngineHttpRequest{
			AppEngineHttpRequest: appEngineRequest,
		},
	}
	if len(task.Name) > 0 {
		pbTask.Name = fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", queue.ProjectID, queue.Region, queue.Name, task.Name)
	}
	if !task.ScheduleTime.IsZero() {
		stpb, err := ptypes.TimestampProto(task.ScheduleTime)
		if err != nil {
			return nil, NewErrInvalidArgument("invalid ScheduleTime", map[string]interface{}{"ScheduledTime": task.ScheduleTime}, err)
		}
		pbTask.ScheduleTime = stpb
	}
	if task.DispatchDeadline != 0 {
		pbTask.DispatchDeadline = ptypes.DurationProto(task.DispatchDeadline)
	}

	return &taskspb.CreateTaskRequest{
		Parent: queue.Parent(),
		Task:   pbTask,
	}, nil
}

// CreateTask is QueueにTaskを作成する
func (s *serviceImple) CreateTask(ctx context.Context, queue *Queue, task *Task, ops ...CreateTaskOptions) (string, error) {
	opt := createTaskOptions{}
	for _, o := range ops {
		o(&opt)
	}

	taskReq, err := task.ToCreateTaskRequestProto(queue)
	if err != nil {
		return "", err
	}

	t, err := s.taskClient.CreateTask(ctx, taskReq)
	if err != nil {
		sts, ok := status.FromError(err)
		if ok {
			if sts.Code() == codes.AlreadyExists {
				if opt.ignoreAlreadyExists {
					return taskReq.GetTask().Name, nil
				}
				return "", NewErrAlreadyExists(fmt.Sprintf("%s is already exists.", task.Name), map[string]interface{}{"taskName": task.Name}, err)
			}
		}
		return "", err
	}
	return t.Name, nil
}

// CreateTask is QueueにTaskを作成する
func (s *serviceImple) CreateTaskMulti(ctx context.Context, queue *Queue, tasks []*Task, ops ...CreateTaskOptions) ([]string, error) {
	results := make([]string, len(tasks))
	merr := MultiError{}
	wg := &sync.WaitGroup{}
	for i, task := range tasks {
		wg.Add(1)
		go func(i int, task *Task) {
			defer wg.Done()
			tn, err := s.CreateTask(ctx, queue, task, ops...)
			if err != nil {
				appErr := &Error{}
				if xerrors.As(err, &appErr) && appErr.Code == ErrAlreadyExists.Code {
					appErr.KV["index"] = i
					merr.Append(appErr)
					return
				}

				merr.Append(NewErrCreateMultiTask("failed CreateTask", map[string]interface{}{"index": i, "taskName": task.Name}, err))
				return
			}
			results[i] = tn
		}(i, task)
	}
	wg.Wait()
	return results, merr.ErrorOrNil()
}

// JsonPostTask is JsonをBodyに入れるTask
type JsonPostTask struct {
	// Task を Unique にしたい場合に設定する ID
	//
	// optional
	// 中で projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID 形式に設定するので、TASK_ID の部分を設定する
	Name string

	// Task を到達させる App Engine の Service/Version
	// 設定しない場合は Queue の設定に従う
	// optional
	Routing *Routing

	// 任意の HTTP Request Header
	// optional
	Headers map[string]string

	// Task を到達させる path
	// "/" で始まる必要がある
	RelativeURI string

	// Body is JSON にして格納するもの
	Body interface{}

	// Task を実行する時刻
	// optional 省略した場合は即時実行
	ScheduleTime time.Time

	// Worker で Task を実行する Deadline
	// optional 省略した場合は App Engine の Instance class に従う (frontend 10min, backend 24h)
	DispatchDeadline time.Duration
}

// ToTask is JsonPostTask convert to Task
func (jpTask *JsonPostTask) ToTask() (*Task, error) {
	body, err := json.Marshal(jpTask.Body)
	if err != nil {
		return nil, xerrors.Errorf("failed json.Marshal(). task=%#v : %w", jpTask, err)
	}
	var header map[string]string
	if jpTask.Headers != nil {
		header = jpTask.Headers
		header["Content-Type"] = "application/json"
	} else {
		header = map[string]string{"Content-Type": "application/json"}
	}

	return &Task{
		Name:             jpTask.Name,
		Routing:          jpTask.Routing,
		Headers:          header,
		Method:           http.MethodPost,
		RelativeURI:      jpTask.RelativeURI,
		Body:             body,
		ScheduleTime:     jpTask.ScheduleTime,
		DispatchDeadline: jpTask.DispatchDeadline,
	}, nil
}

// CreateJsonPostTask is BodyにJsonを入れるTaskを作る
func (s *serviceImple) CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask, ops ...CreateTaskOptions) (string, error) {
	if task == nil {
		return "", xerrors.Errorf("failed CreateJsonPostTask. task is nil")
	}

	t, err := task.ToTask()
	if err != nil {
		return "", err
	}

	return s.CreateTask(ctx, queue, t, ops...)
}

// CreateJsonPostTaskMulti is Queue に 複数の JsonPostTask を作成する
func (s *serviceImple) CreateJsonPostTaskMulti(ctx context.Context, queue *Queue, tasks []*JsonPostTask, ops ...CreateTaskOptions) ([]string, error) {
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
				if xerrors.As(err, &appErr) && appErr.Code == ErrAlreadyExists.Code {
					appErr.KV["index"] = i
					merr.Append(appErr)
					return
				}

				merr.Append(NewErrCreateMultiTask("failed CreateJsonPostTask", map[string]interface{}{"index": i, "taskName": task.Name}, err))
				return
			}
			results[i] = tn
		}(i, task)
	}
	wg.Wait()
	return results, merr.ErrorOrNil()
}

// GetTask is Get Request 用の Task
type GetTask struct {

	// Task を Unique にしたい場合に設定する ID
	//
	// optional
	// 中で projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID/tasks/TASK_ID 形式に設定するので、TASK_ID の部分を設定する
	Name string

	// Task を到達させる App Engine の Service/Version
	// 設定しない場合は Queue の設定に従う
	// optional
	Routing *Routing

	// 任意の HTTP Request Header
	// optional
	Headers map[string]string

	// Task を到達させる path
	// "/" で始まる必要がある
	RelativeURI string

	// Task を実行する時刻
	// optional 省略した場合は即時実行
	ScheduleTime time.Time

	// Worker で Task を実行する Deadline
	// optional 省略した場合は App Engine の Instance class に従う (frontend 10min, backend 24h)
	DispatchDeadline time.Duration
}

// ToTask is GetTask convert to Task
func (gTask *GetTask) ToTask() (*Task, error) {
	return &Task{
		Name:             gTask.Name,
		Routing:          gTask.Routing,
		Headers:          gTask.Headers,
		Method:           http.MethodGet,
		RelativeURI:      gTask.RelativeURI,
		Body:             nil,
		ScheduleTime:     gTask.ScheduleTime,
		DispatchDeadline: gTask.DispatchDeadline,
	}, nil
}

// CreateGetTask is Get Request 用の Task を作る
func (s *serviceImple) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask, ops ...CreateTaskOptions) (string, error) {
	if task == nil {
		return "", xerrors.Errorf("failed CreateGetTask. task is nil")
	}

	t, err := task.ToTask()
	if err != nil {
		return "", err
	}

	return s.CreateTask(ctx, queue, t, ops...)
}

// CreateGetTaskMulti is Queue に複数の GetTask を作成する
func (s *serviceImple) CreateGetTaskMulti(ctx context.Context, queue *Queue, tasks []*GetTask, ops ...CreateTaskOptions) ([]string, error) {
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
				if xerrors.As(err, &appErr) && appErr.Code == ErrAlreadyExists.Code {
					appErr.KV["index"] = i
					merr.Append(appErr)
					return
				}

				merr.Append(NewErrCreateMultiTask("failed CreateGetTask", map[string]interface{}{"index": i, "taskName": task.Name}, err))
				return
			}
			results[i] = tn
		}(i, task)
	}
	wg.Wait()
	return results, merr.ErrorOrNil()
}
