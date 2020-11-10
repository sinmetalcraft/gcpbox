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
type Service struct {
	taskClient *cloudtasks.Client
}

// NewService is return Service
func NewService(ctx context.Context, taskClient *cloudtasks.Client) (*Service, error) {
	return &Service{
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

// CreateTask is QueueにTaskを作成する
func (s *Service) CreateTask(ctx context.Context, queue *Queue, task *Task) (string, error) {
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
	}

	appEngineRequest := &taskspb.AppEngineHttpRequest{
		Headers:     task.Headers,
		HttpMethod:  method,
		RelativeUri: task.RelativeURI,
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
			return "", NewErrInvalidArgument("invalid ScheduleTime", map[string]interface{}{"ScheduledTime": task.ScheduleTime}, err)
		}
		pbTask.ScheduleTime = stpb
	}
	if task.DispatchDeadline != 0 {
		pbTask.DispatchDeadline = ptypes.DurationProto(task.DispatchDeadline)
	}
	taskReq := &taskspb.CreateTaskRequest{
		Parent: queue.Parent(),
		Task:   pbTask,
	}

	t, err := s.taskClient.CreateTask(ctx, taskReq)
	if err != nil {
		return "", err
	}
	return t.Name, nil
}

// CreateTask is QueueにTaskを作成する
func (s *Service) CreateTaskMulti(ctx context.Context, queue *Queue, tasks []*Task) ([]string, error) {
	results := make([]string, len(tasks))
	merr := MultiError{}
	wg := &sync.WaitGroup{}
	for i, task := range tasks {
		wg.Add(1)
		go func(i int, task *Task) {
			defer wg.Done()
			tn, err := s.CreateTask(ctx, queue, task)
			if err != nil {
				sts, ok := status.FromError(err)
				if ok {
					if sts.Code() == codes.AlreadyExists {
						merr.Append(NewErrAlreadyExists(fmt.Sprintf("%s is already exists.", task.Name), map[string]interface{}{"index": i, "taskName": task.Name}, err))
						return
					}
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

// CreateJsonPostTask is BodyにJsonを入れるTaskを作る
func (s *Service) CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask) (string, error) {
	body, err := json.Marshal(task.Body)
	if err != nil {
		return "", xerrors.Errorf("failed json.Marshal(). body=%+v : %w", task.Body, err)
	}
	return s.CreateTask(ctx, queue, &Task{
		Routing:     task.Routing,
		Headers:     map[string]string{"Content-Type": "application/json"},
		Method:      http.MethodPost,
		RelativeURI: task.RelativeURI,
		Body:        body,
	})
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

// CreateGetTask is Get Request 用の Task を作る
func (s *Service) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask) (string, error) {
	return s.CreateTask(ctx, queue, &Task{
		Routing:     task.Routing,
		Headers:     task.Headers,
		Method:      http.MethodGet,
		RelativeURI: task.RelativeURI,
	})
}
