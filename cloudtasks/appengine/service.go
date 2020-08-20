package appengine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"golang.org/x/xerrors"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
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
	Routing     *Routing
	Headers     map[string]string
	Method      string
	RelativeUri string
	Body        []byte
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
		RelativeUri: task.RelativeUri,
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

// JsonPostTask is JsonをBodyに入れるTask
type JsonPostTask struct {
	Routing     *Routing
	RelativeUri string
	Body        interface{}
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
		RelativeUri: task.RelativeUri,
		Body:        body,
	})
}

// GetTask is Get Request 用の Task
type GetTask struct {
	Routing     *Routing
	Headers     map[string]string
	RelativeUri string
}

// CreateGetTask is Get Request 用の Task を作る
func (s *Service) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask) (string, error) {
	return s.CreateTask(ctx, queue, &Task{
		Routing:     task.Routing,
		Headers:     task.Headers,
		Method:      http.MethodGet,
		RelativeUri: task.RelativeUri,
	})
}
