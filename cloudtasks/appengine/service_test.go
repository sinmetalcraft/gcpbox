package appengine_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/google/uuid"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks/appengine"
)

type Body struct {
	Content string
}

func TestService_CreateTaskForError(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}

	task := &tasksbox.Task{
		Method:       http.MethodGet,
		RelativeURI:  "/",
		ScheduleTime: time.Date(0, 1, 1, 1, 1, 1, 1, time.Local),
	}
	_, err := s.CreateTask(ctx, queue, task)
	if err == nil {
		t.Fatal("want error but err is nil")
	}
	t.Log(err.Error())
}

func TestService_CreateJsonPostTask(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}
	taskName, err := s.CreateJsonPostTask(ctx, queue, &tasksbox.JsonPostTask{
		Routing: &tasksbox.Routing{
			Service: "gcpbox",
		},
		RelativeURI: "/cloudtasks/appengine/json-post-task",
		Body: &Body{
			Content: "Hello JsonPostTask",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(taskName) < 1 {
		t.Error("task name is empty")
	}
}

func TestService_CreateGetTask(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}
	taskName, err := s.CreateGetTask(ctx, queue, &tasksbox.GetTask{
		Routing: &tasksbox.Routing{
			Service: "gcpbox",
		},
		RelativeURI: "/cloudtasks/appengine/get-task",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(taskName) < 1 {
		t.Error("task name is empty")
	}
}

func TestService_CreateTaskMulti(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}

	var tasks []*tasksbox.Task
	baseid := uuid.New().String()
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.Task{
			Name: fmt.Sprintf("%s-%d", baseid, i),
			Routing: &tasksbox.Routing{
				Service: "gcpbox",
			},
			Method:      http.MethodGet,
			RelativeURI: "/cloudtasks/appengine/get-task",
		})
	}
	tns, err := s.CreateTaskMulti(ctx, queue, tasks)
	if err != nil {
		t.Fatal(err)
	}
	for i, tn := range tns {
		if len(tn) < 1 {
			t.Errorf("%d taskname is nothing...", i)
		}
	}
}

func newService(t *testing.T) *tasksbox.Service {
	ctx := context.Background()

	taskClient, err := cloudtasks.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := tasksbox.NewService(ctx, taskClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
