package appengine_test

import (
	"context"
	"testing"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"

	. "github.com/sinmetal/gcpbox/cloudtasks/appengine"
)

type Body struct {
	Content string
}

func TestService_CreateJsonPostTask(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}
	taskName, err := s.CreateJsonPostTask(ctx, queue, &JsonPostTask{
		Routing: &Routing{
			Service: "gcpbox",
		},
		RelativeUri: "/cloudtasks/appengine/json-post-task",
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

	queue := &Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}
	taskName, err := s.CreateGetTask(ctx, queue, &GetTask{
		Routing: &Routing{
			Service: "gcpbox",
		},
		RelativeUri: "/cloudtasks/appengine/get-task",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(taskName) < 1 {
		t.Error("task name is empty")
	}
}

func newService(t *testing.T) *Service {
	ctx := context.Background()

	taskClient, err := cloudtasks.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewService(ctx, taskClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
