package cloudtasks_test

import (
	"context"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
)

func TestService_CreateJsonPostTask(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}
	type Body struct {
		Content string
	}

	const runHandlerURI = "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task"
	taskName, err := s.CreateJsonPostTask(ctx, queue, &tasksbox.JsonPostTask{
		Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
		RelativeURI: runHandlerURI,
		Deadline:    time.Duration(30 * time.Minute),
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

	const runHandlerURI = "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task"
	taskName, err := s.CreateGetTask(ctx, queue, &tasksbox.GetTask{
		Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
		RelativeURI: runHandlerURI,
		Deadline:    time.Duration(30 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(taskName) < 1 {
		t.Error("task name is empty")
	}
}

func newService(t *testing.T) *tasksbox.Service {
	ctx := context.Background()

	taskClient, err := cloudtasks.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Cloud Build の SA ではなぜか `rpc error: code = InvalidArgument desc = Request contains an invalid argument.` と返ってくるので、App Engine SA を使っている
	s, err := tasksbox.NewService(ctx, taskClient, "sinmetal-ci@appspot.gserviceaccount.com")
	if err != nil {
		t.Fatal(err)
	}
	return s
}
