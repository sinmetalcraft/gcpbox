package cloudtasks_test

import (
	"context"
	"testing"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
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

	const runHandlerUri = "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task"
	taskName, err := s.CreateJsonPostTask(ctx, queue, &tasksbox.JsonPostTask{
		Audience:    runHandlerUri,
		RelativeUri: runHandlerUri,
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

func newService(t *testing.T) *tasksbox.Service {
	ctx := context.Background()

	taskClient, err := cloudtasks.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}

	sa, err := metadatabox.ServiceAccountEmail()
	if err != nil {
		t.Fatal(err)
	}
	s, err := tasksbox.NewService(ctx, taskClient, sa)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
