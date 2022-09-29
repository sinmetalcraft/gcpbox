package cloudtasks_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"github.com/google/uuid"

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

func TestService_CreateJsonPostTaskMulti(t *testing.T) {
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
	var tasks []*tasksbox.JsonPostTask
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.JsonPostTask{
			Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
			RelativeURI: runHandlerURI,
			Deadline:    time.Duration(30 * time.Minute),
			Body: &Body{
				Content: "Hello JsonPostTask",
			},
		})
	}
	tns, err := s.CreateJsonPostTaskMulti(ctx, queue, tasks)
	if err != nil {
		t.Fatal(err)
	}
	for i, tn := range tns {
		if len(tn) < 1 {
			t.Errorf("%d : task name is empty", i)
		}
	}
}

func TestService_CreateJsonPostTaskMulti_Retry(t *testing.T) {
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
	baseid := uuid.New().String()
	const alreadyExistsErrIndex = 0
	_, err := s.CreateJsonPostTask(ctx, queue, &tasksbox.JsonPostTask{
		Name:        fmt.Sprintf("%s-%d", baseid, alreadyExistsErrIndex),
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

	var tasks []*tasksbox.JsonPostTask
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.JsonPostTask{
			Name:        fmt.Sprintf("%s-%d", baseid, i),
			Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
			RelativeURI: runHandlerURI,
			Deadline:    time.Duration(30 * time.Minute),
			Body: &Body{
				Content: "Hello JsonPostTask",
			},
		})
	}
	_, err = s.CreateJsonPostTaskMulti(ctx, queue, tasks)
	merr := &tasksbox.MultiError{}
	if errors.As(err, &merr) {
		for _, ierr := range merr.Errors {
			index, ok := ierr.KV["index"].(int)
			if !ok {
				t.Error(ierr)
				continue
			}
			if len(tasks) <= index {
				t.Error(ierr)
				continue
			}
			if ierr.Code == tasksbox.ErrAlreadyExists.Code && index == alreadyExistsErrIndex {
				continue
			}
			t.Errorf("%s is failed. err=%v", tasks[index].Name, ierr)
		}
	} else {
		t.Errorf("want tasksbox.MultiError but got %#v", err)
	}
}

func TestService_CreateJsonPostTaskMulti_WithIgnoreAlreadyExists(t *testing.T) {
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
	baseid := uuid.New().String()
	const alreadyExistsErrIndex = 0
	_, err := s.CreateJsonPostTask(ctx, queue, &tasksbox.JsonPostTask{
		Name:        fmt.Sprintf("%s-%d", baseid, alreadyExistsErrIndex),
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

	var tasks []*tasksbox.JsonPostTask
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.JsonPostTask{
			Name:        fmt.Sprintf("%s-%d", baseid, i),
			Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
			RelativeURI: runHandlerURI,
			Deadline:    time.Duration(30 * time.Minute),
			Body: &Body{
				Content: "Hello JsonPostTask",
			},
		})
	}
	_, err = s.CreateJsonPostTaskMulti(ctx, queue, tasks, tasksbox.WithIgnoreAlreadyExists())
	if err != nil {
		t.Fatal(err)
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

func TestService_CreateGetTaskMulti(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}

	const runHandlerURI = "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task"
	var tasks []*tasksbox.GetTask
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.GetTask{
			Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
			RelativeURI: runHandlerURI,
			Deadline:    time.Duration(30 * time.Minute),
		})
	}
	tns, err := s.CreateGetTaskMulti(ctx, queue, tasks)
	if err != nil {
		t.Fatal(err)
	}
	for i, tn := range tns {
		if len(tn) < 1 {
			t.Errorf("%d : task name is empty", i)
		}
	}
}

func TestService_CreateGetTaskMulti_Error(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}

	const runHandlerURI = "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task"
	baseid := uuid.New().String()
	const alreadyExistsErrIndex = 0
	_, err := s.CreateGetTask(ctx, queue, &tasksbox.GetTask{
		Name:        fmt.Sprintf("%s-%d", baseid, alreadyExistsErrIndex),
		Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
		RelativeURI: runHandlerURI,
		Deadline:    time.Duration(30 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	var tasks []*tasksbox.GetTask
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.GetTask{
			Name:        fmt.Sprintf("%s-%d", baseid, i),
			Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
			RelativeURI: runHandlerURI,
			Deadline:    time.Duration(30 * time.Minute),
		})
	}
	_, err = s.CreateGetTaskMulti(ctx, queue, tasks)
	merr := &tasksbox.MultiError{}
	if errors.As(err, &merr) {
		for _, ierr := range merr.Errors {
			index, ok := ierr.KV["index"].(int)
			if !ok {
				t.Error(ierr)
				continue
			}
			if len(tasks) <= index {
				t.Error(ierr)
				continue
			}
			if ierr.Code == tasksbox.ErrAlreadyExists.Code && index == alreadyExistsErrIndex {
				continue
			}
			t.Errorf("%s is failed. err=%v", tasks[index].Name, ierr)
		}
	} else {
		t.Errorf("want tasksbox.MultiError but got %#v", err)
	}
}

func TestService_CreateGetTaskMulti_WithIgnoreAlreadyExists(t *testing.T) {
	ctx := context.Background()

	s := newService(t)

	queue := &tasksbox.Queue{
		ProjectID: "sinmetal-ci",
		Region:    "asia-northeast1",
		Name:      "gcpboxtest",
	}

	const runHandlerURI = "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task"
	baseid := uuid.New().String()
	const alreadyExistsErrIndex = 0
	_, err := s.CreateGetTask(ctx, queue, &tasksbox.GetTask{
		Name:        fmt.Sprintf("%s-%d", baseid, alreadyExistsErrIndex),
		Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
		RelativeURI: runHandlerURI,
		Deadline:    time.Duration(30 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	var tasks []*tasksbox.GetTask
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &tasksbox.GetTask{
			Name:        fmt.Sprintf("%s-%d", baseid, i),
			Audience:    "", // Cloud Run Invoker に投げる時は空っぽ
			RelativeURI: runHandlerURI,
			Deadline:    time.Duration(30 * time.Minute),
		})
	}
	_, err = s.CreateGetTaskMulti(ctx, queue, tasks, tasksbox.WithIgnoreAlreadyExists())
	if err != nil {
		t.Fatal(err)
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
