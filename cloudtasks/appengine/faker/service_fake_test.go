package faker_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2"
	"github.com/google/go-cmp/cmp"
	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks/appengine"
	"github.com/sinmetalcraft/gcpbox/cloudtasks/appengine/faker"
)

func TestService_fake_CreateGetTask(t *testing.T) {
	ctx := context.Background()

	testQueue := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue",
	}

	cases := []struct {
		name    string
		getTask *tasksbox.GetTask
	}{
		{"all setting task",
			&tasksbox.GetTask{
				Name: "hellotask",
				Routing: &tasksbox.Routing{
					Service: "background",
					Version: "",
				},
				Headers:          map[string]string{"x-sinmetal": "hello"},
				RelativeURI:      "/tq/hoge",
				ScheduleTime:     time.Now().Add(1 * time.Minute),
				DispatchDeadline: 60 * time.Second,
			},
		},
		{"最小構成",
			&tasksbox.GetTask{
				Routing:     nil,
				RelativeURI: "/tq/hoge",
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			s, tasksFaker := newFakeService(t)
			defer tasksFaker.Stop()

			tn, err := s.CreateGetTask(ctx, testQueue, tt.getTask)
			if err != nil {
				t.Fatal(err)
			}
			if len(tt.getTask.Name) > 0 {
				if e, g := fmt.Sprintf("%s/tasks/%s", testQueue.Parent(), tt.getTask.Name), tn; e != g {
					t.Errorf("want TaskName is %s but got %s", e, g)
				}
			} else {
				if !strings.HasPrefix(tn, testQueue.Parent()) || len(tn) < len(testQueue.Parent()) {
					t.Errorf("invalid TaskName got %s", tn)
				}
			}
			if e, g := 1, tasksFaker.GetCreateTaskCallCount(); e != g {
				t.Errorf("want CreateTaskCallCount is %d but got %d", e, g)
				return
			}
			for i := 0; i < tasksFaker.GetCreateTaskCallCount(); i++ {
				task, err := tt.getTask.ToTask()
				if err != nil {
					t.Fatal(err)
				}
				got, err := tasksFaker.GetTask(i)
				if err != nil {
					t.Fatal(err)
				}
				if e, g := task, got; cmp.Equal(e, g) {
					t.Errorf("want task %#v but got %#v", e, g)
				}
			}
		})
	}
}

func TestService_fake_CreateGetTaskMulti(t *testing.T) {
	ctx := context.Background()

	testQueue := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue",
	}

	cases := []struct {
		name    string
		getTask []*tasksbox.GetTask
	}{
		{"all setting task",
			[]*tasksbox.GetTask{
				&tasksbox.GetTask{
					Name: "hellotask",
					Routing: &tasksbox.Routing{
						Service: "background",
						Version: "",
					},
					Headers:          map[string]string{"x-sinmetal": "hello"},
					RelativeURI:      "/tq/hoge",
					ScheduleTime:     time.Now().Add(1 * time.Minute),
					DispatchDeadline: 60 * time.Second,
				},
				&tasksbox.GetTask{
					Routing:     nil,
					RelativeURI: "/tq/hoge",
				},
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			s, tasksFaker := newFakeService(t)
			defer tasksFaker.Stop()

			tns, err := s.CreateGetTaskMulti(ctx, testQueue, tt.getTask)
			if err != nil {
				t.Fatal(err)
			}
			for i, tn := range tns {
				task := tt.getTask[i]
				if len(task.Name) > 0 {
					if e, g := fmt.Sprintf("%s/tasks/%s", testQueue.Parent(), task.Name), tn; e != g {
						t.Errorf("want TaskName is %s but got %s", e, g)
					}
				} else {
					if !strings.HasPrefix(tn, testQueue.Parent()) || len(tn) < len(testQueue.Parent()) {
						t.Errorf("invalid TaskName got %s", tn)
					}
				}
			}

			if e, g := len(tt.getTask), tasksFaker.GetCreateTaskCallCount(); e != g {
				t.Errorf("want CreateTaskCallCount is %d but got %d", e, g)
				return
			}
			for i := 0; i < tasksFaker.GetCreateTaskCallCount(); i++ {
				task, err := tt.getTask[i].ToTask()
				if err != nil {
					t.Fatal(err)
				}
				got, err := tasksFaker.GetTask(i)
				if err != nil {
					t.Fatal(err)
				}
				if e, g := task, got; cmp.Equal(e, g) {
					t.Errorf("want task %#v but got %#v", e, g)
				}
			}
		})
	}
}

func TestService_fake_CreateJsonPostTask(t *testing.T) {
	ctx := context.Background()

	testQueue := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue",
	}

	hoges := []string{"hoge", "fuga"}

	cases := []struct {
		name         string
		jsonPostTask *tasksbox.JsonPostTask
	}{
		{"all setting task",
			&tasksbox.JsonPostTask{
				Name: "hellotask",
				Routing: &tasksbox.Routing{
					Service: "background",
					Version: "",
				},
				Headers:          map[string]string{"x-sinmetal": "hello"},
				RelativeURI:      "/tq/hoge",
				ScheduleTime:     time.Now().Add(1 * time.Minute),
				DispatchDeadline: 60 * time.Second,
				Body:             hoges,
			},
		},
		{"最小構成",
			&tasksbox.JsonPostTask{
				Routing:     nil,
				RelativeURI: "/tq/hoge",
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			s, tasksFaker := newFakeService(t)
			defer tasksFaker.Stop()

			tn, err := s.CreateJsonPostTask(ctx, testQueue, tt.jsonPostTask)
			if err != nil {
				t.Fatal(err)
			}

			if len(tt.jsonPostTask.Name) > 0 {
				if e, g := fmt.Sprintf("%s/tasks/%s", testQueue.Parent(), tt.jsonPostTask.Name), tn; e != g {
					t.Errorf("want TaskName is %s but got %s", e, g)
				}
			} else {
				if !strings.HasPrefix(tn, testQueue.Parent()) || len(tn) < len(testQueue.Parent()) {
					t.Errorf("invalid TaskName got %s", tn)
				}
			}

			if e, g := 1, tasksFaker.GetCreateTaskCallCount(); e != g {
				t.Errorf("want CreateTaskCallCount is %d but got %d", e, g)
				return
			}
			for i := 0; i < tasksFaker.GetCreateTaskCallCount(); i++ {
				task, err := tt.jsonPostTask.ToTask()
				if err != nil {
					t.Fatal(err)
				}
				got, err := tasksFaker.GetTask(i)
				if err != nil {
					t.Fatal(err)
				}
				if e, g := task, got; cmp.Equal(e, g) {
					t.Errorf("want task %#v but got %#v", e, g)
				}
			}
		})
	}
}

func TestService_fake_CreateJsonPostTaskMulti(t *testing.T) {
	ctx := context.Background()

	testQueue := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue",
	}

	hoges := []string{"hoge", "fuga"}

	cases := []struct {
		name          string
		jsonPostTasks []*tasksbox.JsonPostTask
	}{
		{"all setting task",
			[]*tasksbox.JsonPostTask{
				&tasksbox.JsonPostTask{
					Name: "hellotask",
					Routing: &tasksbox.Routing{
						Service: "background",
						Version: "",
					},
					Headers:          map[string]string{"x-sinmetal": "hello"},
					RelativeURI:      "/tq/hoge",
					ScheduleTime:     time.Now().Add(1 * time.Minute),
					DispatchDeadline: 60 * time.Second,
					Body:             hoges,
				},
				&tasksbox.JsonPostTask{
					Routing:     nil,
					RelativeURI: "/tq/hoge",
				},
			},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			s, tasksFaker := newFakeService(t)
			defer tasksFaker.Stop()

			tns, err := s.CreateJsonPostTaskMulti(ctx, testQueue, tt.jsonPostTasks)
			if err != nil {
				t.Fatal(err)
			}

			for i, tn := range tns {
				task := tt.jsonPostTasks[i]
				if len(task.Name) > 0 {
					if e, g := fmt.Sprintf("%s/tasks/%s", testQueue.Parent(), task.Name), tn; e != g {
						t.Errorf("want TaskName is %s but got %s", e, g)
					}
				} else {
					if !strings.HasPrefix(tn, testQueue.Parent()) || len(tn) < len(testQueue.Parent()) {
						t.Errorf("invalid TaskName got %s", tn)
					}
				}
			}
			if e, g := len(tt.jsonPostTasks), tasksFaker.GetCreateTaskCallCount(); e != g {
				t.Errorf("want CreateTaskCallCount is %d but got %d", e, g)
				return
			}
			for i := 0; i < tasksFaker.GetCreateTaskCallCount(); i++ {
				task, err := tt.jsonPostTasks[i].ToTask()
				if err != nil {
					t.Fatal(err)
				}
				got, err := tasksFaker.GetTask(i)
				if err != nil {
					t.Fatal(err)
				}
				if e, g := task, got; cmp.Equal(e, g) {
					t.Errorf("want task %#v but got %#v", e, g)
				}
			}
		})
	}
}

func TestService_fake_Heavy(t *testing.T) {
	ctx := context.Background()

	testQueue1 := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue1",
	}
	testQueue2 := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue2",
	}

	cases := []struct {
		name      string
		callCount int
		queue     *tasksbox.Queue
		getTask   *tasksbox.GetTask
	}{
		{"100", 100, testQueue1, &tasksbox.GetTask{RelativeURI: "/tq/hoge"}},
		{"200", 200, testQueue2, &tasksbox.GetTask{RelativeURI: "/tq/hoge"}},
		{"300", 300, testQueue1, &tasksbox.GetTask{RelativeURI: "/tq/hoge"}},
		{"400", 400, testQueue1, &tasksbox.GetTask{RelativeURI: "/tq/hoge"}},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, tasksFaker := newFakeService(t)
			defer tasksFaker.Stop()

			for i := 0; i < tt.callCount; i++ {
				_, err := s.CreateGetTask(ctx, tt.queue, tt.getTask)
				if err != nil {
					t.Fatal(err)
				}
			}

			if e, g := tt.callCount, tasksFaker.GetCreateTaskCallCount(); e != g {
				t.Errorf("want CreateTaskCallCount is %d but got %d", e, g)
				return
			}
			for i := 0; i < tasksFaker.GetCreateTaskCallCount(); i++ {
				task, err := tt.getTask.ToTask()
				if err != nil {
					t.Fatal(err)
				}
				got, err := tasksFaker.GetTask(i)
				if err != nil {
					t.Fatal(err)
				}
				if e, g := task, got; cmp.Equal(e, g) {
					t.Errorf("want task %#v but got %#v", e, g)
				}
			}
		})
	}
}

func newFakeService(t *testing.T) (*tasksbox.Service, *faker.Faker) {
	ctx := context.Background()

	tasksFaker := faker.NewFaker(t)
	taskClient, err := cloudtasks.NewClient(ctx, tasksFaker.ClientOption())
	if err != nil {
		t.Fatal(err)
	}

	s, err := tasksbox.NewService(ctx, taskClient)
	if err != nil {
		t.Fatal(err)
	}
	return s, tasksFaker
}
