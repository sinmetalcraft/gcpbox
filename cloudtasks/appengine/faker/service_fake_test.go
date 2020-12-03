package faker_test

import (
	"context"
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
			tn, err := s.CreateGetTask(ctx, testQueue, tt.getTask)
			if err != nil {
				t.Fatal(err)
			}
			t.Log(tn)
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
