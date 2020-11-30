package appengine_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2"
	faketasks "github.com/sinmetal/fake/cloudtasks"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/protobuf/proto"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks/appengine"
)

func TestService_fake_CreateGetTask(t *testing.T) {
	ctx := context.Background()

	s, faker := newFakeService(t)

	testQueue := &tasksbox.Queue{
		ProjectID: "unittest",
		Region:    "asia-northeast1",
		Name:      "testqueue",
	}

	var name string = fmt.Sprintf("name%d", rand.Int())
	var dispatchCount int32 = 0
	var responseCount int32 = 0
	var expectedResponse = &taskspb.Task{
		Name:          name,
		DispatchCount: dispatchCount,
		ResponseCount: responseCount,
	}
	faker.AddMockResponse(nil, expectedResponse)
	gtask := &tasksbox.GetTask{
		Name: "hellotask",
		Routing: &tasksbox.Routing{
			Service: "background",
			Version: "",
		},
		Headers:          map[string]string{"x-sinmetal": "hello"},
		RelativeURI:      "/tq/hoge",
		ScheduleTime:     time.Now().Add(1 * time.Minute),
		DispatchDeadline: 60 * time.Second,
	}
	tn, err := s.CreateGetTask(ctx, testQueue, gtask)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(tn)
	if e, g := 1, faker.GetCreateTaskCallCount(); e != g {
		t.Errorf("want CreateTaskCallCount is %d but got %d", e, g)
		return
	}
	for i := 0; i < faker.GetCreateTaskCallCount(); i++ {
		task, err := gtask.ToTask()
		if err != nil {
			t.Fatal(err)
		}
		req, err := task.ToCreateTaskRequestProto(testQueue)
		if err != nil {
			t.Fatal(err)
		}
		ms, err := faker.GetCreateTaskRequest(i)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range ms {
			if e, g := req, m; !proto.Equal(e, g) {
				t.Errorf("request want %q, but got %q", e, g)
			}
		}
	}
}

func newFakeService(t *testing.T) (tasksbox.Service, *faketasks.Faker) {
	ctx := context.Background()

	tasksFaker := faketasks.NewFaker(t)
	taskClient, err := cloudtasks.NewClient(ctx, tasksFaker.ClientOpt)
	if err != nil {
		t.Fatal(err)
	}

	s, err := tasksbox.NewService(ctx, taskClient)
	if err != nil {
		t.Fatal(err)
	}
	return s, tasksFaker
}
