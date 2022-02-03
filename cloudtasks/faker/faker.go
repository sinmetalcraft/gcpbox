package faker

import (
	"fmt"
	"testing"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
	tasksfaker "github.com/sinmetalcraft/gcpfaker/cloudtasks"
	"google.golang.org/api/option"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/protobuf/proto"
)

// Faker is UnitTestのために Fake 実装
type Faker struct {
	org *tasksfaker.Faker
}

// NewFaker is Faker を返す
func NewFaker(t *testing.T) *Faker {
	return &Faker{tasksfaker.NewFaker(t)}
}

// Stop is Stop
func (f *Faker) Stop() {
	f.org.Stop()
}

// ClientOption is cloudtasks.Client に 設定する ClientOption
func (f *Faker) ClientOption() option.ClientOption {
	return f.org.ClientOpt
}

// GetCreateTaskCallCount is CreateTask が実行された回数を返す
func (s *Faker) GetCreateTaskCallCount() int {
	return s.org.GetCreateTaskCallCount()
}

// GetTask is 作成された Task を取得する
func (s *Faker) GetTask(i int) (*tasksbox.Task, error) {
	req, err := s.org.GetCreateTaskRequest(i)
	if err != nil {
		return nil, err
	}
	buf, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	var tr taskspb.CreateTaskRequest
	if err := proto.Unmarshal(buf, &tr); err != nil {
		return nil, err
	}
	t := tr.GetTask()
	httpReq := t.GetHttpRequest()
	if httpReq == nil {
		return nil, fmt.Errorf("http request is required")
	}

	method, err := tasksbox.HttpMethodProtoToHttpMethod(httpReq.GetHttpMethod())
	if err != nil {
		return nil, err
	}

	return &tasksbox.Task{
		Audience:     "", // TODO AuthorizationHeader
		RelativeURI:  httpReq.Url,
		Method:       method,
		ScheduleTime: t.GetScheduleTime().AsTime(),
		Deadline:     t.GetDispatchDeadline().AsDuration(),
		Body:         httpReq.GetBody(),
		Name:         t.GetName(),
	}, nil
}
