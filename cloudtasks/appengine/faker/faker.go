package faker

import (
	"fmt"
	"testing"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks/appengine"
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
	mlist, err := s.org.GetCreateTaskRequest(i)
	if err != nil {
		return nil, err
	}
	buf, err := proto.Marshal(mlist[0])
	if err != nil {
		return nil, err
	}
	var tr taskspb.CreateTaskRequest
	if err := proto.Unmarshal(buf, &tr); err != nil {
		return nil, err
	}
	t := tr.GetTask()
	appEngineReq := t.GetAppEngineHttpRequest()
	if appEngineReq == nil {
		return nil, fmt.Errorf("AppEngineHttpRequest is required")
	}
	rouing, err := tasksbox.AppEngineRoutingProtoToRouting(appEngineReq.GetAppEngineRouting())
	if err != nil {
		return nil, err
	}
	method, err := tasksbox.HttpMethodProtoToHttpMethod(appEngineReq.GetHttpMethod())
	if err != nil {
		return nil, err
	}
	return &tasksbox.Task{
		Name:             t.GetName(),
		Routing:          rouing,
		Headers:          appEngineReq.GetHeaders(),
		Method:           method,
		RelativeURI:      appEngineReq.GetRelativeUri(),
		Body:             appEngineReq.GetBody(),
		ScheduleTime:     t.GetScheduleTime().AsTime(),
		DispatchDeadline: t.GetDispatchDeadline().AsDuration(),
	}, nil
}
