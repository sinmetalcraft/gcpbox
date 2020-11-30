package appengine

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type MockService struct {
	mu sync.RWMutex
	addedTasks []*MockResult
}

type MockResult struct {
	Queue *Queue
	Tasks []*Task
	OPs []CreateTaskOptions
}

func (s *MockService) GetAddedTasks() []*MockResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	t := s.addedTasks
	s.addedTasks = []*MockResult{} // MockService が使い回されることを考えて、一度取ったら、消すようにしている
	return t
}

func (s *MockService) addAddedTasks(queue *Queue, tasks []*Task, ops ...CreateTaskOptions) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.addedTasks = append(s.addedTasks, &MockResult{
		Queue: queue,
		Tasks: tasks,
		OPs: ops,
	})
}

func (s *MockService) CreateTask(ctx context.Context, queue *Queue, task *Task, ops ...CreateTaskOptions) (string, error) {
	s.addAddedTasks(queue, []*Task{task}, ops...)
	return s.createMockTaskName(task), nil
}

func (s *MockService) CreateTaskMulti(ctx context.Context, queue *Queue, tasks []*Task, ops ...CreateTaskOptions) ([]string, error){
	s.addAddedTasks(queue,tasks, ops...)
	tns := make([]string,len(tasks))
	for _, task := range tasks {
		tns = append(tns, s.createMockTaskName(task))
	}
	return tns, nil
}

func (s *MockService) createMockTaskName(task *Task) string {
	if len(task.Name) > 0 {
		return task.Name
	}
	return fmt.Sprintf("mock_task_name:%s", uuid.New().String())
}

func (s *MockService) CreateJsonPostTask(ctx context.Context, queue *Queue, task *JsonPostTask, ops ...CreateTaskOptions) (string, error){
	t, err := task.ToTask()
	if err != nil {
		return "", err
	}

	return s.CreateTask(ctx, queue, t, ops...)
}

func (s *MockService) CreateJsonPostTaskMulti(ctx context.Context, queue *Queue, tasks []*JsonPostTask, ops ...CreateTaskOptions) ([]string, error){
	var ts []*Task
	merr := MultiError{}
	for _, task := range tasks {
		t, err := task.ToTask()
		if err != nil {
			merr.Append(&Error{
				Code: ErrInvalidArgument.Code,
				Message: "failed JsonPostTask.ToTask",
				err : err,
			})
		}
		ts =append(ts, t)
	}
	err := merr.ErrorOrNil()
	if err != nil {
		return nil, err
	}
	return s.CreateTaskMulti(ctx, queue, ts, ops...)
}

func (s *MockService) CreateGetTask(ctx context.Context, queue *Queue, task *GetTask, ops ...CreateTaskOptions) (string, error){
	t, err := task.ToTask()
	if err != nil {
		return "", err
	}

	return s.CreateTask(ctx, queue, t, ops...)
}

func (s *MockService) CreateGetTaskMulti(ctx context.Context, queue *Queue, tasks []*GetTask, ops ...CreateTaskOptions) ([]string, error){
	var ts []*Task
	merr := MultiError{}
	for _, task := range tasks {
		t, err := task.ToTask()
		if err != nil {
			merr.Append(&Error{
				Code: ErrInvalidArgument.Code,
				Message: "failed GetTask.ToTask",
				err : err,
			})
		}
		ts =append(ts, t)
	}
	err := merr.ErrorOrNil()
	if err != nil {
		return nil, err
	}
	return s.CreateTaskMulti(ctx, queue, ts, ops...)
}

