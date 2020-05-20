package cloudtasks

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// ErrNotFoundHeader is Cloud Tasks の Headerがない時に利用されます。
var ErrNotFoundHeader = errors.New("not found cloudtasks header")

const (

	// AppEngineTaskName Header Key
	AppEngineTaskName = "X-AppEngine-TaskName"

	// AppEngineQueueName Header Key
	AppEngineQueueName = "X-AppEngine-QueueName"

	// AppEngineTaskRetryCount Header Key
	AppEngineTaskRetryCount = "X-AppEngine-TaskRetryCount"

	// AppEngineTaskExecutionCount Header Key
	AppEngineTaskExecutionCount = "X-AppEngine-TaskExecutionCount"

	// AppEngineTaskETA Header Key
	AppEngineTaskETA = "X-AppEngine-TaskETA"

	// AppEngineTaskPreviousResponse Header Key
	AppEngineTaskPreviousResponse = "X-AppEngine-TaskPreviousResponse"

	// AppEngineTaskRetryReason Header Key
	AppEngineTaskRetryReason = "X-AppEngine-TaskRetryReason"

	// AppEngineFailFast Header Key
	AppEngineFailFast = "X-AppEngine-FailFast"
)

// AppEngineHeader is App Engine task handlers
// plz see https://cloud.google.com/tasks/docs/creating-appengine-handlers
type AppEngineHeader struct {

	// QueueName is The name of the queue.
	// Always there
	QueueName string

	// TaskName is The "short" name of the task, or, if no name was specified at creation, a unique system-generated id.
	// This is the 'my-task-id' value in the complete task name, ie, task_name = projects/my-project-id/locations/my-location/queues/my-queue-id/tasks/my-task-id.
	// Always there
	TaskName string

	// TaskRetryCount is The number of times this task has been retried.
	// For the first attempt, this value is 0.
	// This number includes attempts where the task failed due to a lack of available instances and never reached the execution phase.
	// Always there
	TaskRetryCount int64

	// TaskExecutionCount is The total number of times that the task has received a response from the handler.
	// Since Cloud Tasks deletes the task once a successful response has been received, all previous handler responses were failures.
	// This number does not include failures due to a lack of available instances.
	// Always there
	TaskExecutionCount int64

	// TaskEAT is The schedule time of the task
	// Always there
	TaskETA time.Time

	// TaskPreviousResponse is The HTTP response code from the previous retry.
	// optional
	TaskPreviousResponse string

	// TaskRetryReason is The reason for retrying the task.
	// optional
	TaskRetryReason string

	// FailFast is Indicates that a task fails immediately if an existing instance is not available.
	// optional
	FailFast bool
}

// GetAppEngineHeader return App Engine task header
func GetAppEngineHeader(r *http.Request) (*AppEngineHeader, error) {
	var ret AppEngineHeader

	v, ok := r.Header[AppEngineTaskName]
	if ok {
		ret.TaskName = v[0]
	} else {
		return nil, ErrNotFoundHeader
	}

	v, ok = r.Header[AppEngineQueueName]
	if ok {
		ret.QueueName = v[0]
	}

	v, ok = r.Header[AppEngineTaskRetryCount]
	if ok {
		i, err := strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskRetryCount, v)
		}
		ret.TaskRetryCount = i
	}

	v, ok = r.Header[AppEngineTaskExecutionCount]
	if ok {
		i, err := strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskExecutionCount, v)
		}
		ret.TaskExecutionCount = i
	}

	v, ok = r.Header[AppEngineTaskETA]
	if ok {
		i, err := strconv.ParseInt(v[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskETA, v)
		}
		ret.TaskETA = time.Unix(i, 0)
	}

	v, ok = r.Header[AppEngineTaskPreviousResponse]
	if ok {
		ret.TaskPreviousResponse = v[0]
	}

	v, ok = r.Header[AppEngineTaskRetryReason]
	if ok {
		ret.TaskRetryReason = v[0]
	}

	v, ok = r.Header[AppEngineFailFast]
	if ok {
		ret.FailFast = true
	}

	return &ret, nil
}
