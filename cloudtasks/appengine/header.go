package appengine

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

	// GoogleInternalSkipAdminCheck Header Key
	GoogleInternalSkipAdminCheck = "X-Google-Internal-Skipadmincheck"
)

// Header is App Engine task handlers
// plz see https://cloud.google.com/tasks/docs/creating-appengine-handlers
type Header struct {

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

// GetHeader return App Engine task header
func GetHeader(r *http.Request) (*Header, error) {
	var ret Header

	v := r.Header.Get(GoogleInternalSkipAdminCheck)
	if v != "true" {
		return nil, ErrNotFoundHeader
	}

	v = r.Header.Get(AppEngineTaskName)
	if len(v) > 0 {
		ret.TaskName = v
	}

	v = r.Header.Get(AppEngineQueueName)
	if len(v) > 0 {
		ret.QueueName = v
	}

	v = r.Header.Get(AppEngineTaskRetryCount)
	if len(v) > 0 {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskRetryCount, v)
		}
		ret.TaskRetryCount = i
	}

	v = r.Header.Get(AppEngineTaskExecutionCount)
	if len(v) > 0 {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskExecutionCount, v)
		}
		ret.TaskExecutionCount = i
	}

	v = r.Header.Get(AppEngineTaskETA)
	if len(v) > 0 {
		l := strings.Split(v, ".")
		if len(l) < 2 {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskETA, v)
		}
		sec, err := strconv.ParseInt(l[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskETA, v)
		}
		microsec, err := strconv.ParseInt(l[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s. v=%v", AppEngineTaskETA, v)
		}
		ret.TaskETA = time.Unix(sec, microsec*1000)
	}

	v = r.Header.Get(AppEngineTaskPreviousResponse)
	if len(v) > 0 {
		ret.TaskPreviousResponse = v
	}

	v = r.Header.Get(AppEngineTaskRetryReason)
	if len(v) > 0 {
		ret.TaskRetryReason = v
	}

	v = r.Header.Get(AppEngineFailFast)
	if len(v) > 0 {
		ret.FailFast = true
	}

	return &ret, nil
}
