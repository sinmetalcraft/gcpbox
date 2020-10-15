package cloudtasks

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (

	// QueueName is X-CloudTasks-QueueName Header Key
	QueueName = "X-CloudTasks-QueueName"

	// TaskName is X-CloudTasks-TaskName Header Key
	TaskName = "X-CloudTasks-TaskName"

	// RetryCount is X-CloudTasks-TaskRetryCount Header Key
	RetryCount = "X-CloudTasks-TaskRetryCount"

	// ExecutionCount is X-CloudTasks-TaskExecutionCount Header Key
	ExecutionCount = "X-CloudTasks-TaskExecutionCount"

	// ETA is X-CloudTasks-TaskETA Header Key
	ETA = "X-CloudTasks-TaskETA"

	// PreviousResponse is X-CloudTasks-TaskPreviousResponse Header Key
	PreviousResponse = "X-CloudTasks-TaskPreviousResponse"

	// RetryReason is X-CloudTasks-TaskRetryReason Header Key
	RetryReason = "X-CloudTasks-TaskRetryReason"
)

// Header is Cloud Tasks から来た Request の Header
// https://cloud.google.com/tasks/docs/creating-http-target-tasks#handler
type Header struct {

	// QueueName is Queue Name
	QueueName string

	// TaskName is Task の Short Name
	// または、作成時に名前が指定されなかった場合は、システムによって生成された一意の ID です。
	// これは、完全なタスク名（task_name = projects/my-project-id/locations/my-location/queues/my-queue-id/tasks/my-task-id）の my-task-id 値になります。
	TaskName string

	// RetryCount is このタスクが再試行された回数。
	// 最初の試行の場合は、この値は 0 です。
	// この試行回数には、インスタンス数不足が原因でタスクが異常終了したため実行フェーズに到達できなかった試行も含まれています。
	RetryCount int

	// ExecutionCount is タスクがハンドラからレスポンスを受け取った合計回数。
	// Cloud Tasks は成功のレスポンスを受け取った時点でタスクを削除するため、それ以前のハンドラからのレスポンスはすべて失敗を意味します。
	// この回数には、インスタンス数不足が原因の失敗は含まれていません。
	ExecutionCount int

	// タスクのスケジュール時間。
	ETA time.Time

	// PreviousResponse is 前回の再試行の HTTP レスポンス コード。
	// optional
	PreviousResponse string

	// RetryReason is タスクを再試行する理由。
	// optional
	RetryReason string
}

// GetHeader is return Cloud Task Header
func GetHeader(r *http.Request) (*Header, error) {
	var ret Header

	{
		v := r.Header.Get(QueueName)
		if len(v) < 1 {
			return nil, NewErrInvalidHeader("QueueName not found", map[string]interface{}{}, nil)
		}
		ret.QueueName = v
	}
	{
		v := r.Header.Get(TaskName)
		if len(v) < 1 {
			return nil, NewErrInvalidHeader("TaskName not found", map[string]interface{}{}, nil)
		}
		ret.TaskName = v
	}
	{
		v := r.Header.Get(RetryCount)
		if len(v) < 1 {
			return nil, NewErrInvalidHeader("RetryCount not found", map[string]interface{}{}, nil)
		}
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, NewErrInvalidHeader("RetryCount invalid format", map[string]interface{}{"RetryCount": v}, err)
		}
		ret.RetryCount = i
	}
	{
		v := r.Header.Get(ExecutionCount)
		if len(v) < 1 {
			return nil, NewErrInvalidHeader("ExecutionCount not found", map[string]interface{}{}, nil)
		}
		i, err := strconv.Atoi(v)
		if err != nil {
			return nil, NewErrInvalidHeader("ExecutionCount invalid format", map[string]interface{}{"ExecutionCount": v}, err)
		}
		ret.ExecutionCount = i
	}
	{
		v := r.Header.Get(ETA)
		if len(v) < 1 {
			return nil, NewErrInvalidHeader("ETA not found", map[string]interface{}{}, nil)
		}
		l := strings.Split(v, ".")
		if len(l) < 2 {
			return nil, NewErrInvalidHeader("ETA invalid format", map[string]interface{}{"ETA": v}, nil)
		}
		sec, err := strconv.ParseInt(l[0], 10, 64)
		if err != nil {
			return nil, NewErrInvalidHeader("ETA invalid format", map[string]interface{}{"ETA": v}, err)
		}
		microsec, err := strconv.ParseInt(l[1], 10, 64)
		if err != nil {
			return nil, NewErrInvalidHeader("ETA invalid format", map[string]interface{}{"ETA": v}, err)
		}
		ret.ETA = time.Unix(sec, microsec*1000)
	}

	ret.PreviousResponse = r.Header.Get(PreviousResponse)
	ret.RetryReason = r.Header.Get(RetryReason)
	return &ret, nil
}
