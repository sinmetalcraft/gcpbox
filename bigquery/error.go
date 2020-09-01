package bigquery

import (
	"fmt"
	"strings"
	"sync"
)

var _ error = &StreamingInsertErrors{}
var _ error = &StreamingInsertError{}

type StreamingInsertErrors struct {
	mutex  sync.Mutex
	Errors []*StreamingInsertError
}

func (e *StreamingInsertErrors) Error() string {
	builder := strings.Builder{}
	for i, v := range e.Errors {
		builder.WriteString(v.Error())
		if i < len(e.Errors) {
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func (e *StreamingInsertErrors) Append(err *StreamingInsertError) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.Errors = append(e.Errors, err)
}

func (e *StreamingInsertErrors) ErrorOrNil() error {
	if len(e.Errors) > 0 {
		return e
	}
	return nil
}

type StreamingInsertError struct {
	InsertID string
	Err      error
}

func (e *StreamingInsertError) Error() string {
	return fmt.Errorf("InsertID:%s : %w", e.InsertID, e.Err).Error()
}
