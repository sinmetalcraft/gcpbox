package errors

import (
	"fmt"
	"strings"
	"sync"
)

var _ error = &BigQueryStreamingInsertErrors{}
var _ error = &BigQueryStreamingInsertError{}

type BigQueryStreamingInsertErrors struct {
	mutex  *sync.Mutex
	Errors []*BigQueryStreamingInsertError
}

func (e *BigQueryStreamingInsertErrors) Error() string {
	builder := strings.Builder{}
	for i, v := range e.Errors {
		builder.WriteString(v.Error())
		if i < len(e.Errors) {
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func (e *BigQueryStreamingInsertErrors) Append(err *BigQueryStreamingInsertError) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.Errors = append(e.Errors, err)
}

func (e *BigQueryStreamingInsertErrors) ErrorOrNil() error {
	if len(e.Errors) > 0 {
		return e
	}
	return nil
}

type BigQueryStreamingInsertError struct {
	InsertID string
	Err      error
}

func (e *BigQueryStreamingInsertError) Error() string {
	return fmt.Errorf("InsertID:%s : %w", e.InsertID, e.Err).Error()
}
