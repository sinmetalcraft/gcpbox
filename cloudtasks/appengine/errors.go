package appengine

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/xerrors"
)

var _ error = &MultiError{}

// ErrInvalidHeader is Header が invalid な時に返す
var ErrInvalidHeader = &Error{
	Code:    "InvalidHeader",
	Message: "InvalidHeader",
	KV:      map[string]interface{}{},
}

// ErrInvalidRequest is 引数が invalid な時に返す
var ErrInvalidArgument = &Error{
	Code:    "InvalidArgument",
	Message: "InvalidArgument",
	KV:      map[string]interface{}{},
}

// ErrCreateMultiTask is CreateMultiTask の時に MultiError に入れる Error
var ErrCreateMultiTask = &Error{
	Code:    "FailedCreateMultiTask",
	Message: "FailedCreateMultiTask",
	KV:      map[string]interface{}{},
}

// ErrAlreadyExists is すでに存在している場合の Error
// 主に TaskName が重複した場合に返す https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues.tasks/create#body.request_body.FIELDS.task
var ErrAlreadyExists = &Error{
	Code:    "AlreadyExists",
	Message: "AlreadyExists",
	KV:      map[string]interface{}{},
}

// Error is Error情報を保持する struct
type Error struct {
	Code    string
	Message string
	KV      map[string]interface{}
	err     error
}

// Error is error interface func
func (e *Error) Error() string {
	if e.KV == nil || len(e.KV) < 1 {
		return fmt.Sprintf("%s: %s: %s", e.Code, e.Message, e.err)
	}
	return fmt.Sprintf("%s: %s: attribute:%+v :%s", e.Code, e.Message, e.KV, e.err)
}

// Is is err equal check
func (e *Error) Is(target error) bool {
	var appErr *Error
	if !xerrors.As(target, &appErr) {
		return false
	}
	return e.Code == appErr.Code
}

// Unwrap is return unwrap error
func (e *Error) Unwrap() error {
	return e.err
}

// MultiError is 複数の error を返す
type MultiError struct {
	mutex  sync.Mutex
	Errors []*Error
}

func (e *MultiError) Error() string {
	builder := strings.Builder{}
	for i, v := range e.Errors {
		builder.WriteString(v.Error())
		if i < len(e.Errors) {
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

// Is is err equal check
func (e *MultiError) Is(target error) bool {
	var appErr *MultiError
	return xerrors.As(target, &appErr)
}

// Unwrap is return unwrap error
func (e *MultiError) Unwrap() error {
	return nil
}

func (e *MultiError) Append(err *Error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.Errors = append(e.Errors, err)
}

func (e *MultiError) ErrorOrNil() error {
	if len(e.Errors) > 0 {
		return e
	}
	return nil
}

// NewErrInvalidHeader is return ErrInvalidHeader
func NewErrInvalidHeader(message string, kv map[string]interface{}, err error) error {
	return &Error{
		Code:    ErrInvalidHeader.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}

// NewErrInvalidArgument is return ErrInvalidArgument
func NewErrInvalidArgument(message string, kv map[string]interface{}, err error) error {
	return &Error{
		Code:    ErrInvalidArgument.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}

// NewErrAlreadyExists is return ErrAlreadyExists
func NewErrAlreadyExists(message string, kv map[string]interface{}, err error) *Error {
	return &Error{
		Code:    ErrAlreadyExists.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}

// NewErrCreateMultiTask is return ErrCreateMultiTask
func NewErrCreateMultiTask(message string, kv map[string]interface{}, err error) *Error {
	return &Error{
		Code:    ErrCreateMultiTask.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}
