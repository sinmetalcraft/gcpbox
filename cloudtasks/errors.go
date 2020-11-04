package cloudtasks

import (
	"fmt"

	"golang.org/x/xerrors"
)

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
