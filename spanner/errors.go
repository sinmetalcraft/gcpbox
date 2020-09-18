package spanner

import (
	"fmt"

	"golang.org/x/xerrors"
)

// ErrNotFound is 見つからなかった時に返す
var ErrNotFound = &Error{
	Code:    "NotFound",
	Message: "not found",
	KV:      map[string]interface{}{},
}

// ErrInvalidArgument is 引数に問題がある時に返す
var ErrInvalidArgument = &Error{
	Code:    "InvalidArgument",
	Message: "invalid argument",
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
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: attribute:%+v", e.Code, e.Message, e.KV)
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

// NewErrNotFound is return ErrNotFound
func NewErrNotFound(key string, err error) error {
	return &Error{
		Code:    ErrNotFound.Code,
		Message: ErrNotFound.Message,
		KV: map[string]interface{}{
			"Target": key,
		},
		err: err,
	}
}

// NewErrInvalidArgument is return InvalidArgument
func NewErrInvalidArgument(message string, kv map[string]interface{}, err error) error {
	return &Error{
		Code:    ErrInvalidArgument.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}
