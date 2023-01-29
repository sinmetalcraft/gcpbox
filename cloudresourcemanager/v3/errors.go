package cloudresourcemanager

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrPermissionDenied is 権限エラーの時に返す
var ErrPermissionDenied = &Error{
	Code:    "PermissionDenied",
	Message: "permission denied",
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

	v, err := json.Marshal(e.KV)
	if err != nil {
		return fmt.Sprintf("%s: %s: failed attribute marshal.err=%s. internal err=%s", e.Code, e.Message, err, e.err)
	}
	return fmt.Sprintf("%s:%s attribute:%+v internal err=%s", e.Code, e.Message, string(v), e.err)
}

// Is is err equal check
func (e *Error) Is(target error) bool {
	var appErr *Error
	if !errors.As(target, &appErr) {
		return false
	}
	return e.Code == appErr.Code
}

// Unwrap is return unwrap error
func (e *Error) Unwrap() error {
	return e.err
}

// NewErrPermissionDenied is return ErrPermissionDenied
func NewErrPermissionDenied(message string, kv map[string]interface{}, err error) error {
	return &Error{
		Code:    ErrPermissionDenied.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}
