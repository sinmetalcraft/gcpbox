package cloudresourcemanager

import (
	"fmt"

	"golang.org/x/xerrors"
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

// NewErrPermissionDenied is return ErrPermissionDenied
func NewErrPermissionDenied(message string, kv map[string]interface{}, err error) error {
	return &Error{
		Code:    ErrPermissionDenied.Code,
		Message: message,
		KV:      kv,
		err:     err,
	}
}
