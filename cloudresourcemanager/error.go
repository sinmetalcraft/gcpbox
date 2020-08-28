package cloudresourcemanager

import (
	"fmt"

	"golang.org/x/xerrors"
)

// ErrPermissionDenied is 権限エラーの時に返す
var ErrPermissionDenied = &Error{
	Code:    "403",
	Target:  "unknown",
	Message: "permission denied",
}

// Error is Error情報を保持する struct
// Target には Permission を持っている対象の相手
// Message は付加情報が入っている
type Error struct {
	Code    string
	Target  string
	Message string
	err     error
}

// NewErrPermissionDenied is return ErrPermissionDenied
func NewErrPermissionDenied(target string, message string, err error) error {
	return &Error{
		Code:    ErrPermissionDenied.Code,
		Target:  target,
		Message: message,
		err:     err,
	}
}

// Error is error interface func
func (e *Error) Error() string {
	return fmt.Sprintf("PermissionDenied: target=%s: %s", e.Target, e.Message)
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
