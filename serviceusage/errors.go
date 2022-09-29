package serviceusage

import (
	"errors"
	"fmt"
)

// ErrUnsupportedState is サポートしていないStateが渡された
var ErrUnsupportedState = &Error{
	Code:    "UnsupportedState",
	Message: "unsupported state",
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
	if !errors.As(target, &appErr) {
		return false
	}
	return e.Code == appErr.Code
}

// Unwrap is return unwrap error
func (e *Error) Unwrap() error {
	return e.err
}

// NewUnsupportedState is return ErrNotFound
func NewUnsupportedState(state string) error {
	return &Error{
		Code:    ErrUnsupportedState.Code,
		Message: fmt.Sprintf("%s is unsupported state. plz state is %s or %s", state, StateEnabled, StateDisabled),
		KV:      map[string]interface{}{},
	}
}
