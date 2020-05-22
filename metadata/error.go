package metadata

import "fmt"

// Error is Error interface
type Error interface {
	Code() ErrCode
	error
}

type appError struct {
	C ErrCode
	M string
}

func (e *appError) Code() ErrCode {
	return e.C
}

func (e *appError) Error() string {
	return e.M
}

// ErrCode is Error Code
type ErrCode int

const (
	// ErrUnknownCode is 予期せぬエラー
	ErrUnknownCode ErrCode = iota

	// ErrNotFoundCode is Not Found
	ErrNotFoundCode

	// ErrInvalidArgumentCode is Invalid Argument
	ErrInvalidArgumentCode
)

func errNotFound(msg string) Error {
	return &appError{C: ErrNotFoundCode, M: msg}
}

func errInvalidArgument(expected string, argument string) Error {
	return &appError{C: ErrInvalidArgumentCode, M: fmt.Sprintf("invalid argument. expected is %v, argument is = %v", expected, argument)}
}

// Is is 何のエラーか？
func Is(err error, code ErrCode) bool {
	e, ok := err.(Error)
	if !ok {
		return false
	}
	return e.Code() == code
}
