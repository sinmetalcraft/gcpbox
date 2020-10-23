package errors

import (
	"errors"
	"fmt"

	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrGRPC is GRPC Error Wrapper
var ErrGRPC = &GRPCErrorWrapper{
	KV: map[string]interface{}{},
}

type GRPCErrorWrapper struct {
	// err is the wrapped error that caused this Spanner error. The wrapped
	// error can be read with the Unwrap method.
	err error

	Message string

	KV map[string]interface{}
}

// Error implements error.Error.
func (e *GRPCErrorWrapper) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s : attribute:%+v", e.err.Error(), e.KV)
}

// Is is err equal check
func (e *GRPCErrorWrapper) Is(target error) bool {
	code1 := ErrCode(e.err)
	code2 := ErrCode(target)

	return code1 != codes.Unknown && code1 == code2
}

// Unwrap returns the wrapped error (if any).
func (e *GRPCErrorWrapper) Unwrap() error {
	return e.err
}

// GRPCStatus is GRPCStatus Interface
// https://github.com/grpc/grpc-go/blob/9519efffeb5d1897ae8671568871a6d476986524/status/status.go#L83-L85
func (e *GRPCErrorWrapper) GRPCStatus() *status.Status {
	sts, _ := UnwrapGRPCError(e.err)
	return sts
}

func UnwrapGRPCError(err error) (*status.Status, bool) {
	cerr := err
	for {
		sts, ok := status.FromError(cerr)
		if ok {
			return sts, true
		}
		nerr := errors.Unwrap(cerr)
		if nerr == nil {
			return nil, false
		}
		cerr = nerr
	}
}

// ErrCode is 元になった gRPC の ErrCode を返す
func ErrCode(err error) codes.Code {
	sts, ok := UnwrapGRPCError(err)
	if !ok {
		return codes.Unknown
	}
	return sts.Code()
}

// ErrMessage is return Message
func ErrMessage(err error) string {
	var se *GRPCErrorWrapper
	if !xerrors.As(err, &se) {
		return err.Error()
	}
	return se.Message
}

func NewErrGRPC(message string, kv map[string]interface{}, err error) error {
	if err == nil {
		return &GRPCErrorWrapper{
			Message: message,
			KV:      kv,
			err:     fmt.Errorf("The original error is empty"),
		}
	}
	return &GRPCErrorWrapper{
		Message: message,
		KV:      kv,
		err:     err,
	}
}
