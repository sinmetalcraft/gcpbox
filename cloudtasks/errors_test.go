package cloudtasks_test

import (
	"errors"
	"fmt"
	"testing"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
)

func TestMultiError_As(t *testing.T) {
	var err error = &tasksbox.MultiError{}
	err = fmt.Errorf("hello : %w", err)
	var target *tasksbox.MultiError
	if !errors.As(err, &target) {
		t.Error("failed As...")
	}
}
