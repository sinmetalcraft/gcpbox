package appengine_test

import (
	"testing"

	"golang.org/x/xerrors"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks/appengine"
)

func TestMultiError_As(t *testing.T) {
	var err error = &tasksbox.MultiError{}
	err = xerrors.Errorf("hello : %w", err)
	var target *tasksbox.MultiError
	if !xerrors.As(err, &target) {
		t.Error("failed As...")
	}
}
