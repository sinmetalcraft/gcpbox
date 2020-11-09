package cloudtasks_test

import (
	"testing"

	"golang.org/x/xerrors"

	"github.com/sinmetalcraft/gcpbox/cloudtasks"
)

func TestMultiError_As(t *testing.T) {
	var err error = &cloudtasks.MultiError{}
	err = xerrors.Errorf("hello : %w", err)
	var target *cloudtasks.MultiError
	if !xerrors.As(err, &target) {
		t.Error("failed As...")
	}
}
