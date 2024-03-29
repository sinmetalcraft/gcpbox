package cloudrun_test

import (
	"errors"
	"testing"

	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"

	cloudrunmetadatabox "github.com/sinmetalcraft/gcpbox/metadata/cloudrun"
)

func TestOnCloudRunReal(t *testing.T) {
	v := cloudrunmetadatabox.OnCloudRunReal()
	if v {
		t.Errorf("want OnCloudRunReal is false") // Cloud Run上ではTestを回さないので、常にfalseになる
	}
}

func TestCloudRunService(t *testing.T) {
	_, err := cloudrunmetadatabox.Service()
	if !errors.Is(err, metadatabox.ErrNotFound) {
		t.Errorf("want ErrNotFound but got %v", err) // Cloud RunではTestを回さないので、NotFoundになる
	}
}
