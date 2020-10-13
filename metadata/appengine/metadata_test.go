package appengine_test

import (
	"testing"

	gaemetadatabox "github.com/sinmetalcraft/gcpbox/metadata/appengine"
)

func TestOnGAEReal(t *testing.T) {
	if gaemetadatabox.OnGAEReal() {
		t.Errorf("want OnGAEReal is false.") // TestはGAE上で実行しないので、falseになる
	}
}
