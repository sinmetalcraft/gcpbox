package appengine

import (
	"net/http"
	"testing"
)

func TestGetHeader(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/hoge", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Appengine-Taskname", "hoge")

	_, err = GetHeader(r)
	if err != nil {
		t.Fatal(err)
	}
}
