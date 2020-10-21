package auth_test

import (
	"net/http"
	"testing"

	"github.com/sinmetalcraft/gcpbox/auth"
)

func TestIsGCPInternal(t *testing.T) {
	r, err := http.NewRequest(http.MethodPost, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if auth.IsGCPInternal(r) {
		t.Errorf("want IsGCPInternal() is false...")
	}

	r.Header.Set("X-Google-Internal-Skipadmincheck", "true")
	if !auth.IsGCPInternal(r) {
		t.Errorf("want IsGCPInternal() is true...")
	}
}
