package appengine_test

import (
	"context"
	"net/http"
	"testing"

	iapbox "github.com/sinmetal/gcpbox/iap/appengine"
)

func TestGetUser(t *testing.T) {
	cases := []struct {
		name      string
		admin     string
		wantAdmin bool
	}{
		{"admin", "1", true},
		{"non-admin", "0", false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodGet, "/", nil)
			if err != nil {
				t.Fatal(err)
			}
			const id = "11111"
			const email = "sinmetal@sinmetalcraft.jp"
			const nickname = "sinmetal"
			r.Header.Set(iapbox.UserIDKey, id)
			r.Header.Set(iapbox.UserEmailKey, email)
			r.Header.Set(iapbox.UserNicknameKey, nickname)
			r.Header.Set(iapbox.UserIsAdminKey, tt.admin)

			u, err := iapbox.GetUser(r)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := id, u.ID; e != g {
				t.Errorf("id want %v but got %v", e, g)
			}
			if e, g := email, u.Email; e != g {
				t.Errorf("email want %v but got %v", e, g)
			}
			if e, g := nickname, u.Nickname; e != g {
				t.Errorf("nickname want %v but got %v", e, g)
			}
			if e, g := tt.wantAdmin, u.Admin; e != g {
				t.Errorf("admin want %v but got %v", e, g)
			}
		})
	}
}

func TestGetUserNotLogin(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = iapbox.GetUser(r)
	if err != iapbox.ErrNotLogin {
		t.Errorf("got err is %v", err)
	}
}

func TestWithContextValue(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111"
	const email = "sinmetal@sinmetalcraft.jp"
	const nickname = "sinmetal"
	r.Header.Set(iapbox.UserIDKey, id)
	r.Header.Set(iapbox.UserEmailKey, email)
	r.Header.Set(iapbox.UserNicknameKey, nickname)
	r.Header.Set(iapbox.UserIsAdminKey, "1")

	u, err := iapbox.GetUser(r)
	if err != nil {
		t.Fatal(err)
	}

	ctx := iapbox.WithContextValue(context.Background(), u)
	got, ok := iapbox.CurrentUser(ctx)
	if !ok {
		t.Errorf("user not found")
	}
	if e, g := id, got.ID; e != g {
		t.Errorf("id want %v but got %v", e, g)
	}
}
