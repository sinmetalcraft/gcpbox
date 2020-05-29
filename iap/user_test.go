package iap_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	crmbox "github.com/sinmetal/gcpbox/cloudresourcemanager"
	"github.com/sinmetal/gcpbox/iap"
	"google.golang.org/api/cloudresourcemanager/v1"
)

func TestGetUser(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111"
	const email = "sinmetal@sinmetalcraft.jp"
	r.Header.Set(iap.AuthenticatedUserIDKey, fmt.Sprintf("accounts.google.com:%s", id))
	r.Header.Set(iap.AuthenticatedUserEmailKey, fmt.Sprintf("accounts.google.com:%s", email))

	u, err := iap.GetUser(r)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := id, u.ID; e != g {
		t.Errorf("id want %v but got %v", e, g)
	}
	if e, g := email, u.Email; e != g {
		t.Errorf("email want %v but got %v", e, g)
	}
}

func TestGetUserNotLogin(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = iap.GetUser(r)
	if err != iap.ErrNotLogin {
		t.Errorf("got err is %v", err)
	}
}

func TestGetUserForAppEngine(t *testing.T) {
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
			r.Header.Set(iap.AppEngineUserIDKey, id)
			r.Header.Set(iap.AppEngineUserEmailKey, email)
			r.Header.Set(iap.AppEngineUserNicknameKey, nickname)
			r.Header.Set(iap.AppEngineUserIsAdminKey, tt.admin)

			u, err := iap.GetUserForAppEngine(r)
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

func TestGetUserForAppEngineNotLogin(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = iap.GetUserForAppEngine(r)
	if err != iap.ErrNotLogin {
		t.Errorf("got err is %v", err)
	}
}

func TestUserService_IsAdmin(t *testing.T) {
	ctx := context.Background()

	us := newTestUserService(t)

	cases := []struct {
		name string
		mail string
		want bool
	}{
		{"empty", "", false},
		{"admin", "sinmetal@sinmetalcraft.jp", true},
		{"non-admin", "example@example.com", false},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			u := &iap.User{
				Email: "sinmetal@sinmetalcraft.jp",
			}
			got, err := us.IsAdmin(ctx, u)
			if err != nil {
				t.Fatal(err)
			}
			if e, g := tt.want, got; e != g {
				t.Errorf("want %v but got %v", e, g)
			}
		})
	}
}

func newTestUserService(t *testing.T) *iap.UserService {
	ctx := context.Background()

	crmService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rmService, err := crmbox.NewResourceManagerService(ctx, crmService)
	if err != nil {
		t.Fatal(err)
	}
	us, err := iap.NewUserService(ctx, rmService)
	if err != nil {
		t.Fatal(err)
	}
	return us
}
