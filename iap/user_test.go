package iap_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	crmbox "github.com/sinmetal/gcpbox/cloudresourcemanager"
	iapbox "github.com/sinmetal/gcpbox/iap"
	"google.golang.org/api/cloudresourcemanager/v1"
)

func TestGetUser(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111"
	const email = "sinmetal@sinmetalcraft.jp"
	r.Header.Set(iapbox.AuthenticatedUserIDKey, fmt.Sprintf("accounts.google.com:%s", id))
	r.Header.Set(iapbox.AuthenticatedUserEmailKey, fmt.Sprintf("accounts.google.com:%s", email))

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
	r.Header.Set(iapbox.AuthenticatedUserIDKey, fmt.Sprintf("accounts.google.com:%s", id))
	r.Header.Set(iapbox.AuthenticatedUserEmailKey, fmt.Sprintf("accounts.google.com:%s", email))

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
			u := &iapbox.User{
				Email: tt.mail,
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

func newTestUserService(t *testing.T) *iapbox.UserService {
	ctx := context.Background()

	crmService, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rmService, err := crmbox.NewResourceManagerService(ctx, crmService)
	if err != nil {
		t.Fatal(err)
	}
	us, err := iapbox.NewUserService(ctx, rmService)
	if err != nil {
		t.Fatal(err)
	}
	return us
}
