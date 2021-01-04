package iap_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager"
	iapbox "github.com/sinmetalcraft/gcpbox/iap"
	crmv1 "google.golang.org/api/cloudresourcemanager/v1"
	crmv2 "google.golang.org/api/cloudresourcemanager/v2"
)

func TestCurrentUserWithContext(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111"
	const email = "sinmetal@sinmetalcraft.jp"
	r.Header.Set(iapbox.AuthenticatedUserIDKey, fmt.Sprintf("accounts.google.com:%s", id))
	r.Header.Set(iapbox.AuthenticatedUserEmailKey, fmt.Sprintf("accounts.google.com:%s", email))

	ctx := context.Background()
	ctx, u := iapbox.CurrentUserWithContext(ctx, r)
	_ = ctx
	if u == nil {
		t.Fatal("not login")
	}
	if e, g := id, u.ID; e != g {
		t.Errorf("id want %v but got %v", e, g)
	}
	if e, g := email, u.Email; e != g {
		t.Errorf("email want %v but got %v", e, g)
	}
}

func TestCurrentUserWithContextNotLogin(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	ctx, user := iapbox.CurrentUserWithContext(ctx, r)
	_ = ctx
	if user != nil {
		t.Errorf("user login")
	}
}

func TestCurrentUser(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	const id = "11111"
	const email = "sinmetal@sinmetalcraft.jp"
	r.Header.Set(iapbox.AuthenticatedUserIDKey, fmt.Sprintf("accounts.google.com:%s", id))
	r.Header.Set(iapbox.AuthenticatedUserEmailKey, fmt.Sprintf("accounts.google.com:%s", email))

	ctx := context.Background()
	ctx, u := iapbox.CurrentUserWithContext(ctx, r)
	if u == nil {
		t.Fatal("not login")
	}

	got := iapbox.CurrentUser(ctx)
	if got == nil {
		t.Fatal("not login")
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
			const id = "11111"

			r, err := http.NewRequest(http.MethodGet, "/", nil)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set(iapbox.AuthenticatedUserIDKey, fmt.Sprintf("accounts.google.com:%s", id))
			r.Header.Set(iapbox.AuthenticatedUserEmailKey, fmt.Sprintf("accounts.google.com:%s", tt.mail))

			ctx, _ := iapbox.CurrentUserWithContext(ctx, r)
			got, err := us.IsAdmin(ctx)
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

	crmv1Service, err := crmv1.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	crmv2Service, err := crmv2.NewService(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rmService, err := crmbox.NewResourceManagerService(ctx, crmv1Service, crmv2Service)
	if err != nil {
		t.Fatal(err)
	}
	us, err := iapbox.NewUserService(ctx, rmService)
	if err != nil {
		t.Fatal(err)
	}
	return us
}
