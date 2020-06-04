package appengine

import (
	"context"
	"net/http"
)

const (
	// UserIDKey is App Engine User ID Header Key
	UserIDKey = "X-Appengine-User-Id"

	// UserEmailKey is App Engine User Email Header Key
	UserEmailKey = "X-Appengine-User-Email"

	// UserNicknameKey is App Engine User Nickname Header Key
	UserNicknameKey = "X-Appengine-User-Nickname"

	// UserIsAdminKey is App Engine User Is Admin Key
	UserIsAdminKey = "X-Appengine-User-Is-Admin"
)

type contextUser struct{}

// User is IAPを通してログインしているUser
// App Engine 用
//
// note: Every user has the same user ID for all App Engine applications.
// If your app uses the user ID in public data, such as by including it in a URL parameter, you should use a hash algorithm with a "salt" value added to obscure the ID.
// Exposing raw IDs could allow someone to associate a user's activity in one app with that in another, or get the user's email address by coercing the user to sign in to another app.
type User struct {
	ID       string
	Email    string
	Nickname string
	Admin    bool
}

// CurrentUserWithContext is IAPを通してログインしているUserを取得する
func CurrentUserWithContext(ctx context.Context, r *http.Request) (context.Context, *User) {
	id := r.Header.Get(UserIDKey)
	email := r.Header.Get(UserEmailKey)
	nickname := r.Header.Get(UserNicknameKey)
	isAdmin := r.Header.Get(UserIsAdminKey)

	if id == "" {
		return ctx, nil
	}

	user := &User{
		ID:       id,
		Email:    email,
		Nickname: nickname,
		Admin:    (isAdmin == "1"),
	}

	return context.WithValue(ctx, contextUser{}, user), user
}

// CurrentUser is context からUserを取得する
//
// CurrentUserWithContext()で返ってきたcontextを渡すと、その中からuserを取り出す
func CurrentUser(ctx context.Context) *User {
	user, ok := ctx.Value(contextUser{}).(*User)
	if ok {
		return user
	}
	return nil
}
