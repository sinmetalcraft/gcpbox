package iap

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/sinmetal/gcpbox/cloudresourcemanager"
	"github.com/sinmetal/gcpbox/metadata"
)

const (
	// AppEngineUserIDKey is App Engine User ID Header Key
	AppEngineUserIDKey = "X-Appengine-User-Id"

	// AppEngineUserEmailKey is App Engine User Email Header Key
	AppEngineUserEmailKey = "X-Appengine-User-Email"

	// AppEngineUserNicknameKey is App Engine User Nickname Header Key
	AppEngineUserNicknameKey = "X-Appengine-User-Nickname"

	// AppEngineUserIsAdmin is App Engine User Is Admin Key
	AppEngineUserIsAdminKey = "X-Appengine-User-Is-Admin"
)

const (
	// AuthenticatedUserId is IAP User ID Header Key
	AuthenticatedUserIDKey = "X-Goog-Authenticated-User-Id"

	// AuthenticatedUserEmail is IAP User Email Header Key
	AuthenticatedUserEmailKey = "X-Goog-Authenticated-User-Email"
)

// ErrNotLogin is Loginしてない時に返す
var ErrNotLogin = errors.New("not login")

// UserForAppEngine is IAPを通してログインしているUser
// App Engine 用
//
// note: Every user has the same user ID for all App Engine applications.
// If your app uses the user ID in public data, such as by including it in a URL parameter, you should use a hash algorithm with a "salt" value added to obscure the ID.
// Exposing raw IDs could allow someone to associate a user's activity in one app with that in another, or get the user's email address by coercing the user to sign in to another app.
type UserForAppEngine struct {
	ID       string
	Email    string
	Nickname string
	Admin    bool
}

// GetUserForAppEngine is IAPを通してログインしているUserを取得する
// App Engine 用
func GetUserForAppEngine(r *http.Request) (*UserForAppEngine, error) {
	id := r.Header.Get(AppEngineUserIDKey)
	email := r.Header.Get(AppEngineUserEmailKey)
	nickname := r.Header.Get(AppEngineUserNicknameKey)
	isAdmin := r.Header.Get(AppEngineUserIsAdminKey)

	if id == "" {
		return nil, ErrNotLogin
	}

	return &UserForAppEngine{
		ID:       id,
		Email:    email,
		Nickname: nickname,
		Admin:    (isAdmin == "1"),
	}, nil
}

// User is IAPを通してログインしているUser
//
// note: Every user has the same user ID for all App Engine applications.
// If your app uses the user ID in public data, such as by including it in a URL parameter, you should use a hash algorithm with a "salt" value added to obscure the ID.
// Exposing raw IDs could allow someone to associate a user's activity in one app with that in another, or get the user's email address by coercing the user to sign in to another app.
type User struct {
	ID    string
	Email string
}

// GetUser is IAPを通してログインしているUserを取得する
//
// accounts.google.com:XXXXXX という値が入っているはずなので、後ろ側の部分を取って設定している
// https://cloud.google.com/iap/docs/identity-howto?hl=en#getting_the_users_identity_with_signed_headers
func GetUser(r *http.Request) (*User, error) {
	id := r.Header.Get(AuthenticatedUserIDKey)
	email := r.Header.Get(AuthenticatedUserEmailKey)

	if id == "" {
		return nil, ErrNotLogin
	}

	idSplits := strings.Split(id, ":")
	if len(idSplits) < 2 {
		return nil, ErrNotLogin
	}
	emailSplits := strings.Split(email, ":")
	if len(emailSplits) < 2 {
		return nil, ErrNotLogin
	}

	return &User{
		ID:    idSplits[1],
		Email: emailSplits[1],
	}, nil
}

// UserService is App Engine User Serviceっぽいものを実装している
type UserService struct {
	crmService *cloudresourcemanager.ResourceManagerService
}

// NewUserService is return UserService
func NewUserService(ctx context.Context, crmService *cloudresourcemanager.ResourceManagerService) (*UserService, error) {
	return &UserService{
		crmService: crmService,
	}, nil
}

// IsAdminForAppEngine is App Engine User ServiceのようにProjectの権限を持っているかどうかを返す
// Cloud Resource Manager Serviceを利用して、実行ProjectのIAMを見るので、実行するクライアントがIAMを閲覧できる権限を持っている必要がある。
//
// need resourcemanager.projects.getIamPolicy
// resourcemanager.projects.getIamPolicy を持っている規定済みIAMは以下辺り
// https://cloud.google.com/iam/docs/understanding-roles?hl=en#primitive_roles
// https://cloud.google.com/iam/docs/understanding-roles?hl=en#resource-manager-roles
// https://cloud.google.com/iam/docs/understanding-roles?hl=en#project-roles
func (s *UserService) IsAdmin(ctx context.Context, u *User) (bool, error) {
	pID, err := metadata.ProjectID()
	if err != nil {
		return false, err
	}
	b, err := s.crmService.ExistsMemberInGCPProject(ctx, pID, u.Email, "roles/viewer", "roles/editor", "roles/owner")
	if err != nil {
		return false, err
	}
	return b, nil
}
