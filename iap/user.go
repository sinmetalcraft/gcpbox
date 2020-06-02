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
	// AuthenticatedUserId is IAP User ID Header Key
	AuthenticatedUserIDKey = "X-Goog-Authenticated-User-Id"

	// AuthenticatedUserEmail is IAP User Email Header Key
	AuthenticatedUserEmailKey = "X-Goog-Authenticated-User-Email"
)

type contextUser struct{}

// ErrNotLogin is Loginしてない時に返す
var ErrNotLogin = errors.New("not login")

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

// WithContextValue is context に user をセットする
func WithContextValue(ctx context.Context, user *User) context.Context {
	return context.WithValue(ctx, contextUser{}, user)
}

// CurrentUser is context からUserを取得する
//
// 先に WithContextValue() でセットされていることが前提
func CurrentUser(ctx context.Context) (*User, bool) {
	user, ok := ctx.Value(contextUser{}).(*User)
	if ok {
		return user, true
	}
	return nil, false
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
// inherited されてる 権限は見ていない
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
