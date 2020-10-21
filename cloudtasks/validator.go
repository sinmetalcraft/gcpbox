package cloudtasks

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sinmetalcraft/gcpbox/auth"
	"google.golang.org/api/idtoken"
)

// ValidateJWT is JWT を検証して、 Cloud Run Invoker を持った Service Account からの Request なのかを確かめる
// Cloud Run Invoker の場合は Authorization Header に JWT が入って Request が飛んでくる
// audience には Cloud Task からの Request の URL を指定する
func ValidateJWTFromInvoker(ctx context.Context, r *http.Request, audience string) (*auth.JWTPayload, error) {
	authzHeader := r.Header.Get("Authorization")
	tokens := strings.Split(authzHeader, " ")
	if len(tokens) < 1 {
		return nil, fmt.Errorf("invalid token format")
	}
	authzHeader = tokens[1]

	_, err := idtoken.Validate(ctx, authzHeader, audience)
	if err != nil {
		return nil, fmt.Errorf("failed idtoken.Validate: %v", err)
	}
	payload, err := auth.ParseJWTPayload(authzHeader)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
