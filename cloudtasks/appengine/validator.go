package appengine

import (
	"context"
	"fmt"
	"net/http"

	"github.com/sinmetalcraft/gcpbox/auth"
	"google.golang.org/api/idtoken"
)

// ValidateJWTFromHttpTargetTask is Http Target Task を App Engine で受けた時の JWT の検証を行う
// App Engine Task の場合は JWT はくっついていないので、 auth.IsGCPInternal() をチェックすることになる
func ValidateJWTFromHttpTargetTask(ctx context.Context, r *http.Request, projectNumber string, projectID string) (*auth.JWTPayload, error) {
	iapJWT := r.Header.Get("X-Goog-IAP-JWT-Assertion")
	aud := fmt.Sprintf("/projects/%s/apps/%s", projectNumber, projectID)

	_, err := idtoken.Validate(ctx, iapJWT, aud)
	if err != nil {
		return nil, fmt.Errorf("idtoken.Validate: %v", err)
	}

	payload, err := auth.ParseJWTPayload(iapJWT)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
