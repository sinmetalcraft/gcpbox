package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// JWTPayload is JWT Payload
type JWTPayload struct {
	Audience        string `json:"aud"`            // JWTを受け取る予定の対象者 example:https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task
	AuthorizedParty string `json:"azp"`            // 認可された対象者 example:102542703233071533897
	Email           string `json:"email"`          // JWT発行者のEmail example:sinmetal-ci@appspot.gserviceaccount.com
	EmailVerified   bool   `json:"email_verified"` // メールアドレスが検証済みか
	Expires         int    `json:"exp"`            // 有効期限(EpochTime seconds) example:1602514972
	IssuedAt        int    `json:"iat"`            // 発行日時(EpochTime seconds) example:1602511372
	Issuer          string `json:"iss"`            // 発行者 (issuer) example:https://accounts.google.com
	Subject         string `json:"sub"`            // UserID example:102542703233071533897
}

// ParseJWTPayload is JWT全体から の Payload を抜き出して返す
func ParseJWTPayload(jwt string) (*JWTPayload, error) {
	list := strings.Split(jwt, ".")
	if len(list) != 3 {
		return nil, fmt.Errorf("invalid JWT format")
	}

	var payload *JWTPayload
	r := base64.NewDecoder(base64.RawStdEncoding, strings.NewReader(list[1]))
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return nil, fmt.Errorf("invalid JWT : %w", err)
	}
	return payload, nil
}
