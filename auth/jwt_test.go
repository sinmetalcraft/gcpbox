package auth_test

import (
	"testing"

	"github.com/sinmetalcraft/gcpbox/auth"
)

func TestParseJWTPayload(t *testing.T) {
	payload, err := auth.ParseJWTPayload("Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjdkYTc4NjNlODYzN2Q2NjliYzJhMTI2MjJjZWRlMmE4ODEzZDExYjEiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJodHRwczovL2djcGJveHRlc3QtNzN6cnk0eWZ2cS1hbi5hLnJ1bi5hcHAvY2xvdWR0YXNrcy9ydW4vanNvbi1wb3N0LXRhc2siLCJhenAiOiIxMDI1NDI3MDMyMzMwNzE1MzM4OTciLCJlbWFpbCI6InNpbm1ldGFsLWNpQGFwcHNwb3QuZ3NlcnZpY2VhY2NvdW50LmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJleHAiOjE2MDI2Njc0MDQsImlhdCI6MTYwMjY2MzgwNCwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tIiwic3ViIjoiMTAyNTQyNzAzMjMzMDcxNTMzODk3In0.署名")
	if err != nil {
		t.Fatal(err)
	}
	if e, g := "https://gcpboxtest-73zry4yfvq-an.a.run.app/cloudtasks/run/json-post-task", payload.Audience; e != g {
		t.Errorf("want Audience %s but got %s", e, g)
	}
	if e, g := "102542703233071533897", payload.AuthorizedParty; e != g {
		t.Errorf("want AuthorizedParty %s but got %s", e, g)
	}
	if e, g := "sinmetal-ci@appspot.gserviceaccount.com", payload.Email; e != g {
		t.Errorf("want Email %s but got %s", e, g)
	}
	if e, g := true, payload.EmailVerified; e != g {
		t.Errorf("want EmailVerified %t but got %t", e, g)
	}
	if e, g := 1602667404, payload.Expires; e != g {
		t.Errorf("want Expires %d but got %d", e, g)
	}
	if e, g := 1602663804, payload.IssuedAt; e != g {
		t.Errorf("want IssuedAt %d but got %d", e, g)
	}
	if e, g := "https://accounts.google.com", payload.Issuer; e != g {
		t.Errorf("want Issuer %s but got %s", e, g)
	}
	if e, g := "102542703233071533897", payload.Subject; e != g {
		t.Errorf("want Subject %s but got %s", e, g)
	}
}
