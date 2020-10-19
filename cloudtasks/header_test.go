package cloudtasks_test

import (
	"net/http"
	"testing"

	tasksbox "github.com/sinmetalcraft/gcpbox/cloudtasks"
)

// TestGetHeader is Get Header Test
//
// Sample Header
// X-Goog-XXXX は IAP 通した時だけ付いているもの。App Engineで受けた時のものなので、X-Appengine header も付いてる
/*
{
"Accept-Encoding": ["gzip,deflate,br"],
"Content-Length": ["22"],
"Forwarded": ["for=\"107.178.198.37\";proto=https"],
"Traceparent": ["00-8776346c8342bf2965086545fc998243-d62f64b9d0de713b-01"],
"User-Agent": ["Google-Cloud-Tasks"],
"X-Appengine-Auth-Domain": ["gmail.com"],
"X-Appengine-Country": ["ZZ"],
"X-Appengine-Default-Version-Hostname": ["sinmetal-ci.an.r.appspot.com"],
"X-Appengine-Https": ["on"],
"X-Appengine-Request-Log-Id": ["5f87afc100ff0f3b934423743a0001627e73696e6d6574616c2d63690001676370626f783a323032303130313574303031393136000100"],
"X-Appengine-Timeout-Ms": ["599999"],
"X-Appengine-User-Email": ["sinmetal-ci@appspot.gserviceaccount.com"],
"X-Appengine-User-Id": ["102542703233071533897"],
"X-Appengine-User-Ip": ["107.178.198.37"],
"X-Appengine-User-Is-Admin": ["1"],
"X-Appengine-User-Nickname": ["sinmetal-ci"],
"X-Appengine-User-Organization": [""],
"X-Cloud-Trace-Context": ["8776346c8342bf2965086545fc998243/15433665197257945403;o=1"],
"X-Cloudtasks-Queuename": ["gcpboxtest"],
"X-Cloudtasks-Tasketa": ["1602727873.950779"],
"X-Cloudtasks-Taskexecutioncount": ["0"],
"X-Cloudtasks-Taskname": ["85770091340881016951"],
"X-Cloudtasks-Taskpreviousresponse": ["0"],
"X-Cloudtasks-Taskretrycount": ["25"],
"X-Cloudtasks-Taskretryreason": [""],
"X-Forwarded-For": ["107.178.198.37, 169.254.1.1"],
"X-Forwarded-Proto": ["https"],
"X-Goog-Authenticated-User-Email": ["accounts.google.com:sinmetal-ci@appspot.gserviceaccount.com"],
"X-Goog-Authenticated-User-Id": ["accounts.google.com:102542703233071533897"],
"X-Goog-Iap-Jwt-Assertion": ["eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjBvZUxjUSJ9.eyJhdWQiOiIvcHJvamVjdHMvNDAxNTgwOTc5ODE5L2FwcHMvc2lubWV0YWwtY2kiLCJlbWFpbCI6InNpbm1ldGFsLWNpQGFwcHNwb3QuZ3NlcnZpY2VhY2NvdW50LmNvbSIsImV4cCI6MTYwMjcyODQ3NCwiaWF0IjoxNjAyNzI3ODc0LCJpc3MiOiJodHRwczovL2Nsb3VkLmdvb2dsZS5jb20vaWFwIiwic3ViIjoiYWNjb3VudHMuZ29vZ2xlLmNvbToxMDI1NDI3MDMyMzMwNzE1MzM4OTcifQ.署名"]
}
*/
func TestGetHeader(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/hoge", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Cloudtasks-Queuename", "gcpboxtest")
	r.Header.Set("X-Cloudtasks-Tasketa", "1602727873.950779")
	r.Header.Set("X-Cloudtasks-Taskexecutioncount", "1")
	r.Header.Set("X-Cloudtasks-Taskname", "85770091340881016951")
	r.Header.Set("X-Cloudtasks-Taskpreviousresponse", "0")
	r.Header.Set("X-Cloudtasks-Taskretrycount", "25")
	r.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:sinmetal-ci@appspot.gserviceaccount.com")
	r.Header.Set("X-Goog-Authenticated-User-Id", "accounts.google.com:102542703233071533897")

	th, err := tasksbox.GetHeader(r)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := "gcpboxtest", th.QueueName; e != g {
		t.Errorf("want QueueName %s but got %s", e, g)
	}
	if e, g := "85770091340881016951", th.TaskName; e != g {
		t.Errorf("want TaskName %s but got %s", e, g)
	}
	if e, g := 25, th.RetryCount; e != g {
		t.Errorf("want RetryCount %d but got %d", e, g)
	}
	if e, g := 1, th.ExecutionCount; e != g {
		t.Errorf("want ExecutionCount %d but got %d", e, g)
	}
	if th.ETA.IsZero() {
		t.Errorf("ETA is Zero")
	}
	if e, g := "0", th.PreviousResponse; e != g {
		t.Errorf("want PreviousResponse %s but got %s", e, g)
	}
}
