package appengine

import (
	"net/http"
	"testing"
)

// TestGetHeader is Get Header Test
//
// Sample Header
//  {
// 	"Content-Length": ["32"],
// 	"Content-Type": ["application/json"],
// 	"Forwarded": ["for=\"0.1.0.2\";proto=http"],
// 	"User-Agent": ["AppEngine-Google; (+http://code.google.com/appengine)"],
// 	"X-Appengine-Country": ["ZZ"],
// 	"X-Appengine-Default-Version-Hostname": ["sinmetal-ci.an.r.appspot.com"],
// 	"X-Appengine-Https": ["off"],
// 	"X-Appengine-Queuename": ["gcpboxtest"],
// 	"X-Appengine-Request-Log-Id": ["5ec5336300ff081ebd2028745b0001627e73696e6d6574616c2d63690001676370626f783a323032303035323074323233343530000100"],
// 	"X-Appengine-Tasketa": ["1589978096.945286"],
// 	"X-Appengine-Taskexecutioncount": ["0"],
// 	"X-Appengine-Taskname": ["65157617470264206751"],
// 	"X-Appengine-Taskretrycount": ["0"],
// 	"X-Appengine-User-Ip": ["0.1.0.2"],
// 	"X-Cloud-Trace-Context": ["ae3bc8f44b7b9a71d5836d2b313886e5/6038307574680104597;o=1"],
// 	"X-Forwarded-For": ["0.1.0.2, 169.254.1.1"],
// 	"X-Forwarded-Proto": ["http"],
// 	"X-Google-Internal-Skipadmincheck": ["true"]
// }
func TestGetHeader(t *testing.T) {
	r, err := http.NewRequest(http.MethodGet, "/hoge", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Google-Internal-Skipadmincheck", "true")
	r.Header.Set("X-Appengine-Taskname", "hoge")
	r.Header.Set("X-Appengine-Tasketa", "1589978096.945286")

	_, err = GetHeader(r)
	if err != nil {
		t.Fatal(err)
	}
}
