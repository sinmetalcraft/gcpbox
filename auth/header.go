package auth

import "net/http"

// IsGCPInternal is Request が GCP 内部の Component から送られてきたかを返す
//
// 該当するもの
// Cloud Tasks App Engine Task
func IsGCPInternal(r *http.Request) bool {
	return r.Header.Get("X-Google-Internal-Skipadmincheck") == "true"
}
