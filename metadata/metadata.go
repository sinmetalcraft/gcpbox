package metadata

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/compute/metadata"
)

// OnGCP is GCP上で動いているかどうかを返す
// GCP上と判断されるか確認したのは以下
// Google App Engine Standard for Go 1.11
// Google Compute Engine
// Google Kubernetes Engine
func OnGCP() bool {
	return metadata.OnGCE()
}

// ProjectID is Return current GCP ProjectID
// GCP上で動いている場合は、Project Metadataから取得し、そうでなければ、環境変数から取得する
func ProjectID() (string, error) {
	if !OnGCP() {
		p := os.Getenv("GOOGLE_CLOUD_PROJECT")
		if p != "" {
			return p, nil
		}
		p = os.Getenv("GCLOUD_PROJECT")
		if p != "" {
			return p, nil
		}
		return "", NewErrNotFound("project id environment valiable is not found. plz set $GOOGLE_CLOUD_PROJECT", nil, nil)
	}

	projectID, err := metadata.ProjectID()
	if err != nil {
		return "", fmt.Errorf("failed get project id from metadata server: %w", err)
	}
	if projectID == "" {
		return "", NewErrNotFound("project id is not found", nil, nil)
	}
	return projectID, nil
}

// NumericProjectID is Return current Numeric GCP ProjectID
// GCP上で動いている場合は、Project Metadataから取得し、そうでなければ、環境変数から取得する
func NumericProjectID() (string, error) {
	if !OnGCP() {
		p := os.Getenv("NUMERIC_GOOGLE_CLOUD_PROJECT")
		if p != "" {
			return p, nil
		}
		return "", NewErrNotFound("numeric project id environment valiable is not found. plz set $NUMERIC_GOOGLE_CLOUD_PROJECT", nil, nil)
	}
	numericProjectID, err := metadata.NumericProjectID()
	if err != nil {
		return "", fmt.Errorf("failed get numeric project id from metadata server: %w", err)
	}
	return numericProjectID, nil
}

// ServiceAccountEmail is Return current Service Account Email
// GCP上で動いている場合は、Metadataから取得し、そうでなければ、環境変数から取得する
func ServiceAccountEmail() (string, error) {
	if !OnGCP() {
		return os.Getenv("GCLOUD_SERVICE_ACCOUNT"), nil
	}
	sa, err := getMetadata("service-accounts/default/email")
	if err != nil {
		return "", fmt.Errorf("failed get ServiceAccountEmail : %w", err)
	}
	return string(sa), nil
}

// ServiceAccountName is Return current Service Account Name
// ServiceAccountEmailの@より前の部分を返す
func ServiceAccountName() (string, error) {
	sa, err := ServiceAccountEmail()
	if err != nil {
		return "", err
	}
	l := strings.Split(string(sa), "@")
	if len(l) != 2 {
		return "", fmt.Errorf("invalid ServiceAccountEmail. email=%s", sa)
	}
	return l[0], nil
}

// ServiceAccountID is Return current Service Account ID
// fmt "projects/$PROJECT_ID/serviceAccounts/$SERVICE_ACCOUNT_EMAIL"
func ServiceAccountID() (string, error) {
	sa, err := ServiceAccountEmail()
	if err != nil {
		return "", err
	}
	pID, err := ProjectID()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("projects/%s/serviceAccounts/%s", pID, sa), nil
}

// Region is Appが動いているRegionを取得する
func Region() (string, error) {
	if !OnGCP() {
		return os.Getenv("INSTANCE_REGION"), nil
	}
	zone, err := getMetadata("zone")
	if err != nil {
		return "", fmt.Errorf("failed get Zone : %w", err)
	}

	return ExtractionRegion(string(zone))
}

// Zone is Appが動いているZoneを取得する
func Zone() (string, error) {
	if !OnGCP() {
		return os.Getenv("INSTANCE_ZONE"), nil
	}
	zone, err := getMetadata("zone")
	if err != nil {
		return "", fmt.Errorf("failed get Zone : %w", err)
	}

	return ExtractionZone(string(zone))
}

// ExtractionRegion is Metadata Serverから取得する projects/[NUMERIC_PROJECT_ID]/zones/[ZONE] 形式の文字列から、Region部分を取り出す
func ExtractionRegion(metaZone string) (string, error) {
	l := strings.Split(string(metaZone), "/")
	if len(l) < 1 {
		return "", NewErrInvalidArgument("required format : projects/[NUMERIC_PROJECT_ID]/zones/[ZONE]", map[string]interface{}{"input_argument": metaZone}, nil)
	}
	v := l[len(l)-1]
	if len(v) < 3 {
		return "", NewErrInvalidArgument("required format : projects/[NUMERIC_PROJECT_ID]/zones/[ZONE]", map[string]interface{}{"input_argument": metaZone}, nil)
	}
	v = v[:len(v)-2]
	return v, nil
}

// ExtractionZone is Metadata Serverから取得する projects/[NUMERIC_PROJECT_ID]/zones/[ZONE] 形式の文字列から、Zone部分を取り出す
func ExtractionZone(metaZone string) (string, error) {
	l := strings.Split(string(metaZone), "/")
	if len(l) < 1 {
		return "", NewErrInvalidArgument("required format : projects/[NUMERIC_PROJECT_ID]/zones/[ZONE]", map[string]interface{}{"input_argument": metaZone}, nil)
	}
	return l[len(l)-1], nil
}

// InstanceID is Metadata ServerからInstanceIDを取得する
// /computeMetadata/v1/instance/id
func InstanceID() (string, error) {
	if !OnGCP() {
		return os.Getenv("INSTANCE_ID"), nil
	}
	id, err := getMetadata("id")
	if err != nil {
		return "", fmt.Errorf("failed get instance id : %w", err)
	}
	return string(id), nil
}

// Hostname is Metadata ServerからHostnameを取得する
// /computeMetadata/v1/hostname
func Hostname() (string, error) {
	if !OnGCP() {
		return os.Getenv("HOSTNAME"), nil
	}
	id, err := getMetadata("hostname")
	if err != nil {
		return "", fmt.Errorf("failed get hostname : %w", err)
	}
	return string(id), nil
}

// ServiceAccountDefaultToken is Metadata ServerからServiceAccountDefaultTokenを取得する
// /computeMetadata/v1/instance/service-accounts/default/token
func ServiceAccountDefaultToken() (string, error) {
	if !OnGCP() {
		return os.Getenv("SERVICE_ACCOUNTS_DEFAULT_TOKEN"), nil
	}
	v, err := getMetadata("service-accounts/default/token")
	if err != nil {
		return "", fmt.Errorf("failed get service account default token : %w", err)
	}
	return string(v), nil
}

// GetInstanceAttribute is Instance Metadataを取得する
// GCP以外で動いている時は、環境変数を取得する
func GetInstanceAttribute(key string) (string, error) {
	if !OnGCP() {
		return os.Getenv(fmt.Sprintf("INSTANCE_%s", key)), nil
	}

	v, err := metadata.InstanceAttributeValue(key)
	if err != nil {
		return "", err
	}
	return v, nil
}

// GetProjectAttribute is Project Metadataを取得する
// GCP以外で動いている時は、環境変数を取得する
func GetProjectAttribute(key string) (string, error) {
	if !OnGCP() {
		return os.Getenv(fmt.Sprintf("PROJECT_%s", key)), nil
	}

	v, err := metadata.ProjectAttributeValue(key)
	if err != nil {
		return "", err
	}
	return v, nil
}

func getMetadata(path string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://metadata.google.internal/computeMetadata/v1/instance/%s", path), nil)
	if err != nil {
		return nil, fmt.Errorf("failed http.NewRequest. path=%s : %w", path, err)
	}
	req.Header.Set("Metadata-Flavor", "Google")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed http.SendReq. path=%s : %w", path, err)
	}
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed read response.Body. path=%s : %w", path, err)
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metadata server response is %v:%v", res.StatusCode, string(b))
	}

	return b, nil
}
