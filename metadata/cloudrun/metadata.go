// cloudrun is Cloud Run 固有のmetadataを扱う
// https://cloud.google.com/run/docs/reference/container-contract#env-vars
package cloudrun

import (
	"fmt"
	"os"
	"strings"

	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
)

// ServiceKey is Cloud Run Service Env Key
const ServiceKey = "K_SERVICE"

// RevisionKey is Cloud Run Revision Env Key
const RevisionKey = "K_REVISION"

// ConfigurationKey is Cloud Run Configuration Env Key
const ConfigurationKey = "K_CONFIGURATION"

// OnCloudRun is AppがCloud Runで動いてるかどうか
// 環境変数見てるだけなので、偽装可能
func OnCloudRun() bool {
	_, err := Service()
	return err == nil
}

// OnCloudRunReal is AppがCloudRunで動いてるかどうか
// 環境変数およびOnGCP()も見てるので、本当にCloud Runで動いてるか
func OnCloudRunReal() bool {
	return OnCloudRun() && metadatabox.OnGCP()
}

// Service is return Cloud Run service id
// Example gcpboxtest
func Service() (string, error) {
	v := os.Getenv(ServiceKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("CloudRun Service id environment valiable is not found. plz set $K_SERVICE", nil, nil)
}

// Service is return Cloud Run service revision id
// Example gcpboxtest-00009-xiz
func Revision() (string, error) {
	v := os.Getenv(RevisionKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("CloudRun Service id environment valiable is not found. plz set $K_REVISION", nil, nil)
}

// Service is return Cloud Run service revision id
// The name of the Cloud Run configuration that created the revision.
// Example gcpboxtest
func Configuration() (string, error) {
	v := os.Getenv(ConfigurationKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("CloudRun Service id environment valiable is not found. plz set $K_CONFIGURATION", nil, nil)
}

// ProjectID is return ProjectID
// Example gcpboxtest
//
// GCP上で動いていない場合、$GOOGLE_CLOUD_PROJECT $GCLOUD_PROJECT を返す
func ProjectID() (string, error) {
	return metadatabox.ProjectID()
}

// NumericProjectID is return Project Number
//
// GCP上で動いていない場合、$NUMERIC_GOOGLE_CLOUD_PROJECT を返す
func NumericProjectID() (string, error) {
	return metadatabox.NumericProjectID()
}

// Region is return Cloud Runが動作しているRegionを返す
// Example asia-northeast1
//
// GCP上で動いていない場合、 $INSTANCE_REGION を返す
func Region() (string, error) {
	v, err := metadatabox.Region()
	if err != nil {
		return "", err
	}
	return metadatabox.ExtractionRegion(v)
}

// InstanceID is Cloud Runが動いているWorkerのIDを返す
//
// GCP上で動いていない場合、 $INSTANCE_ID を返す
func InstanceID() (string, error) {
	return metadatabox.InstanceID()
}

// ServiceAccountsDefaultToken is default service accountのAccess Tokenを返す
//
// GCP上で動いていない場合、 $SERVICE_ACCOUNTS_DEFAULT_TOKEN を返す
func ServiceAccountsDefaultToken() (string, error) {
	return metadatabox.ServiceAccountDefaultToken()
}

// ServiceAccountEmail is default service accountのEmailを返す
//
// GCP上で動いていない場合、 $GCLOUD_SERVICE_ACCOUNT を返す
func ServiceAccountEmail() (string, error) {
	return metadatabox.ServiceAccountEmail()
}

// ServiceAccountName is Return current Service Account Name
// ServiceAccountEmailの@より前の部分を返す
//
// GCP上で動いていない場合、 $GCLOUD_SERVICE_ACCOUNT を見て返す
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
//
// GCP上で動いていない場合、 $GOOGLE_CLOUD_PROJECT $GCLOUD_PROJECT $GCLOUD_SERVICE_ACCOUNT を見て返す
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
