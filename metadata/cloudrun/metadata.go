// cloudrun is Cloud Run 固有のmetadataを扱う
// https://cloud.google.com/run/docs/reference/container-contract#env-vars
package cloudrun

import (
	"os"

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
