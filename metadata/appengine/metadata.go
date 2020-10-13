// appengine is App Engine 固有のmetadataを扱う
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
package appengine

import (
	"os"

	metadatabox "github.com/sinmetalcraft/gcpbox/metadata"
)

// ServiceKey is App Engine Service Env Key
const ServiceKey = "GAE_SERVICE"

// VersionKey is App Engine Version Env Key
const VersionKey = "GAE_VERSION"

// InstanceKey is App Engine Instance Env Key
const InstanceKey = "GAE_INSTANCE"

// RuntimeKey is App Engine Runtime Env Key
const RuntimeKey = "GAE_RUNTIME"

// MemoryMBKey is App Engine Memory MB Env Key
const MemoryMBKey = "GAE_MEMORY_MB"

// DeploymentIDKey is App Engine Deployment ID Key
const DeploymentIDKey = "GAE_DEPLOYMENT_ID"

// EnvKey is App Engine Env Env Key
const EnvKey = "GAE_ENV"

// OnGAE is GAE上で動いているかどうかを返す
// GAE用の環境変数があるかどうかを見てるだけなので、偽装可能
func OnGAE() bool {
	_, err := Service()
	if err != nil {
		return false
	}
	return true
}

// OnGAEReal is AppがGAEで動いてるかどうか
// 環境変数およびOnGCP()も見てるので、本当にGAEで動いてるか
func OnGAEReal() bool {
	return OnGAE() && metadatabox.OnGCP()
}

// Service is return service id
// The service name specified in your app.yaml file. If no service name is specified, it is set to default.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func Service() (string, error) {
	v := os.Getenv(ServiceKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine Service id environment valiable is not found. plz set $GAE_SERVICE", nil, nil)
}

// Version is return version id
// The current version label of your service.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func Version() (string, error) {
	v := os.Getenv(VersionKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine Version id environment valiable is not found. plz set $GAE_VERSION", nil, nil)
}

// Instance is return version id
// The ID of the instance on which your service is currently running.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func Instance() (string, error) {
	v := os.Getenv(InstanceKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine Instance id environment valiable is not found. plz set $GAE_INSTANCE", nil, nil)
}

// Runtime is return runtime
// The runtime specified in your app.yaml file.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func Runtime() (string, error) {
	v := os.Getenv(RuntimeKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine Runtime id environment valiable is not found. plz set $GAE_RUNTIME", nil, nil)
}

// MemoryMB is return MemoryMB
// The amount of memory available to the application process, in MB.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func MemoryMB() (string, error) {
	v := os.Getenv(MemoryMBKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine MemoryMB id environment valiable is not found. plz set $GAE_MEMORY_MB", nil, nil)
}

// DeploymentID is return deployment id
// The ID of the current deployment.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func DeploymentID() (string, error) {
	v := os.Getenv(DeploymentIDKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine Deployment id environment valiable is not found. plz set $GAE_DEPLOYMENT_ID", nil, nil)
}

// Env is return env
// The App Engine environment. Set to standard.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func Env() (string, error) {
	v := os.Getenv(EnvKey)
	if v != "" {
		return v, nil
	}
	return "", metadatabox.NewErrNotFound("AppEngine Deployment id environment valiable is not found. plz set $GAE_ENV", nil, nil)
}
