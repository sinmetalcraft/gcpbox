package metadata

import "os"

// AppEngineServiceKey is App Engine Service Env Key
const AppEngineServiceKey = "GAE_SERVICE"

// AppEngineVersionKey is App Engine Version Env Key
const AppEngineVersionKey = "GAE_VERSION"

// AppEngineInstanceKey is App Engine Instance Env Key
const AppEngineInstanceKey = "GAE_INSTANCE"

// AppEngineRuntimeKey is App Engine Runtime Env Key
const AppEngineRuntimeKey = "GAE_RUNTIME"

// AppEngineMemoryMBKey is App Engine Memory MB Env Key
const AppEngineMemoryMBKey = "GAE_MEMORY_MB"

// AppEngineDeploymentIDKey is App Engine Deployment ID Key
const AppEngineDeploymentIDKey = "GAE_DEPLOYMENT_ID"

// AppEngineEnvKey is App Engine Env Env Key
const AppEngineEnvKey = "GAE_ENV"

// AppEngineService is return service id
// The service name specified in your app.yaml file. If no service name is specified, it is set to default.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineService() (string, error) {
	v := os.Getenv(AppEngineServiceKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine Service id environment valiable is not found. plz set $GAE_SERVICE", nil, nil)
}

// AppEngineVersion is return version id
// The current version label of your service.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineVersion() (string, error) {
	v := os.Getenv(AppEngineVersionKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine Version id environment valiable is not found. plz set $GAE_VERSION", nil, nil)
}

// AppEngineInstance is return version id
// The ID of the instance on which your service is currently running.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineInstance() (string, error) {
	v := os.Getenv(AppEngineInstanceKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine Instance id environment valiable is not found. plz set $GAE_INSTANCE", nil, nil)
}

// AppEngineRuntime is return runtime
// The runtime specified in your app.yaml file.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineRuntime() (string, error) {
	v := os.Getenv(AppEngineRuntimeKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine Runtime id environment valiable is not found. plz set $GAE_RUNTIME", nil, nil)
}

// AppEngineMemoryMB is return MemoryMB
// The amount of memory available to the application process, in MB.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineMemoryMB() (string, error) {
	v := os.Getenv(AppEngineMemoryMBKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine MemoryMB id environment valiable is not found. plz set $GAE_MEMORY_MB", nil, nil)
}

// AppEngineDeploymentID is return deployment id
// The ID of the current deployment.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineDeploymentID() (string, error) {
	v := os.Getenv(AppEngineDeploymentIDKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine Deployment id environment valiable is not found. plz set $GAE_DEPLOYMENT_ID", nil, nil)
}

// AppEngineEnv is return env
// The App Engine environment. Set to standard.
// https://cloud.google.com/appengine/docs/standard/go/runtime#environment_variables
func AppEngineEnv() (string, error) {
	v := os.Getenv(AppEngineEnvKey)
	if v != "" {
		return v, nil
	}
	return "", NewErrNotFound("AppEngine Deployment id environment valiable is not found. plz set $GAE_ENV", nil, nil)
}
