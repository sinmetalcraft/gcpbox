package metricsscope

import crmbox "github.com/sinmetalcraft/gcpbox/cloudresourcemanager/v3"

type importServiceOptions struct {
	skipResources []*crmbox.ResourceID
}

type ImportServiceOptions func(option *importServiceOptions)

// WithSkipResources is SkipするResourceを指定する
// folderを指定した場合はfolder配下すべてをSkipする
func WithSkipResources(resources ...*crmbox.ResourceID) ImportServiceOptions {
	return func(ops *importServiceOptions) {
		ops.skipResources = resources
	}
}
