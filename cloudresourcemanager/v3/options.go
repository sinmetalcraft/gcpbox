package cloudresourcemanager

import "time"

type existsMemberInheritOption struct {
	roles         []string
	topNodes      []*ResourceID
	censoredNodes []*ResourceID
	step          int
}

// ExistsMemberInheritOptions is ExistsMemberInGCPProjectWithInherit に利用する options
type ExistsMemberInheritOptions func(*existsMemberInheritOption)

// WithRolesHaveOne is 指定したRoleの中のいずれか1つを持っているかを返す
func WithRolesHaveOne(roles ...string) ExistsMemberInheritOptions {
	return func(ops *existsMemberInheritOption) {
		ops.roles = roles
	}
}

// WithTopNode is 階層を遡る時にそこまでいったらやめるポイントを指定する
func WithTopNode(resource *ResourceID) ExistsMemberInheritOptions {
	return func(ops *existsMemberInheritOption) {
		ops.topNodes = append(ops.topNodes, resource)
	}
}

// WithTopNodes is 階層を遡る時にそこまでいったらやめるポイントを指定する
func WithTopNodes(resources []*ResourceID) ExistsMemberInheritOptions {
	return func(ops *existsMemberInheritOption) {
		ops.topNodes = append(ops.topNodes, resources...)
	}
}

// WithCensoredNodes is 指定したResourceが現れたら、そのResourceの権限はチェックせずに遡るのをやめる
func WithCensoredNodes(resources []*ResourceID) ExistsMemberInheritOptions {
	return func(ops *existsMemberInheritOption) {
		ops.censoredNodes = append(ops.censoredNodes, resources...)
	}
}

// WithStep is 階層を遡る段数の限界を指定する
func WithStep(step int) ExistsMemberInheritOptions {
	return func(ops *existsMemberInheritOption) {
		ops.step = step
	}
}

type getRelatedProjectOptions struct {
	apiCallCount int
	interval     time.Duration
}

// GetRelatedProjectOptions is GetRelatedProject()のOptions
type GetRelatedProjectOptions func(ops *getRelatedProjectOptions)

// WithIntervalRelatedProject is Cloud Resource Manager APIを実行する時にIntervalを置くようになる
// apiCallCountの回数実行後、interval待つ
// apiCallCountに0を指定すると、毎回interval待つ
func WithIntervalRelatedProject(apiCallCount int, interval time.Duration) GetRelatedProjectOptions {
	return func(ops *getRelatedProjectOptions) {
		ops.apiCallCount = apiCallCount
		if apiCallCount == 0 {
			ops.apiCallCount = 1
		}
		ops.interval = interval
	}
}
