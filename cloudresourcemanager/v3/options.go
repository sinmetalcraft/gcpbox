package cloudresourcemanager

type existsMemberInheritOption struct {
	roles   []string
	topNode *ResourceID
	step    int
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
		ops.topNode = resource
	}
}

// WithStep is 階層を遡る段数の限界を指定する
func WithStep(step int) ExistsMemberInheritOptions {
	return func(ops *existsMemberInheritOption) {
		ops.step = step
	}
}
