package cloudtasks

type createTaskOptions struct {
	ignoreAlreadyExists bool
}

// CreateTaskOptions is CreateTask に利用する options
type CreateTaskOptions func(*createTaskOptions)

// WithIgnoreAlreadyExists is CreateTask 時に AlreadyExists を無視する
// TaskName を指定した状態での Retry 時にすでにAddされているものは無視すればよい場合に使う
func WithIgnoreAlreadyExists() CreateTaskOptions {
	return func(ops *createTaskOptions) {
		ops.ignoreAlreadyExists = true
	}
}
