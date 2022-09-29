package bigquery

type apiOptions struct {
	dryRun      bool
	wait        bool
	streamLogFn func(msg string)
}

type APIOptions func(options *apiOptions)

// WithDryRun is 変更が発生するbigqueryのAPIは実行しない
func WithDryRun() APIOptions {
	return func(ops *apiOptions) {
		ops.dryRun = true
	}
}

// WithWait is Jobが一つずつ完了するのを待ってから、次のJobを投入する
func WithWait() APIOptions {
	return func(ops *apiOptions) {
		ops.wait = true
	}
}

// WithStreamLogFn is Query結果を元にAPIを実行した時にログを処理できる関数を指定できる
func WithStreamLogFn(f func(msg string)) APIOptions {
	return func(ops *apiOptions) {
		ops.streamLogFn = f
	}
}
