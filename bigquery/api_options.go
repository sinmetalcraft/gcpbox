package bigquery

type apiOptions struct {
	dryRun    bool
	streamLog chan string
}

type APIOptions func(options *apiOptions)

// WithDryRun is 変更が発生するbigqueryのAPIは実行しない
func WithDryRun() APIOptions {
	return func(ops *apiOptions) {
		ops.dryRun = true
	}
}

// WithStreamLog is Query結果を元にAPIを実行するような場合、変更を行うAPIを1件ずつLog出力する
func WithStreamLog(streamLog chan string) APIOptions {
	return func(ops *apiOptions) {
		ops.streamLog = streamLog
	}
}
