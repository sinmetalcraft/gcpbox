package statscopy

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/xerrors"
)

const queryStatsTopMinute = `
SELECT
  text,
  text_truncated,
  text_fingerprint,
  interval_end,
  execution_count,
  avg_latency_seconds,
  avg_rows,
  avg_bytes,
  avg_rows_scanned,
  avg_cpu_seconds,
  all_failed_execution_count,
  all_failed_avg_latency_seconds,
  cancelled_or_disconnected_execution_count,
  timed_out_execution_count,
FROM {{.Table}}
WHERE interval_end = TIMESTAMP(@IntervalEnd, "UTC")
`

type QueryStatsTopTable string

const (
	QueryStatsTopMinuteTable   QueryStatsTopTable = "spanner_sys.query_stats_top_minute"
	QueryStatsTop10MinuteTable QueryStatsTopTable = "spanner_sys.query_stats_top_10minute"
	QueryStatsTopHourTable     QueryStatsTopTable = "spanner_sys.query_stats_top_hour"
)

type QueryStatsParam struct {
	Table string
}

var _ bigquery.ValueSaver = &QueryStat{}

type QueryStat struct {
	IntervalEnd                           time.Time `spanner:"interval_end"` // End of the time interval that the included query executions occurred in.
	Text                                  string    // SQL query text, truncated to approximately 64KB.
	TextTruncated                         bool      `spanner:"text_truncated"`                            // Whether or not the query text was truncated.
	TextFingerprint                       int64     `spanner:"text_fingerprint"`                          // Hash of the query text.
	ExecuteCount                          int64     `spanner:"execution_count"`                           // Number of times Cloud Spanner saw the query during the interval.
	AvgLatencySeconds                     float64   `spanner:"avg_latency_seconds"`                       // Average length of time, in seconds, for each query execution within the database. This average excludes the encoding and transmission time for the result set as well as overhead.
	AvgRows                               float64   `spanner:"avg_rows"`                                  // Average number of rows that the query returned.
	AvgBytes                              float64   `spanner:"avg_bytes"`                                 // Average number of data bytes that the query returned, excluding transmission encoding overhead.
	AvgRowsScanned                        float64   `spanner:"avg_rows_scanned"`                          // Average number of rows that the query scanned, excluding deleted values.
	AvgCPUSeconds                         float64   `spanner:"avg_cpu_seconds"`                           // Average number of seconds of CPU time Cloud Spanner spent on all operations to execute the query.
	AllFailedExecutionCount               int64     `spanner:"all_failed_execution_count"`                // Number of times the query failed during the interval.
	AllFailedAvgLatencySeconds            float64   `spanner:"all_failed_avg_latency_seconds"`            // Average length of time, in seconds, for each query execution that failed within the database. This average excludes the encoding and transmission time for the result set as well as overhead.
	CancelledOrDisconnectedExecutionCount int64     `spanner:"cancelled_or_disconnected_execution_count"` // Number of times the query was canceled by the user or failed due to broken network connection during the interval.
	TimedOutExecutionCount                int64     `spanner:"timed_out_execution_count"`                 // Number of times the query timed out during the interval.
}

// Save is bigquery.ValueSaver interface
func (s *QueryStat) Save() (map[string]bigquery.Value, string, error) {
	insertID, err := s.InsertID()
	if err != nil {
		return nil, "", xerrors.Errorf("failed InsertID() : %w", err)
	}
	return map[string]bigquery.Value{
		"interval_end":                   s.IntervalEnd,
		"text":                           s.Text,
		"text_truncated":                 s.TextTruncated,
		"text_fingerprint":               s.TextFingerprint,
		"execution_count":                s.ExecuteCount,
		"avg_latency_seconds":            s.AvgLatencySeconds,
		"avg_rows":                       s.AvgRows,
		"avg_bytes":                      s.AvgBytes,
		"avg_rows_scanned":               s.AvgRowsScanned,
		"avg_cpu_seconds":                s.AvgCPUSeconds,
		"all_failed_execution_count":     s.AllFailedExecutionCount,
		"all_failed_avg_latency_seconds": s.AllFailedAvgLatencySeconds,
		"cancelled_or_disconnected_execution_count": s.CancelledOrDisconnectedExecutionCount,
		"timed_out_execution_count":                 s.TimedOutExecutionCount,
	}, insertID, nil
}

// InsertID is 同じデータをBigQueryになるべく入れないようにデータからInsertIDを作成する
func (s *QueryStat) InsertID() (string, error) {
	if s.IntervalEnd.IsZero() {
		return "", xerrors.New("IntervalEnd is required.")
	}
	if s.TextFingerprint == 0 {
		return "", xerrors.New("TextFingerprint is required.")
	}
	return fmt.Sprintf("GCPBOX_SpannerQueryStat-_-%v-_-%v", s.IntervalEnd.Unix(), s.TextFingerprint), nil
}

// QueryStatsBigQueryTableSchema is BigQuery Table Schema
var QueryStatsBigQueryTableSchema = bigquery.Schema{
	{Name: "interval_end", Required: true, Type: bigquery.TimestampFieldType},
	{Name: "text", Required: true, Type: bigquery.StringFieldType},
	{Name: "text_truncated", Required: true, Type: bigquery.BooleanFieldType},
	{Name: "text_fingerprint", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "execution_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "avg_latency_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_rows", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_bytes", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_rows_scanned", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_cpu_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "all_failed_execution_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "all_failed_avg_latency_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "cancelled_or_disconnected_execution_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "timed_out_execution_count", Required: true, Type: bigquery.IntegerFieldType},
}
