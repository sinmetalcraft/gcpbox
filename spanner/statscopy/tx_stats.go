package statscopy

import (
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/xerrors"
)

const txStatsTopMinute = `
SELECT 
  interval_end,
  fprint,
  read_columns,
  write_constructive_columns,
  write_delete_tables,
  commit_attempt_count,
  commit_abort_count,
  commit_retry_count,
  commit_failed_precondition_count,
  avg_participants,
  avg_total_latency_seconds,
  avg_commit_latency_seconds,
  avg_bytes
FROM {{.Table}}
WHERE interval_end = TIMESTAMP(@IntervalEnd, "UTC")
`

type TxStatsTopTable string

const (
	TxStatsTopMinuteTable   TxStatsTopTable = "spanner_sys.txn_stats_top_minute"
	TxStatsTop10MinuteTable TxStatsTopTable = "spanner_sys.txn_stats_top_10minute"
	TxStatsTopHourTable     TxStatsTopTable = "spanner_sys.txn_stats_top_hour"
)

type TxStatsParam struct {
	Table string
}

var _ bigquery.ValueSaver = &TxStat{}

type TxStat struct {
	IntervalEnd              time.Time `spanner:"interval_end"` // End of the time interval that the included query executions occurred in.
	Fprint                   int64     // Fingerprint is the hash calculated based on the operations involved in the transaction. INTERVAL_END and FPRINT together act as an unique key for these tables.
	ReadColumns              []string  `spanner:"read_columns"`               // The set of columns that were read by the transaction.
	WriteConstructiveColumns []string  `spanner:"write_constructive_columns"` // The set of columns that were constructively written (i.e. assigned to new values) by the transaction.
	WriteDeleteTables        []string  `spanner:"write_delete_tables"`        // The set of tables that had rows deleted or replaced by the transaction.
	CommitAttemptCount       int64     `spanner:"commit_attempt_count"`       // 	Total number of commit attempts on the transaction.
	CommitAbortCount         int64     `spanner:"commit_abort_count"`         // Number of times the commits were aborted for the transaction.

	/*
	  Number of commit attempts that are retries from previously aborted attempts.
	  A Cloud Spanner transaction may have been tried multiple times before it commits due to lock contentions or transient events.
	  A high number of retries relative to commit attempts indicates that there may be issues worth investigating.
	  For more information, see Understanding transactions and commit counts on this page.
	  https://cloud.google.com/spanner/docs/introspection/transaction-statistics?hl=en#commit-counts
	*/
	CommitRetryCount int64 `spanner:"commit_retry_count"`

	CommitFailedPreconditionCount int64 `spanner:"commit_failed_precondition_count"` // Total number of precondition failures (FAILED_PRECONDITION) for the transaction.

	AvgParticipants         float64 `spanner:"avg_participants"`           // Average number of participants in each commit attempt. To learn more about participants, see Life of Cloud Spanner Reads & Writes.
	AvgTotalLatencySeconds  float64 `spanner:"avg_total_latency_seconds"`  // Average seconds taken from the first operation of the transaction to commit/abort.
	AvgCommitLatencySeconds float64 `spanner:"avg_commit_latency_seconds"` // Average seconds taken to perform the commit operation.
	AvgBytes                float64 `spanner:"avg_bytes"`                  // Average number of bytes written by the transaction.
}

// Save is bigquery.ValueSaver interface
func (s *TxStat) Save() (map[string]bigquery.Value, string, error) {
	insertID, err := s.InsertID()
	if err != nil {
		return nil, "", xerrors.Errorf("failed InsertID() : %w", err)
	}
	return map[string]bigquery.Value{
		"interval_end":                     s.IntervalEnd,
		"fprint":                           s.Fprint,
		"read_columns":                     s.ReadColumns,
		"write_constructive_columns":       s.WriteConstructiveColumns,
		"write_delete_tables":              s.WriteDeleteTables,
		"commit_attempt_count":             s.CommitAttemptCount,
		"commit_abort_count":               s.CommitAbortCount,
		"commit_retry_count":               s.CommitRetryCount,
		"commit_failed_precondition_count": s.CommitFailedPreconditionCount,
		"avg_participants":                 s.AvgParticipants,
		"avg_total_latency_seconds":        s.AvgTotalLatencySeconds,
		"avg_commit_latency_seconds":       s.AvgCommitLatencySeconds,
		"avg_bytes":                        s.AvgBytes,
	}, insertID, nil
}

// InsertID is 同じデータをBigQueryになるべく入れないようにデータからInsertIDを作成する
func (s *TxStat) InsertID() (string, error) {
	if s.IntervalEnd.IsZero() {
		return "", xerrors.New("IntervalEnd is required.")
	}
	if s.Fprint == 0 {
		return "", xerrors.New("Fprint is required.")
	}
	return fmt.Sprintf("GCPBOX_SpannerTxStat-_-%d-_-%d", s.IntervalEnd.Unix(), s.Fprint), nil
}

// TxStatsBigQueryTableSchema is BigQuery Table Schema
var TxStatsBigQueryTableSchema = bigquery.Schema{
	{Name: "interval_end", Required: true, Type: bigquery.TimestampFieldType},
	{Name: "fprint", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "read_columns", Required: true, Repeated: true, Type: bigquery.StringFieldType},
	{Name: "write_constructive_columns", Required: true, Repeated: true, Type: bigquery.StringFieldType},
	{Name: "write_delete_tables", Required: true, Repeated: true, Type: bigquery.StringFieldType},
	{Name: "commit_attempt_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "commit_abort_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "commit_retry_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "commit_failed_precondition_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "avg_participants", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_total_latency_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_commit_latency_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_bytes", Required: true, Type: bigquery.FloatFieldType},
}
