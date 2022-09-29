package statscopy

import (
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
)

const readStatsTopMinute = `
SELECT 
  interval_end,
  read_columns,
  fprint,
  execution_count,
  avg_rows,
  avg_bytes,
  avg_cpu_seconds,
  avg_locking_delay_seconds,
  avg_client_wait_seconds,
  avg_leader_refresh_delay_seconds,
FROM {{.Table}}
WHERE interval_end = TIMESTAMP(@IntervalEnd, "UTC")
`

type ReadStatsTopTable string

const (
	ReadStatsTopMinuteTable   ReadStatsTopTable = "spanner_sys.read_stats_top_minute"
	ReadStatsTop10MinuteTable ReadStatsTopTable = "spanner_sys.read_stats_top_10minute"
	ReadStatsTopHourTable     ReadStatsTopTable = "spanner_sys.read_stats_top_hour"
)

type ReadStatsParam struct {
	Table string
}

var _ bigquery.ValueSaver = &ReadStat{}

type ReadStat struct {
	IntervalEnd                  time.Time `spanner:"interval_end"` // End of the time interval that the included query executions occurred in.
	ReadColumns                  []string  `spanner:"read_columns"` // The set of columns that were read. These are in alphabetical order.
	Fprint                       int64     // Hash of the read column names.
	ExecutionCount               int64     `spanner:"execution_count"`                  // Number of times Cloud Spanner executed the read shape during the interval.
	AvgRows                      float64   `spanner:"avg_rows"`                         // Average number of rows that the read returned.
	AvgBytes                     float64   `spanner:"avg_bytes"`                        // Average number of data bytes that the read returned, excluding transmission encoding overhead.
	AvgCPUSeconds                float64   `spanner:"avg_cpu_seconds"`                  // Average number of Cloud Spanner server side CPU seconds executing the read, excluding prefetch CPU and other overhead.
	AvgLockingDelaySeconds       float64   `spanner:"avg_locking_delay_seconds"`        // Average number of seconds spent waiting due to locking.
	AvgClientWaitSeconds         float64   `spanner:"avg_client_wait_seconds"`          // Average number of seconds spent waiting due to the client not consuming data as fast as Cloud Spanner could generate it.
	AvgLeaderRefreshDelaySeconds float64   `spanner:"avg_leader_refresh_delay_seconds"` // Average number of seconds spent waiting to confirm with the Paxos leader that all writes have been observed..
}

// Save is bigquery.ValueSaver interface
func (s *ReadStat) Save() (map[string]bigquery.Value, string, error) {
	insertID, err := s.InsertID()
	if err != nil {
		return nil, "", fmt.Errorf("failed InsertID() : %w", err)
	}
	return map[string]bigquery.Value{
		"interval_end":                     s.IntervalEnd,
		"read_columns":                     s.ReadColumns,
		"fprint":                           s.Fprint,
		"execution_count":                  s.ExecutionCount,
		"avg_rows":                         s.AvgRows,
		"avg_bytes":                        s.AvgBytes,
		"avg_cpu_seconds":                  s.AvgCPUSeconds,
		"avg_locking_delay_seconds":        s.AvgLockingDelaySeconds,
		"avg_client_wait_seconds":          s.AvgClientWaitSeconds,
		"avg_leader_refresh_delay_seconds": s.AvgLeaderRefreshDelaySeconds,
	}, insertID, nil
}

// InsertID is 同じデータをBigQueryになるべく入れないようにデータからInsertIDを作成する
func (s *ReadStat) InsertID() (string, error) {
	if s.IntervalEnd.IsZero() {
		return "", errors.New("IntervalEnd is required")
	}
	if s.Fprint == 0 {
		return "", errors.New("Fprint is required")
	}
	return fmt.Sprintf("GCPBOX_SpannerReadStat-_-%d-_-%d", s.IntervalEnd.Unix(), s.Fprint), nil
}

// ReadStatsBigQueryTableSchema is BigQuery Table Schema
var ReadStatsBigQueryTableSchema = bigquery.Schema{
	{Name: "interval_end", Required: true, Type: bigquery.TimestampFieldType},
	{Name: "read_columns", Required: true, Repeated: true, Type: bigquery.StringFieldType},
	{Name: "fprint", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "execution_count", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "avg_rows", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_bytes", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_cpu_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_locking_delay_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_client_wait_seconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "avg_leader_refresh_delay_seconds", Required: true, Type: bigquery.FloatFieldType},
}
