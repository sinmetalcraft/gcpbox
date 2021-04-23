package statscopy

import (
	"encoding/base64"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/xerrors"
)

const lockStatsTopMinute = `
SELECT 
  interval_end,
  row_range_start_key,
  lock_wait_seconds,
  sample_lock_requests
FROM {{.Table}}
WHERE interval_end = TIMESTAMP(@IntervalEnd, "UTC")
`

type LockStatsTopTable string

const (
	LockStatsTopMinuteTable   LockStatsTopTable = "spanner_sys.lock_stats_top_minute"
	LockStatsTop10MinuteTable LockStatsTopTable = "spanner_sys.lock_stats_top_10minute"
	LockStatsTopHourTable     LockStatsTopTable = "spanner_sys.lock_stats_top_hour"
)

type LockStatsParam struct {
	Table string
}

var _ bigquery.ValueSaver = &LockStat{}

type LockStatSampleLockRequest struct {
	LockMode string `spanner:"lock_mode"`
	Column   string `spanner:"column"`
}

// Save is bigquery.ValueSaver interface
func (s *LockStatSampleLockRequest) ToBQValue() map[string]bigquery.Value {
	return map[string]bigquery.Value{
		"lock_mode": s.LockMode,
		"column":    s.Column,
	}
}

type LockStat struct {
	// End of the time interval that the included query executions occurred in.
	IntervalEnd time.Time `spanner:"interval_end"`

	/*
	  The row key where the lock conflict occurred.
	  When the conflict involves a range of rows, this value represents the starting key of that range.
	  A plus sign, +, signifies a range. For more information, see What's a row range start key.
	*/
	RowRangeStartKey []byte `spanner:"row_range_start_key"`

	/*
	  The cumulative lock wait time of lock conflicts recorded for all the columns in the row key range, in seconds.
	*/
	LockWaitSeconds float64 `spanner:"lock_wait_seconds"`

	/*
	  Each entry in this array corresponds to a sample lock request that contributed to the lock conflict on the given row key, or row key range.
	  The maximum number of samples in this array is 20.
	  Each sample contains the following two fields:
	    lock_mode: The lock mode that was requested.
	               For more information, see Lock modes
	    column: The column which encountered the lock conflict.
	            The format of this value is tablename.columnname.
	*/
	SampleLockRequests []*LockStatSampleLockRequest `spanner:"sample_lock_requests"`
}

// Save is bigquery.ValueSaver interface
func (s *LockStat) Save() (map[string]bigquery.Value, string, error) {
	insertID, err := s.InsertID()
	if err != nil {
		return nil, "", xerrors.Errorf("failed InsertID() : %w", err)
	}

	var lockReqs []map[string]bigquery.Value
	for _, lockReq := range s.SampleLockRequests {
		lockReqs = append(lockReqs, lockReq.ToBQValue())
	}

	return map[string]bigquery.Value{
		"interval_end":         s.IntervalEnd,
		"row_range_start_key":  s.RowRangeStartKey,
		"lock_wait_seconds":    s.LockWaitSeconds,
		"sample_lock_requests": lockReqs,
	}, insertID, nil
}

// InsertID is 同じデータをBigQueryになるべく入れないようにデータからInsertIDを作成する
func (s *LockStat) InsertID() (string, error) {
	if s.IntervalEnd.IsZero() {
		return "", xerrors.New("IntervalEnd is required.")
	}
	k := base64.URLEncoding.EncodeToString(s.RowRangeStartKey)
	return fmt.Sprintf("GCPBOX_SpannerLockStat-_-%d-_-%s", s.IntervalEnd.Unix(), k), nil
}

// LockStatsBigQueryTableSchema is BigQuery Table Schema
var LockStatsBigQueryTableSchema = bigquery.Schema{
	{Name: "interval_end", Required: true, Type: bigquery.TimestampFieldType},
	{Name: "row_range_start_key", Required: true, Type: bigquery.BytesFieldType},
	{Name: "lock_wait_seconds", Required: true, Type: bigquery.NumericFieldType},
	{Name: "sample_lock_requests",
		Required: true,
		Repeated: true,
		Type:     bigquery.RecordFieldType,
		Schema: bigquery.Schema{
			{Name: "lock_mode", Required: true, Type: bigquery.StringFieldType},
			{Name: "column", Required: true, Type: bigquery.StringFieldType},
		}},
}
