package spanner

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/spanner"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
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
  avg_cpu_seconds
FROM {{.Table}}
WHERE interval_end = TIMESTAMP(@IntervalEnd, "UTC")
`

var (
	ErrRequiredSpannerClient = xerrors.New("required spanner client.")
)

type QueryStatsTopTable string

const (
	QueryStatsTopMinuteTable   QueryStatsTopTable = "spanner_sys.query_stats_top_minute"
	QueryStatsTop10MinuteTable QueryStatsTopTable = "spanner_sys.query_stats_top_10minute"
	QueryStatsTopHourTable     QueryStatsTopTable = "spanner_sys.query_stats_top_hour"
)

type QueryStatsParam struct {
	Table string
}

type QueryStatsCopyService struct {
	queryStatsTopQueryTemplate *template.Template
	Spanner                    *spanner.Client
	BQ                         *bigquery.Client
}

// NewQueryStatsCopyService is QueryStatsCopyServiceを生成する
func NewQueryStatsCopyService(ctx context.Context, bq *bigquery.Client) (*QueryStatsCopyService, error) {
	return NewQueryStatsCopyServiceWithSpannerClient(ctx, bq, nil)
}

// NewQueryStatsCopyServiceWithSpannerClient is Statsを取得したいSpanner DBが1つしかないのであれば、Spanner Clientを設定して、QueryStatsCopyServiceを作成する
func NewQueryStatsCopyServiceWithSpannerClient(ctx context.Context, bq *bigquery.Client, spannerClient *spanner.Client) (*QueryStatsCopyService, error) {
	tmpl, err := template.New("getQueryStatsTopQuery").Parse(queryStatsTopMinute)
	if err != nil {
		return nil, err
	}

	return &QueryStatsCopyService{
		queryStatsTopQueryTemplate: tmpl,
		Spanner:                    spannerClient,
		BQ:                         bq,
	}, nil
}

type Database struct {
	ProjectID string
	Instance  string
	Database  string
}

// ToSpannerDatabaseName is Spanner Database Name として指定できる形式の文字列を返す
func (d *Database) ToSpannerDatabaseName() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", d.ProjectID, d.Instance, d.Database)
}

// SplitDatabaseName is projects/{PROJECT_ID}/instances/{INSTANCE}/databases/{DB} 形式の文字列をstructにして返す
func SplitDatabaseName(database string) (*Database, error) {
	l := strings.Split(database, "/")
	if len(l) < 6 {
		return nil, fmt.Errorf("invalid argument. The expected format is projects/{PROJECT_ID}/instances/{INSTANCE}/databases/{DB}. but get %s", database)
	}

	return &Database{
		ProjectID: l[1],
		Instance:  l[3],
		Database:  l[5],
	}, nil
}

var _ bigquery.ValueSaver = &QueryStat{}

type QueryStat struct {
	IntervalEnd       time.Time `spanner:"interval_end"` // End of the time interval that the included query executions occurred in.
	Text              string    // SQL query text, truncated to approximately 64KB.
	TextTruncated     bool      `spanner:"text_truncated"`      // Whether or not the query text was truncated.
	TextFingerprint   int64     `spanner:"text_fingerprint"`    // Hash of the query text.
	ExecuteCount      int64     `spanner:"execution_count"`     // Number of times Cloud Spanner saw the query during the interval.
	AvgLatencySeconds float64   `spanner:"avg_latency_seconds"` // Average length of time, in seconds, for each query execution within the database. This average excludes the encoding and transmission time for the result set as well as overhead.
	AvgRows           float64   `spanner:"avg_rows"`            // Average number of rows that the query returned.
	AvgBytes          float64   `spanner:"avg_bytes"`           // Average number of data bytes that the query returned, excluding transmission encoding overhead.
	AvgRowsScanned    float64   `spanner:"avg_rows_scanned"`    // Average number of rows that the query scanned, excluding deleted values.
	AvgCPUSeconds     float64   `spanner:"avg_cpu_seconds"`     // Average number of seconds of CPU time Cloud Spanner spent on all operations to execute the query.
}

// Save is bigquery.ValueSaver interface
func (s *QueryStat) Save() (map[string]bigquery.Value, string, error) {
	insertID, err := s.InsertID()
	if err != nil {
		return nil, "", xerrors.Errorf("failed InsertID() : %w", err)
	}
	return map[string]bigquery.Value{
		"interval_end":        s.IntervalEnd,
		"text":                s.Text,
		"text_truncated":      s.TextTruncated,
		"text_fingerprint":    s.TextFingerprint,
		"execution_count":     s.ExecuteCount,
		"avg_latency_seconds": s.AvgLatencySeconds,
		"avg_rows":            s.AvgRows,
		"avg_bytes":           s.AvgBytes,
		"avg_rows_scanned":    s.AvgRowsScanned,
		"avg_cpu_seconds":     s.AvgCPUSeconds,
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

func (s *QueryStatsCopyService) Close() error {
	if s.Spanner != nil {
		s.Spanner.Close()
	}
	if s.BQ != nil {
		return s.BQ.Close()
	}

	return nil
}

// GetQueryStats is SpannerからQueryStatsを取得する
func (s *QueryStatsCopyService) GetQueryStats(ctx context.Context, table QueryStatsTopTable, intervalEnd time.Time) ([]*QueryStat, error) {
	if s.Spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetQueryStatsWithSpannerClient(ctx, table, s.Spanner, intervalEnd)
}

// GetQueryStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからQueryStatsを取得する
func (s *QueryStatsCopyService) GetQueryStatsWithSpannerClient(ctx context.Context, table QueryStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) ([]*QueryStat, error) {
	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.queryStatsTopQueryTemplate.Execute(&tpl, QueryStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEnd.Format("2006-01-02 15:04:05"),
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	rets := []*QueryStat{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		var result QueryStat
		if err := row.ToStruct(&result); err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}
		rets = append(rets, &result)
	}

	return rets, nil
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
}

// ToBigQuery is QueryStatsをBigQueryにStreamingInsertでInsertする
func (s *QueryStatsCopyService) CreateTable(ctx context.Context, dataset *bigquery.Dataset, table string) error {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: QueryStatsBigQueryTableSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		},
	})
}

// InsertQueryStatsToBigQuery is QueryStatsをBigQueryにStreamingInsertでInsertする
// Errorがある場合、github.com/sinmetal/gcpbox/errors.StreamingInsertErrors が返ってくる
func (s *QueryStatsCopyService) InsertQueryStatsToBigQuery(ctx context.Context, dataset *bigquery.Dataset, table string, qss []*QueryStat) error {
	if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(table).Inserter().Put(ctx, qss); err != nil {
		return xerrors.Errorf(": %w", err)
	}
	return nil
}

// Copy is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *QueryStatsCopyService) Copy(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, queryStatsTable QueryStatsTopTable, intervalEnd time.Time) (int, error) {
	if s.Spanner == nil {
		return 0, ErrRequiredSpannerClient
	}
	return s.CopyWithSpannerClient(ctx, dataset, bigQueryTable, queryStatsTable, s.Spanner, intervalEnd)
}

// CopyWithSpannerClient is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *QueryStatsCopyService) CopyWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, queryStatsTable QueryStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (int, error) {
	if spannerClient == nil {
		return 0, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.queryStatsTopQueryTemplate.Execute(&tpl, QueryStatsParam{Table: string(queryStatsTable)}); err != nil {
		return 0, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEnd.Format("2006-01-02 15:04:05"),
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	var insertCount int
	var qss []*QueryStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		var qs QueryStat
		if err := row.ToStruct(&qs); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		qss = append(qss, &qs)
		if len(qss) > 99 {
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, qss); err != nil {
				return insertCount, xerrors.Errorf(": %w", err)
			}
			insertCount += len(qss)
			qss = []*QueryStat{}
		}
	}
	if len(qss) > 0 {
		if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, qss); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}
		insertCount += len(qss)
		qss = []*QueryStat{}
	}

	return insertCount, nil
}
