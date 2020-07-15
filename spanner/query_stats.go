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
	"github.com/pkg/errors"
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
`

var (
	ErrRequiredSpannerClient = errors.New("required spanner client.")
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
	spanner                    *spanner.Client
	bq                         *bigquery.Client
}

// NewQueryStatsCopyService is QueryStatsCopyServiceを生成する
func NewQueryStatsCopyService(ctx context.Context, bqClient *bigquery.Client) (*QueryStatsCopyService, error) {
	return NewQueryStatsCopyServiceWithSpannerClient(ctx, bqClient, nil)
}

// NewQueryStatsCopyServiceWithSpannerClient is Statsを取得したいSpanner DBが1つしかないのであれば、Spanner Clientを設定して、QueryStatsCopyServiceを作成する
func NewQueryStatsCopyServiceWithSpannerClient(ctx context.Context, bqClient *bigquery.Client, spannerClient *spanner.Client) (*QueryStatsCopyService, error) {
	tmpl, err := template.New("getQueryStatsTopQuery").Parse(queryStatsTopMinute)
	if err != nil {
		return nil, err
	}

	return &QueryStatsCopyService{
		queryStatsTopQueryTemplate: tmpl,
		spanner:                    spannerClient,
		bq:                         bqClient,
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

type QueryStat struct {
	InsertID          string
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

// ToInsertID is 同じデータをBigQueryになるべく入れないようにデータからInsertIDを作成する
func (s *QueryStat) ToInsertID() string {
	s.InsertID = fmt.Sprintf("%v-_-%v", s.IntervalEnd.Unix(), s.TextFingerprint)
	return s.InsertID
}

func (s *QueryStatsCopyService) Close() error {
	if s.spanner != nil {
		s.spanner.Close()
	}
	if s.bq != nil {
		return s.bq.Close()
	}

	return nil
}

// GetQueryStats is SpannerからQueryStatsを取得する
func (s *QueryStatsCopyService) GetQueryStats(ctx context.Context, table QueryStatsTopTable) ([]*QueryStat, error) {
	if s.spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetQueryStatsWithSpannerClient(ctx, table, s.spanner)
}

// GetQueryStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからQueryStatsを取得する
func (s *QueryStatsCopyService) GetQueryStatsWithSpannerClient(ctx context.Context, table QueryStatsTopTable, spannerClient *spanner.Client) ([]*QueryStat, error) {
	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.queryStatsTopQueryTemplate.Execute(&tpl, QueryStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	iter := spannerClient.Single().Query(ctx, spanner.NewStatement(tpl.String()))
	defer iter.Stop()

	rets := []*QueryStat{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var result QueryStat
		if err := row.ToStruct(&result); err != nil {
			return nil, errors.WithStack(err)
		}
		rets = append(rets, &result)
	}

	return rets, nil
}

// QueryStatsBigQueryTableSchema is BigQuery Table Schema
var QueryStatsBigQueryTableSchema = bigquery.Schema{
	{Name: "IntervalEnd", Required: true, Type: bigquery.TimestampFieldType},
	{Name: "Text", Required: true, Type: bigquery.StringFieldType},
	{Name: "TextTruncated", Required: true, Type: bigquery.BooleanFieldType},
	{Name: "TextFingerprint", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "ExecuteCount", Required: true, Type: bigquery.IntegerFieldType},
	{Name: "AvgLatencySeconds", Required: true, Type: bigquery.FloatFieldType},
	{Name: "AvgRows", Required: true, Type: bigquery.FloatFieldType},
	{Name: "AvgBytes", Required: true, Type: bigquery.FloatFieldType},
	{Name: "AvgRowsScanned", Required: true, Type: bigquery.FloatFieldType},
	{Name: "AvgCPUSeconds", Required: true, Type: bigquery.FloatFieldType},
}

// ToBigQuery is QueryStatsをBigQueryにStreamingInsertでInsertする
func (s *QueryStatsCopyService) CreateTable(ctx context.Context, dataset *bigquery.Dataset, table string) error {

	return s.bq.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: QueryStatsBigQueryTableSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		},
	})
}

// ToBigQuery is QueryStatsをBigQueryにStreamingInsertでInsertする
func (s *QueryStatsCopyService) ToBigQuery(ctx context.Context, dataset *bigquery.Dataset, table string, qss []*QueryStat) error {
	var sss []*bigquery.StructSaver
	for _, qs := range qss {
		insertID := qs.ToInsertID()
		sss = append(sss, &bigquery.StructSaver{
			Schema:   QueryStatsBigQueryTableSchema,
			InsertID: insertID,
			Struct:   qs,
		})
	}

	if err := s.bq.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(table).Inserter().Put(ctx, sss); err != nil {
		return err
	}
	return nil
}

// Copy is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyする一連の流れを実行する便利メソッド
func (s *QueryStatsCopyService) Copy(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, queryStatsTable QueryStatsTopTable) error {
	qss, err := s.GetQueryStats(ctx, queryStatsTable)
	if err != nil {
		return errors.WithMessage(err, "failed spanner.GetQueryStats")
	}

	if err := s.ToBigQuery(ctx, dataset, bigQueryTable, qss); err != nil {
		return errors.WithMessage(err, "failed bigQuery.ToPut")
	}

	return nil
}
