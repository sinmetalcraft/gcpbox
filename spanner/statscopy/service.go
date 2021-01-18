package statscopy

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

var (
	ErrRequiredSpannerClient = xerrors.New("required spanner client.")
)

// Database is Spanner Database
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

type Service struct {
	queryStatsTopQueryTemplate *template.Template
	readStatsTopQueryTemplate  *template.Template
	txStatsTopQueryTemplate    *template.Template
	Spanner                    *spanner.Client
	BQ                         *bigquery.Client
}

// NewService is Serviceを生成する
func NewService(ctx context.Context, bq *bigquery.Client) (*Service, error) {
	return NewServiceWithSpannerClient(ctx, bq, nil)
}

// NewServiceWithSpannerClient is Statsを取得したいSpanner DBが1つしかないのであれば、Spanner Clientを設定して、Serviceを作成する
func NewServiceWithSpannerClient(ctx context.Context, bq *bigquery.Client, spannerClient *spanner.Client) (*Service, error) {
	queryStatsTmpl, err := template.New("getQueryStatsTopQuery").Parse(queryStatsTopMinute)
	if err != nil {
		return nil, err
	}
	readStatsTmpl, err := template.New("getReadStatsTopQuery").Parse(readStatsTopMinute)
	if err != nil {
		return nil, err
	}
	txStatsTmpl, err := template.New("getTxStatsTopQuery").Parse(txStatsTopMinute)
	if err != nil {
		return nil, err
	}

	return &Service{
		queryStatsTopQueryTemplate: queryStatsTmpl,
		readStatsTopQueryTemplate:  readStatsTmpl,
		txStatsTopQueryTemplate:    txStatsTmpl,
		Spanner:                    spannerClient,
		BQ:                         bq,
	}, nil
}

func (s *Service) Close() error {
	if s.Spanner != nil {
		s.Spanner.Close()
	}
	if s.BQ != nil {
		return s.BQ.Close()
	}

	return nil
}

// GetQueryStats is SpannerからQueryStatsを取得する
func (s *Service) GetQueryStats(ctx context.Context, table QueryStatsTopTable, intervalEnd time.Time) ([]*QueryStat, error) {
	if s.Spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetQueryStatsWithSpannerClient(ctx, table, s.Spanner, intervalEnd)
}

// GetReadStats is SpannerからTxStatsを取得する
func (s *Service) GetReadStats(ctx context.Context, table ReadStatsTopTable, intervalEnd time.Time) ([]*ReadStat, error) {
	if s.Spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetReadStatsWithSpannerClient(ctx, table, s.Spanner, intervalEnd)
}

// GetTxStats is SpannerからTxStatsを取得する
func (s *Service) GetTxStats(ctx context.Context, table TxStatsTopTable, intervalEnd time.Time) ([]*TxStats, error) {
	if s.Spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetTxStatsWithSpannerClient(ctx, table, s.Spanner, intervalEnd)
}

// GetQueryStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからQueryStatsを取得する
func (s *Service) GetQueryStatsWithSpannerClient(ctx context.Context, table QueryStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) ([]*QueryStat, error) {
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

// GetReadStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからQueryStatsを取得する
func (s *Service) GetReadStatsWithSpannerClient(ctx context.Context, table ReadStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) ([]*ReadStat, error) {
	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.readStatsTopQueryTemplate.Execute(&tpl, ReadStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEnd.Format("2006-01-02 15:04:05"),
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	rets := []*ReadStat{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		var result ReadStat
		if err := row.ToStruct(&result); err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}
		rets = append(rets, &result)
	}

	return rets, nil
}

// GetTxStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからTxStatsを取得する
func (s *Service) GetTxStatsWithSpannerClient(ctx context.Context, table TxStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) ([]*TxStats, error) {
	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.txStatsTopQueryTemplate.Execute(&tpl, TxStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEnd.Format("2006-01-02 15:04:05"),
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	rets := []*TxStats{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		var result TxStats
		if err := row.ToStruct(&result); err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}
		rets = append(rets, &result)
	}

	return rets, nil
}

// CreateQueryStatsTable is QueryStatsをCopyするTableをBigQueryに作成する
func (s *Service) CreateQueryStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) error {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: QueryStatsBigQueryTableSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		},
	})
}

// UpdateQueryStatsTable is BigQuery上にあるQueryStats TableのSchemaをUpdateする
// 途中でColumnが追加されたときに使う
func (s *Service) UpdateQueryStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) (*bigquery.TableMetadata, error) {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: QueryStatsBigQueryTableSchema,
	}, "")
}

// CreateReadStatsTable is ReadStatsをCopyするTableをBigQueryに作成する
func (s *Service) CreateReadStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) error {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: ReadStatsBigQueryTableSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		},
	})
}

// CreateTxStatsTable is TxStatsをCopyするTableをBigQueryに作成する
func (s *Service) CreateTxStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) error {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: TxStatsBigQueryTableSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		},
	})
}

// CopyQueryStats is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyQueryStats(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, queryStatsTable QueryStatsTopTable, intervalEnd time.Time) (int, error) {
	if s.Spanner == nil {
		return 0, ErrRequiredSpannerClient
	}
	return s.CopyQueryStatsWithSpannerClient(ctx, dataset, bigQueryTable, queryStatsTable, s.Spanner, intervalEnd)
}

// CopyQueryStats is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyReadStats(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, readStatsTable ReadStatsTopTable, intervalEnd time.Time) (int, error) {
	if s.Spanner == nil {
		return 0, ErrRequiredSpannerClient
	}
	return s.CopyReadStatsWithSpannerClient(ctx, dataset, bigQueryTable, readStatsTable, s.Spanner, intervalEnd)
}

// CopyTxStats is SpannerからTx Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyTxStats(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, txStatsTable TxStatsTopTable, intervalEnd time.Time) (int, error) {
	if s.Spanner == nil {
		return 0, ErrRequiredSpannerClient
	}
	return s.CopyTxStatsWithSpannerClient(ctx, dataset, bigQueryTable, txStatsTable, s.Spanner, intervalEnd)
}

// CopyQueryStatsWithSpannerClient is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyQueryStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, queryStatsTable QueryStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (int, error) {
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
	var statsList []*QueryStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		var stats QueryStat
		if err := row.ToStruct(&stats); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		statsList = append(statsList, &stats)
		if len(statsList) > 99 {
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
				return insertCount, xerrors.Errorf(": %w", err)
			}
			insertCount += len(statsList)
			statsList = []*QueryStat{}
		}
	}
	if len(statsList) > 0 {
		if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}
		insertCount += len(statsList)
	}

	return insertCount, nil
}

// CopyReadStatsWithSpannerClient is SpannerからRead Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyReadStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, readStatsTable ReadStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (int, error) {
	if spannerClient == nil {
		return 0, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.readStatsTopQueryTemplate.Execute(&tpl, ReadStatsParam{Table: string(readStatsTable)}); err != nil {
		return 0, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEnd.Format("2006-01-02 15:04:05"),
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	var insertCount int
	var statsList []*ReadStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		var stats ReadStat
		if err := row.ToStruct(&stats); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		statsList = append(statsList, &stats)
		if len(statsList) > 99 {
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
				return insertCount, xerrors.Errorf(": %w", err)
			}
			insertCount += len(statsList)
			statsList = []*ReadStat{}
		}
	}
	if len(statsList) > 0 {
		if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}
		insertCount += len(statsList)
	}

	return insertCount, nil
}

// CopyTxStatsWithSpannerClient is SpannerからTx Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyTxStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, txStatsTable TxStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (int, error) {
	if spannerClient == nil {
		return 0, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.txStatsTopQueryTemplate.Execute(&tpl, TxStatsParam{Table: string(txStatsTable)}); err != nil {
		return 0, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEnd.Format("2006-01-02 15:04:05"),
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	var insertCount int
	var statsList []*TxStats
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		var stats TxStats
		if err := row.ToStruct(&stats); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		statsList = append(statsList, &stats)
		if len(statsList) > 99 {
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
				return insertCount, xerrors.Errorf(": %w", err)
			}
			insertCount += len(statsList)
			statsList = []*TxStats{}
		}
	}
	if len(statsList) > 0 {
		if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}
		insertCount += len(statsList)
	}

	return insertCount, nil
}
