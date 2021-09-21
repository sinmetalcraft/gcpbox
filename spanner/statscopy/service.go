package statscopy

import (
	"bytes"
	"context"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/spanner"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
	spabox "github.com/sinmetalcraft/gcpbox/spanner"
	"golang.org/x/xerrors"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

var (
	ErrRequiredSpannerClient = xerrors.New("required spanner client.")
)

type Service struct {
	queryStatsTopQueryTemplate *template.Template
	readStatsTopQueryTemplate  *template.Template
	txStatsTopQueryTemplate    *template.Template
	lockStatsTopQueryTemplate  *template.Template
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
	lockStatsTmpl, err := template.New("getLockStatsTopQuery").Parse(lockStatsTopMinute)
	if err != nil {
		return nil, err
	}

	return &Service{
		queryStatsTopQueryTemplate: queryStatsTmpl,
		readStatsTopQueryTemplate:  readStatsTmpl,
		txStatsTopQueryTemplate:    txStatsTmpl,
		lockStatsTopQueryTemplate:  lockStatsTmpl,
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
func (s *Service) GetTxStats(ctx context.Context, table TxStatsTopTable, intervalEnd time.Time) ([]*TxStat, error) {
	if s.Spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetTxStatsWithSpannerClient(ctx, table, s.Spanner, intervalEnd)
}

// GetLockStats is SpannerからLockStatsを取得する
func (s *Service) GetLockStats(ctx context.Context, table TxStatsTopTable, intervalEnd time.Time) ([]*LockStat, error) {
	if s.Spanner == nil {
		return nil, ErrRequiredSpannerClient
	}
	return s.GetLockStatsWithSpannerClient(ctx, table, s.Spanner, intervalEnd)
}

// GetQueryStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからQueryStatsを取得する
func (s *Service) GetQueryStatsWithSpannerClient(ctx context.Context, table QueryStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (stats []*QueryStat, err error) {
	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")

	ctx = trace.StartSpan(ctx, "spanner.statscopy.GetQueryStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"statsCount":  len(stats),
			"table":       table,
			"intervalEnd": intervalEndParam,
		})
		trace.EndSpan(ctx, err)
	}()

	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.queryStatsTopQueryTemplate.Execute(&tpl, QueryStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
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
func (s *Service) GetReadStatsWithSpannerClient(ctx context.Context, table ReadStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (stats []*ReadStat, err error) {
	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")

	ctx = trace.StartSpan(ctx, "spanner.statscopy.GetReadStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"statsCount":  len(stats),
			"table":       table,
			"intervalEnd": intervalEndParam,
		})
		trace.EndSpan(ctx, err)
	}()

	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.readStatsTopQueryTemplate.Execute(&tpl, ReadStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
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
func (s *Service) GetTxStatsWithSpannerClient(ctx context.Context, table TxStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (stats []*TxStat, err error) {
	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")

	ctx = trace.StartSpan(ctx, "spanner.statscopy.GetTxStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"statsCount":  len(stats),
			"table":       table,
			"intervalEnd": intervalEndParam,
		})
		trace.EndSpan(ctx, err)
	}()

	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.txStatsTopQueryTemplate.Execute(&tpl, TxStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	rets := []*TxStat{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		var result TxStat
		if err := row.ToStruct(&result); err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}
		rets = append(rets, &result)
	}

	return rets, nil
}

// GetLockStatsWithSpannerClient is 指定したSpannerClientを利用して、SpannerからLockStatsを取得する
func (s *Service) GetLockStatsWithSpannerClient(ctx context.Context, table TxStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (stats []*LockStat, err error) {
	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")

	ctx = trace.StartSpan(ctx, "spanner.statscopy.GetLockStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"statsCount":  len(stats),
			"table":       table,
			"intervalEnd": intervalEndParam,
		})
		trace.EndSpan(ctx, err)
	}()

	if spannerClient == nil {
		return nil, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.lockStatsTopQueryTemplate.Execute(&tpl, TxStatsParam{Table: string(table)}); err != nil {
		return nil, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	rets := []*LockStat{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		var result LockStat
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

// UpdateReadStatsTable is BigQuery上にあるReadStats TableのSchemaをUpdateする
// 途中でColumnが追加された時に使う
func (s *Service) UpdateReadStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) (*bigquery.TableMetadata, error) {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: ReadStatsBigQueryTableSchema,
	}, "")
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

// UpdateTxStatsTable is BigQuery上にあるTxStats TableのSchemaをUpdateする
// 途中でColumnが追加された時に使う
func (s *Service) UpdateTxStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) (*bigquery.TableMetadata, error) {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: TxStatsBigQueryTableSchema,
	}, "")
}

// CreateLockStatsTable is LockStatsをCopyするTableをBigQueryに作成する
func (s *Service) CreateLockStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) error {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: LockStatsBigQueryTableSchema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type: bigquery.DayPartitioningType,
		},
	})
}

// UpdateLockStatsTable is BigQuery上にあるTxStats TableのSchemaをUpdateする
// 途中でColumnが追加された時に使う
func (s *Service) UpdateLockStatsTable(ctx context.Context, dataset *bigquery.Dataset, table string) (*bigquery.TableMetadata, error) {

	return s.BQ.Dataset(dataset.DatasetID).Table(table).Update(ctx, bigquery.TableMetadataToUpdate{
		Schema: LockStatsBigQueryTableSchema,
	}, "")
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

// CopyLockStats is SpannerからLock Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyLockStats(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, lockStatsTable LockStatsTopTable, intervalEnd time.Time) (int, error) {
	if s.Spanner == nil {
		return 0, ErrRequiredSpannerClient
	}
	return s.CopyLockStatsWithSpannerClient(ctx, dataset, bigQueryTable, lockStatsTable, s.Spanner, intervalEnd)
}

// CopyQueryStatsWithSpannerClient is SpannerからQuery Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyQueryStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, queryStatsTable QueryStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (insertCount int, err error) {
	var readRowCount int

	ctx = trace.StartSpan(ctx, "spanner.statscopy.CopyQueryStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"insertCount":  insertCount,
			"readRowCount": readRowCount,
		})
		trace.EndSpan(ctx, err)
	}()

	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")
	trace.SetAttributesKV(ctx, map[string]interface{}{
		"dstDatasetProjectID": dataset.ProjectID,
		"dstDatasetID":        dataset.DatasetID,
		"dstTable":            bigQueryTable,
		"queryStatsTable":     queryStatsTable,
		"intervalEnd":         intervalEndParam,
	})

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

	var statsList []*QueryStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				return insertCount, spabox.NewErrNotFound("", err) // Spanner Instanceの情報はspannerClientが保持していて分からないので、Keyが空
			}
			return insertCount, xerrors.Errorf(": %w", err)
		}
		readRowCount++

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
func (s *Service) CopyReadStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, readStatsTable ReadStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (insertCount int, err error) {
	var readRowCount int

	ctx = trace.StartSpan(ctx, "spanner.statscopy.CopyReadStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"insertCount":  insertCount,
			"readRowCount": readRowCount,
		})
		trace.EndSpan(ctx, err)
	}()

	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")
	trace.SetAttributesKV(ctx, map[string]interface{}{
		"dstDatasetProjectID": dataset.ProjectID,
		"dstDatasetID":        dataset.DatasetID,
		"dstTable":            bigQueryTable,
		"readStatsTable":      readStatsTable,
		"intervalEnd":         intervalEndParam,
	})

	if spannerClient == nil {
		return 0, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.readStatsTopQueryTemplate.Execute(&tpl, ReadStatsParam{Table: string(readStatsTable)}); err != nil {
		return 0, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	var statsList []*ReadStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				return insertCount, spabox.NewErrNotFound("", err) // Spanner Instanceの情報はspannerClientが保持していて分からないので、Keyが空
			}
			return insertCount, xerrors.Errorf(": %w", err)
		}
		readRowCount++

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
func (s *Service) CopyTxStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, txStatsTable TxStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (insertCount int, err error) {
	var readRowCount int

	ctx = trace.StartSpan(ctx, "spanner.statscopy.CopyTxStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"insertCount":  insertCount,
			"readRowCount": readRowCount,
		})
		trace.EndSpan(ctx, err)
	}()

	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")
	trace.SetAttributesKV(ctx, map[string]interface{}{
		"dstDatasetProjectID": dataset.ProjectID,
		"dstDatasetID":        dataset.DatasetID,
		"dstTable":            bigQueryTable,
		"txStatsTable":        txStatsTable,
		"intervalEnd":         intervalEndParam,
	})

	if spannerClient == nil {
		return 0, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.txStatsTopQueryTemplate.Execute(&tpl, TxStatsParam{Table: string(txStatsTable)}); err != nil {
		return 0, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	var statsList []*TxStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				return insertCount, spabox.NewErrNotFound("", err) // Spanner Instanceの情報はspannerClientが保持していて分からないので、Keyが空
			}
			return insertCount, xerrors.Errorf(": %w", err)
		}
		readRowCount++

		var stats TxStat
		if err := row.ToStruct(&stats); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		statsList = append(statsList, &stats)
		if len(statsList) > 99 {
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
				return insertCount, xerrors.Errorf(": %w", err)
			}
			insertCount += len(statsList)
			statsList = []*TxStat{}
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

// CopyLockStatsWithSpannerClient is SpannerからLock Statsを引っ張ってきて、BigQueryにCopyしていく
func (s *Service) CopyLockStatsWithSpannerClient(ctx context.Context, dataset *bigquery.Dataset, bigQueryTable string, lockStatsTable LockStatsTopTable, spannerClient *spanner.Client, intervalEnd time.Time) (insertCount int, err error) {
	var readRowCount int

	ctx = trace.StartSpan(ctx, "spanner.statscopy.CopyLockStatsWithSpannerClient")
	defer func() {
		trace.SetAttributesKV(ctx, map[string]interface{}{
			"insertCount":  insertCount,
			"readRowCount": readRowCount,
		})
		trace.EndSpan(ctx, err)
	}()

	intervalEndParam := intervalEnd.Format("2006-01-02 15:04:05")
	trace.SetAttributesKV(ctx, map[string]interface{}{
		"dstDatasetProjectID": dataset.ProjectID,
		"dstDatasetID":        dataset.DatasetID,
		"dstTable":            bigQueryTable,
		"lockStatsTable":      lockStatsTable,
		"intervalEnd":         intervalEndParam,
	})

	if spannerClient == nil {
		return 0, ErrRequiredSpannerClient
	}

	var tpl bytes.Buffer
	if err := s.lockStatsTopQueryTemplate.Execute(&tpl, LockStatsParam{Table: string(lockStatsTable)}); err != nil {
		return 0, err
	}
	statement := spanner.NewStatement(tpl.String())
	statement.Params = map[string]interface{}{
		"IntervalEnd": intervalEndParam,
	}
	iter := spannerClient.Single().Query(ctx, statement)
	defer iter.Stop()

	var statsList []*LockStat
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				return insertCount, spabox.NewErrNotFound("", err) // Spanner Instanceの情報はspannerClientが保持していて分からないので、Keyが空
			}
			return insertCount, xerrors.Errorf(": %w", err)
		}
		readRowCount++

		var stats LockStat
		if err := row.ToStruct(&stats); err != nil {
			return insertCount, xerrors.Errorf(": %w", err)
		}

		statsList = append(statsList, &stats)
		if len(statsList) > 99 {
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(bigQueryTable).Inserter().Put(ctx, statsList); err != nil {
				return insertCount, xerrors.Errorf(": %w", err)
			}
			insertCount += len(statsList)
			statsList = []*LockStat{}
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
