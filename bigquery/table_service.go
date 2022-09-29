package bigquery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type TableService struct {
	BQ *bigquery.Client
}

func NewTableService(ctx context.Context, bq *bigquery.Client) (*TableService, error) {
	return &TableService{
		BQ: bq,
	}, nil
}

func (s *TableService) Close(ctx context.Context) error {
	if err := s.BQ.Close(); err != nil {
		return err
	}
	return nil
}

type DateShardingTableTarget struct {
	// Prefix is TableID Prefix
	Prefix string

	// Start is Start YYYYMMDD
	Start string

	// End is End YYYYMMDD
	End string
}

func (t *DateShardingTableTarget) Match(tableID string) (bool, error) {
	if len(t.Prefix) > 0 {
		if !strings.HasPrefix(tableID, t.Prefix) {
			return false, nil
		}
	}

	b, err := WithInRangeForDateShardingTable(tableID, t.Start, t.End)
	if err != nil {
		return false, err
	}
	return b, nil
}

// WithInRangeForDateShardingTable is tableで指定したテーブル名がstart, endで指定したYYYYMMDDの範囲にあるかを返す
// Ex: table=hoge20190101 start=20180101 end=20190102
func WithInRangeForDateShardingTable(tableID string, start string, end string) (bool, error) {
	s, err := time.Parse("20060102", start)
	if err != nil {
		return false, fmt.Errorf("failed time.Parse %s", start)
	}
	e, err := time.Parse("20060102", end)
	if err != nil {
		return false, fmt.Errorf("failed time.Parse %s", end)
	}

	c := utf8.RuneCountInString(tableID)
	tdate := tableID[c-8:]
	t, err := time.Parse("20060102", tdate)
	if err != nil {
		return false, fmt.Errorf("failed time.Parse %s", tdate)
	}

	if s.Unix() > t.Unix() {
		return false, nil
	}
	if e.Unix() < t.Unix() {
		return false, nil
	}

	return true, nil
}

// DeleteByPrefix is 指定したPrefixに合致するTableを削除する
//
// 削除したTableIDの一覧を返す
// 途中で削除に失敗した場合もそれまで削除したTableIDの一覧は返す
func (s *TableService) DeleteByPrefix(ctx context.Context, projectID string, datasetID string, tablePrefix string, ops ...APIOptions) ([]string, error) {
	opt := apiOptions{}
	for _, o := range ops {
		o(&opt)
	}

	var deleteTableIDs []string
	iter := s.BQ.DatasetInProject(projectID, datasetID).Tables(ctx)
	for {
		table, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return deleteTableIDs, fmt.Errorf("failed list tables : %w", err)
		}
		if !strings.HasPrefix(table.TableID, tablePrefix) {
			if opt.streamLogFn != nil {
				opt.streamLogFn(fmt.Sprintf("%s is has not prefix", table.TableID))
			}
			continue
		}
		msg := fmt.Sprintf("delete %s", table.TableID)
		if opt.dryRun {
			msg = fmt.Sprintf("DryRun: %s", msg)
		} else {
			if err := s.BQ.DatasetInProject(projectID, datasetID).Table(table.TableID).Delete(ctx); err != nil {
				return deleteTableIDs, fmt.Errorf("failed delete table %s.%s.%s : %w", projectID, datasetID, table.TableID, err)
			}
		}
		if opt.streamLogFn != nil {
			opt.streamLogFn(msg)
		}
		deleteTableIDs = append(deleteTableIDs, table.TableID)
	}
	return deleteTableIDs, nil
}

type JobResult struct {
	TableID string
	Job     *bigquery.Job
}

// RunDMLToShardingTables is ShardingTableに対してDMLを実行する
func (s *TableService) RunDMLToShardingTables(ctx context.Context, projectID string, datasetID string, target *DateShardingTableTarget, dml string, ops ...APIOptions) ([]string, error) {
	opt := apiOptions{}
	for _, o := range ops {
		o(&opt)
	}

	templ, err := template.New("dml").Parse(dml)
	if err != nil {
		return nil, fmt.Errorf("failed template.New :%w", err)
	}

	var targetTableIDs []string
	iter := s.BQ.DatasetInProject(projectID, datasetID).Tables(ctx)
	for {
		table, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return targetTableIDs, fmt.Errorf("failed list tables : %w", err)
		}

		ok, err := target.Match(table.TableID)
		if err != nil {
			return targetTableIDs, fmt.Errorf("failed target match : %w", err)
		}
		if !ok {
			if opt.streamLogFn != nil {
				opt.streamLogFn(fmt.Sprintf("%s is not match", table.TableID))
			}
			continue
		}
		msg := fmt.Sprintf("target %s", table.TableID)
		if opt.dryRun {
			msg = fmt.Sprintf("DryRun: %s", msg)
		} else {
			bu := new(bytes.Buffer)
			data := struct {
				TableID string
			}{
				TableID: fmt.Sprintf("%s.%s.%s", table.ProjectID, table.DatasetID, table.TableID),
			}
			if err := templ.Execute(bu, data); err != nil {
				return nil, err
			}
			fixDML := bu.String()
			job, err := s.BQ.Query(fixDML).Run(ctx)
			if err != nil {
				// TODO ErrorStatusで続きをすすめるかどうかを決めたいところではある
				return nil, fmt.Errorf("failed run job dml=%s : %w", fixDML, err)
			}
			if opt.wait {
				sts, err := job.Wait(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed job.Wait() dml=%s : %w", fixDML, err)
				}
				if sts.Err() != nil {
					return nil, fmt.Errorf("failed job.Status.Err dml=%s : %w", fixDML, err)
				}
				if opt.streamLogFn != nil {
					if sts.Statistics != nil {
						d := sts.Statistics.EndTime.Sub(sts.Statistics.StartTime)
						opt.streamLogFn(fmt.Sprintf("%s job:%s TotalBytesProcessed:%d WorkTime:%s StartTime:%s EndTime:%s", table.TableID, job.ID(), sts.Statistics.TotalBytesProcessed, d, sts.Statistics.StartTime, sts.Statistics.EndTime))
					} else {
						opt.streamLogFn(fmt.Sprintf("%s job:%s", table.TableID, job.ID()))
					}
				}
			}
		}
		if opt.streamLogFn != nil {
			opt.streamLogFn(msg)
		}
		targetTableIDs = append(targetTableIDs, table.TableID)
	}
	return targetTableIDs, nil
}

type BQColumn struct {
	Name     string
	Children map[string]*BQColumn
	Target   bool
}

// ExistColumn is 対象のTableに対象のColumnがある場合は、trueを返す
// ARRAY(STRUCT)には対応していない
func (s *TableService) ExistColumn(ctx context.Context, projectID string, datasetID string, tableID string, target string) (bool, error) {
	tm, err := s.BQ.DatasetInProject(projectID, datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return false, err
	}

	_, hit := s.searchColumn("", tm.Schema, target, &BQColumn{})
	if !hit {
		return false, nil
	}
	return true, nil
}

// searchTargetColumn is 対象のColumnが存在するかをチェックする
// ARRAY(STRUCT)には対応していない
func (s *TableService) searchColumn(parentName string, fields []*bigquery.FieldSchema, target string, chainColumn *BQColumn) (*BQColumn, bool) {
	var hit bool
	for _, field := range fields {
		chainName := field.Name
		current := &BQColumn{Name: field.Name}
		if parentName != "" {
			chainName = fmt.Sprintf("%s.%s", parentName, field.Name)
			chainColumn.Children = map[string]*BQColumn{current.Name: current}
		} else {
			chainColumn = current
		}

		if field.Type == "RECORD" {
			_, h := s.searchColumn(chainName, field.Schema, target, current)
			if h {
				return chainColumn, true
			}
			continue // RECORDごとTargetにすることはないので、target checkは行わない
		}
		if chainName == target {
			current.Target = true
			return chainColumn, true
		}
	}
	return nil, hit
}
