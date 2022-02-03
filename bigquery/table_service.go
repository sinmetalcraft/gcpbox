package bigquery

import (
	"context"
	"fmt"
	"strings"

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
