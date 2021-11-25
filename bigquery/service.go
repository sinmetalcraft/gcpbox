package bigquery

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type Service struct {
	BQ *bigquery.Client
}

func NewService(ctx context.Context, bq *bigquery.Client) (*Service, error) {
	return &Service{
		BQ: bq,
	}, nil
}

func (s *Service) Close(ctx context.Context) error {
	if err := s.BQ.Close(); err != nil {
		return err
	}
	return nil
}

// DeleteTablesByTablePrefix is 指定したPrefixに合致するTableを削除する
//
// 削除したTableIDの一覧を返す
// 途中で削除に失敗した場合もそれまで削除したTableIDの一覧は返す
func (s *Service) DeleteTablesByTablePrefix(ctx context.Context, projectID string, datasetID string, tablePrefix string, ops ...APIOptions) ([]string, error) {
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
			if opt.streamLog != nil {
				opt.streamLog <- fmt.Sprintf("%s is has not prefix", table.TableID)
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
		if opt.streamLog != nil {
			opt.streamLog <- msg
		}
		deleteTableIDs = append(deleteTableIDs, table.TableID)
	}
	return deleteTableIDs, nil
}
