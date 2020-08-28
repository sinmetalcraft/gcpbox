package bigquery

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
)

type BigQueryService struct {
	BQ *bigquery.Client
}

func NewBigQueryService(bq *bigquery.Client) (*BigQueryService, error) {
	return &BigQueryService{
		bq,
	}, nil
}

func (s *BigQueryService) Close() error {
	if s.BQ != nil {
		return s.BQ.Close()
	}
	return nil
}

// InsertStructSaverToBigQuery is StructSaverをBigQueryにStreamingInsertでInsertする
// InsertはAtomicには行われない。 Error時は StreamingInsertErrors を返す。
func (s *BigQueryService) Insert(ctx context.Context, dataset *bigquery.Dataset, table string, sss []*bigquery.StructSaver) error {
	if len(sss) < 101 {
		if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(table).Inserter().Put(ctx, sss); err != nil {
			return err
		}
	}

	const size = 100
	errResult := &StreamingInsertErrors{}
	wg := &sync.WaitGroup{}
	for i := 0; i < len(sss); i += size {
		end := i + size
		if len(sss) < end {
			end = len(sss)
		}
		wg.Add(1)
		go func(list []*bigquery.StructSaver) {
			defer wg.Done()
			if err := s.BQ.DatasetInProject(dataset.ProjectID, dataset.DatasetID).Table(table).Inserter().Put(ctx, list); err != nil {
				for _, v := range list {
					errResult.Append(&StreamingInsertError{
						InsertID: v.InsertID,
						Err:      err,
					})
				}
			}
		}(sss[i:end])
	}
	wg.Wait()
	return errResult.ErrorOrNil()
}
