package bigquery_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	. "github.com/sinmetal/gcpbox/bigquery"
)

var SampleTableSchema = bigquery.Schema{
	{Name: "Text", Required: true, Type: bigquery.StringFieldType},
	{Name: "Count", Required: true, Type: bigquery.IntegerFieldType},
}

type Sample struct {
	Text  string
	Count int64
}

func TestBigQueryService_Insert(t *testing.T) {
	ctx := context.Background()

	s := newBigQueryService(t)

	dataset := &bigquery.Dataset{ProjectID: "sinmetal-ci", DatasetID: "bqbox"}
	table := fmt.Sprintf("insert_%d", time.Now().Unix())
	err := s.BQ.Dataset(dataset.DatasetID).Table(table).Create(ctx, &bigquery.TableMetadata{
		Name:   table,
		Schema: SampleTableSchema,
	})
	if err != nil {
		t.Fatal(err)
	}

	txt := time.Now().String()
	var sss []*bigquery.StructSaver
	for i := 0; i < 30001; i++ {
		sss = append(sss, &bigquery.StructSaver{
			InsertID: fmt.Sprintf("%v", i),
			Schema:   SampleTableSchema,
			Struct: &Sample{
				Text:  txt,
				Count: int64(i),
			},
		})
	}

	if err := s.Insert(ctx, dataset, table, sss); err != nil {
		t.Fatal(err)
	}
}

func newBigQueryService(t *testing.T) *BigQueryService {
	ctx := context.Background()
	const projectID = "sinmetal-ci"

	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewBigQueryService(bqClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
