package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"

	bqbox "github.com/sinmetalcraft/gcpbox/bigquery"
)

func TestService_DeleteTablesByTablePrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newService(ctx, t)
	defer func() {
		if err := s.Close(ctx); err != nil {
			t.Logf("failed Service.Close %s", err)
		}
	}()

	const tableIDPrefix = "insert_"
	tableID := fmt.Sprintf("%s%d", tableIDPrefix, time.Now().UnixNano())
	err := s.BQ.DatasetInProject(testProjectID(t), bqboxDatasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{
		Name: tableID,
	})
	if err != nil {
		t.Fatal(err)
	}
	streamLogFn := func(msg string) {
		t.Log(msg)
	}

	// DryRunの確認
	{
		got, err := s.DeleteTablesByTablePrefix(ctx, testProjectID(t), bqboxDatasetID, tableIDPrefix, bqbox.WithDryRun())
		if err != nil {
			t.Fatal(err)
		}
		if len(got) < 1 {
			t.Errorf("delete table ids emtpty")
		}

		// Tableが消えていないことを確認
		_, err = s.BQ.DatasetInProject(testProjectID(t), bqboxDatasetID).Table(tableID).Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 削除することを確認
	{
		got, err := s.DeleteTablesByTablePrefix(ctx, testProjectID(t), bqboxDatasetID, tableIDPrefix, bqbox.WithStreamLogFn(streamLogFn))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) < 1 {
			t.Errorf("delete table ids emtpty")
		}

		// Tableが消えていることを確認
		_, err = s.BQ.DatasetInProject(testProjectID(t), bqboxDatasetID).Table(tableID).Metadata(ctx)
		var ae *googleapi.Error
		if ok := errors.As(err, &ae); ok {
			if ae.Code == 404 {
				// 消えているのでOK
			} else {
				t.Fatal(err)
			}
		} else {
			t.Fatal(err)
		}
	}
}

func newService(ctx context.Context, t *testing.T) *bqbox.Service {
	bq, err := bigquery.NewClient(ctx, "sinmetal-ci")
	if err != nil {
		t.Fatal(err)
	}
	s, err := bqbox.NewService(ctx, bq)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
