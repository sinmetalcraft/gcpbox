package bigquery_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/api/googleapi"

	bqbox "github.com/sinmetalcraft/gcpbox/bigquery"
)

const bqboxDatasetID = "bqbox"

func TestTableService_DeleteTablesByTablePrefix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newTableService(ctx, t)
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
		got, err := s.DeleteByPrefix(ctx, testProjectID(t), bqboxDatasetID, tableIDPrefix, bqbox.WithDryRun())
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
		got, err := s.DeleteByPrefix(ctx, testProjectID(t), bqboxDatasetID, tableIDPrefix, bqbox.WithStreamLogFn(streamLogFn))
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

func TestTableService_ExistTargetColumn(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name         string
		project      string
		dataset      string
		table        string
		targetColumn string
		want         bool
	}{
		{"1階層目", testProjectID(t), "bqtool", "json",
			"id",
			true,
		},
		{"2階層以上潜る", testProjectID(t), "bqtool", "json",
			"request.meta.dragon.hp",
			true,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			s := newTableService(ctx, t)

			got, err := s.ExistColumn(ctx, tt.project, tt.dataset, tt.table, tt.targetColumn)
			if err != nil {
				t.Fatal(err)
			}
			if df := cmp.Diff(tt.want, got); len(df) > 0 {
				t.Errorf("%s\n", df)
			}
		})
	}
}

func testProjectID(t *testing.T) string {
	pID := os.Getenv("GCPBOX_CI_PROJECT")
	if pID == "" {
		t.Fatal("GCPBOX_CI_PROJECT is required")
	}
	return pID
}

func newTableService(ctx context.Context, t *testing.T) *bqbox.TableService {
	bq, err := bigquery.NewClient(ctx, "sinmetal-ci")
	if err != nil {
		t.Fatal(err)
	}
	s, err := bqbox.NewTableService(ctx, bq)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
