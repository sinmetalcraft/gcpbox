package spanner_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/spanner"
	"google.golang.org/api/googleapi"

	. "github.com/sinmetal/gcpbox/spanner"
)

const (
	projectID = "sinmetal-ci"
)

func TestQueryStatsCopyService_GetQueryStats(t *testing.T) {
	ctx := context.Background()

	s := newQueryStatsCopyService(t)
	_, err := s.GetQueryStats(ctx, QueryStatsTopMinuteTable)
	if err != nil {
		t.Fatal(err)
	}
}

func TestQueryStatsCopyService_Copy(t *testing.T) {
	ctx := context.Background()

	s := newQueryStatsCopyService(t)

	dataset := &bigquery.Dataset{ProjectID: projectID, DatasetID: "spanner_query_stats"}
	table := "minutes"
	if err := s.CreateTable(ctx, dataset, table); err != nil {
		var ae *googleapi.Error
		if ok := errors.As(err, &ae); ok {
			if ae.Code == 409 {
				// noop
			} else {
				t.Fatal(ae)
			}
		} else {
			t.Fatal(err)
		}
	}
	if err := s.Copy(ctx, dataset, table, QueryStatsTop10MinuteTable); err != nil {
		t.Fatal(err)
	}
}

func TestQueryStatsCopyService_Copy_TableCreate(t *testing.T) {
	ctx := context.Background()

	s := newQueryStatsCopyService(t)

	dataset := &bigquery.Dataset{ProjectID: projectID, DatasetID: "spanner_query_stats"}
	table := "not_found"
	if err := s.Copy(ctx, dataset, table, QueryStatsTop10MinuteTable); err != nil {
		var ae *googleapi.Error
		if ok := errors.As(err, &ae); ok {
			if ae.Code == 404 {
				if err := s.CreateTable(ctx, dataset, table); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Fatal(err)
			}
		} else {
			t.Fatal(err)
		}
	}
}

func newQueryStatsCopyService(t *testing.T) *QueryStatsCopyService {
	ctx := context.Background()

	spannerClient, err := spanner.NewClientWithConfig(ctx,
		fmt.Sprintf("projects/%s/instances/%s/databases/%s", "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal"),
		spanner.ClientConfig{
			SessionPoolConfig: spanner.SessionPoolConfig{
				MinOpened:     1,  // 基本的に同時に投げるのは 1 query
				MaxOpened:     10, // 基本的に同時に投げるのは 1 query なので、そんなに開くことはない
				WriteSessions: 0,  // Readしかしないので、WriteSessionsをPoolする必要はない
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewQueryStatsCopyService(ctx, spannerClient, bqClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
