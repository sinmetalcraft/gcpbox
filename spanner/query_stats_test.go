package spanner

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/spanner"
)

func TestQueryStatsCopyService_GetQueryStats(t *testing.T) {
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
	bqClient, err := bigquery.NewClient(ctx, "sinmetal-lab")
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewQueryStatsCopyService(ctx, spannerClient, bqClient)
	if err != nil {
		t.Fatal(err)
	}
	qss, err := s.GetQueryStats(ctx, queryStatsTopMinuteTable)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.ToBigQuery(ctx, &bigquery.Dataset{ProjectID: "sinmetal-lab", DatasetID: "spanner_query_stats"}, "minutes", qss); err != nil {
		t.Fatal(err)
	}
}
