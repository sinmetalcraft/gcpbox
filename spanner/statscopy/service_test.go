package statscopy_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/spanner"
	sadDatabase "cloud.google.com/go/spanner/admin/database/apiv1"
	sadInstance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/dgryski/go-farm"
	"google.golang.org/api/googleapi"
	sdbproto "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	protoInstance "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sinmetalcraft/gcpbox/spanner/statscopy"
)

const (
	projectID                       = "sinmetal-ci"
	queryStatsDummyTable            = "QUERY_STATS_DUMMY"
	dummyQueryStatsTableCreateTable = `
CREATE TABLE QUERY_STATS_DUMMY (
    INTERVAL_END TIMESTAMP,
    TEXT_FINGERPRINT INT64,
    AVG_BYTES FLOAT64,
    AVG_CPU_SECONDS FLOAT64,
    AVG_LATENCY_SECONDS FLOAT64,
    AVG_ROWS FLOAT64,
    AVG_ROWS_SCANNED FLOAT64,
    EXECUTION_COUNT INT64,
    TEXT STRING(MAX),
    TEXT_TRUNCATED BOOL,
) PRIMARY KEY (INTERVAL_END DESC, TEXT_FINGERPRINT)`

	txStatsDummyTable            = "TRANSACTION_STATS_DUMMY"
	dummyTxStatsTableCreateTable = `
CREATE TABLE TRANSACTION_STATS_DUMMY (
    INTERVAL_END TIMESTAMP,
    FPRINT INT64,
    READ_COLUMNS ARRAY<STRING>,
    WRITE_CONSTRUCTIVE_COLUMNS ARRAY<STRING>,
    WRITE_DELETE_TABLES ARRAY<STRING>,
    COMMIT_ATTEMPT_COUNT INT64,
    COMMIT_FAILED_PRECONDITION_COUNT INT64,
    COMMIT_ABORT_COUNT INT64,
    AVG_PARTICIPANTS FLOAT64,
    AVG_TOTAL_LATENCY_SECONDS FLOAT64,
    AVG_COMMIT_LATENCY_SECONDS FLOAT64,
    AVG_BYTES FLOAT64,
) PRIMARY KEY (INTERVAL_END DESC, TEXT_FINGERPRINT)`
)

func TestSplitDatabaseName(t *testing.T) {
	const project = "gcpug-public-spanner"
	const instance = "merpay-sponsored-instance"
	const database = "sinmetal"
	dbname := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database)

	got, err := statscopy.SplitDatabaseName(dbname)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := project, got.ProjectID; e != g {
		t.Errorf("project want %v but got %v", e, g)
	}
	if e, g := instance, got.Instance; e != g {
		t.Errorf("instance want %v but got %v", e, g)
	}
	if e, g := database, got.Database; e != g {
		t.Errorf("database want %v but got %v", e, g)
	}
}

func TestSplitDatabaseName_Err(t *testing.T) {
	_, err := statscopy.SplitDatabaseName("projects/%s/instances/%s/databases")
	if err == nil {
		t.Fatal("want err....")
	}
}

func TestDatabase_ToSpannerDatabaseName(t *testing.T) {
	d := statscopy.Database{
		ProjectID: "gcpug-public-spanner",
		Instance:  "merpay-sponsored-instance",
		Database:  "sinmetal",
	}
	if e, g := "projects/gcpug-public-spanner/instances/merpay-sponsored-instance/databases/sinmetal", d.ToSpannerDatabaseName(); e != g {
		t.Errorf("want %v but got %v", e, g)
	}
}

func TestService_GetQueryStats(t *testing.T) {
	ctx := context.Background()

	const project = "hoge"
	const instance = "fuga"
	database := fmt.Sprintf("test%d", rand.Intn(10000000))
	intervalEnd := time.Date(2020, 8, 13, 1, 1, 0, 0, time.UTC)

	newQueryStatsDummyData(t, project, instance, database, intervalEnd)

	s := newService(t, project, instance, database)
	_, err := s.GetQueryStats(ctx, queryStatsDummyTable, intervalEnd)
	if err != nil {
		t.Fatal(err)
	}
}

func TestService_CopyQueryStats(t *testing.T) {
	ctx := context.Background()

	const project = "sinmetal-ci"
	const instance = "fuga"
	database := fmt.Sprintf("test%d", rand.Intn(10000000))
	intervalEnd := time.Date(2020, 8, 20, 1, 1, 0, 0, time.UTC)

	newSpannerDatabase(t, project, instance, fmt.Sprintf("CREATE DATABASE %s", database), []string{dummyQueryStatsTableCreateTable})
	newQueryStatsDummyData(t, project, instance, database, intervalEnd)

	s := newService(t, project, instance, database)

	dataset := &bigquery.Dataset{ProjectID: projectID, DatasetID: "spanner_query_stats"}
	table := "minutes"
	if err := s.CreateQueryStatsTable(ctx, dataset, table); err != nil {
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

	count, err := s.CopyQueryStats(ctx, dataset, table, queryStatsDummyTable, intervalEnd)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("insert count is %d", count)
}

func TestService_CopyTxStats(t *testing.T) {
	ctx := context.Background()

	const project = "sinmetal-ci"
	const instance = "fuga"
	database := fmt.Sprintf("test%d", rand.Intn(10000000))
	intervalEnd := time.Date(2020, 8, 20, 1, 1, 0, 0, time.UTC)

	newSpannerDatabase(t, project, instance, fmt.Sprintf("CREATE DATABASE %s", database), []string{dummyTxStatsTableCreateTable})
	newTxStatsDummyData(t, project, instance, database, intervalEnd)

	s := newService(t, project, instance, database)

	dataset := &bigquery.Dataset{ProjectID: projectID, DatasetID: "spanner_tx_stats"}
	table := "minutes"
	if err := s.CreateTxStatsTable(ctx, dataset, table); err != nil {
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

	count, err := s.CopyQueryStats(ctx, dataset, table, txStatsDummyTable, intervalEnd)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("insert count is %d", count)
}

func TestService_CopyQueryStats_Real(t *testing.T) {
	seh := os.Getenv("SPANNER_EMULATOR_HOST")
	if len(seh) > 0 {
		t.SkipNow()
	}

	ctx := context.Background()

	const project = "gcpug-public-spanner"
	const instance = "merpay-sponsored-instance"
	const database = "sinmetal"

	s := newService(t, project, instance, database)

	dataset := &bigquery.Dataset{ProjectID: "sinmetal-ci", DatasetID: "spanner_query_stats"}
	table := "minutes"
	if err := s.CreateQueryStatsTable(ctx, dataset, table); err != nil {
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
	utc := time.Date(2020, 8, 13, 1, 1, 0, 0, time.UTC)
	_, err := s.CopyQueryStats(ctx, dataset, table, statscopy.QueryStatsTopMinuteTable, utc)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTimestamp(t *testing.T) {
	utc := time.Date(2020, 8, 13, 1, 1, 0, 0, time.UTC)
	if e, g := "2020-08-13 01:01:00", utc.Format("2006-01-02 15:04:05"); e != g {
		t.Errorf("want %v but got %v", e, g)
	}
}

func TestService_CreateQueryStatsTable(t *testing.T) {
	ctx := context.Background()

	const project = "hoge"
	const instance = "fuga"
	database := fmt.Sprintf("test%d", rand.Intn(10000000))

	newSpannerDatabase(t, project, instance, fmt.Sprintf("CREATE DATABASE %s", database), []string{dummyQueryStatsTableCreateTable})

	s := newService(t, project, instance, database)

	dataset := &bigquery.Dataset{ProjectID: projectID, DatasetID: "spanner_query_stats"}
	table := "not_found"
	utc := time.Date(2020, 8, 13, 1, 1, 0, 0, time.UTC)
	_, err := s.CopyQueryStats(ctx, dataset, table, queryStatsDummyTable, utc)
	if err != nil {
		var ae *googleapi.Error
		if ok := errors.As(err, &ae); ok {
			if ae.Code == 404 {
				if err := s.CreateQueryStatsTable(ctx, dataset, table); err != nil {
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

func newService(t *testing.T, project string, instance string, database string) *statscopy.Service {
	ctx := context.Background()

	spannerClient, err := spanner.NewClientWithConfig(ctx,
		fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database),
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

	s, err := statscopy.NewServiceWithSpannerClient(ctx, bqClient, spannerClient)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newQueryStatsDummyData(t *testing.T, project string, instance string, database string, intervalEnd time.Time) {
	ctx := context.Background()

	createStatement := fmt.Sprintf("CREATE DATABASE %s", database)
	extraStatements := []string{
		dummyQueryStatsTableCreateTable,
	}
	newSpannerDatabase(t, project, instance, createStatement, extraStatements)

	sc, err := spanner.NewClientWithConfig(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database),
		spanner.ClientConfig{
			SessionPoolConfig: spanner.SessionPoolConfig{
				MinOpened:     1,  // 1query投げておしまいので、1でOK
				MaxOpened:     10, // 1query投げておしまいなので、そんなにたくさんは要らない
				WriteSessions: 0,  // Readしかしないので、WriteSessionsをPoolする必要はない
			},
		})
	if err != nil {
		t.Fatal(err)
	}

	var mus []*spanner.Mutation
	for i := 0; i < 10; i++ {
		var list []string
		list = append(list, fmt.Sprintf("%d,", i))
		for n := 0; n < 1+rand.Intn(30); n++ {
			list = append(list, fmt.Sprintf("%d", n))
		}
		v := strings.Join(list, ",")
		queryText := "SELECT " + v
		stat := &statscopy.QueryStat{
			IntervalEnd:       intervalEnd,
			Text:              queryText,
			TextTruncated:     false,
			TextFingerprint:   int64(farm.Fingerprint64([]byte(queryText))),
			ExecuteCount:      rand.Int63n(1000),
			AvgLatencySeconds: rand.Float64(),
			AvgRows:           rand.Float64(),
			AvgBytes:          rand.Float64(),
			AvgRowsScanned:    rand.Float64(),
			AvgCPUSeconds:     rand.Float64(),
		}
		mu, err := spanner.InsertStruct("QUERY_STATS_DUMMY", stat)
		if err != nil {
			t.Fatal(err)
		}
		mus = append(mus, mu)
	}
	_, err = sc.Apply(ctx, mus)
	if err != nil {
		t.Fatal(err)
	}
}

func newTxStatsDummyData(t *testing.T, project string, instance string, database string, intervalEnd time.Time) {
	ctx := context.Background()

	createStatement := fmt.Sprintf("CREATE DATABASE %s", database)
	extraStatements := []string{
		dummyTxStatsTableCreateTable,
	}
	newSpannerDatabase(t, project, instance, createStatement, extraStatements)

	sc, err := spanner.NewClientWithConfig(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database),
		spanner.ClientConfig{
			SessionPoolConfig: spanner.SessionPoolConfig{
				MinOpened:     1,  // 1query投げておしまいので、1でOK
				MaxOpened:     10, // 1query投げておしまいなので、そんなにたくさんは要らない
				WriteSessions: 0,  // Readしかしないので、WriteSessionsをPoolする必要はない
			},
		})
	if err != nil {
		t.Fatal(err)
	}

	var mus []*spanner.Mutation
	for i := 0; i < 10; i++ {
		stat := &statscopy.TxStats{
			IntervalEnd:                   intervalEnd,
			Fprint:                        rand.Int63n(1000),
			ReadColumns:                   []string{"ReadHoge"},
			WriteConstructiveColumns:      []string{"ConFuga"},
			WriteDeleteTables:             []string{"DeleteMoge"},
			CommitAttemptCount:            rand.Int63n(1000),
			CommitFailedPreconditionCount: rand.Int63n(1000),
			CommitAbortCount:              rand.Int63n(1000),
			AvgParticipants:               rand.Float64(),
			AvgTotalLatencySeconds:        rand.Float64(),
			AvgCommitLatencySeconds:       rand.Float64(),
			AvgBytes:                      rand.Float64(),
		}
		mu, err := spanner.InsertStruct("TRANSACTION_STATS_DUMMY", stat)
		if err != nil {
			t.Fatal(err)
		}
		mus = append(mus, mu)
	}
	_, err = sc.Apply(ctx, mus)
	if err != nil {
		t.Fatal(err)
	}
}

func newSpannerDatabase(t *testing.T, project string, instance string, createStatement string, extraStatements []string) {
	seh := os.Getenv("SPANNER_EMULATOR_HOST")
	if len(seh) < 1 {
		t.Fatal("Required $SPANNER_EMULATOR_HOST")
	}

	ctx := context.Background()

	spannerInstanceAdminClient, err := sadInstance.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spannerInstanceAdminClient.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	spannerDatabaseAdminClient, err := sadDatabase.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := spannerDatabaseAdminClient.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	_, err = spannerInstanceAdminClient.CreateInstance(ctx, &protoInstance.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", project),
		InstanceId: instance,
		Instance: &protoInstance.Instance{
			Name:      fmt.Sprintf("projects/%s/instances/%s", project, instance),
			NodeCount: 1,
		},
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			// noop
		} else {
			t.Fatal(err)
		}
	}
	_, err = spannerDatabaseAdminClient.CreateDatabase(ctx, &sdbproto.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", project, instance),
		CreateStatement: createStatement,
		ExtraStatements: extraStatements,
	})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			// noop
		} else {
			t.Fatal(err)
		}
	}
}
