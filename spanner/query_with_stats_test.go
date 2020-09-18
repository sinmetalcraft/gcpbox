package spanner_test

import (
	"testing"

	spabox "github.com/sinmetal/gcpbox/spanner"
)

func TestConvertQueryWithStats(t *testing.T) {
	m := map[string]interface{}{}
	m["elapsed_time"] = "38.52 secs"
	m["query_plan_creation_time"] = "39.04 msecs"
	m["statistics_load_time"] = "0"
	m["cpu_time"] = "292.82 secs"
	m["runtime_creation_time"] = "1.17 secs"
	m["deleted_rows_scanned"] = "0"
	m["remote_server_calls"] = "4771/4771"
	m["bytes_returned"] = "17480"
	m["rows_scanned"] = "26018674"
	m["optimizer_statistics_package"] = ""
	m["query_text"] = "SELECT 1"
	m["data_bytes_read"] = "19758968877"
	m["rows_returned"] = "100"
	m["optimizer_version"] = "2"
	m["filesystem_delay_seconds"] = "30.4 secs"

	v, err := spabox.ConvertQueryWithStats(m)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := "38.52 secs", v.ElapsedTime; e != g {
		t.Errorf("ElaspedTime want %v but got %v", e, g)
	}
	if e, g := "39.04 msecs", v.QueryPlanCreationTime; e != g {
		t.Errorf("QueryPlanCreationTime want %v but got %v", e, g)
	}
	if e, g := "0", v.StatisticsLoadTime; e != g {
		t.Errorf("StatisticsLoadTime want %v but got %v", e, g)
	}
	if e, g := "292.82 secs", v.CPUTime; e != g {
		t.Errorf("CPUTime want %v but got %v", e, g)
	}
	if e, g := "1.17 secs", v.RuntimeCreationTime; e != g {
		t.Errorf("RuntimeCreationTime want %v but got %v", e, g)
	}
	if e, g := int64(0), v.DeletedRowsScanned; e != g {
		t.Errorf("DeletedRowsScanned want %v but got %v", e, g)
	}
	if e, g := "4771/4771", v.RemoteServerCalls; e != g {
		t.Errorf("RemoteServerCalls want %v but got %v", e, g)
	}
	if e, g := int64(17480), v.BytesReturned; e != g {
		t.Errorf("BytesReturned want %v but got %v", e, g)
	}
	if e, g := int64(26018674), v.RowsScanned; e != g {
		t.Errorf("RowsScanned want %v but got %v", e, g)
	}
	if e, g := "", v.OptimizerStatisticsPackage; e != g {
		t.Errorf("OptimizerStatisticsPackage want %v but got %v", e, g)
	}
	if e, g := "SELECT 1", v.QueryText; e != g {
		t.Errorf("QueryText want %v but got %v", e, g)
	}
	if e, g := int64(19758968877), v.DataBytesRead; e != g {
		t.Errorf("DataBytesRead want %v but got %v", e, g)
	}
	if e, g := int64(100), v.RowsReturned; e != g {
		t.Errorf("DataBytesRead want %v but got %v", e, g)
	}
	if e, g := "2", v.OptimizerVersion; e != g {
		t.Errorf("OptimizerVersion want %v but got %v", e, g)
	}
	if e, g := "30.4 secs", v.FilesystemDelaySeconds; e != g {
		t.Errorf("FilesystemDelaySeconds want %v but got %v", e, g)
	}
}
