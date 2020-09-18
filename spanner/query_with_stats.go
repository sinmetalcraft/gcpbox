package spanner

import (
	"fmt"
	"strconv"
)

const (
	QueryWithStatsElapsedTimeKey                = "elapsed_time"
	QueryWithStatsCPUTimeKey                    = "cpu_time"
	QueryWithStatsQueryPlanCreationTimeKey      = "query_plan_creation_time"
	QueryWithStatsRuntimeCreationTimeKey        = "runtime_creation_time"
	QueryWithStatsStatisticsLoadTimeKey         = "statistics_load_time"
	QueryWithStatsFilesystemDelaySecondsKey     = "filesystem_delay_seconds"
	QueryWithStatsDeletedRowsScannedKey         = "deleted_rows_scanned"
	QueryWithStatsRemoteServerCallsKey          = "remote_server_calls"
	QueryWithStatsRowsReturnedKey               = "rows_returned"
	QueryWithStatsRowsScannedKey                = "rows_scanned"
	QueryWithStatsDataBytesReadKey              = "data_bytes_read"
	QueryWithStatsBytesReturnedKey              = "bytes_returned"
	QueryWithStatsOptimizerStatisticsPackageKey = "optimizer_statistics_package"
	QueryWithStatsQueryTextKey                  = "query_text"
	QueryWithStatsOptimizerVersionKey           = "optimizer_version"
)

// QueryWithStats is Query 実行時に返ってくるStatsの情報を持つstruct
type QueryWithStats struct {
	ElapsedTime                string `json:"elapsedTime"`
	CPUTime                    string `json:"cpuTime"`
	QueryPlanCreationTime      string `json:"queryPlanCreationTime"`
	RuntimeCreationTime        string `json:"runtimeCreationTime"`
	StatisticsLoadTime         string `json:"statisticsLoadTime"`
	FilesystemDelaySeconds     string `json:"filesystemDelaySeconds"`
	DeletedRowsScanned         int64  `json:"deletedRowsScanned"`
	RemoteServerCalls          string `json:"remoteServerCalls"`
	RowsReturned               int64  `json:"rowsReturned"`
	RowsScanned                int64  `json:"rowsScanned"`
	DataBytesRead              int64  `json:"dataBytesRead"`
	BytesReturned              int64  `json:"bytesReturned"`
	OptimizerStatisticsPackage string `json:"optimizerStatisticsPackage"`
	QueryText                  string `json:"queryText"`
	OptimizerVersion           string `json:"optimizerVersion"`
}

// ConvertQueryWithStats is Query 実行時に返ってくる stats の map を struct に割り当てる
func ConvertQueryWithStats(stats map[string]interface{}) (*QueryWithStats, error) {
	var result QueryWithStats
	var err error
	result.ElapsedTime, err = getQueryWithStatsStringValue(stats, QueryWithStatsElapsedTimeKey)
	if err != nil {
		return nil, err
	}
	result.CPUTime, err = getQueryWithStatsStringValue(stats, QueryWithStatsCPUTimeKey)
	if err != nil {
		return nil, err
	}
	result.QueryPlanCreationTime, err = getQueryWithStatsStringValue(stats, QueryWithStatsQueryPlanCreationTimeKey)
	if err != nil {
		return nil, err
	}
	result.RuntimeCreationTime, err = getQueryWithStatsStringValue(stats, QueryWithStatsRuntimeCreationTimeKey)
	if err != nil {
		return nil, err
	}
	result.StatisticsLoadTime, err = getQueryWithStatsStringValue(stats, QueryWithStatsStatisticsLoadTimeKey)
	if err != nil {
		return nil, err
	}
	result.FilesystemDelaySeconds, err = getQueryWithStatsStringValue(stats, QueryWithStatsFilesystemDelaySecondsKey)
	if err != nil {
		return nil, err
	}
	result.DeletedRowsScanned, err = getQueryWithStatsIntegerValue(stats, QueryWithStatsDeletedRowsScannedKey)
	if err != nil {
		return nil, err
	}
	result.RemoteServerCalls, err = getQueryWithStatsStringValue(stats, QueryWithStatsRemoteServerCallsKey)
	if err != nil {
		return nil, err
	}
	result.RowsReturned, err = getQueryWithStatsIntegerValue(stats, QueryWithStatsRowsReturnedKey)
	if err != nil {
		return nil, err
	}
	result.RowsScanned, err = getQueryWithStatsIntegerValue(stats, QueryWithStatsRowsScannedKey)
	if err != nil {
		return nil, err
	}
	result.DataBytesRead, err = getQueryWithStatsIntegerValue(stats, QueryWithStatsDataBytesReadKey)
	if err != nil {
		return nil, err
	}
	result.BytesReturned, err = getQueryWithStatsIntegerValue(stats, QueryWithStatsBytesReturnedKey)
	if err != nil {
		return nil, err
	}
	result.OptimizerStatisticsPackage, err = getQueryWithStatsStringValue(stats, QueryWithStatsOptimizerStatisticsPackageKey)
	if err != nil {
		return nil, err
	}
	result.QueryText, err = getQueryWithStatsStringValue(stats, QueryWithStatsQueryTextKey)
	if err != nil {
		return nil, err
	}
	result.OptimizerVersion, err = getQueryWithStatsStringValue(stats, QueryWithStatsOptimizerVersionKey)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func getQueryWithStatsStringValue(stats map[string]interface{}, key string) (string, error) {
	v, ok := stats[key]
	if !ok {
		return "", NewErrNotFound(key, nil)
	}
	str, ok := v.(string)
	if !ok {
		return "", NewErrInvalidArgument(fmt.Sprintf("%s:%v not string. type is %T", key, v, v), nil, nil)
	}
	return str, nil
}

func getQueryWithStatsIntegerValue(stats map[string]interface{}, key string) (int64, error) {
	v, ok := stats[key]
	if !ok {
		return 0, NewErrNotFound(key, nil)
	}

	str, ok := v.(string)
	if !ok {
		return 0, NewErrInvalidArgument(fmt.Sprintf("%s:%v not string. type is %T", key, v, v), nil, nil)
	}

	i, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, NewErrInvalidArgument(fmt.Sprintf("%s:%v not int.", key, v), nil, err)
	}

	return i, nil
}
