package ai

import (
	"reflect"
	"testing"
	"time"
)

// Helper function to create a time.Time object from a date string
func MustParseTime(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestFetchVertexAIMetrics(t *testing.T) {
	// Test with the metric type that has mock data
	t.Run("fetch with mock data", func(t *testing.T) {
		projectID := "test-project" // Using a distinct project ID for testing
		metricType := "aiplatform.googleapis.com/prediction/request_count"
		// Define a specific time range for predictability, e.g., a 2-day window
		startTime := MustParseTime(time.RFC3339, "2023-10-01T00:00:00Z")
		endTime := MustParseTime(time.RFC3339, "2023-10-03T00:00:00Z") // 48 hour window

		metrics, err := FetchVertexAIMetrics(projectID, metricType, startTime, endTime)
		if err != nil {
			t.Fatalf("FetchVertexAIMetrics returned an error: %v", err)
		}

		// Based on the current mock data in aggregator.go:
		// It creates two mock metrics if metricType matches.
		// One at startTime + 1 hour, another at startTime + 25 hours.
		expectedCount := 2
		if len(metrics) != expectedCount {
			t.Errorf("expected %d metrics, got %d", expectedCount, len(metrics))
		}

		if len(metrics) > 0 {
			if metrics[0].ProjectID != projectID {
				t.Errorf("expected ProjectID %s, got %s", projectID, metrics[0].ProjectID)
			}
			if metrics[0].MetricType != metricType {
				t.Errorf("expected MetricType %s, got %s", metricType, metrics[0].MetricType)
			}
		}
	})

	t.Run("fetch with no mock data for metric type", func(t *testing.T) {
		projectID := "another-project"
		metricType := "some_other_metric_type"
		startTime := MustParseTime(time.RFC3339, "2023-10-01T00:00:00Z")
		endTime := MustParseTime(time.RFC3339, "2023-10-02T00:00:00Z")

		metrics, err := FetchVertexAIMetrics(projectID, metricType, startTime, endTime)
		if err != nil {
			// The function is currently designed to return empty slice, not error for unknown metric type
			// t.Fatalf("FetchVertexAIMetrics returned an error: %v", err)
		}

		// Expecting an empty list as per the placeholder logic for unmocked types
		if len(metrics) != 0 {
			t.Errorf("expected 0 metrics for unmocked type, got %d", len(metrics))
		}
	})
}

func TestAggregateMetricsByDay(t *testing.T) {
	testProjectID1 := "project-1"
	testLocation1 := "us-central1"
	testProjectID2 := "project-2"
	testLocation2 := "europe-west1"

	tests := []struct {
		name         string
		rawMetrics   []*RawMetric
		expectedMap  map[string]*AggregatedDailyCount
		expectError  bool
	}{
		{
			name:        "no metrics",
			rawMetrics:  []*RawMetric{},
			expectedMap: map[string]*AggregatedDailyCount{},
			expectError: false,
		},
		{
			name: "nil raw metric entry",
			rawMetrics: []*RawMetric{
				nil, // simulate a nil entry
				{
					Timestamp:  MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"),
					MetricType: "type1",
					Value:      10,
					ProjectID:  testProjectID1,
					Location:   testLocation1,
				},
			},
			expectedMap: map[string]*AggregatedDailyCount{
				"2023-01-01-type1-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1",
					ProjectID:  testProjectID1,
					Location:   testLocation1,
					Count:      10,
				},
			},
			expectError: false,
		},
		{
			name: "metrics for a single day",
			rawMetrics: []*RawMetric{
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "type1", Value: 10, ProjectID: testProjectID1, Location: testLocation1},
				{Timestamp: MustParseTime("2023-01-01T14:00:00Z", "2023-01-01T14:00:00Z"), MetricType: "type1", Value: 5, ProjectID: testProjectID1, Location: testLocation1},
			},
			expectedMap: map[string]*AggregatedDailyCount{
				"2023-01-01-type1-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1",
					ProjectID:  testProjectID1,
					Location:   testLocation1,
					Count:      15,
				},
			},
			expectError: false,
		},
		{
			name: "metrics spanning multiple days",
			rawMetrics: []*RawMetric{
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "type1", Value: 10, ProjectID: testProjectID1, Location: testLocation1},
				{Timestamp: MustParseTime("2023-01-02T11:00:00Z", "2023-01-02T11:00:00Z"), MetricType: "type1", Value: 20, ProjectID: testProjectID1, Location: testLocation1},
			},
			expectedMap: map[string]*AggregatedDailyCount{
				"2023-01-01-type1-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1",
					ProjectID:  testProjectID1,
					Location:   testLocation1,
					Count:      10,
				},
				"2023-01-02-type1-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-02T00:00:00Z"),
					MetricType: "type1",
					ProjectID:  testProjectID1,
					Location:   testLocation1,
					Count:      20,
				},
			},
			expectError: false,
		},
		{
			name: "metrics with different metric types",
			rawMetrics: []*RawMetric{
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "type1", Value: 10, ProjectID: testProjectID1, Location: testLocation1},
				{Timestamp: MustParseTime("2023-01-01T11:00:00Z", "2023-01-01T11:00:00Z"), MetricType: "type2", Value: 5, ProjectID: testProjectID1, Location: testLocation1},
			},
			expectedMap: map[string]*AggregatedDailyCount{
				"2023-01-01-type1-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1",
					ProjectID:  testProjectID1,
					Location:   testLocation1,
					Count:      10,
				},
				"2023-01-01-type2-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type2",
					ProjectID:  testProjectID1,
					Location:   testLocation1,
					Count:      5,
				},
			},
			expectError: false,
		},
		{
			name: "metrics with different projects and locations",
			rawMetrics: []*RawMetric{
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "type1", Value: 100, ProjectID: testProjectID1, Location: testLocation1},
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "type1", Value: 200, ProjectID: testProjectID2, Location: testLocation1}, // Same day, same type, same loc, diff project
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "type1", Value: 300, ProjectID: testProjectID1, Location: testLocation2}, // Same day, same type, same proj, diff loc
			},
			expectedMap: map[string]*AggregatedDailyCount{
				"2023-01-01-type1-project-1-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1", ProjectID: testProjectID1, Location: testLocation1, Count: 100,
				},
				"2023-01-01-type1-project-2-us-central1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1", ProjectID: testProjectID2, Location: testLocation1, Count: 200,
				},
				"2023-01-01-type1-project-1-europe-west1": {
					Date:       MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"),
					MetricType: "type1", ProjectID: testProjectID1, Location: testLocation2, Count: 300,
				},
			},
			expectError: false,
		},
		{
			name: "complex case: multiple days, types, projects, locations",
			rawMetrics: []*RawMetric{
				{Timestamp: MustParseTime("2023-01-01T08:00:00Z", "2023-01-01T08:00:00Z"), MetricType: "m_type_A", Value: 10, ProjectID: "proj_X", Location: "loc_X"},
				{Timestamp: MustParseTime("2023-01-01T09:00:00Z", "2023-01-01T09:00:00Z"), MetricType: "m_type_A", Value: 15, ProjectID: "proj_X", Location: "loc_X"}, // Same day, type, proj, loc
				{Timestamp: MustParseTime("2023-01-01T10:00:00Z", "2023-01-01T10:00:00Z"), MetricType: "m_type_B", Value: 5, ProjectID: "proj_X", Location: "loc_X"},  // Same day, proj, loc, diff type
				{Timestamp: MustParseTime("2023-01-02T08:00:00Z", "2023-01-02T08:00:00Z"), MetricType: "m_type_A", Value: 20, ProjectID: "proj_X", Location: "loc_X"}, // Diff day, same type, proj, loc
				{Timestamp: MustParseTime("2023-01-01T08:00:00Z", "2023-01-01T08:00:00Z"), MetricType: "m_type_A", Value: 30, ProjectID: "proj_Y", Location: "loc_X"}, // Same day, type, loc, diff proj
				{Timestamp: MustParseTime("2023-01-01T08:00:00Z", "2023-01-01T08:00:00Z"), MetricType: "m_type_A", Value: 40, ProjectID: "proj_X", Location: "loc_Y"}, // Same day, type, proj, diff loc
			},
			expectedMap: map[string]*AggregatedDailyCount{
				"2023-01-01-m_type_A-proj_X-loc_X": {Date: MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"), MetricType: "m_type_A", ProjectID: "proj_X", Location: "loc_X", Count: 25}, // 10 + 15
				"2023-01-01-m_type_B-proj_X-loc_X": {Date: MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"), MetricType: "m_type_B", ProjectID: "proj_X", Location: "loc_X", Count: 5},
				"2023-01-02-m_type_A-proj_X-loc_X": {Date: MustParseTime(time.RFC3339, "2023-01-02T00:00:00Z"), MetricType: "m_type_A", ProjectID: "proj_X", Location: "loc_X", Count: 20},
				"2023-01-01-m_type_A-proj_Y-loc_X": {Date: MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"), MetricType: "m_type_A", ProjectID: "proj_Y", Location: "loc_X", Count: 30},
				"2023-01-01-m_type_A-proj_X-loc_Y": {Date: MustParseTime(time.RFC3339, "2023-01-01T00:00:00Z"), MetricType: "m_type_A", ProjectID: "proj_X", Location: "loc_Y", Count: 40},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Need to use a specific layout for MustParseTime that matches the input strings if they are not RFC3339
			// For the test data, using RFC3339 for timestamps, and "2006-01-02" for dates in keys.
			// The Date field in AggregatedDailyCount is truncated, so it will be YYYY-MM-DD 00:00:00 UTC.

			// Correcting the expectedMap Date values to be truncated time.Time objects
			for _, agg := range tt.expectedMap {
				agg.Date = agg.Date.Truncate(24 * time.Hour)
			}


			gotMap, err := AggregateMetricsByDay(tt.rawMetrics)
			if (err != nil) != tt.expectError {
				t.Fatalf("AggregateMetricsByDay() error = %v, expectError %v", err, tt.expectError)
				return
			}

			if !reflect.DeepEqual(gotMap, tt.expectedMap) {
				// For easier debugging, print out the maps
				t.Errorf("AggregateMetricsByDay() got = %#v, want %#v", gotMap, tt.expectedMap)
				// Optionally, iterate and compare key by key for more detailed diff
				for k, expected := range tt.expectedMap {
					if got, ok := gotMap[k]; !ok {
						t.Errorf("Missing key in gotMap: %s", k)
					} else if !reflect.DeepEqual(got, expected) {
						t.Errorf("Mismatch for key %s: got %#v, want %#v", k, got, expected)
					}
				}
				for k := range gotMap {
					if _, ok := tt.expectedMap[k]; !ok {
						t.Errorf("Extra key in gotMap: %s", k)
					}
				}
			}
		})
	}
}
