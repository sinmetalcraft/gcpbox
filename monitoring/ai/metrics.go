package ai

import "time"

// RawMetric represents a single raw metric data point from Vertex AI API.
type RawMetric struct {
	ID          string            `json:"id"` // Unique identifier for the metric event
	MetricType  string            `json:"metric_type"` // Type of metric (e.g., "vertex_ai_api_request_count")
	Value       float64           `json:"value"`       // Value of the metric
	Timestamp   time.Time         `json:"timestamp"`   // Timestamp of the metric event
	Labels      map[string]string `json:"labels"`      // Labels associated with the metric (e.g., "model_id", "status_code")
	ProjectID   string            `json:"project_id"`  // GCP Project ID
	Location    string            `json:"location"`    // GCP Location (region/zone)
	Description string            `json:"description,omitempty"` // Optional description
}

// AggregatedDailyCount represents the aggregated daily count for a specific metric type.
type AggregatedDailyCount struct {
	Date       time.Time `json:"date"`        // Date of aggregation (YYYY-MM-DD)
	MetricType string    `json:"metric_type"` // Type of metric
	Count      int64     `json:"count"`       // Total count for the day
	ProjectID  string    `json:"project_id"`  // GCP Project ID
	Location   string    `json:"location"`    // GCP Location (region/zone)
}

// TODO: Add any other necessary structs or helper functions.
