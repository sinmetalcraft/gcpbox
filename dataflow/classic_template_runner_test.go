package dataflow_test

import (
	"context"
	"testing"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	dataflowbox "github.com/sinmetalcraft/gcpbox/dataflow"
)

func TestClassicTemplateRunner_LaunchTemplateJob(t *testing.T) {
	ctx := context.Background()

	templatesCli, err := dataflow.NewTemplatesClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobCli, err := dataflow.NewJobsV1Beta3Client(ctx)
	if err != nil {
		t.Fatal(err)
	}

	runner, err := dataflowbox.NewClassicTemplateRunner(ctx, templatesCli, jobCli)
	if err != nil {
		t.Fatal(err)
	}

	const (
		projectID = "sinmetal-ci"
		location  = "us-central1"
	)

	resp, err := runner.LaunchSpannerToAvroOnGCSJob(ctx,
		&dataflowbox.SpannerToAvroOnGCSJobRequest{
			ProjectID: projectID,
			Location:  location,
			OutputDir: "gs://sinmetal-ci-spanner-export/gcpbox",
			// OutputDirAddCurrentDateJST: true,
			AvroTempDirectory: "gs://sinmetal-ci-work-us-central1/avro/temp/",
			// SnapshotTime:      nowJST.In(utc),
			// SnapshotTimeJSTDayChangeTime: true,
			SpannerProjectID:  "gcpug-public-spanner",
			SpannerInstanceID: "merpay-sponsored-instance",
			SpannerDatabaseID: "sinmetal",
			DataBoostEnabled:  true,
		},
		&dataflowbox.ClassicLaunchTemplateRuntimeEnvironment{
			ServiceAccountEmail: "dataflow-worker@sinmetal-ci.iam.gserviceaccount.com",
			TempLocation:        "gs://sinmetal-ci-work-us-central1/temp/",
			Subnetwork:          "regions/us-central1/subnetworks/default",
		})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("JobID:%s\n", resp.GetJob().GetId())

	var waitCount int
	var retryCount int
	for {
		if retryCount > 3 {
			t.Fatalf("Retry count exceeded 3")
		}
		time.Sleep(5 * time.Minute)
		finish, err := runner.IsFinishJob(ctx, projectID, location, resp.GetJob().GetId())
		if err != nil {
			t.Log(err)
			retryCount++
			continue
		}
		if finish {
			break
		}
		waitCount++

		if waitCount > 10 {
			t.Fatalf("Wait count exceeded 10")
		}
	}

	job, err := runner.GetJob(ctx, projectID, location, resp.GetJob().GetId())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Job Finish State is %s\n", dataflowpb.JobState_name[int32(job.GetCurrentState())])
}
