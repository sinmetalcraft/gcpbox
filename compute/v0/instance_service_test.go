package compute_test

import (
	"context"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"github.com/google/uuid"
	computebox "github.com/sinmetalcraft/gcpbox/compute/v0"
)

const (
	ciProjectID    = "sinmetal-ci"
	ciInstanceZone = "us-central1-a"
	ciInstanceName = "ci"
)

// TestInstanceService_StartAndStop is CI用のInstanceをStartした後、Stopする
func TestInstanceService_StartAndStop(t *testing.T) {
	ctx := context.Background()

	is := newTestInstanceService(t)

	son, err := is.Start(ctx, ciProjectID, ciInstanceZone, ciInstanceName, uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = is.GetOperation(ctx, ciProjectID, ciInstanceZone, son)
	if err != nil {
		t.Fatal(err)
	}
	_, err = is.Stop(ctx, ciProjectID, ciInstanceZone, ciInstanceName, uuid.New().String())
	if err != nil {
		t.Fatal(err)
	}
}

func newTestInstanceService(t *testing.T) *computebox.InstanceService {
	ctx := context.Background()

	instancesClient, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	zoneOpeClient, err := compute.NewZoneOperationsRESTClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	is, err := computebox.NewInstanceService(ctx, instancesClient, zoneOpeClient)
	if err != nil {
		t.Fatal(err)
	}
	return is
}
