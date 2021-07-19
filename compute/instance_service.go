package compute

import (
	"context"
	"fmt"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "google.golang.org/genproto/googleapis/cloud/compute/v1"
)

type InstanceService struct {
	instancesClient *compute.InstancesClient
	zoneOpeClient   *compute.ZoneOperationsClient
}

func NewInstanceService(ctx context.Context, instancesClient *compute.InstancesClient, zoneOpeClient *compute.ZoneOperationsClient) (*InstanceService, error) {
	return &InstanceService{
		instancesClient: instancesClient,
		zoneOpeClient:   zoneOpeClient,
	}, nil
}

// Start is 指定したInstanceをStartする
// 指定したInstanceがすでにRunningの場合、何も起こらない
// requestIDにはUUIDを指定する
func (s *InstanceService) Start(ctx context.Context, project string, zone string, instance string, requestID string) (opeID string, err error) {
	ope, err := s.instancesClient.Start(ctx, &computepb.StartInstanceRequest{
		Project:   project,
		Zone:      zone,
		Instance:  instance,
		RequestId: &requestID, // UUID
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", *ope.Id), nil
}

// Stop is 指定したInstanceをStopする
// 指定したInstanceがすでにTerminetedの場合、何も起こらない
// requestIDにはUUIDを指定する
func (s *InstanceService) Stop(ctx context.Context, project string, zone string, instance string, requestID string) (opeID string, err error) {
	ope, err := s.instancesClient.Stop(ctx, &computepb.StopInstanceRequest{
		Project:   project,
		Zone:      zone,
		Instance:  instance,
		RequestId: &requestID, // UUID
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", *ope.Id), nil
}

// GetOperation is 指定したOperationを取得する
func (s *InstanceService) GetOperation(ctx context.Context, project string, zone string, opeID string) (ope *computepb.Operation, err error) {
	ope, err = s.zoneOpeClient.Get(ctx, &computepb.GetZoneOperationRequest{
		Project:   project,
		Zone:      zone,
		Operation: opeID,
	})
	if err != nil {
		return nil, err
	}
	return ope, nil
}

// WaitOperation is 指定したOperationが完了するまで待つ
func (s *InstanceService) WaitOperation(ctx context.Context, project string, zone string, opeID string) (ope *computepb.Operation, err error) {
	ope, err = s.zoneOpeClient.Wait(ctx, &computepb.WaitZoneOperationRequest{
		Project:   project,
		Zone:      zone,
		Operation: opeID,
	})
	if err != nil {
		return nil, err
	}
	return ope, nil
}
