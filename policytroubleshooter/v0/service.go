package policytroubleshooter

import (
	"context"

	pt "cloud.google.com/go/policytroubleshooter/apiv1"
	ptpb "cloud.google.com/go/policytroubleshooter/apiv1/policytroubleshooterpb"
	"github.com/sinmetalcraft/gcpbox/internal/trace"
)

type Service struct {
	iamCheckerClient *pt.IamCheckerClient
}

func NewService(ctx context.Context, iamCheckerClient *pt.IamCheckerClient) (*Service, error) {
	return &Service{
		iamCheckerClient: iamCheckerClient,
	}, nil
}

func (s *Service) Close(ctx context.Context) error {
	if err := s.iamCheckerClient.Close(); err != nil {
		return err
	}
	return nil
}

// HasPermission is fullResourceNameで指定したResourceにprincipalがpermissionを持っているかを返す
//
// principal : Google Account or Service Account Email For example sinmetal@example.com
// fullResourceName : see https://cloud.google.com/iam/docs/full-resource-names For example //storage.googleapis.com/projects/_/buckets/bucket_id
// permission : see https://cloud.google.com/iam/docs/understanding-roles For example storage.objects.get
func (s *Service) HasPermission(ctx context.Context, principal string, fullResourceName string, permission string) (has bool, err error) {
	ctx = trace.StartSpan(ctx, "policytroubleshooter.HasPermission")
	defer trace.EndSpan(ctx, err)

	resp, err := s.TroubleshootIamPolicy(ctx, principal, fullResourceName, permission)
	if err != nil {
		return false, err
	}
	return resp.Access == ptpb.AccessState_GRANTED, nil
}

// TroubleshootIamPolicy is https://cloud.google.com/iam/docs/reference/policytroubleshooter/rest/v1/iam/troubleshoot を呼ぶ
func (s *Service) TroubleshootIamPolicy(ctx context.Context, principal string, fullResourceName string, permission string) (resp *ptpb.TroubleshootIamPolicyResponse, err error) {
	ctx = trace.StartSpan(ctx, "policytroubleshooter.TroubleshootIamPolicy")
	defer trace.EndSpan(ctx, err)

	resp, err = s.iamCheckerClient.TroubleshootIamPolicy(ctx, &ptpb.TroubleshootIamPolicyRequest{AccessTuple: &ptpb.AccessTuple{
		Principal:        principal,
		FullResourceName: fullResourceName,
		Permission:       permission,
	}})
	if err != nil {
		return nil, err
	}
	return resp, err
}
