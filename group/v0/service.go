package group

import (
	"context"

	admin "google.golang.org/api/admin/directory/v1"
)

type Service struct {
	svc *admin.Service
}

func NewService(ctx context.Context, svc *admin.Service) (*Service, error) {
	return &Service{
		svc: svc,
	}, nil
}

func (s *Service) HasMember(groupKey string, memberKey string) (bool, error) {
	resp, err := s.svc.Members.HasMember(groupKey, memberKey).Do()
	if err != nil {
		return false, err
	}
	return resp.IsMember, nil
}
