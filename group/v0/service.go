package group

import (
	"context"

	admin "google.golang.org/api/admin/directory/v1"
)

var Scopes = []string{
	"https://www.googleapis.com/auth/admin.chrome.printers",
	"https://www.googleapis.com/auth/admin.chrome.printers.readonly",
	"https://www.googleapis.com/auth/admin.directory.customer",
	"https://www.googleapis.com/auth/admin.directory.customer.readonly",
	"https://www.googleapis.com/auth/admin.directory.device.chromeos",
	"https://www.googleapis.com/auth/admin.directory.device.chromeos.readonly",
	"https://www.googleapis.com/auth/admin.directory.device.mobile",
	"https://www.googleapis.com/auth/admin.directory.device.mobile.action",
	"https://www.googleapis.com/auth/admin.directory.device.mobile.readonly",
	"https://www.googleapis.com/auth/admin.directory.domain",
	"https://www.googleapis.com/auth/admin.directory.domain.readonly",
	"https://www.googleapis.com/auth/admin.directory.group",
	"https://www.googleapis.com/auth/admin.directory.group.member",
	"https://www.googleapis.com/auth/admin.directory.group.member.readonly",
	"https://www.googleapis.com/auth/admin.directory.group.readonly",
	"https://www.googleapis.com/auth/admin.directory.orgunit",
	"https://www.googleapis.com/auth/admin.directory.orgunit.readonly",
	"https://www.googleapis.com/auth/admin.directory.resource.calendar",
	"https://www.googleapis.com/auth/admin.directory.resource.calendar.readonly",
	"https://www.googleapis.com/auth/admin.directory.rolemanagement",
	"https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly",
	"https://www.googleapis.com/auth/admin.directory.user",
	"https://www.googleapis.com/auth/admin.directory.user.alias",
	"https://www.googleapis.com/auth/admin.directory.user.alias.readonly",
	"https://www.googleapis.com/auth/admin.directory.user.readonly",
	"https://www.googleapis.com/auth/admin.directory.user.security",
	"https://www.googleapis.com/auth/admin.directory.userschema",
	"https://www.googleapis.com/auth/admin.directory.userschema.readonly",
	"https://www.googleapis.com/auth/cloud-platform",
}

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
