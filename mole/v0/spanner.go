package mole

import (
	"context"
	"fmt"
)

type SpannerService struct {
}

func NewService() (Service, error) {
	return &SpannerService{}, nil
}

func (s *SpannerService) Get(ctx context.Context, key string) (*Item, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (s *SpannerService) GetMulti(ctx context.Context, keys []string) (map[string]*Item, error) {
	return nil, fmt.Errorf("unimplemented")
}

func (s *SpannerService) Set(ctx context.Context, item *Item) error {
	return fmt.Errorf("unimplemented")
}

func (s *SpannerService) SetMulti(ctx context.Context, items []*Item) error {
	return fmt.Errorf("unimplemented")
}

func (s *SpannerService) Delete(ctx context.Context, key string) error {
	return fmt.Errorf("unimplemented")
}

func (s *SpannerService) DeleteMulti(ctx context.Context, keys []string) error {
	return fmt.Errorf("unimplemented")
}

func (s *SpannerService) FlushAll(ctx context.Context) error {
	return fmt.Errorf("unimplemented")
}
