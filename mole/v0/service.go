package mole

import (
	"context"
	"time"
)

type Item struct {
	Key       string
	Value     []byte
	ExpiredAt time.Time
}

type Service interface {
	Get(ctx context.Context, key string) (*Item, error)
	GetMulti(ctx context.Context, keys []string) (map[string]*Item, error)
	Set(ctx context.Context, item *Item) error
	SetMulti(ctx context.Context, items []*Item) error
	Delete(ctx context.Context, key string) error
	DeleteMulti(ctx context.Context, keys []string) error
	FlushAll(ctx context.Context) error
}
