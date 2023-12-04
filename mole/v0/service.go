package mole

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrCacheMiss is Cacheに存在しない時に返す
	ErrCacheMiss = errors.New("mole: cache miss")
)

type Item struct {
	// Key is 主となるKey
	Key string

	// SurrogateKeys is 代理で利用できるKeyの一覧
	// SurrogateKeyは全体でUniqueである必要はない
	SurrogateKeys []string

	// Value is 値
	Value []byte

	// ExpiredAt is 有効期限
	ExpiredAt time.Time
}

type Service interface {
	Get(ctx context.Context, key string) (*Item, error)
	GetBySurrogateKey(ctx context.Context, surrogateKey string) ([]*Item, error)
	GetMulti(ctx context.Context, keys []string) (map[string]*Item, error)
	Set(ctx context.Context, item *Item) error
	SetMulti(ctx context.Context, items []*Item) error
	Delete(ctx context.Context, key string) error
	DeleteBySurrogateKey(ctx context.Context, surrogateKey string) error
	DeleteMulti(ctx context.Context, keys []string) error
	DeleteMultiBySurrogateKey(ctx context.Context, surrogateKeys []string) error
	FlushAll(ctx context.Context) error
}
