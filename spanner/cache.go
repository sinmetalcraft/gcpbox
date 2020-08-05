package spanner

import (
	"context"
	"time"
)

type StatsCacheService interface {
	SetSpannerStats(ctx context.Context, key string, expiration time.Duration) error
	CheckSpannerStats(ctx context.Context, key string) error
}
