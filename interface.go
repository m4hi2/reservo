package reservo

import (
	"context"
	"time"
)

// RedisClient - to support multiple redis clients
type RedisClient interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	HSet(ctx context.Context, key string, field string, value string) error
	HGet(ctx context.Context, key string, field string) (string, error)
	HDel(ctx context.Context, key string, field string) error
	HKeys(ctx context.Context, key string) ([]string, error)
	Exists(ctx context.Context, keys ...string) (bool, error)
	RPush(ctx context.Context, key string, values ...interface{}) error
	LPop(ctx context.Context, keys string) (string, error)
	Lock(ctx context.Context, key string, ttl time.Duration) (Locker, error)
}

// Locker - to support multiple locking mechanisms
type Locker interface {
	ExtendLock(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
	TTL(ctx context.Context) (time.Duration, error)
}
