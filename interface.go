package reservo

import (
	"context"
	"time"
)

// RedisClient - to support multiple redis clients
type RedisClient interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Exists(ctx context.Context, keys ...string) (bool, error)
	RPush(ctx context.Context, key string, values ...interface{}) error
	LPop(ctx context.Context, keys string) (string, error)
	Lock(ctx context.Context, key string, ttl time.Duration) (Locker, error)
}

type Locker interface {
	Release(ctx context.Context) error
}

// noCopy may be added to structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
//
// Note that it must not be embedded, due to the Lock and Unlock methods.
type noCopy struct{}
