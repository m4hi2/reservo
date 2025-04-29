package adapters

import (
	"context"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"time"
)

type GoRedisAdapter struct {
	Client     *redis.Client
	LockClient *redislock.Client
}

func NewGoRedisAdapter(client *redis.Client) *GoRedisAdapter {
	return &GoRedisAdapter{
		Client:     client,
		LockClient: redislock.New(client),
	}
}

func (adapter *GoRedisAdapter) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return adapter.Client.Set(ctx, key, value, expiration).Err()
}

func (adapter *GoRedisAdapter) Exists(ctx context.Context, keys ...string) (bool, error) {
	c, err := adapter.Client.Exists(ctx, keys...).Result()
	if err != nil {
		return false, err
	}

	if c > 0 {
		return true, nil
	}

	return false, nil
}

func (adapter *GoRedisAdapter) RPush(ctx context.Context, key string, values ...interface{}) error {
	return adapter.Client.RPush(ctx, key, values...).Err()
}
