package adapters

import (
	"context"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/m4hi2/reservo"
	"github.com/redis/go-redis/v9"
	"time"
)

type GoRedisAdapter struct {
	Client     *redis.Client
	LockClient *redislock.Client
}

type AdapterLocker struct {
	l *redislock.Lock
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

func (adapter *GoRedisAdapter) Get(ctx context.Context, key string) (string, error) {
	return adapter.Client.Get(ctx, key).Result()
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

func (adapter *GoRedisAdapter) LPop(ctx context.Context, key string) (string, error) {
	return adapter.Client.LPop(ctx, key).Result()
}

func (adapter *GoRedisAdapter) HKeys(ctx context.Context, key string) ([]string, error) {
	return adapter.Client.HKeys(ctx, key).Result()
}

func (adapter *GoRedisAdapter) Lock(ctx context.Context, key string, ttl time.Duration) (reservo.Locker, error) {
	key = fmt.Sprintf("%s:lock", key)
	l, err := adapter.LockClient.Obtain(ctx, key, ttl, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", reservo.ErrLockNotObtained, err)
	}

	al := &AdapterLocker{
		l: l,
	}

	return al, nil
}

func (adapter *GoRedisAdapter) HSet(ctx context.Context, key string, field string, value string) error {
	return adapter.Client.HSet(ctx, key, field, value).Err()
}

func (adapter *GoRedisAdapter) HGet(ctx context.Context, key string, field string) (string, error) {
	return adapter.Client.HGet(ctx, key, field).Result()
}

func (adapter *GoRedisAdapter) HDel(ctx context.Context, key string, field string) error {
	return adapter.Client.HDel(ctx, key, field).Err()
}

func (al *AdapterLocker) Release(ctx context.Context) error {
	if err := al.l.Release(ctx); err != nil {
		if errors.Is(err, redislock.ErrLockNotHeld) {
			return nil
		}
		return err
	}

	return nil
}

func (al *AdapterLocker) ExtendLock(ctx context.Context, ttl time.Duration) error {
	return al.l.Refresh(ctx, ttl, nil)
}

func (al *AdapterLocker) TTL(ctx context.Context) (time.Duration, error) {
	var td time.Duration = 0
	var err error = nil

	if td, err = al.l.TTL(ctx); err != nil {
		return 0, err
	}

	return td, err
}
