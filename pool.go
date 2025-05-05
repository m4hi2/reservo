package reservo

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// InitFn - function to have
type InitFn func() []*Resource

// NewResourceFn - used when a resource is expired
type NewResourceFn func(key string, optionals ...any) (*Resource, error)

// Pool - manages a collection of resources with a specified TTL, using a RedisClient for backend operations.
// It supports custom initialization and configuration via InitFn and PoolOpts.
// Important: the pool only holds pointer to the actual data. Since the data might have a TTL on redis
// of its own.
type Pool struct {
	name     string
	rc       RedisClient
	initFn   InitFn
	newResFn NewResourceFn
	ttl      time.Duration
	lockTtl  time.Duration
}

// NewPool - creates and initializes a new Pool with the given name, Redis client, and resource initialization function.
// Optional PoolOpts can be provided to customize the Pool configuration.
// Returns the created Pool and an error if initialization fails.
// Default TTL is set to 1 second.
func NewPool(name string, rc RedisClient, initFn InitFn, newResFn NewResourceFn, opts ...PoolOpts) (*Pool, error) {
	p := &Pool{
		name:     name,
		rc:       rc,
		initFn:   initFn,
		newResFn: newResFn,
		ttl:      time.Minute,
		lockTtl:  time.Second,
	}

	for _, opt := range opts {
		opt(p)
	}

	if err := p.createRedisPool(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Pool) GetResource(ctx context.Context) (*Resource, error) {
	/*
		Steps of getting resource:
		1. Pop a key from pool array
		2. Create a lock for the key
		3. Create an entry in the allocated hash
		4. Access the resource

	*/

	resKey, err := p.rc.LPop(ctx, p.getPoolKey())
	if err != nil {
		return nil, err
	}

	if err := p.allocate(ctx, resKey); err != nil {
		return nil, err
	}

	v, l, err := p.getResValue(ctx, resKey)
	if err != nil {
		return nil, err
	}

	res := &Resource{
		noCopy:    noCopy{},
		pool:      p,
		l:         l,
		expiresAt: time.Now().Add(p.ttl),
		Key:       resKey,
		Value:     v,
	}

	return res, nil

}

func (p *Pool) getResValue(ctx context.Context, key string) (string, Locker, error) {
	var v string

	v, err := p.rc.Get(ctx, key)
	if errors.Is(err, redis.Nil) {
		res, err := p.recreateResource(key)
		if err != nil {
			return "", nil, err
		}
		v = res.Value
	}

	if err != nil && !errors.Is(err, redis.Nil) {
		return "", nil, err
	}

	l, err := p.rc.Lock(ctx, key, p.lockTtl)
	if err != nil {
		return "", nil, err
	}

	return v, l, nil
}

func (p *Pool) recreateResource(resKey string) (*Resource, error) {
	res, err := p.newResFn(resKey)
	if err != nil {
		return nil, err
	}

	if err := p.createResInRedis(res); err != nil {
		return nil, err
	}

	return res, nil

}

func (p *Pool) allocate(ctx context.Context, key string) error {

	if err := p.rc.HSet(ctx, p.getAllocatedResourcesKey(), key, "taken"); err != nil {
		return err
	}

	return nil
}

func (p *Pool) deallocate(ctx context.Context, key string) error {
	if err := p.rc.HDel(ctx, p.getAllocatedResourcesKey(), key); err != nil {
		return err
	}

	return nil
}

func (p *Pool) createRedisPool() error {
	exists, err := p.checkPoolExists()
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	lock, err := p.getManagementLock()
	defer func() {
		if lock != nil {
			lock.Release(context.Background())
		}
	}()

	if err != nil {
		return err
	}

	initRes := p.initFn()

	poolKeys := make([]string, 0, len(initRes))

	for _, res := range initRes {
		poolKeys = append(poolKeys, res.Key)
		if err := p.createResInRedis(res); err != nil {
			return err
		}
	}

	if err := p.rc.RPush(context.TODO(), p.getPoolKey(), poolKeys); err != nil {
		return err
	}

	return nil
}

func (p *Pool) checkPoolExists() (bool, error) {
	return p.rc.Exists(context.TODO(), p.getPoolKey(), p.getAllocatedResourcesKey())
}

func (p *Pool) getPoolKey() string {
	return fmt.Sprintf("%s:%s", PoolNamePreFix, p.name)
}

func (p *Pool) getManagementLock() (Locker, error) {
	managementKey := fmt.Sprintf("%s:management", p.getPoolKey())

	l, err := p.rc.Lock(context.Background(), managementKey, p.lockTtl)
	if err != nil {
		return nil, err
	}

	return l, nil

}

func (p *Pool) getAllocatedResourcesKey() string {
	return fmt.Sprintf("%s:%s", AllocatedPreFix, p.name)
}

func (p *Pool) createResInRedis(res *Resource) error {
	l, err := p.rc.Lock(context.Background(), res.Key, p.lockTtl)
	defer func() {
		if l != nil {
			l.Release(context.Background())
		}
	}()
	if err != nil {
		return err
	}

	//resKey := fmt.Sprintf("%s:%s", ResourceNamePreFix, res.Key)
	if err := p.rc.Set(context.TODO(), res.Key, res.Value, p.ttl); err != nil {
		return err
	}

	return nil
}

func (p *Pool) returnToPool(ctx context.Context, key string) error {

	if err := p.rc.RPush(ctx, p.getPoolKey(), key); err != nil {
		return err
	}

	if err := p.deallocate(ctx, key); err != nil {
		return err
	}

	return nil
}
