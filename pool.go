package reservo

import (
	"context"
	"fmt"
	"time"
)

// InitFn - function to have
type InitFn func() []*Resource

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

// NewResourceFn - used when a resource is expired
type NewResourceFn func(key, value string, optionals ...any) *Resource

// Resource - represents a single resource managed by the Pool.
// It includes a unique key, an associated value, and an expiration time.
// The noCopy field is used to prevent accidental copying of Resource instances.
type Resource struct {
	noCopy noCopy // Too lazy to figure out how to properly implement this rn. Might figure out later.

	expiresAt time.Time // This expiry is for lock
	pool      *Pool
	l         Locker

	Key   string
	Value string
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

// PoolOpts - modifies the pool with functions.
type PoolOpts func(*Pool)

// WithTTL - allows user to choose TTL for how long the user wants to use the resource.
func WithTTL(ttl time.Duration) PoolOpts {
	return func(p *Pool) {
		p.ttl = ttl
	}
}

// WithLockTTL - sets TTL for the resources created
func WithLockTTL(ttl time.Duration) PoolOpts {
	return func(p *Pool) {
		p.lockTtl = ttl
	}
}

func (p *Pool) GetResource() (*Resource, error) {
	/*
		Steps of getting resource:
		1. Pop a key from pool array
		2. Create a lock for the key
		3. Create an entry in the allocated hash
		4. Access the resource

	*/

	ctx := context.Background()

	resKey, err := p.rc.LPop(ctx, p.getPoolKey())
	if err != nil {
		return nil, err
	}

	l, err := p.rc.Lock(ctx, resKey, p.lockTtl)
	if err != nil {
		return nil, err
	}

	if err := p.allocate(resKey); err != nil {
		l.Release(ctx)
		return nil, err
	}

	v, err := p.rc.Get(ctx, resKey)
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

func (p *Pool) allocate(key string) error {
	ctx := context.Background()

	if err := p.rc.HSet(ctx, p.getAllocatedResourcesKey(), key, "taken"); err != nil {
		return err
	}

	return nil
}

func (p *Pool) deallocate(key string) error {
	ctx := context.Background()
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
	defer lock.Release(context.Background())

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
	defer l.Release(context.Background())
	if err != nil {
		return err
	}

	//resKey := fmt.Sprintf("%s:%s", ResourceNamePreFix, res.Key)
	if err := p.rc.Set(context.TODO(), res.Key, res.Value, p.ttl); err != nil {
		return err
	}

	return nil
}
