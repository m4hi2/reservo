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
	name        string
	redisClient RedisClient
	initFn      InitFn
	ttl         time.Duration
	lockTtl     time.Duration
}

// NewResource - used when a resource is expired
type NewResource func(key, value string, optionals ...any) *Resource

// Resource - represents a single resource managed by the Pool.
// It includes a unique key, an associated value, and an expiration time.
// The noCopy field is used to prevent accidental copying of Resource instances.
type Resource struct {
	noCopy noCopy // Too lazy to figure out how to properly implement this rn. Might figure out later.

	expiresAt time.Time // This expiry is for lock
	new       NewResource
	pool      *Pool

	Key   string
	Value string
}

// NewPool - creates and initializes a new Pool with the given name, Redis client, and resource initialization function.
// Optional PoolOpts can be provided to customize the Pool configuration.
// Returns the created Pool and an error if initialization fails.
// Default TTL is set to 1 second.
func NewPool(name string, rc RedisClient, initFn InitFn, opts ...PoolOpts) (*Pool, error) {
	p := &Pool{
		name:        name,
		redisClient: rc,
		initFn:      initFn,
		ttl:         time.Minute,
		lockTtl:     time.Second,
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

func (p *Pool) createRedisPool() error {
	exists, err := p.checkPoolExists()
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	initRes := p.initFn()

	poolKeys := make([]string, 0, len(initRes))

	for _, res := range initRes {
		poolKeys = append(poolKeys, res.Key)
		if err := p.createResInRedis(res); err != nil {
			return err
		}
	}

	if err := p.redisClient.RPush(context.TODO(), p.getPoolName(), poolKeys); err != nil {
		return err
	}

	return nil
}

func (p *Pool) checkPoolExists() (bool, error) {
	return p.redisClient.Exists(context.TODO(), p.getPoolName(), p.getAllocatedResourcesKey())
}

func (p *Pool) getPoolName() string {
	return fmt.Sprintf("%s:%s", PoolNamePreFix, p.name)
}

func (p *Pool) getAllocatedResourcesKey() string {
	return fmt.Sprintf("%s:%s", AllocatedPreFix, p.name)
}

func (p *Pool) createResInRedis(res *Resource) error {
	resKey := fmt.Sprintf("%s:%s", ResourceNamePreFix, res.Key)
	if err := p.redisClient.Set(context.TODO(), resKey, res.Value, p.ttl); err != nil {
		return err
	}

	return nil
}
