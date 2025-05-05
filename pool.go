package reservo

import (
	"context"
	"time"
)

// InitFn - used to seed the pool with resources
// Only set Key and Value
type InitFn func() []*Resource

// ResRecreateFn - used when a resource is expired
// Only set Key and Value
type ResRecreateFn func(key string, optionals ...any) (*Resource, error)

// Pool - manages a collection of resources with a specified TTL, using a RedisClient for backend operations.
// It supports custom initialization and configuration via InitFn and PoolOpts.
// Important: the pool only holds pointer to the actual data. Since the data might have a TTL on redis
// of its own.
type Pool struct {
	name          string
	rc            RedisClient
	initFn        InitFn
	resRecreateFn ResRecreateFn
	ttl           time.Duration
	lockTtl       time.Duration
	mtncDelay     time.Duration
}

// NewPool - creates and initializes a new Pool with the given name, Redis client, and resource initialization function.
// Optional PoolOpts can be provided to customize the Pool configuration.
// Returns the created Pool and an error if initialization fails.
func NewPool(name string, rc RedisClient, initFn InitFn, resRecreateFn ResRecreateFn, opts ...PoolOpts) (*Pool, error) {
	p := &Pool{
		name:          name,
		rc:            rc,
		initFn:        initFn,
		resRecreateFn: resRecreateFn,
		ttl:           time.Minute,
		lockTtl:       time.Second,
		mtncDelay:     time.Minute * 5,
	}

	for _, opt := range opts {
		opt(p)
	}

	if err := p.createRedisPool(); err != nil {
		return nil, err
	}

	p.maintenance()

	return p, nil
}

// GetResource - returns a *Resource from the pool
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
		pool:      p,
		l:         l,
		expiresAt: time.Now().Add(p.ttl),
		Key:       resKey,
		Value:     v,
	}

	return res, nil
}

// PoolOpts - modifies the pool with functions.
type PoolOpts func(*Pool)

// WithMaintenanceDelay - sets delay for running the maintenance job
func WithMaintenanceDelay(delay time.Duration) PoolOpts {
	return func(p *Pool) {
		p.mtncDelay = delay
	}
}

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
