package reservo

import "time"

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
