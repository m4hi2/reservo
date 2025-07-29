package reservo

import (
	"context"
	"time"
)

// Resource - represents a single resource managed by the Pool.
// It includes a unique key, an associated value, and an expiration time.
// The noCopy field is used to prevent accidental copying of Resource instances.
type Resource struct {
	expiresAt time.Time // This expiry is for lock
	pool      *Pool
	l         Locker

	Key   string
	Value string
}

// Release - returns the resource to the pool
func (r *Resource) Release(ctx context.Context) error {

	if err := r.l.Release(ctx); err != nil {
		return err
	}

	if err := r.pool.returnToPool(ctx, r.Key); err != nil {
		return err
	}

	return nil
}

// Refresh - extends lock on a resource
func (r *Resource) Refresh(ctx context.Context, t time.Duration) error {
	if err := r.l.ExtendLock(ctx, t); err != nil {
		return err
	}

	r.expiresAt.Add(t)
	return nil
}

// IsExpired - checks if the resource lock is still held
func (r *Resource) IsExpired(ctx context.Context) (bool, error) {
	t, err := r.l.TTL(ctx)
	if err != nil {
		return true, err
	}

	if t == 0 || time.Now().After(r.expiresAt) {
		return true, nil
	}

	return false, nil
}

// TTL - returns the ttl of the resource lock
func (r *Resource) TTL(ctx context.Context) (time.Duration, error) {
	return r.l.TTL(ctx)
}
