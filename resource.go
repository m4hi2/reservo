package reservo

import (
	"context"
	"time"
)

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

func (r *Resource) Release(ctx context.Context) error {

	if err := r.l.Release(ctx); err != nil {
		return err
	}

	if err := r.pool.returnToPool(ctx, r.Key); err != nil {
		return err
	}

	return nil
}
