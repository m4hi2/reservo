package reservo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (p *Pool) getResValue(ctx context.Context, key string) (string, Locker, error) {
	var v string

	p.logger.Debugf("Getting resource value for key: %s", key)
	v, err := p.rc.Get(ctx, key)
	if errors.Is(err, redis.Nil) {
		p.logger.Infof(formatLogMsg("Resource not found in redis, recreating: %s"), key)
		res, err := p.recreateResource(key)
		if err != nil {
			p.logger.Errorf(formatLogMsg("Failed to recreate resource %s: %v"), key, err)
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
	res, err := p.resRecreateFn(resKey)
	if err != nil {
		return nil, err
	}

	if err := p.createResInRedis(res); err != nil {
		return nil, err
	}

	return res, nil

}

func (p *Pool) allocate(ctx context.Context, key string) error {
	p.logger.Debugf(formatLogMsg("Allocating resource: %s"), key)
	if err := p.rc.HSet(ctx, p.getAllocatedResourcesKey(), key, "taken"); err != nil {
		p.logger.Errorf(formatLogMsg("Failed to allocate resource %s: %v"), key, err)
		return err
	}
	p.logger.Debugf(formatLogMsg("Successfully allocated resource: %s"), key)
	return nil
}

func (p *Pool) deallocate(ctx context.Context, key string) error {
	if err := p.rc.HDel(ctx, p.getAllocatedResourcesKey(), key); err != nil {
		return err
	}

	return nil
}

func (p *Pool) createRedisPool(retry int) error {
	p.logger.Debugf(formatLogMsg("Creating redis pool (attempt %d/%d)"), retry+1, p.retryCount+1)
	exists, err := p.checkPoolExists()
	if err != nil {
		p.logger.Errorf(formatLogMsg("Failed to check if pool exists: %v"), err)
		return err
	}

	if exists {
		p.logger.Debugf(formatLogMsg("Pool already exists, skipping creation"))
		return nil
	}

	lock, err := p.getManagementLock()
	defer func() {
		if lock != nil {
			lock.Release(context.Background())
		}
	}()

	if err != nil {
		if errors.Is(err, ErrLockNotObtained) {
			p.logger.Debugf(formatLogMsg("Could not obtain management lock, retrying..."))
			if p.retryCount >= retry {
				time.Sleep(p.lockTtl)
				return p.createRedisPool(retry + 1)
			}
		}
		p.logger.Errorf(formatLogMsg("Failed to get management lock: %v"), err)
		return err
	}

	p.logger.Infof(formatLogMsg("Initializing new pool with %d resources"), len(p.initFn()))
	initRes := p.initFn()

	poolKeys := make([]string, 0, len(initRes))
	for _, res := range initRes {
		poolKeys = append(poolKeys, res.Key)
		p.logger.Debugf(formatLogMsg("Creating resource in redis: %s"), res.Key)
		if err := p.createResInRedis(res); err != nil {
			p.logger.Errorf(formatLogMsg("Failed to create resource %s: %v"), res.Key, err)
			return err
		}
	}

	if err := p.rc.RPush(context.TODO(), p.getPoolKey(), poolKeys); err != nil {
		p.logger.Errorf(formatLogMsg("Failed to push pool keys to redis: %v"), err)
		return err
	}

	p.logger.Infof(formatLogMsg("Successfully created pool with %d resources"), len(poolKeys))
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

	// Looks like the management delay has to be a bit larger than regular lock ttl.
	managementTTL := p.mtncDelay - time.Millisecond*100

	l, err := p.rc.Lock(context.Background(), managementKey, managementTTL)
	if err != nil {
		return nil, err
	}

	return l, nil

}

func (p *Pool) getAllocatedResourcesKey() string {
	return fmt.Sprintf("%s:%s", AllocatedPreFix, p.name)
}

func (p *Pool) createResInRedis(res *Resource) error {
	p.logger.Debugf(formatLogMsg("Creating resource in redis: %s"), res.Key)
	l, err := p.rc.Lock(context.Background(), res.Key, p.lockTtl)
	defer func() {
		if l != nil {
			l.Release(context.Background())
		}
	}()
	if err != nil {
		p.logger.Errorf(formatLogMsg("Failed to lock resource %s: %v"), res.Key, err)
		return err
	}

	if err := p.rc.Set(context.TODO(), res.Key, res.Value, p.ttl); err != nil {
		p.logger.Errorf(formatLogMsg("Failed to set resource %s in redis: %v"), res.Key, err)
		return err
	}

	p.logger.Debugf(formatLogMsg("Successfully created resource: %s"), res.Key)
	return nil
}

func (p *Pool) returnToPool(ctx context.Context, key string) error {
	p.logger.Debugf(formatLogMsg("Returning resource to pool: %s"), key)
	if err := p.deallocate(ctx, key); err != nil {
		p.logger.Errorf(formatLogMsg("Failed to deallocate resource %s: %v"), key, err)
		return err
	}

	if err := p.rc.RPush(ctx, p.getPoolKey(), key); err != nil {
		p.logger.Errorf(formatLogMsg("Failed to return resource %s to pool: %v"), key, err)
		return err
	}

	p.logger.Debugf(formatLogMsg("Successfully returned resource to pool: %s"), key)
	return nil
}

func (p *Pool) maintenance() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// restart after short delay
				time.Sleep(2 * time.Second)
				p.maintenance()
			}
		}()

		ticker := time.NewTicker(p.mtncDelay)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_ = p.maintenanceJob()
			}
		}
	}()
}

func (p *Pool) maintenanceJob() error {
	p.logger.Debugf(formatLogMsg("Starting maintenance job for pool: %s"), p.name)
	ctx := context.Background()
	l, err := p.getManagementLock()
	defer func() {
		if l != nil {
			l.Release(ctx)
		}
	}()

	if err != nil && errors.Is(err, ErrLockNotObtained) {
		p.logger.Warnf(formatLogMsg("Management lock is held by another container: %v"), err)
		return nil
	}

	if err != nil {
		p.logger.Errorf(formatLogMsg("Failed to get management lock: %v"), err)
		return err
	}

	// This means other pod have a management lock.
	// Can assume they have run the maintenance job.
	if l == nil {
		p.logger.Debugf(formatLogMsg("Management lock already held by another instance"))
		return nil
	}

	keys, err := p.rc.HKeys(ctx, p.getAllocatedResourcesKey())
	if err != nil {
		p.logger.Errorf(formatLogMsg("Failed to get allocated resources: %v"), err)
		return err
	}

	p.logger.Infof(formatLogMsg("Checking %d allocated resources"), len(keys))
	for _, key := range keys {
		locked, err := p.isLocked(ctx, key)
		if err != nil {
			p.logger.Warnf(formatLogMsg("Failed to check lock status for %s: %v"), key, err)
			continue
		}

		if !locked {
			p.logger.Infof(formatLogMsg("Returning abandoned resource to pool: %s"), key)
			if err := p.returnToPool(ctx, key); err != nil {
				p.logger.Warnf(formatLogMsg("Failed to return resource %s to pool: %v"), key, err)
				continue
			}
		}
	}

	p.logger.Debugf(formatLogMsg("Completed maintenance job for pool: %s"), p.name)
	return nil
}

func (p *Pool) isLocked(ctx context.Context, key string) (bool, error) {
	lockKey := fmt.Sprintf("%s:lock", key)
	return p.rc.Exists(ctx, lockKey)
}
