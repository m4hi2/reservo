package reservo

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

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

func (p *Pool) createRedisPool(retry int) error {
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
		if errors.Is(err, ErrLockNotObtained) {
			if p.retryCount >= retry {
				time.Sleep(p.lockTtl)
				return p.createRedisPool(retry + 1)
			}

		}
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
	if err := p.deallocate(ctx, key); err != nil {
		return err
	}

	if err := p.rc.RPush(ctx, p.getPoolKey(), key); err != nil {
		return err
	}

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
	ctx := context.Background()
	l, err := p.getManagementLock()
	defer func() {
		if l != nil {
			l.Release(ctx)
		}
	}()
	if err != nil {
		return err
	}

	// This means other pod have a management lock.
	// Can assume they have run the maintenance job.
	if l == nil {
		return nil
	}

	keys, err := p.rc.HKeys(ctx, p.getAllocatedResourcesKey())
	if err != nil {
		return err
	}

	for _, key := range keys {
		locked, err := p.isLocked(ctx, key)
		if err != nil {
			continue
		}

		if !locked {
			if err := p.returnToPool(ctx, key); err != nil {
				continue
			}
		}
	}

	return nil
}

func (p *Pool) isLocked(ctx context.Context, key string) (bool, error) {
	lockKey := fmt.Sprintf("%s:lock", key)
	return p.rc.Exists(ctx, lockKey)
}
