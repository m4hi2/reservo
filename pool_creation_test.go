package reservo_test

import (
	"context"
	"log"

	"github.com/m4hi2/reservo"
	"github.com/m4hi2/reservo/adapters"
	"github.com/redis/go-redis/v9"

	"strconv"
	"testing"
	"time"
)

func TestPoolCreation(t *testing.T) {
	//assert := assertLib.New(t)
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	gra := adapters.NewGoRedisAdapter(c)

	initFn := func() []*reservo.Resource {
		res := make([]*reservo.Resource, 0, 10)
		for i := 0; i < 10; i++ {
			res = append(res, &reservo.Resource{
				Key:   "key" + strconv.Itoa(i),
				Value: strconv.Itoa(i),
			})

		}

		return res
	}

	newResFn := func(key string, opts ...any) (*reservo.Resource, error) {
		return &reservo.Resource{
			Key:   key,
			Value: key,
		}, nil
	}

	pool, err := reservo.NewPool(
		"gg",
		gra,
		initFn,
		newResFn,
		reservo.WithLockTTL(time.Second*5),
		reservo.WithTTL(time.Second*100),
		reservo.WithMaintenanceDelay(time.Second*10),
	)

	if err != nil {
		t.Fatal(err)
	}

	//poolSize, err := c.LLen(context.Background(), "reservo:pool:gg").Result()
	//if err != nil {
	//	t.Fatal(err)
	//}

	//assert.Equal(int64(10), poolSize)

	res, err := pool.GetResource(context.TODO())

	if err != nil {
		t.Fatal(err)
	}

	log.Println(res.Key, res.Value)

	if err := res.Release(context.TODO()); err != nil {
		t.Fatal(err)
	}

}
