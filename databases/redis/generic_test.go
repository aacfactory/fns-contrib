package redis_test

import (
	"fmt"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/tests"
	"testing"
	"time"
)

func setup() (err error) {
	config := tests.Config()
	config.AddService("redis", configs.Config{
		InitAddress: []string{"127.0.0.1:16379"},
	})
	err = tests.Setup(redis.New(), tests.WithConfig(config))
	return
}

func TestExpire(t *testing.T) {
	setupErr := setup()
	if setupErr != nil {
		t.Error(fmt.Sprintf("%+v", setupErr))
		return
	}
	defer tests.Teardown()
	ctx := tests.TODO()
	_, setErr := redis.Do(ctx, redis.Set(fmt.Sprintf("some_exp"), time.Now().Format(time.RFC3339)).Ex(10*time.Second))
	if setErr != nil {
		t.Error(fmt.Sprintf("%+v", setErr))
		return
	}

	_, expErr := redis.Do(ctx, redis.Expire("some_exp").Seconds(10))
	if expErr != nil {
		t.Errorf("%+v", expErr)
		return
	}
	time.Sleep(5 * time.Second)
	_, _ = redis.Do(ctx, redis.Expire("some_exp").Seconds(10))
	ttl, ttlErr := redis.Do(ctx, redis.TTL("some_exp"))
	if ttlErr != nil {
		t.Errorf("%+v", ttlErr)
		return
	}
	n, nErr := ttl.AsInt()
	if nErr != nil {
		t.Errorf("%+v", nErr)
		return
	}
	fmt.Println(n)

}
