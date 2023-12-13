package redis_test

import (
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns-contrib/databases/redis/configs"
	"github.com/aacfactory/fns/tests"
)

func setup() (err error) {
	config := tests.Config()
	config.AddService("redis", configs.Config{
		InitAddress: []string{"127.0.0.1:16379"},
	})
	err = tests.Setup(redis.New(), tests.WithConfig(config))
	return
}
