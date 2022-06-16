package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	KEYS    = "KEYS"
	DEL     = "DEL"
	EXISTS  = "EXISTS"
	EXPIRE  = "EXPIRE"
	PERSIST = "PERSIST"
	SCAN    = "SCAN"
)

func keys(ctx context.Context, client Client, pattern string) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().Keys(ctx, pattern).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: no keys fetched")
			return
		}
		err = errors.ServiceError("redis: handle keys command failed").WithCause(doErr)
		return
	}
	return
}

func del(ctx context.Context, client Client, key ...string) (err errors.CodeError) {
	doErr := client.Writer().Del(ctx, key...).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle del command failed").WithCause(doErr)
		return
	}
	return
}

func exists(ctx context.Context, client Client, key string) (has bool, err errors.CodeError) {
	v, doErr := client.Reader().Exists(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle exists command failed").WithCause(doErr)
		return
	}
	has = v > 0
	return
}

func expire(ctx context.Context, client Client, key string, expiration time.Duration) (err errors.CodeError) {
	_, doErr := client.Writer().Expire(ctx, key, expiration).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle expire command failed").WithCause(doErr)
		return
	}
	return
}

func persist(ctx context.Context, client Client, key string) (err errors.CodeError) {
	_, doErr := client.Writer().Persist(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle persist command failed").WithCause(doErr)
		return
	}
	return
}

func scan(ctx context.Context, client Client, cursor uint64, match string, count int64) (keys []string, next uint64, err errors.CodeError) {
	var doErr error
	keys, next, doErr = client.Reader().Scan(ctx, cursor, match, count).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle scan command failed").WithCause(doErr)
		return
	}
	return
}
