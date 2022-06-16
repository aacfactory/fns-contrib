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

func keys(ctx context.Context, client Client, params []interface{}) (v []string, err errors.CodeError) {
	pattern := params[0].(string)
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

func del(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := make([]string, 0, 1)
	for _, param := range params {
		key = append(key, param.(string))
	}
	doErr := client.Writer().Del(ctx, key...).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle del command failed").WithCause(doErr)
		return
	}
	return
}

func exists(ctx context.Context, client Client, params []interface{}) (has bool, err errors.CodeError) {
	key := params[0].(string)
	v, doErr := client.Reader().Exists(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle exists command failed").WithCause(doErr)
		return
	}
	has = v > 0
	return
}

func expire(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	expiration := params[1].(time.Duration)
	_, doErr := client.Writer().Expire(ctx, key, expiration).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle expire command failed").WithCause(doErr)
		return
	}
	return
}

func persist(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	_, doErr := client.Writer().Persist(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle persist command failed").WithCause(doErr)
		return
	}
	return
}

func scan(ctx context.Context, client Client, params []interface{}) (keys []string, next uint64, err errors.CodeError) {
	cursor := params[0].(uint64)
	match := params[1].(string)
	count := params[2].(int64)
	var doErr error
	keys, next, doErr = client.Reader().Scan(ctx, cursor, match, count).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle scan command failed").WithCause(doErr)
		return
	}
	return
}
