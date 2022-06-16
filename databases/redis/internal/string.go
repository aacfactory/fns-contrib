package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	SET    = "SET"
	SETNX  = "SETNX"
	GET    = "GET"
	GETSET = "GETSET"
	MGET   = "MGET"
	MSET   = "MSET"
	SETEX  = "SETEX"
	INCR   = "INCR"
	INCRBY = "INCRBY"
	DECR   = "DECR"
	DECRBY = "DECRBY"
	APPEND = "APPEND"
)

func set(ctx context.Context, client Client, key string, value interface{}, expiration time.Duration) (err errors.CodeError) {
	var doErr error
	doErr = client.Writer().Set(ctx, key, value, expiration).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle set command failed").WithCause(doErr)
		return
	}
	return
}

func setNX(ctx context.Context, client Client, key string, value interface{}, expiration time.Duration) (err errors.CodeError) {
	var doErr error
	doErr = client.Writer().SetNX(ctx, key, value, expiration).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle setnx command failed").WithCause(doErr)
		return
	}
	return
}

func get(ctx context.Context, client Client, key string) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().Get(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle get command failed").WithCause(doErr)
		return
	}
	return
}

func getSet(ctx context.Context, client Client, key string, value interface{}) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().GetSet(ctx, key, value).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle getset command failed").WithCause(doErr)
		return
	}
	return
}

func mget(ctx context.Context, client Client, key ...string) (v []interface{}, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().MGet(ctx, key...).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle mget command failed").WithCause(doErr)
		return
	}
	return
}

func mset(ctx context.Context, client Client, values ...interface{}) (err errors.CodeError) {
	var doErr error
	_, doErr = client.Writer().MSet(ctx, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle mset command failed").WithCause(doErr)
		return
	}
	return
}

func setEX(ctx context.Context, client Client, key string, value interface{}, expiration time.Duration) (err errors.CodeError) {
	var doErr error
	_, doErr = client.Writer().SetEX(ctx, key, value, expiration).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle setex command failed").WithCause(doErr)
		return
	}
	return
}

func incr(ctx context.Context, client Client, key string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().Incr(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle incr command failed").WithCause(doErr)
		return
	}
	return
}

func incrBy(ctx context.Context, client Client, key string, value int64) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().IncrBy(ctx, key, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle incrby command failed").WithCause(doErr)
		return
	}
	return
}

func decr(ctx context.Context, client Client, key string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().Decr(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle decr command failed").WithCause(doErr)
		return
	}
	return
}

func decrBy(ctx context.Context, client Client, key string, value int64) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().DecrBy(ctx, key, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle decrby command failed").WithCause(doErr)
		return
	}
	return
}

func append(ctx context.Context, client Client, key string, value string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().Append(ctx, key, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle append command failed").WithCause(doErr)
		return
	}
	return
}
