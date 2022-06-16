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

func set(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	value := params[1]
	expiration := time.Duration(0)
	if len(params) > 2 {
		expiration = params[2].(time.Duration)
	}
	var doErr error
	doErr = client.Writer().Set(ctx, key, value, expiration).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle set command failed").WithCause(doErr)
		return
	}
	return
}

func setNX(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	value := params[1]
	expiration := time.Duration(0)
	if len(params) > 2 {
		expiration = params[2].(time.Duration)
	}
	var doErr error
	doErr = client.Writer().SetNX(ctx, key, value, expiration).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle setnx command failed").WithCause(doErr)
		return
	}
	return
}

func get(ctx context.Context, client Client, params []interface{}) (v string, err errors.CodeError) {
	key := params[0].(string)
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

func getSet(ctx context.Context, client Client, params []interface{}) (v string, err errors.CodeError) {
	key := params[0].(string)
	value := params[1]
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

func mget(ctx context.Context, client Client, params []interface{}) (v []interface{}, err errors.CodeError) {
	key := make([]string, 0, 1)
	for _, param := range params {
		key = append(key, param.(string))
	}
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

func mset(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	var doErr error
	_, doErr = client.Writer().MSet(ctx, params...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle mset command failed").WithCause(doErr)
		return
	}
	return
}

func setEX(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	value := params[1]
	expiration := time.Duration(0)
	if len(params) > 2 {
		expiration = params[2].(time.Duration)
	}
	var doErr error
	_, doErr = client.Writer().SetEX(ctx, key, value, expiration).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle setex command failed").WithCause(doErr)
		return
	}
	return
}

func incr(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Writer().Incr(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle incr command failed").WithCause(doErr)
		return
	}
	return
}

func incrBy(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	value := params[1].(int64)
	var doErr error
	v, doErr = client.Writer().IncrBy(ctx, key, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle incrby command failed").WithCause(doErr)
		return
	}
	return
}

func decr(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Writer().Decr(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle decr command failed").WithCause(doErr)
		return
	}
	return
}

func decrBy(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	value := params[1].(int64)
	var doErr error
	v, doErr = client.Writer().DecrBy(ctx, key, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle decrby command failed").WithCause(doErr)
		return
	}
	return
}

func append0(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	value := params[1].(string)
	var doErr error
	v, doErr = client.Writer().Append(ctx, key, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle append command failed").WithCause(doErr)
		return
	}
	return
}
