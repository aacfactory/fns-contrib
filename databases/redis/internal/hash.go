package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
)

const (
	HDEL     = "HDEL"
	HEXISTS  = "HEXISTS"
	HGET     = "HGET"
	HGETALL  = "HGETALL"
	HINCERBY = "HINCRBY"
	HKEYS    = "HKEYS"
	HLEN     = "HLEN"
	HMGET    = "HMGET"
	HMSET    = "HMSET"
	HSET     = "HSET"
	HSETNX   = "HSETNX"
	HVALS    = "HVALS"
	HSCAN    = "HSCAN"
)

func hdel(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	fields := make([]string, 0, 1)
	for _, param := range params[1:] {
		fields = append(fields, param.(string))
	}
	var doErr error
	doErr = client.Writer().HDel(ctx, key, fields...).Err()
	if doErr != nil {
		err = errors.ServiceError("redis: handle hdel command failed").WithCause(doErr)
		return
	}
	return
}

func hexist(ctx context.Context, client Client, params []interface{}) (has bool, err errors.CodeError) {
	key := params[0].(string)
	field := params[1].(string)
	var doErr error
	has, doErr = client.Reader().HExists(ctx, key, field).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle hexist command failed").WithCause(doErr)
		return
	}
	return
}

func hget(ctx context.Context, client Client, params []interface{}) (v string, err errors.CodeError) {
	key := params[0].(string)
	field := params[1].(string)
	var doErr error
	v, doErr = client.Reader().HGet(ctx, key, field).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hget command failed").WithCause(doErr)
		return
	}
	return
}

func hgetall(ctx context.Context, client Client, params []interface{}) (v map[string]string, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Reader().HGetAll(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hgetall command failed").WithCause(doErr)
		return
	}
	return
}

func hincrby(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	field := params[1].(string)
	value := params[2].(int64)
	var doErr error
	v, doErr = client.Writer().HIncrBy(ctx, key, field, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle hincrby command failed").WithCause(doErr)
		return
	}
	return
}

func hkeys(ctx context.Context, client Client, params []interface{}) (v []string, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Reader().HKeys(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hkeys command failed").WithCause(doErr)
		return
	}
	return
}

func hlen(ctx context.Context, client Client, params []interface{}) (v int64, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Reader().HLen(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hlen command failed").WithCause(doErr)
		return
	}
	return
}

func hmget(ctx context.Context, client Client, params []interface{}) (v []interface{}, err errors.CodeError) {
	key := params[0].(string)
	fields := make([]string, 0, 1)
	for _, param := range params[1:] {
		fields = append(fields, param.(string))
	}
	var doErr error
	v, doErr = client.Reader().HMGet(ctx, key, fields...).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hmget command failed").WithCause(doErr)
		return
	}
	return
}

func hmset(ctx context.Context, client Client, params []interface{}) (v bool, err errors.CodeError) {
	key := params[0].(string)
	values := make([]interface{}, 0, 1)
	for _, param := range params[1:] {
		values = append(values, param)
	}
	var doErr error
	v, doErr = client.Writer().HMSet(ctx, key, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle hmset command failed").WithCause(doErr)
		return
	}
	return
}

func hset(ctx context.Context, client Client, params []interface{}) (err errors.CodeError) {
	key := params[0].(string)
	values := make([]interface{}, 0, 1)
	for _, param := range params[1:] {
		values = append(values, param)
	}
	var doErr error
	_, doErr = client.Writer().HSet(ctx, key, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle hset command failed").WithCause(doErr)
		return
	}
	return
}

func hsetnx(ctx context.Context, client Client, params []interface{}) (ok bool, err errors.CodeError) {
	key := params[0].(string)
	field := params[1].(string)
	value := params[2]
	var doErr error
	ok, doErr = client.Writer().HSetNX(ctx, key, field, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle hsetnx command failed").WithCause(doErr)
		return
	}
	return
}

func hvals(ctx context.Context, client Client, params []interface{}) (v []string, err errors.CodeError) {
	key := params[0].(string)
	var doErr error
	v, doErr = client.Reader().HVals(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hvals command failed").WithCause(doErr)
		return
	}
	return
}

func hscan(ctx context.Context, client Client, params []interface{}) (keys []string, next uint64, err errors.CodeError) {
	key := params[0].(string)
	cursor := params[1].(uint64)
	match := params[2].(string)
	count := params[3].(int64)
	var doErr error
	keys, next, doErr = client.Reader().HScan(ctx, key, cursor, match, count).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle hscan command failed").WithCause(doErr)
		return
	}
	return
}
