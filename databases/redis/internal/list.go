package internal

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	BLPOP      = "BLPOP"
	BRPOP      = "BRPOP"
	BRPOPLPUSH = "BRPOPLPUSH"
	LINDEX     = "LINDEX"
	LINSERT    = "LINSERT"
	LLEN       = "LLEN"
	LPOP       = "LPOP"
	LPUSH      = "LPUSH"
	LPUSHX     = "LPUSHX"
	LRANGE     = "LRANGE"
	LREM       = "LREM"
	LSET       = "LSET"
	LTRIM      = "LTRIM"
	RPOP       = "RPOP"
	RPOPLPUSH  = "RPOPLPUSH"
	RPUSH      = "RPUSH"
	RPUSHX     = "RPUSHX"
)

func blpop(ctx context.Context, client Client, timeout time.Duration, keys ...string) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().BLPop(ctx, timeout, keys...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle blpop command failed").WithCause(doErr)
		return
	}
	return
}

func brpop(ctx context.Context, client Client, timeout time.Duration, keys ...string) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().BRPop(ctx, timeout, keys...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle blpop command failed").WithCause(doErr)
		return
	}
	return
}

func brpoplpush(ctx context.Context, client Client, src string, dst string, timeout time.Duration) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().BRPopLPush(ctx, src, dst, timeout).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle brpoplpush command failed").WithCause(doErr)
		return
	}
	return
}

func lindex(ctx context.Context, client Client, key string, idx int64) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().LIndex(ctx, key, idx).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle lindex command failed").WithCause(doErr)
		return
	}
	return
}

func linsert(ctx context.Context, client Client, key, op string, pivot, value interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LInsert(ctx, key, op, pivot, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle linsert command failed").WithCause(doErr)
		return
	}
	return
}

func llen(ctx context.Context, client Client, key string) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().LLen(ctx, key).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle llen command failed").WithCause(doErr)
		return
	}
	return
}

func lpop(ctx context.Context, client Client, key string) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LPop(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle lpop command failed").WithCause(doErr)
		return
	}
	return
}

func lpush(ctx context.Context, client Client, key string, values ...interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LPush(ctx, key, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle lpush command failed").WithCause(doErr)
		return
	}
	return
}

func lpushx(ctx context.Context, client Client, key string, values ...interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LPushX(ctx, key, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle lpushx command failed").WithCause(doErr)
		return
	}
	return
}

func lrange(ctx context.Context, client Client, key string, start, stop int64) (v []string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Reader().LRange(ctx, key, start, stop).Result()
	if doErr != nil {
		if doErr == redis.Nil {
			err = errors.NotFound("redis: nil")
			return
		}
		err = errors.ServiceError("redis: handle lrange command failed").WithCause(doErr)
		return
	}
	return
}

func lrem(ctx context.Context, client Client, key string, count int64, value interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LRem(ctx, key, count, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle lrem command failed").WithCause(doErr)
		return
	}
	return
}

func lset(ctx context.Context, client Client, key string, index int64, value interface{}) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LSet(ctx, key, index, value).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle lset command failed").WithCause(doErr)
		return
	}
	return
}

func ltrim(ctx context.Context, client Client, key string, start, stop int64) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().LTrim(ctx, key, start, stop).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle ltrim command failed").WithCause(doErr)
		return
	}
	return
}

func rpop(ctx context.Context, client Client, key string) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().RPop(ctx, key).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle rpop command failed").WithCause(doErr)
		return
	}
	return
}

func rpoplpush(ctx context.Context, client Client, source, destination string) (v string, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().RPopLPush(ctx, source, destination).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle rpoplpush command failed").WithCause(doErr)
		return
	}
	return
}

func rpush(ctx context.Context, client Client, key string, values ...interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().RPush(ctx, key, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle rpush command failed").WithCause(doErr)
		return
	}
	return
}

func rpushx(ctx context.Context, client Client, key string, values ...interface{}) (v int64, err errors.CodeError) {
	var doErr error
	v, doErr = client.Writer().RPushX(ctx, key, values...).Result()
	if doErr != nil {
		err = errors.ServiceError("redis: handle rpushx command failed").WithCause(doErr)
		return
	}
	return
}
