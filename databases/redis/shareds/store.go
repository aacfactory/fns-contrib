package shareds

import (
	"context"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns-contrib/databases/redis"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/shared"
	"time"
)

func Store() shared.Store {
	return &store{}
}

type store struct{}

func (store *store) Get(ctx context.Context, key []byte) (value []byte, has bool, err errors.CodeError) {
	v, getErr := redis.Get(ctx, bytex.ToString(key))
	if getErr != nil {
		err = errors.Warning("redis: shared store get failed").WithCause(getErr)
		return
	}
	has = v.Has
	if has {
		value = bytex.FromString(v.Value)
	}
	return
}

func (store *store) Set(ctx context.Context, key []byte, value []byte) (err errors.CodeError) {
	err = redis.Set(ctx, redis.SetParam{
		Key:        bytex.ToString(key),
		Value:      bytex.ToString(value),
		Expiration: 0,
	})
	if err != nil {
		err = errors.Warning("redis: shared store set failed").WithCause(err)
		return
	}
	return
}

func (store *store) SetWithTTL(ctx context.Context, key []byte, value []byte, ttl time.Duration) (err errors.CodeError) {
	err = redis.Set(ctx, redis.SetParam{
		Key:        bytex.ToString(key),
		Value:      bytex.ToString(value),
		Expiration: ttl,
	})
	if err != nil {
		err = errors.Warning("redis: shared store set with ttl failed").WithCause(err)
		return
	}
	return
}

func (store *store) Incr(ctx context.Context, key []byte, delta int64) (v int64, err errors.CodeError) {
	if delta > 0 {
		v, err = redis.IncrBy(ctx, redis.IncrByParam{
			Key:   bytex.ToString(key),
			Value: delta,
		})
		if err != nil {
			err = errors.Warning("redis: shared store incr failed").WithCause(err)
			return
		}
	} else if delta < 0 {
		v, err = redis.DecrBy(ctx, redis.DecrByParam{
			Key:   bytex.ToString(key),
			Value: delta,
		})
		if err != nil {
			err = errors.Warning("redis: shared store incr failed").WithCause(err)
			return
		}
	} else {
		err = errors.Warning("redis: shared store incr failed").WithCause(errors.Warning("delta is zero"))
		return
	}
	return
}

func (store *store) ExpireKey(ctx context.Context, key []byte, ttl time.Duration) (err errors.CodeError) {
	err = redis.Expire(ctx, redis.ExpireParam{
		Key:        bytex.ToString(key),
		Expiration: ttl,
	})
	if err != nil {
		err = errors.Warning("redis: shared store expire key failed").WithCause(err)
		return
	}
	return
}

func (store *store) Remove(ctx context.Context, key []byte) (err errors.CodeError) {
	err = redis.Del(ctx, []string{bytex.ToString(key)})
	if err != nil {
		err = errors.Warning("redis: shared store remove failed").WithCause(err)
		return
	}
	return
}

func (store *store) Close() {}
