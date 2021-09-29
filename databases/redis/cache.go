package redis

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns"
	"github.com/aacfactory/json"
	"time"
)

func CacheGetWithSingleFight(ctx fns.Context, key string, timeout time.Duration, fn func() (result json.RawMessage, err errors.CodeError)) (result json.RawMessage, err errors.CodeError) {
	if fn == nil {
		err = errors.ServiceError("fns redis: cache get with single fight failed").WithMeta("fn", "fn is nil")
		return
	}
	getResult, getErr := CacheGet(ctx, key)
	if getErr != nil {
		err = errors.ServiceError("fns redis: cache get with single fight failed").WithCause(getErr)
		return
	}
	if getResult.Has {
		result = getResult.Value
		return
	}
	sfk := fmt.Sprintf("cache_sf_%s", key)
	gsr, gsrErr := GetSet(ctx, SetParam{
		Key:        sfk,
		Value:      []byte("[]"),
		Expiration: 2 * time.Second,
	})
	if gsrErr != nil {
		err = errors.ServiceError("fns redis: cache get with single fight failed").WithCause(gsrErr)
		return
	}
	if gsr.Has {
		if string(gsr.Value) == "{}" {
			getResult, getErr = CacheGet(ctx, key)
			if getErr != nil {
				err = errors.ServiceError("fns redis: cache get with single fight failed").WithCause(getErr)
				return
			}
			if getResult.Has {
				result = getResult.Value
				return
			}
		} else {
			time.Sleep(500 * time.Microsecond)
			result, err = CacheGetWithSingleFight(ctx, key, timeout, fn)
		}
		return
	} else {
		n, createErr := fn()
		if createErr != nil {
			err = createErr
			_ = Remove(ctx, sfk)
			return
		}
		if n == nil {
			return
		}
		setErr := CacheSet(ctx, key, timeout, n)
		if setErr != nil {
			_ = Remove(ctx, sfk)
			err = errors.ServiceError("fns redis: cache get with single fight failed").WithCause(setErr)
			return
		}
		_ = Set(ctx, SetParam{
			Key:        sfk,
			Value:      []byte("{}"),
			Expiration: 2 * time.Second,
		})
		result = n
	}
	return
}

func CacheGet(ctx fns.Context, key string) (result *GetResult, err errors.CodeError) {
	result, err = Get(ctx, key)
	if err != nil {
		err = errors.ServiceError("fns redis: cache get failed").WithCause(err)
		return
	}
	return
}

func CacheSet(ctx fns.Context, key string, timeout time.Duration, value json.RawMessage) (err errors.CodeError) {
	setErr := Set(ctx, SetParam{
		Key:        key,
		Value:      value,
		Expiration: timeout,
	})
	if setErr != nil {
		err = errors.ServiceError("fns redis: cache set failed").WithCause(setErr)
		return
	}
	return
}

func CacheRem(ctx fns.Context, key string) (err errors.CodeError) {
	rmErr := Remove(ctx, key)
	if rmErr != nil {
		err = errors.ServiceError("fns redis: cache remove failed").WithCause(rmErr)
		return
	}
	return
}
